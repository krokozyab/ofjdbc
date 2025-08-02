package my.jdbc.wsdl_driver

import org.apache.commons.text.StringEscapeUtils
import org.slf4j.Logger
import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.math.RoundingMode
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.sql.SQLException
import java.sql.Time
import java.sql.Timestamp
import java.util.Calendar
import java.util.*
import java.io.Closeable
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.sql.SQLFeatureNotSupportedException
import java.sql.NClob

import java.sql.RowId
import java.sql.SQLTimeoutException
import java.util.Locale
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathExpression
import javax.xml.xpath.XPathFactory

/**
 * A ResultSet implementation that fetches rows from the server in pages.
 *
 * @param originalSql The original SQL query (without pagination clauses).
 * @param wsdlEndpoint The WSDL endpoint URL.
 * @param username The user name.
 * @param password The password.
 * @param reportPath The report path.
 * @param fetchSize The number of rows per page.
 * @param logger A logger instance.
 */
class PaginatedResultSet(
    private val originalSql: String,
    private val wsdlEndpoint: String,
    private val username: String,
    private val password: String,
    private val reportPath: String,
    private var fetchSize: Int,
    private val logger: Logger,
    private val resource: Closeable? = null   // optional underlying stream/response
) : ResultSet {

    /** JDBC fetch direction (only FETCH_FORWARD is supported) */
    @Volatile
    private var fetchDirection: Int = ResultSet.FETCH_FORWARD

    companion object {
        private val XPATH_FACTORY: XPathFactory = XPathFactory.newInstance()
        /**
         * Compiled XPath that returns all elements whose local‑name() is ROW,
         * regardless of namespace or depth.
         */
        private val ROW_EXPR: XPathExpression =
            XPATH_FACTORY.newXPath().compile("//*[local-name()='ROW']")
    }

    // --- retry / timeout configuration ---------------------------------------
    private val maxRetries = 3
    private val initialDelayMillis = 1_000L
    /** preserves first‑seen order of column names (lower‑case) across pages */
    private val orderedCols: LinkedHashSet<String> = linkedSetOf()
    /** lower‑case → original column name (first appearance wins) */
    private val originalByLc: LinkedHashMap<String, String> = linkedMapOf()
    /** lowercase → 1‑based column index (populated after metadata is built) */
    private val indexByLc: MutableMap<String, Int> = mutableMapOf()

    /** cached metadata built from orderedCols; initialized after first page */
    @Volatile
    private var cachedMeta: ResultSetMetaData? = null

    // Accumulated rows (each row is a map of column names (lowercase) to values)
    private val rows: MutableList<Map<String, String>> = mutableListOf()
    /**
     * Pool of MutableMaps we can recycle between page fetches to avoid
     * allocating a brand‑new map for every row of every page.
     */
    private val rowPool: ArrayDeque<MutableMap<String, String>> = ArrayDeque()

    /** index of the current row (-1 = before first).  Volatile for cross‑thread visibility */
    @Volatile
    private var currentIndex = -1

    /** total rows already fetched (used as OFFSET for the next page) */
    @Volatile
    private var currentOffset = 0

    /** true when the most recent page was full (so another page *might* exist) */
    @Volatile
    private var lastPageFull = true

    @Volatile
    private var lastWasNull: Boolean = false

    @Volatile
    private var closed: Boolean = false
    private var warnings: SQLWarning? = null

    init {
        // Fetch the first page upon initialization.
        fetchNextPage()
    }

    /**
     * Rewrite a SELECT SQL query to include Oracle‑style OFFSET/FETCH pagination.
     * This implementation preserves the original SQL casing, but detects keywords case-insensitively.
     */
    private fun rewriteQueryForPagination(originalSql: String, offset: Int, fetchSize: Int): String {
        // Skip pagination if fetchSize is zero or negative
        if (fetchSize <= 0) return originalSql.trim()

        // Strip single‑line (--) and multi‑line (/* */) comments
        val sqlWithoutComments = originalSql
            .replace(Regex("--.*?(\\r?\\n|$)"), " ")
            .replace(Regex("/\\*.*?\\*/", RegexOption.DOT_MATCHES_ALL), " ")

        // Collapse excessive whitespace
        val normalizedSql = sqlWithoutComments.replace("\\s+".toRegex(), " ").trim()

        // Perform detection on an UPPER‑cased copy, but keep the original case for output
        val detect = normalizedSql.uppercase(Locale.ROOT)

        // Only paginate plain SELECTs that aren’t already paginated/limited
        if (!detect.startsWith("SELECT")) return originalSql
        if (detect.contains(" OFFSET ") || detect.contains(" FETCH ")) return originalSql
        if (detect.contains(" ROWNUM ")) return originalSql

        // Append Oracle‑style OFFSET/FETCH clause
        return "$normalizedSql OFFSET $offset ROWS FETCH NEXT $fetchSize ROWS ONLY"
    }

    /**
     * Fetch the next page of rows from the server and append them to [rows].
     */
    private fun fetchNextPage() {
        // Only fetch if the previous page was full (i.e. there might be more rows).
        if (!lastPageFull) return

        // Recycle row maps from the previous page.
        rows.forEach { (it as? MutableMap<String, String>)?.let { map -> rowPool.add(map) } }
        // Row-pool cap: remove excess recycled maps if needed
        while (rowPool.size > fetchSize && fetchSize > 0) rowPool.removeFirst()
        rows.clear()

        val effectiveSql = rewriteQueryForPagination(originalSql, currentOffset, fetchSize)
        logger.debug("Fetching page: SQL='{}'", effectiveSql)
        val responseXml = fetchPageXml(effectiveSql)
        val doc: Document = parseXml(responseXml)
        // Grab all <ROW> elements anywhere in the payload with a single XPath pass.
        var nodeList = ROW_EXPR.evaluate(doc, XPathConstants.NODESET) as NodeList
        // If not found, the rows might be inside an escaped <RESULT> block → one extra parse.
        if (nodeList.length == 0) {
            val resultNodes = doc.getElementsByTagName("RESULT")
            if (resultNodes.length > 0) {
                val unescaped = StringEscapeUtils.unescapeXml(resultNodes.item(0).textContent.trim())
                val innerDoc: Document = parseXml(unescaped)
                nodeList = ROW_EXPR.evaluate(innerDoc, XPathConstants.NODESET) as NodeList
            }
        }
        logger.debug("Fetched {} rows.", nodeList.length)
        // Parse rows from the NodeList.
        val newRows = mutableListOf<Map<String, String>>()
        for (i in 0 until nodeList.length) {
            val node = nodeList.item(i)
            if (node.nodeType == Node.ELEMENT_NODE) {
                val rowMap: MutableMap<String, String> =
                    if (rowPool.isEmpty()) mutableMapOf() else rowPool.removeFirst().also { it.clear() }
                val children = node.childNodes
                for (j in 0 until children.length) {
                    val child = children.item(j)
                    if (child.nodeType == Node.ELEMENT_NODE) {
                        val original = child.nodeName
                        val lc       = original.lowercase()
                        orderedCols += lc
                        originalByLc.putIfAbsent(lc, original)   // preserve first‑seen case
                        rowMap[lc] = child.textContent.trim()
                    }
                }
                newRows.add(rowMap)
            }
        }
        // Build metadata if not yet initialized.
        if (cachedMeta == null) {
            cachedMeta = DefaultResultSetMetaData(originalByLc.values.toList())
            // build fast index for findColumn()
            originalByLc.keys.forEachIndexed { idx, lc -> indexByLc[lc] = idx + 1 } // 1‑based
        }
        // Update state.
        rows.addAll(newRows)
        // Only attempt another page if the last fetch returned a full, non-empty page.
        lastPageFull = fetchSize > 0 && newRows.isNotEmpty() && newRows.size == fetchSize
        currentOffset += newRows.size
        currentIndex = -1 // Reset index for the new page
    }

    /**
     * Call Utils.sendSqlViaWsdl with a simple exponential‑backoff retry.
     * Throws SQLTimeoutException after [maxRetries] unsuccessful attempts.
     */
    private fun fetchPageXml(sql: String): String {
        var attempt = 0
        var delay = initialDelayMillis
        while (true) {
            try {
                // Utils.sendSqlViaWsdl already includes its own connect/read timeouts.
                return sendSqlViaWsdl(wsdlEndpoint, sql, username, password, reportPath)
            } catch (ex: Exception) {
                attempt++
                if (attempt >= maxRetries) {
                    throw SQLTimeoutException(
                        "Failed to fetch page after $attempt attempts: ${ex.message}",
                        ex
                    )
                }
                logger.warn(
                    "Page fetch attempt {}/{} failed ({}). Retrying in {} ms …",
                    attempt, maxRetries, ex.javaClass.simpleName + ": " + ex.message, delay
                )
                try {
                    Thread.sleep(delay)
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw SQLTimeoutException("Retry interrupted", ex)
                }
                delay = delay * 2
            }
        }
    }

    override fun next(): Boolean {
        // If the next row index is already loaded, use it.
        if (currentIndex + 1 < rows.size) {
            currentIndex++
            return true
        }
        // If the last page was full, there might be more rows.
        if (lastPageFull) {
            fetchNextPage()
            if (currentIndex + 1 < rows.size) {
                currentIndex++
                return true
            }
        }
        return false
    }

    private fun currentRow(): Map<String, String> {
        if (currentIndex !in rows.indices)
            throw SQLException("No current row available")
        return rows[currentIndex]
    }

    /** Parse string to the requested numeric type or throw SQLException. */
    private inline fun <T> parseNumeric(
        raw: String?,
        columnLabel: String,
        defaultWhenNull: T,
        convert: (String) -> T?
    ): T {
        if (raw == null) {
            lastWasNull = true
            return defaultWhenNull
        }
        lastWasNull = false
        return convert(raw.trim()) ?: throw SQLException(
            "Cannot convert value '$raw' in column '$columnLabel' to numeric"
        )
    }

    override fun getString(columnLabel: String): String? {
        val v = currentRow()[columnLabel.lowercase()]
        lastWasNull = v == null
        return v
    }

    override fun getString(columnIndex: Int): String? {
        val meta = metaData
        val colName = meta.getColumnName(columnIndex)
        return getString(colName)
    }

    override fun getInt(columnLabel: String): Int =
        parseNumeric(getString(columnLabel), columnLabel, 0) { it.toIntOrNull() }

    override fun getInt(columnIndex: Int): Int =
        getInt(metaData.getColumnName(columnIndex))

    override fun getObject(columnLabel: String): Any? {
        val value = getString(columnLabel)          // lastWasNull already set
        if (lastWasNull) return null
        return when {
            value!!.matches(Regex("^-?\\d+$"))            -> value.toInt()
            value.matches(Regex("^-?\\d+\\.\\d+$"))       -> value.toDouble()
            value.matches(Regex("^-?\\d+\\.?\\d*[Ee][+-]?\\d+$")) -> value.toDouble()
            else                                          -> value
        }
    }
    override fun getObject(columnIndex: Int): Any? = getString(columnIndex)

    override fun getMetaData(): ResultSetMetaData =
        cachedMeta ?: DefaultResultSetMetaData(emptyList())
    override fun wasNull(): Boolean = lastWasNull
    override fun close() {
        if (closed) return          // idempotent
        closed = true

        // Help GC: clear row buffer
        rows.clear()
        orderedCols.clear()
        originalByLc.clear()

        // Attempt to close the underlying resource if supplied
        try {
            resource?.close()
        } catch (ex: Exception) {
            warnings = SQLWarning("Error closing resource", ex)
        }
    }

    // --- Stub methods ---
    override fun getBoolean(columnLabel: String): Boolean {
        val raw = getString(columnLabel)          // sets lastWasNull
        if (lastWasNull) return false             // JDBC: return false on SQL NULL
        val v = raw!!.trim().lowercase()
        return when (v) {
            "true", "t", "yes", "y", "1"  -> true
            "false", "f", "no", "n", "0"  -> false
            else -> throw SQLException("Cannot convert value '$raw' in column '$columnLabel' to BOOLEAN")
        }
    }

    override fun getBoolean(columnIndex: Int): Boolean =
        getBoolean(metaData.getColumnName(columnIndex))

    override fun getByte(columnLabel: String): Byte =
        parseNumeric(getString(columnLabel), columnLabel, 0.toByte()) { it.toByteOrNull() }

    override fun getByte(columnIndex: Int): Byte =
        getByte(metaData.getColumnName(columnIndex))

    override fun getShort(columnLabel: String): Short =
        parseNumeric(getString(columnLabel), columnLabel, 0.toShort()) { it.toShortOrNull() }

    override fun getShort(columnIndex: Int): Short =
        getShort(metaData.getColumnName(columnIndex))

    override fun getLong(columnLabel: String): Long =
        parseNumeric(getString(columnLabel), columnLabel, 0L) { it.toLongOrNull() }

    override fun getLong(columnIndex: Int): Long =
        getLong(metaData.getColumnName(columnIndex))

    override fun getFloat(columnLabel: String): Float =
        parseNumeric(getString(columnLabel), columnLabel, 0f) { it.toFloatOrNull() }

    override fun getFloat(columnIndex: Int): Float =
        getFloat(metaData.getColumnName(columnIndex))

    override fun getDouble(columnLabel: String): Double =
        parseNumeric(getString(columnLabel), columnLabel, 0.0) { it.toDoubleOrNull() }

    override fun getDouble(columnIndex: Int): Double =
        getDouble(metaData.getColumnName(columnIndex))

    @Deprecated("Deprecated in Java")
    override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal {
        val bd = getBigDecimal(columnIndex)  // delegate to the non-deprecated method
        if (wasNull()) {
            return BigDecimal.ZERO
        }
        return bd.setScale(scale, RoundingMode.HALF_UP)
    }

    @Deprecated("Deprecated in Java")
    override fun getBigDecimal(columnLabel: String, scale: Int): BigDecimal {
        val bd = getBigDecimal(columnLabel)  // delegate to the non-deprecated method
        if (wasNull()) {
            return BigDecimal.ZERO
        }
        return bd.setScale(scale, RoundingMode.HALF_UP)
    }

    override fun getBytes(columnIndex: Int): ByteArray {
        val s = getString(columnIndex)
        if (wasNull()) {
            return ByteArray(0)
        }
        return try {
            s!!.toByteArray()
        } catch (ex: Exception) {
            throw SQLException("Cannot convert value '$s' to ByteArray", ex)
        }
    }

    override fun getBytes(columnLabel: String?): ByteArray {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        val s = getString(columnLabel)
        if (wasNull()) {
            return ByteArray(0)
        }
        return try {
            s!!.toByteArray()
        } catch (ex: Exception) {
            throw SQLException("Cannot convert value '$s' to ByteArray", ex)
        }
    }

    private fun parseSqlDate(str: String?): java.sql.Date? =
        str?.takeIf { it.length >= 10 }?.let { java.sql.Date.valueOf(it.substring(0, 10)) }

    private fun parseSqlTime(str: String?): java.sql.Time? =
        str?.takeIf { it.length >= 8 }?.let { java.sql.Time.valueOf(it.substring(11, 19)) }

    private fun parseSqlTimestamp(str: String?): java.sql.Timestamp? =
        str?.let { runCatching { java.sql.Timestamp.valueOf(it.replace('T', ' ').substring(0, 19)) }.getOrNull() }

    override fun getDate(columnLabel: String): java.sql.Date? {
        val d = parseSqlDate(getString(columnLabel))
        lastWasNull = d == null
        return d
    }
    override fun getDate(columnIndex: Int): java.sql.Date? =
        getDate(metaData.getColumnName(columnIndex))

    @Deprecated("Use getDate(columnIndex) instead")
    override fun getDate(columnIndex: Int, cal: Calendar?): Date? {
        // Delegate to the single-argument version to parse the underlying string
        val d: Date? = getDate(columnIndex)
        // If that version found SQL NULL, just return null
        if (wasNull()) return null

        if (cal == null || d == null) {
            // No calendar adjustment needed
            return d
        }

        // Adjust milliseconds to the given Calendar's timezone.
        val originalMillis = d.time
        val tzOffset = cal.timeZone.getOffset(originalMillis)
        val adjustedMillis = originalMillis - tzOffset

        return Date(adjustedMillis)
    }

    @Deprecated("Use getDate(columnLabel) instead")
    override fun getDate(columnLabel: String?, cal: Calendar?): Date? {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        // Delegate to the single-argument version
        val d: Date? = getDate(columnLabel)
        if (wasNull()) return null

        if (cal == null || d == null) {
            return d
        }

        // Same timezone-adjustment logic as above
        val originalMillis = d.time
        val tzOffset = cal.timeZone.getOffset(originalMillis)
        val adjustedMillis = originalMillis - tzOffset

        return Date(adjustedMillis)
    }

    override fun getTime(columnLabel: String): java.sql.Time? {
        val t = parseSqlTime(getString(columnLabel))
        lastWasNull = t == null
        return t
    }

    override fun getTime(columnIndex: Int): java.sql.Time? =
        getTime(metaData.getColumnName(columnIndex))

    @Deprecated("Use getTime(columnIndex) instead")
    override fun getTime(columnIndex: Int, cal: Calendar?): Time? {
        // Delegate to the single-argument version to parse the underlying string
        val t: Time? = getTime(columnIndex)
        // If that version found SQL NULL, just return null
        if (wasNull()) return null

        if (cal == null || t == null) {
            // No calendar adjustment needed
            return t
        }

        // Adjust milliseconds to the given Calendar's timezone.
        val originalMillis = t.time
        val tzOffset = cal.timeZone.getOffset(originalMillis)
        val adjustedMillis = originalMillis - tzOffset

        return Time(adjustedMillis)
    }

    @Deprecated("Use getTime(columnLabel) instead")
    override fun getTime(columnLabel: String?, cal: Calendar?): Time? {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        // Delegate to the single-argument version
        val t: Time? = getTime(columnLabel)
        if (wasNull()) return null

        if (cal == null || t == null) {
            return t
        }

        // Same timezone-adjustment logic as above
        val originalMillis = t.time
        val tzOffset = cal.timeZone.getOffset(originalMillis)
        val adjustedMillis = originalMillis - tzOffset

        return Time(adjustedMillis)
    }

    override fun getTimestamp(columnLabel: String): java.sql.Timestamp? {
        val ts = parseSqlTimestamp(getString(columnLabel))
        lastWasNull = ts == null
        return ts
    }

    override fun getTimestamp(columnIndex: Int): java.sql.Timestamp? =
        getTimestamp(metaData.getColumnName(columnIndex))

    @Deprecated("Use getTimestamp(columnIndex) instead")
    override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp? {
        // Delegate to the single-argument version to parse the underlying string
        val ts: Timestamp? = getTimestamp(columnIndex)
        // If that version found SQL NULL, just return null
        if (wasNull()) return null

        if (cal == null || ts == null) {
            // No calendar adjustment needed
            return ts
        }

        // Adjust milliseconds to the given Calendar's timezone.
        val originalMillis = ts.time
        val tzOffset = cal.timeZone.getOffset(originalMillis)
        val adjustedMillis = originalMillis - tzOffset

        return Timestamp(adjustedMillis)
    }

    @Deprecated("Use getTimestamp(columnLabel) instead")
    override fun getTimestamp(columnLabel: String?, cal: Calendar?): Timestamp? {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        // Delegate to the single-argument version
        val ts: Timestamp? = getTimestamp(columnLabel)
        if (wasNull()) return null

        if (cal == null || ts == null) {
            return ts
        }

        // Same timezone-adjustment logic as above
        val originalMillis = ts.time
        val tzOffset = cal.timeZone.getOffset(originalMillis)
        val adjustedMillis = originalMillis - tzOffset

        return Timestamp(adjustedMillis)
    }

    override fun getAsciiStream(columnIndex: Int): java.io.InputStream {
        val raw: String? = getString(columnIndex)
        if (wasNull() || raw == null) {
            return ByteArrayInputStream(ByteArray(0))
        }
        val bytes = raw.toByteArray(StandardCharsets.US_ASCII)
        return ByteArrayInputStream(bytes)
    }

    override fun getAsciiStream(columnLabel: String?): java.io.InputStream {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        val raw: String? = getString(columnLabel)
        if (wasNull() || raw == null) {
            return ByteArrayInputStream(ByteArray(0))
        }
        val bytes = raw.toByteArray(StandardCharsets.US_ASCII)
        return ByteArrayInputStream(bytes)
    }

    @Deprecated("Deprecated in Java")
    override fun getUnicodeStream(columnIndex: Int): java.io.InputStream {
        val raw: String? = getString(columnIndex)
        if (wasNull() || raw == null) {
            return ByteArrayInputStream(ByteArray(0))
        }
        val bytes = raw.toByteArray(StandardCharsets.UTF_8)
        return ByteArrayInputStream(bytes)
    }

    @Deprecated("Deprecated in Java")
    override fun getUnicodeStream(columnLabel: String?): java.io.InputStream {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        val raw: String? = getString(columnLabel)
        if (wasNull() || raw == null) {
            return ByteArrayInputStream(ByteArray(0))
        }
        val bytes = raw.toByteArray(StandardCharsets.UTF_8)
        return ByteArrayInputStream(bytes)
    }

    override fun getBinaryStream(columnIndex: Int): java.io.InputStream {
        val raw: String? = getString(columnIndex)
        if (wasNull() || raw == null) {
            return ByteArrayInputStream(ByteArray(0))
        }
        // If raw is base64 encoded, use Base64.getDecoder().decode(raw)
        val bytes = raw.toByteArray(StandardCharsets.UTF_8)
        return ByteArrayInputStream(bytes)
    }

    override fun getBinaryStream(columnLabel: String?): java.io.InputStream {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        val raw: String? = getString(columnLabel)
        if (wasNull() || raw == null) {
            return ByteArrayInputStream(ByteArray(0))
        }
        // If raw is base64 encoded, use Base64.getDecoder().decode(raw)
        val bytes = raw.toByteArray(StandardCharsets.UTF_8)
        return ByteArrayInputStream(bytes)
    }

    override fun getWarnings(): SQLWarning? = warnings

    override fun clearWarnings() { warnings = null }

    override fun getCursorName(): String {
        throw SQLFeatureNotSupportedException("getCursorName is not supported for forward-only result sets")
    }

    override fun getObject(columnLabel: String, map: MutableMap<String, Class<*>>?): Any? {
        // The type-map is ignored; simply return the default object
        return getObject(columnLabel)
    }

    override fun getCharacterStream(columnLabel: String): Reader =
        java.io.StringReader(getString(columnLabel) ?: "")

    override fun getCharacterStream(columnIndex: Int): Reader =
        getCharacterStream(metaData.getColumnName(columnIndex))

    // ─── Cursor position helpers ────────────────────────────────────────────
    override fun isBeforeFirst(): Boolean = currentIndex < 0 && rows.isNotEmpty()

    override fun isAfterLast(): Boolean   = currentIndex >= rows.size && !lastPageFull

    override fun isFirst(): Boolean       = currentIndex == 0

    override fun isLast(): Boolean        = !lastPageFull && currentIndex == rows.size - 1

    // Forward‑only cursor – disallowed moves
    override fun beforeFirst() = throw SQLException("TYPE_FORWARD_ONLY")

    override fun afterLast()  = throw SQLException("TYPE_FORWARD_ONLY")

    override fun first(): Boolean = throw SQLException("TYPE_FORWARD_ONLY")

    override fun last(): Boolean  = throw SQLException("TYPE_FORWARD_ONLY")

    override fun getBigDecimal(columnLabel: String): BigDecimal {
        val s = getString(columnLabel)
        if (lastWasNull) return BigDecimal.ZERO
        return runCatching { BigDecimal(s) }.getOrElse {
            throw SQLException("Cannot convert '$s' to BigDecimal", it)
        }
    }

    override fun getBigDecimal(columnIndex: Int): BigDecimal =
        getBigDecimal(metaData.getColumnName(columnIndex))

    override fun getRow(): Int =
        if (currentIndex < 0) 0 else currentOffset - rows.size + currentIndex + 1

    override fun absolute(row: Int): Boolean {
        throw SQLException("absolute($row) is not supported for TYPE_FORWARD_ONLY result sets")
    }

    override fun relative(rows: Int): Boolean {
        throw SQLException("relative($rows) is not supported for TYPE_FORWARD_ONLY result sets")
    }

    override fun previous(): Boolean {
        throw SQLException("previous() is not supported for TYPE_FORWARD_ONLY result sets")
    }

    override fun setFetchDirection(direction: Int) {
        if (direction != ResultSet.FETCH_FORWARD)
            throw SQLException("Only FETCH_FORWARD is supported")
        fetchDirection = direction
    }

    override fun getFetchDirection(): Int = fetchDirection

    /**
     * Sets the fetch size for this ResultSet. This affects how many rows
     * are fetched in each page from the server.
     */
    override fun setFetchSize(rows: Int) {
        val oldFetchSize = fetchSize
        fetchSize = WsdlStatement.validateFetchSize(rows, "PaginatedResultSet.setFetchSize")
        
        if (fetchSize != oldFetchSize) {
            logger.info("PaginatedResultSet fetch size changed from {} to {} (requested: {})", 
                oldFetchSize, fetchSize, rows)
        }
    }

    /**
     * Returns the current fetch size for this ResultSet
     */
    override fun getFetchSize(): Int = fetchSize

    override fun getType(): Int = ResultSet.TYPE_FORWARD_ONLY

    override fun getConcurrency(): Int = ResultSet.CONCUR_READ_ONLY

    override fun rowUpdated(): Boolean = false

    override fun rowInserted(): Boolean = false

    override fun rowDeleted(): Boolean = false

    override fun updateNull(columnIndex: Int) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateNull($columnIndex) is not supported")
    }

    override fun updateBoolean(columnIndex: Int, x: Boolean) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBoolean($columnIndex) is not supported")
    }

    override fun updateByte(columnIndex: Int, x: Byte) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateByte($columnIndex) is not supported")
    }

    override fun updateShort(columnIndex: Int, x: Short) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateShort($columnIndex) is not supported")
    }

    override fun updateInt(columnIndex: Int, x: Int) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateInt($columnIndex) is not supported")
    }

    override fun updateLong(columnIndex: Int, x: Long) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateLong($columnIndex) is not supported")
    }

    override fun updateFloat(columnIndex: Int, x: Float) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateFloat($columnIndex) is not supported")
    }

    override fun updateDouble(columnIndex: Int, x: Double) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateDouble($columnIndex) is not supported")
    }

    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBigDecimal($columnIndex) is not supported")
    }

    override fun updateString(columnIndex: Int, x: String?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateString($columnIndex) is not supported")
    }

    override fun updateBytes(columnIndex: Int, x: ByteArray?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBytes($columnIndex) is not supported")
    }

    override fun updateDate(columnIndex: Int, x: java.sql.Date?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateDate($columnIndex) is not supported")
    }

    override fun updateTime(columnIndex: Int, x: java.sql.Time?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateTime($columnIndex) is not supported")
    }

    override fun updateTimestamp(columnIndex: Int, x: java.sql.Timestamp?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateTimestamp($columnIndex) is not supported")
    }

    override fun updateAsciiStream(columnIndex: Int, x: java.io.InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateAsciiStream($columnIndex, length=$length) is not supported")
    }

    override fun updateBinaryStream(columnIndex: Int, x: java.io.InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBinaryStream($columnIndex, length=$length) is not supported")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBinaryStream('$columnLabel', length=$length) is not supported")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBinaryStream($columnIndex, length=$length) is not supported")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBinaryStream('$columnLabel', length=$length) is not supported")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBinaryStream($columnIndex) is not supported")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBinaryStream('$columnLabel') is not supported")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateCharacterStream($columnIndex, length=$length) is not supported")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Int) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateCharacterStream('$columnLabel', length=$length) is not supported")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateCharacterStream($columnIndex, length=$length) is not supported")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateCharacterStream('$columnLabel', length=$length) is not supported")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateCharacterStream($columnIndex) is not supported")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateCharacterStream('$columnLabel') is not supported")
    }

    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateObject($columnIndex, scaleOrLength=$scaleOrLength) is not supported")
    }

    override fun updateObject(columnIndex: Int, x: Any?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateObject($columnIndex) is not supported")
    }

    override fun updateNull(columnLabel: String) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateNull('$columnLabel') is not supported")
    }

    override fun updateBoolean(columnLabel: String, x: Boolean) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBoolean('$columnLabel') is not supported")
    }

    override fun updateByte(columnLabel: String, x: Byte) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateByte('$columnLabel') is not supported")
    }

    override fun updateShort(columnLabel: String, x: Short) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateShort('$columnLabel') is not supported")
    }

    override fun updateInt(columnLabel: String, x: Int) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateInt('$columnLabel') is not supported")
    }

    override fun updateLong(columnLabel: String, x: Long) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateLong('$columnLabel') is not supported")
    }

    override fun updateFloat(columnLabel: String, x: Float) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateFloat('$columnLabel') is not supported")
    }

    override fun updateDouble(columnLabel: String, x: Double) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateDouble('$columnLabel') is not supported")
    }

    override fun updateBigDecimal(columnLabel: String, x: BigDecimal?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBigDecimal('$columnLabel') is not supported")
    }

    override fun updateString(columnLabel: String, x: String?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateString('$columnLabel') is not supported")
    }

    override fun updateBytes(columnLabel: String, x: ByteArray?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBytes('$columnLabel') is not supported")
    }

    override fun updateDate(columnLabel: String?, x: java.sql.Date?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateDate('$columnLabel') is not supported")
    }

    override fun updateTime(columnLabel: String, x: java.sql.Time?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateTime('$columnLabel') is not supported")
    }

    override fun updateTimestamp(columnLabel: String, x: java.sql.Timestamp?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateTimestamp('$columnLabel') is not supported")
    }

    override fun updateAsciiStream(columnLabel: String, x: java.io.InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateAsciiStream('$columnLabel', length=$length) is not supported")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateAsciiStream($columnIndex, length=$length) is not supported")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateAsciiStream('$columnLabel', length=$length) is not supported")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateAsciiStream($columnIndex) is not supported")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateAsciiStream('$columnLabel') is not supported")
    }

    override fun updateObject(columnLabel: String, x: Any?, scaleOrLength: Int) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateObject('$columnLabel', scaleOrLength=$scaleOrLength) is not supported")
    }

    override fun updateObject(columnLabel: String, x: Any?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateObject('$columnLabel') is not supported")
    }

    override fun insertRow() {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; insertRow() is not supported")
    }

    override fun updateRow() {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateRow() is not supported")
    }

    override fun deleteRow() {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; deleteRow() is not supported")
    }

    override fun refreshRow() {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; refreshRow() is not supported")
    }

    override fun cancelRowUpdates() {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; cancelRowUpdates() is not supported")
    }

    override fun moveToInsertRow() {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; moveToInsertRow() is not supported")
    }

    override fun moveToCurrentRow() {
        throw SQLFeatureNotSupportedException("moveToCurrentRow() is not supported for TYPE_FORWARD_ONLY result sets")
    }

    override fun getStatement(): java.sql.Statement {
        throw SQLFeatureNotSupportedException("getStatement() is not supported by PaginatedResultSet")
    }

    override fun getRef(columnIndex: Int): Ref? {
        throw SQLFeatureNotSupportedException("getRef($columnIndex) is not supported by this driver")
    }

    override fun getRef(columnLabel: String?): Ref {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        throw SQLFeatureNotSupportedException("getRef('$columnLabel') is not supported by this driver")
    }

    override fun getBlob(columnIndex: Int): Blob {
        throw SQLFeatureNotSupportedException("getBlob($columnIndex) is not supported by this driver")
    }

    override fun getBlob(columnLabel: String?): Blob {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        throw SQLFeatureNotSupportedException("getBlob('$columnLabel') is not supported by this driver")
    }

    override fun getClob(columnIndex: Int): Clob {
        throw SQLFeatureNotSupportedException("getClob($columnIndex) is not supported by this driver")
    }

    override fun getClob(columnLabel: String?): Clob {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        throw SQLFeatureNotSupportedException("getClob('$columnLabel') is not supported by this driver")
    }

    override fun getArray(columnIndex: Int): java.sql.Array {
        throw SQLFeatureNotSupportedException("getArray($columnIndex) is not supported by this driver")
    }

    override fun getArray(columnLabel: String?): java.sql.Array {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        throw SQLFeatureNotSupportedException("getArray('$columnLabel') is not supported by this driver")
    }

    override fun getURL(columnIndex: Int): java.net.URL {
        throw SQLFeatureNotSupportedException("getURL($columnIndex) is not supported by this driver")
    }

    override fun getURL(columnLabel: String?): java.net.URL {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        throw SQLFeatureNotSupportedException("getURL('$columnLabel') is not supported by this driver")
    }

    override fun updateRef(columnIndex: Int, x: Ref?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateRef($columnIndex) is not supported")
    }

    override fun updateRef(columnLabel: String?, x: Ref?) {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateRef('$columnLabel') is not supported")
    }

    override fun updateBlob(columnIndex: Int, x: Blob?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBlob($columnIndex) is not supported")
    }

    override fun updateBlob(columnLabel: String?, x: Blob?) {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBlob('$columnLabel') is not supported")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBlob($columnIndex, length=$length) is not supported")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?, length: Long) {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBlob('$columnLabel', length=$length) is not supported")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBlob($columnIndex) is not supported")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?) {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateBlob('$columnLabel') is not supported")
    }

    override fun updateClob(columnIndex: Int, x: Clob?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateClob($columnIndex) is not supported")
    }

    override fun updateClob(columnLabel: String?, x: Clob?) {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateClob('$columnLabel') is not supported")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateClob($columnIndex, length=$length) is not supported")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?, length: Long) {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateClob('$columnLabel', length=$length) is not supported")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateClob($columnIndex) is not supported")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?) {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateClob('$columnLabel') is not supported")
    }

    override fun updateArray(columnIndex: Int, x: java.sql.Array?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateArray($columnIndex) is not supported")
    }

    override fun updateArray(columnLabel: String?, x: java.sql.Array?) {
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateArray('$columnLabel') is not supported")
    }

    override fun getRowId(columnIndex: Int): RowId {
        throw SQLFeatureNotSupportedException("getRowId($columnIndex) is not supported by this driver")
    }

    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?): Any? {
        return getObject(columnIndex)
    }

    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>?): T {
        throw SQLFeatureNotSupportedException("getObject($columnIndex, type=$type) is not supported by this driver")
    }

    override fun <T : Any?> getObject(columnLabel: String, type: Class<T>?): T {
        throw SQLFeatureNotSupportedException("getObject('$columnLabel', type=$type) is not supported by this driver")
    }

    override fun findColumn(columnLabel: String?): Int {
        columnLabel ?: throw SQLException("Column label cannot be null")
        return indexByLc[columnLabel.lowercase()]
            ?: throw SQLException(
                "Column '$columnLabel' not found. Available: ${originalByLc.values.joinToString()}"
            )
    }

    override fun getRowId(columnLabel: String): RowId {
        if (columnLabel.isBlank()) {
            throw SQLException("Column label cannot be null or empty")
        }
        throw SQLFeatureNotSupportedException("getRowId('$columnLabel') is not supported by this driver")
    }

    override fun updateRowId(columnIndex: Int, x: RowId?) {
        throw SQLFeatureNotSupportedException("updateRowId($columnIndex) is not supported by this driver")
    }

    override fun updateRowId(columnLabel: String, x: RowId?) {
        if (columnLabel.isBlank()) {
            throw SQLException("Column label cannot be null or empty")
        }
        throw SQLFeatureNotSupportedException("updateRowId('$columnLabel') is not supported by this driver")
    }

    override fun getHoldability(): Int = ResultSet.HOLD_CURSORS_OVER_COMMIT

    override fun isClosed(): Boolean = closed

    override fun updateNString(columnIndex: Int, nString: String?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateNString($columnIndex) is not supported")
    }

    override fun updateNString(columnLabel: String?, nString: String?) {
        if (columnLabel == null || columnLabel.isBlank()) {
            throw SQLException("Column label cannot be null or empty")
        }
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateNString('$columnLabel') is not supported")
    }

    override fun updateNClob(columnIndex: Int, nClob: NClob?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateNClob($columnIndex) is not supported")
    }

    override fun updateNClob(columnLabel: String?, nClob: NClob?) {
        if (columnLabel == null || columnLabel.isBlank()) {
            throw SQLException("Column label cannot be null or empty")
        }
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateNClob('$columnLabel') is not supported")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateNClob($columnIndex, length=$length) is not supported")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) {
        if (columnLabel == null || columnLabel.isBlank()) {
            throw SQLException("Column label cannot be null or empty")
        }
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateNClob('$columnLabel', length=$length) is not supported")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateNClob($columnIndex) is not supported")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?) {
        if (columnLabel == null || columnLabel.isBlank()) {
            throw SQLException("Column label cannot be null or empty")
        }
        throw SQLFeatureNotSupportedException("ResultSet is read-only; updateNClob('$columnLabel') is not supported")
    }

    override fun getNClob(columnIndex: Int): NClob {
        throw SQLFeatureNotSupportedException("getNClob($columnIndex) is not supported by this driver")
    }

    override fun getNClob(columnLabel: String?): NClob {
        if (columnLabel == null || columnLabel.isBlank()) {
            throw SQLException("Column label cannot be null or empty")
        }
        throw SQLFeatureNotSupportedException("getNClob('$columnLabel') is not supported by this driver")
    }

    override fun getSQLXML(columnIndex: Int): SQLXML {
        throw SQLFeatureNotSupportedException("getSQLXML($columnIndex) is not supported by this driver")
    }

    override fun getSQLXML(columnLabel: String?): SQLXML {
        if (columnLabel.isNullOrBlank()) {
            throw SQLException("Column label cannot be null or empty")
        }
        throw SQLFeatureNotSupportedException("getSQLXML('$columnLabel') is not supported by this driver")
    }

    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML?) {
        throw SQLFeatureNotSupportedException("updateSQLXML($columnIndex) is not supported by this driver")
    }

    override fun updateSQLXML(columnLabel: String?, xmlObject: SQLXML?) {
        if (columnLabel.isNullOrBlank()) {
            throw SQLException("Column label cannot be null or empty")
        }
        throw SQLFeatureNotSupportedException("updateSQLXML('$columnLabel') is not supported by this driver")
    }

    // getNString methods delegate to getString, so they remain unchanged
    override fun getNString(columnLabel: String): String? = getString(columnLabel)

    override fun getNString(columnIndex: Int): String? = getString(columnIndex)

    override fun getNCharacterStream(columnIndex: Int): Reader {
        throw SQLFeatureNotSupportedException("getNCharacterStream($columnIndex) is not supported by this driver")
    }

    override fun getNCharacterStream(columnLabel: String?): Reader {
        if (columnLabel.isNullOrBlank()) {
            throw SQLException("Column label cannot be null or empty")
        }
        throw SQLFeatureNotSupportedException("getNCharacterStream('$columnLabel') is not supported by this driver")
    }

    override fun updateNCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("updateNCharacterStream($columnIndex, length=$length) is not supported by this driver")
    }

    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        if (columnLabel.isNullOrBlank()) {
            throw SQLException("Column label cannot be null or empty")
        }
        throw SQLFeatureNotSupportedException("updateNCharacterStream('$columnLabel', length=$length) is not supported by this driver")
    }

    override fun updateNCharacterStream(columnIndex: Int, x: Reader?) {
        throw SQLFeatureNotSupportedException("updateNCharacterStream($columnIndex) is not supported by this driver")
    }

    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?) {
        if (columnLabel.isNullOrBlank()) {
            throw SQLException("Column label cannot be null or empty")
        }
        throw SQLFeatureNotSupportedException("updateNCharacterStream('$columnLabel') is not supported by this driver")
    }

    override fun <T> unwrap(iface: Class<T>?): T {
        if (iface == null) throw SQLException("Interface cannot be null")
        if (iface.isInstance(this)) return iface.cast(this)
        throw SQLException("Not a wrapper for " + iface.name)
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean =
        iface != null && iface.isInstance(this)
}
