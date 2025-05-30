package my.jdbc.wsdl_driver

import org.apache.commons.text.StringEscapeUtils
import org.slf4j.Logger
import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.util.*
import java.io.Closeable

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
    override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal = throw UnsupportedOperationException("Not implemented 14")

    @Deprecated("Deprecated in Java")
    override fun getBigDecimal(columnLabel: String, scale: Int): BigDecimal = throw UnsupportedOperationException("Not implemented 15")

    override fun getBytes(columnIndex: Int): ByteArray = throw UnsupportedOperationException("Not implemented 16")

    override fun getBytes(columnLabel: String?): ByteArray = throw UnsupportedOperationException("Not implemented 17")

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

    override fun getDate(columnIndex: Int, cal: Calendar?): Date {
        TODO("Not yet implemented 1")
    }

    override fun getDate(columnLabel: String?, cal: Calendar?): Date {
        TODO("Not yet implemented 2")
    }

    override fun getTime(columnLabel: String): java.sql.Time? {
        val t = parseSqlTime(getString(columnLabel))
        lastWasNull = t == null
        return t
    }

    override fun getTime(columnIndex: Int): java.sql.Time? =
        getTime(metaData.getColumnName(columnIndex))

    override fun getTime(columnIndex: Int, cal: Calendar?): Time {
        TODO("Not yet implemented 3")
    }

    override fun getTime(columnLabel: String?, cal: Calendar?): Time {
        TODO("Not yet implemented 4")
    }

    override fun getTimestamp(columnLabel: String): java.sql.Timestamp? {
        val ts = parseSqlTimestamp(getString(columnLabel))
        lastWasNull = ts == null
        return ts
    }

    override fun getTimestamp(columnIndex: Int): java.sql.Timestamp? =
        getTimestamp(metaData.getColumnName(columnIndex))

    override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp {
        TODO("Not yet implemented 5")
    }

    override fun getTimestamp(columnLabel: String?, cal: Calendar?): Timestamp {
        TODO("Not yet implemented 6")
    }

    override fun getAsciiStream(columnIndex: Int): java.io.InputStream = throw UnsupportedOperationException("Not implemented 24")

    override fun getAsciiStream(columnLabel: String?): java.io.InputStream = throw UnsupportedOperationException("Not implemented 25")

    @Deprecated("Deprecated in Java", ReplaceWith("throw UnsupportedOperationException(\"Not implemented 26\")"))
    override fun getUnicodeStream(columnIndex: Int): java.io.InputStream = throw UnsupportedOperationException("Not implemented 26")

    @Deprecated("Deprecated in Java", ReplaceWith("throw UnsupportedOperationException(\"Not implemented 27\")"))
    override fun getUnicodeStream(columnLabel: String?): java.io.InputStream = throw UnsupportedOperationException("Not implemented 27")

    override fun getBinaryStream(columnIndex: Int): java.io.InputStream = throw UnsupportedOperationException("Not implemented 28")

    override fun getBinaryStream(columnLabel: String?): java.io.InputStream = throw UnsupportedOperationException("Not implemented 29")

    override fun getWarnings(): SQLWarning? = warnings

    override fun clearWarnings() { warnings = null }

    override fun getCursorName(): String = throw UnsupportedOperationException("Not implemented 30")

    override fun getObject(columnLabel: String, map: MutableMap<String, Class<*>>?): Any = throw UnsupportedOperationException("Not implemented 31")

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

    override fun absolute(row: Int): Boolean = throw UnsupportedOperationException("Not implemented 48")

    override fun relative(rows: Int): Boolean = throw UnsupportedOperationException("Not implemented 49")

    override fun previous(): Boolean = throw UnsupportedOperationException("Not implemented 50")

    override fun setFetchDirection(direction: Int) {
        if (direction != ResultSet.FETCH_FORWARD)
            throw SQLException("Only FETCH_FORWARD is supported")
        fetchDirection = direction
    }

    override fun getFetchDirection(): Int = fetchDirection

    override fun setFetchSize(rows: Int) {
        fetchSize = rows
        logger.info("Fetch size set to {}", rows)
    }

    override fun getFetchSize(): Int = fetchSize

    override fun getType(): Int = ResultSet.TYPE_FORWARD_ONLY

    override fun getConcurrency(): Int = ResultSet.CONCUR_READ_ONLY

    override fun rowUpdated(): Boolean = false

    override fun rowInserted(): Boolean = false

    override fun rowDeleted(): Boolean = false

    override fun updateNull(columnIndex: Int) = throw UnsupportedOperationException("Not implemented 59")

    override fun updateBoolean(columnIndex: Int, x: Boolean) = throw UnsupportedOperationException("Not implemented 60")

    override fun updateByte(columnIndex: Int, x: Byte) = throw UnsupportedOperationException("Not implemented 61")

    override fun updateShort(columnIndex: Int, x: Short) = throw UnsupportedOperationException("Not implemented 62")

    override fun updateInt(columnIndex: Int, x: Int) = throw UnsupportedOperationException("Not implemented 63")

    override fun updateLong(columnIndex: Int, x: Long) = throw UnsupportedOperationException("Not implemented 64")

    override fun updateFloat(columnIndex: Int, x: Float) = throw UnsupportedOperationException("Not implemented 65")

    override fun updateDouble(columnIndex: Int, x: Double) = throw UnsupportedOperationException("Not implemented 66")

    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) = throw UnsupportedOperationException("Not implemented 67")

    override fun updateString(columnIndex: Int, x: String?) = throw UnsupportedOperationException("Not implemented 68")

    override fun updateBytes(columnIndex: Int, x: ByteArray?) = throw UnsupportedOperationException("Not implemented 69")

    override fun updateDate(columnIndex: Int, x: java.sql.Date?) = throw UnsupportedOperationException("Not implemented 70")

    override fun updateTime(columnIndex: Int, x: java.sql.Time?) = throw UnsupportedOperationException("Not implemented 71")

    override fun updateTimestamp(columnIndex: Int, x: java.sql.Timestamp?) = throw UnsupportedOperationException("Not implemented 72")

    override fun updateAsciiStream(columnIndex: Int, x: java.io.InputStream?, length: Int) = throw UnsupportedOperationException("Not implemented 73")

    override fun updateBinaryStream(columnIndex: Int, x: java.io.InputStream?, length: Int) = throw UnsupportedOperationException("Not implemented 74")

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Int) {
        TODO("Not yet implemented 7")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) {
        TODO("Not yet implemented 8")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Long) {
        TODO("Not yet implemented 9")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) {
        TODO("Not yet implemented 10")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?) {
        TODO("Not yet implemented 11")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) {
        TODO("Not yet implemented 12")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Int) {
        TODO("Not yet implemented 13")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        TODO("Not yet implemented 14")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented 15")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?) {
        TODO("Not yet implemented 16")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented 17")
    }

    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) = throw UnsupportedOperationException("Not implemented 75")

    override fun updateObject(columnIndex: Int, x: Any?) = throw UnsupportedOperationException("Not implemented 76")

    override fun updateNull(columnLabel: String) = throw UnsupportedOperationException("Not implemented 77")

    override fun updateBoolean(columnLabel: String, x: Boolean) = throw UnsupportedOperationException("Not implemented 78")

    override fun updateByte(columnLabel: String, x: Byte) = throw UnsupportedOperationException("Not implemented 79")

    override fun updateShort(columnLabel: String, x: Short) = throw UnsupportedOperationException("Not implemented 80")

    override fun updateInt(columnLabel: String, x: Int) = throw UnsupportedOperationException("Not implemented 81")

    override fun updateLong(columnLabel: String, x: Long) = throw UnsupportedOperationException("Not implemented 82")

    override fun updateFloat(columnLabel: String, x: Float) = throw UnsupportedOperationException("Not implemented 83")

    override fun updateDouble(columnLabel: String, x: Double) = throw UnsupportedOperationException("Not implemented 84")

    override fun updateBigDecimal(columnLabel: String, x: BigDecimal?) = throw UnsupportedOperationException("Not implemented 85")

    override fun updateString(columnLabel: String, x: String?) = throw UnsupportedOperationException("Not implemented 86")

    override fun updateBytes(columnLabel: String, x: ByteArray?) = throw UnsupportedOperationException("Not implemented 87")

    override fun updateDate(columnLabel: String?, x: java.sql.Date?) = throw UnsupportedOperationException("Not implemented 88")

    override fun updateTime(columnLabel: String, x: java.sql.Time?) = throw UnsupportedOperationException("Not implemented 89")

    override fun updateTimestamp(columnLabel: String, x: java.sql.Timestamp?) = throw UnsupportedOperationException("Not implemented 90")

    override fun updateAsciiStream(columnLabel: String, x: java.io.InputStream?, length: Int) = throw UnsupportedOperationException("Not implemented 91")

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) {
        TODO("Not yet implemented 18")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Long) {
        TODO("Not yet implemented 19")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) {
        TODO("Not yet implemented 20")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?) {
        TODO("Not yet implemented 21")
    }

    override fun updateObject(columnLabel: String, x: Any?, scaleOrLength: Int) = throw UnsupportedOperationException("Not implemented 92")

    override fun updateObject(columnLabel: String, x: Any?) = throw UnsupportedOperationException("Not implemented 93")

    override fun insertRow() = throw UnsupportedOperationException("Not implemented 94")

    override fun updateRow() = throw UnsupportedOperationException("Not implemented 95")

    override fun deleteRow() = throw UnsupportedOperationException("Not implemented 96")

    override fun refreshRow() = throw UnsupportedOperationException("Not implemented 97")

    override fun cancelRowUpdates() = throw UnsupportedOperationException("Not implemented 98")

    override fun moveToInsertRow() = throw UnsupportedOperationException("Not implemented 99")

    override fun moveToCurrentRow() = throw UnsupportedOperationException("Not implemented 100")

    override fun getStatement(): java.sql.Statement = throw UnsupportedOperationException("Not implemented 101")

    override fun getRef(columnIndex: Int): Ref? = throw UnsupportedOperationException("Not implemented 102")

    override fun getRef(columnLabel: String?): Ref {
        TODO("Not yet implemented 22")
    }

    override fun getBlob(columnIndex: Int): Blob {
        TODO("Not yet implemented 23")
    }

    override fun getBlob(columnLabel: String?): Blob {
        TODO("Not yet implemented 24")
    }

    override fun getClob(columnIndex: Int): Clob {
        TODO("Not yet implemented 25")
    }

    override fun getClob(columnLabel: String?): Clob {
        TODO("Not yet implemented 26")
    }

    override fun getArray(columnIndex: Int): Array {
        TODO("Not yet implemented 27")
    }

    override fun getArray(columnLabel: String?): Array {
        TODO("Not yet implemented 28")
    }

    override fun getURL(columnIndex: Int): URL {
        TODO("Not yet implemented 29")
    }

    override fun getURL(columnLabel: String?): URL {
        TODO("Not yet implemented 30")
    }

    override fun updateRef(columnIndex: Int, x: Ref?) {
        TODO("Not yet implemented 31")
    }

    override fun updateRef(columnLabel: String?, x: Ref?) {
        TODO("Not yet implemented 32")
    }

    override fun updateBlob(columnIndex: Int, x: Blob?) {
        TODO("Not yet implemented 33")
    }

    override fun updateBlob(columnLabel: String?, x: Blob?) {
        TODO("Not yet implemented 34")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) {
        TODO("Not yet implemented 35")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?, length: Long) {
        TODO("Not yet implemented 36")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) {
        TODO("Not yet implemented 37")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?) {
        TODO("Not yet implemented 38")
    }

    override fun updateClob(columnIndex: Int, x: Clob?) {
        TODO("Not yet implemented 39")
    }

    override fun updateClob(columnLabel: String?, x: Clob?) {
        TODO("Not yet implemented 40")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) {
        TODO("Not yet implemented 41")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented 42")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?) {
        TODO("Not yet implemented 43")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented 44")
    }

    override fun updateArray(columnIndex: Int, x: Array?) {
        TODO("Not yet implemented 45")
    }

    override fun updateArray(columnLabel: String?, x: Array?) {
        TODO("Not yet implemented 46")
    }

    override fun getRowId(columnIndex: Int): RowId {
        TODO("Not yet implemented 47")
    }

    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?): Any = throw UnsupportedOperationException("Not implemented 103")

    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>?): T = throw UnsupportedOperationException("Not implemented 104")

    override fun <T : Any?> getObject(columnLabel: String, type: Class<T>?): T = throw UnsupportedOperationException("Not implemented 105")

    override fun findColumn(columnLabel: String?): Int {
        columnLabel ?: throw SQLException("Column label cannot be null")
        return indexByLc[columnLabel.lowercase()]
            ?: throw SQLException(
                "Column '$columnLabel' not found. Available: ${originalByLc.values.joinToString()}"
            )
    }

    override fun getRowId(columnLabel: String): java.sql.RowId = throw UnsupportedOperationException("Not implemented 107")

    override fun updateRowId(columnIndex: Int, x: java.sql.RowId?) = throw UnsupportedOperationException("Not implemented 108")

    override fun updateRowId(columnLabel: String, x: java.sql.RowId?) = throw UnsupportedOperationException("Not implemented 109")

    override fun getHoldability(): Int = ResultSet.HOLD_CURSORS_OVER_COMMIT

    override fun isClosed(): Boolean = closed

    override fun updateNString(columnIndex: Int, nString: String?) = throw UnsupportedOperationException("Not implemented 111")

    override fun updateNString(columnLabel: String?, nString: String?) = throw UnsupportedOperationException("Not implemented 112")

    override fun updateNClob(columnIndex: Int, nClob: java.sql.NClob?) = throw UnsupportedOperationException("Not implemented 113")

    override fun updateNClob(columnLabel: String?, nClob: java.sql.NClob?) = throw UnsupportedOperationException("Not implemented 114")

    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) {
        TODO("Not yet implemented 48")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented 49")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?) {
        TODO("Not yet implemented 50")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented 51")
    }

    override fun getNClob(columnIndex: Int): java.sql.NClob = throw UnsupportedOperationException("Not implemented 115")

    override fun getNClob(columnLabel: String?): java.sql.NClob = throw UnsupportedOperationException("Not implemented 116")

    override fun getSQLXML(columnIndex: Int): java.sql.SQLXML = throw UnsupportedOperationException("Not implemented 117")

    override fun getSQLXML(columnLabel: String?): java.sql.SQLXML = throw UnsupportedOperationException("Not implemented 118")

    override fun updateSQLXML(columnIndex: Int, xmlObject: java.sql.SQLXML?) = throw UnsupportedOperationException("Not implemented 119")

    override fun updateSQLXML(columnLabel: String?, xmlObject: java.sql.SQLXML?) = throw UnsupportedOperationException("Not implemented 120")

    override fun getNString(columnLabel: String): String? = getString(columnLabel)

    override fun getNString(columnIndex: Int): String? = getString(columnIndex)

    override fun getNCharacterStream(columnIndex: Int): java.io.Reader = throw UnsupportedOperationException("Not implemented 123")

    override fun getNCharacterStream(columnLabel: String?): java.io.Reader = throw UnsupportedOperationException("Not implemented 124")

    override fun updateNCharacterStream(columnIndex: Int, x: java.io.Reader?, length: Long) = throw UnsupportedOperationException("Not implemented 125")

    override fun updateNCharacterStream(columnLabel: String?, reader: java.io.Reader?, length: Long) = throw UnsupportedOperationException("Not implemented 126")

    override fun updateNCharacterStream(columnIndex: Int, x: java.io.Reader?) = throw UnsupportedOperationException("Not implemented 127")

    override fun updateNCharacterStream(columnLabel: String?, reader: java.io.Reader?) = throw UnsupportedOperationException("Not implemented 128")

    override fun <T> unwrap(iface: Class<T>?): T {
        if (iface == null) throw SQLException("Interface cannot be null")
        if (iface.isInstance(this)) return iface.cast(this)
        throw SQLException("Not a wrapper for " + iface.name)
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean =
        iface != null && iface.isInstance(this)
}
