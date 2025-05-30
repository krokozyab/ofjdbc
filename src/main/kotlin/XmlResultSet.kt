package my.jdbc.wsdl_driver

import java.sql.SQLFeatureNotSupportedException

import java.net.URL
import java.net.MalformedURLException

import java.util.Base64

import java.io.InputStream
import java.io.Reader
import java.io.StringReader
import java.io.Closeable
import java.math.BigDecimal
import java.math.RoundingMode
import java.sql.SQLException
import java.sql.*
import java.sql.Time
import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.*
import java.io.ByteArrayInputStream
import org.slf4j.LoggerFactory

data class ColumnMetadata(val name: String, val type: Int, val typeName: String)

/**
 * @param rows the result rows (will be cleared after close)
 * @param resource optional underlying resource (InputStream, Reader, etc.) that should be closed with this ResultSet
 * @param columns column metadata; if not provided, inferred from rows
 */
class XmlResultSet(
    rows: List<Map<String, String?>>,
    /** optional underlying resource (InputStream, Reader, etc.) that should be closed with this ResultSet */
    private val resource: Closeable? = null,
    // If metadata is not provided, default all columns to VARCHAR.
    private val columns: List<ColumnMetadata> = if (rows.isNotEmpty())
        rows[0].keys.map { key -> ColumnMetadata(key, Types.VARCHAR, "VARCHAR") }
    else
        emptyList()
) : ResultSet {
    private val logger = LoggerFactory.getLogger(XmlResultSet::class.java)
    /** internal mutable copy so we can clear it on close() */
    private val rows: MutableList<Map<String, String?>> = rows.toMutableList()
    /** case‑insensitive column lookup: lowercase name → (original name, 1‑based index) */
    private data class ColInfo(val original: String, val index1: Int)
    private val colInfo: Map<String, ColInfo> =
        columns.mapIndexed { idx, col -> col.name.lowercase() to ColInfo(col.name, idx + 1) }
                .toMap()
    /** Cached metadata object so we don't recreate it on every call */
    private val meta: ResultSetMetaData =
        DefaultResultSetMetaData(columns.map { it.name })
    @Volatile private var currentIndex = -1
    /** true if the last column read had SQL NULL value */
    @Volatile private var lastWasNull: Boolean = false
    /** true after close() is called */
    @Volatile private var closed: Boolean = false
    /** Stores the first SQLWarning (if any) thrown during close */
    private var warnings: SQLWarning? = null
    /** client‑hint for driver fetch size (0 = driver default) */
    private var fetchSize: Int = 0

    private var statement: Statement? = null

    private fun currentRow(): Map<String, String?> {
        if (currentIndex !in rows.indices) {
            throw SQLException("No current row available")
        }
        return rows[currentIndex]
    }

    override fun next(): Boolean {
        currentIndex++
        return currentIndex < rows.size
    }

    override fun getString(columnLabel: String): String? {
        val info  = colInfo[columnLabel.lowercase()]
        val value = currentRow()[info?.original ?: columnLabel]
        lastWasNull = value == null
        return value
    }

    override fun getString(columnIndex: Int): String? =
        getString(columns[columnIndex - 1].name)

    // Instead of throwing an exception if conversion fails, return 0.
    override fun getInt(columnLabel: String): Int {
        val str = getString(columnLabel)   // sets lastWasNull
        if (lastWasNull) return 0
        return str!!.toIntOrNull()
            ?: throw SQLException("Cannot convert value '$str' in column '$columnLabel' to INT")
    }

    override fun getMetaData(): ResultSetMetaData = meta

    override fun wasNull(): Boolean = lastWasNull

    override fun close() {
        // Clear rows to help GC
        rows.clear()
        // Close the underlying resource if supplied
        try {
            resource?.close()
        } catch (ex: Exception) {
            // convert to SQLWarning per JDBC spec so callers can inspect later
            warnings = SQLWarning("Error closing underlying resource", ex)
        }
        closed = true
    }

    // ─────────────────────────────────────────────────────────────
    // Forward-only guard helper
    private fun forwardOnlyError(): Nothing =
        throw SQLException("Operation not allowed for TYPE_FORWARD_ONLY ResultSet")

    // ─────────────────────────────────────────────────────────────
    // Stub implementations with sequential numbering
    // ─────────────────────────────────────────────────────────────
    override fun getBoolean(columnLabel: String): Boolean {
        val s = getString(columnLabel)
        if (lastWasNull) return false
        return when (s!!.trim().lowercase()) {
            "true", "t", "1", "y", "yes"  -> true
            "false", "f", "0", "n", "no"  -> false
            else -> throw SQLException("Cannot convert value '$s' in column '$columnLabel' to BOOLEAN")
        }
    }
    override fun getBoolean(columnIndex: Int): Boolean =
        getBoolean(columns[columnIndex - 1].name)

    override fun getByte(columnIndex: Int): Byte =
        getByte(columns[columnIndex - 1].name)

    override fun getByte(columnLabel: String?): Byte {
        val s = getString(columnLabel!!)
        if (lastWasNull) return 0
        return s!!.toByteOrNull()
            ?: throw SQLException("Cannot convert value '$s' in column '$columnLabel' to BYTE")
    }

    override fun getShort(columnIndex: Int): Short =
        getShort(columns[columnIndex - 1].name)

    override fun getShort(columnLabel: String?): Short {
        val s = getString(columnLabel!!)
        if (lastWasNull) return 0
        return s!!.toShortOrNull()
            ?: throw SQLException("Cannot convert value '$s' in column '$columnLabel' to SHORT")
    }

    override fun getInt(columnIndex: Int): Int =
        getInt(columns[columnIndex - 1].name)


    override fun getLong(columnLabel: String): Long {
        val str = getString(columnLabel)
        if (lastWasNull) return 0L
        return str!!.toLongOrNull()
            ?: throw SQLException("Cannot convert value '$str' in column '$columnLabel' to LONG")
    }

    override fun getLong(columnIndex: Int): Long =
        getLong(columns[columnIndex - 1].name)

    override fun getFloat(columnIndex: Int): Float =
        getFloat(columns[columnIndex - 1].name)

    override fun getFloat(columnLabel: String?): Float {
        val s = getString(columnLabel!!)
        if (lastWasNull) return 0.0F
        return s!!.toFloatOrNull()
            ?: throw SQLException("Cannot convert value '$s' in column '$columnLabel' to FLOAT")
    }

    override fun getDouble(columnLabel: String): Double {
        val s = getString(columnLabel)
        if (lastWasNull) return 0.0
        return s!!.toDoubleOrNull()
            ?: throw SQLException("Cannot convert value '$s' in column '$columnLabel' to DOUBLE")
    }

    override fun getDouble(columnIndex: Int): Double =
        getDouble(columns[columnIndex - 1].name)

@Deprecated("Deprecated in Java")
override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal {
    // Retrieve value as string to handle null and conversion
    val columnName = columns[columnIndex - 1].name
    val raw = getString(columnName)  // sets lastWasNull
    if (lastWasNull) {
        // Return zero value with the correct scale when SQL NULL
        return BigDecimal.ZERO.setScale(scale, java.math.RoundingMode.HALF_UP)
    }
    try {
        // Convert to BigDecimal and apply scale
        return BigDecimal(raw).setScale(scale, java.math.RoundingMode.HALF_UP)
    } catch (ex: Exception) {
        throw java.sql.SQLException("Cannot convert value '$raw' in column '$columnName' to BigDecimal", ex)
    }
}

@Deprecated("Deprecated in Java")
override fun getBigDecimal(columnLabel: String, scale: Int): BigDecimal {
    // Retrieve the raw string value, setting lastWasNull
    val raw = getString(columnLabel)
    if (lastWasNull) {
        // Return zero value at the requested scale when SQL NULL
        return BigDecimal.ZERO.setScale(scale, RoundingMode.HALF_UP)
    }
    try {
        // Convert to BigDecimal and apply scale
        return BigDecimal(raw).setScale(scale, RoundingMode.HALF_UP)
    } catch (ex: Exception) {
        throw SQLException("Cannot convert value '$raw' in column '$columnLabel' to BigDecimal", ex)
    }
}

    override fun getBytes(columnIndex: Int): ByteArray =
        getBytes(columns[columnIndex - 1].name)

    override fun getBytes(columnLabel: String?): ByteArray {
        val s = getString(columnLabel!!)  // sets lastWasNull
        if (lastWasNull) return ByteArray(0)
        return try {
            Base64.getDecoder().decode(s)
        } catch (ex: IllegalArgumentException) {
            // Fallback: return raw UTF-8 bytes
            s!!.toByteArray(Charsets.UTF_8)
        }
    }

    override fun getDate(columnLabel: String): java.sql.Date {
        val s = getString(columnLabel)
        if (lastWasNull) return java.sql.Date(0)
        return try {
            java.sql.Date.valueOf(LocalDate.parse(s))
        } catch (ex: Exception) {
            throw SQLException("Cannot convert value '$s' in column '$columnLabel' to DATE", ex)
        }
    }
    override fun getDate(columnIndex: Int): java.sql.Date =
        getDate(columns[columnIndex - 1].name)

    override fun getTime(columnIndex: Int): Time =
        getTime(columns[columnIndex - 1].name)

    override fun getTime(columnLabel: String?): Time {
        val s = getString(columnLabel!!)
        if (lastWasNull) return Time(0)
        return try {
            Time.valueOf(s)
        } catch (ex: Exception) {
            throw SQLException("Cannot convert value '$s' in column '$columnLabel' to TIME", ex)
        }
    }

    override fun getTimestamp(columnIndex: Int): Timestamp =
        getTimestamp(columns[columnIndex - 1].name)

    override fun getTimestamp(columnLabel: String?): Timestamp {
        val s = getString(columnLabel!!)
        if (lastWasNull) return Timestamp(0)
        return try {
            // Expect ISO-8601 or JDBC timestamp format "yyyy-[m]m-[d]d hh:mm:ss[.f...]"
            Timestamp.valueOf(s)
        } catch (ex: IllegalArgumentException) {
            // Fallback parse with LocalDateTime
            try {
                val ldt = java.time.LocalDateTime.parse(s)
                Timestamp.valueOf(ldt)
            } catch (e2: Exception) {
                throw SQLException("Cannot convert value '$s' in column '$columnLabel' to TIMESTAMP", e2)
            }
        }
    }

    override fun getAsciiStream(columnIndex: Int): InputStream =
        getAsciiStream(columns[columnIndex - 1].name)

    override fun getAsciiStream(columnLabel: String?): InputStream {
        val s = getString(columnLabel!!)
        // Return a ByteArrayInputStream of the ASCII bytes
        val bytes = s?.toByteArray(Charsets.US_ASCII) ?: ByteArray(0)
        lastWasNull = s == null
        return ByteArrayInputStream(bytes)
    }
@Deprecated("Deprecated in Java")
override fun getUnicodeStream(columnIndex: Int): InputStream =
    getUnicodeStream(columns[columnIndex - 1].name)

@Deprecated("Deprecated in Java")
override fun getUnicodeStream(columnLabel: String?): InputStream {
    val s = getString(columnLabel!!)
    lastWasNull = s == null
    // Return UTF-16BE bytes as a stream
    val bytes = s?.toByteArray(Charsets.UTF_16BE) ?: ByteArray(0)
    return ByteArrayInputStream(bytes)
}

override fun getBinaryStream(columnIndex: Int): InputStream =
    getBinaryStream(columns[columnIndex - 1].name)

override fun getBinaryStream(columnLabel: String?): InputStream {
    val s = getString(columnLabel!!)
    lastWasNull = s == null
    if (lastWasNull) return ByteArrayInputStream(ByteArray(0))
    return try {
        // Assume Base64-encoded binary, decode it
        ByteArrayInputStream(Base64.getDecoder().decode(s))
    } catch (ex: IllegalArgumentException) {
        // Fallback: treat raw string as UTF-8 bytes
        ByteArrayInputStream(s!!.toByteArray(Charsets.UTF_8))
    }
}

    override fun getWarnings(): SQLWarning? = warnings

    override fun clearWarnings() {
        warnings = null
    }

    override fun getCursorName(): String = ""

    /**
     * Returns an Object converted based on the provided column metadata.
     */
    override fun getObject(columnLabel: String): Any? {
        val raw = getString(columnLabel) ?: return null  // lastWasNull already set
        val colMeta = columns.find { it.name.equals(columnLabel, ignoreCase = true) } ?: return raw
        logger.info("Column metadata: {} {}", columnLabel, colMeta)
        return when (colMeta.type) {
            Types.INTEGER -> raw.toIntOrNull() ?: 0
            Types.DOUBLE -> raw.toDoubleOrNull() ?: raw
            Types.DECIMAL, Types.NUMERIC -> try { BigDecimal(raw) } catch (_: Exception) { raw }
            Types.DATE -> try {
                val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                java.sql.Date.valueOf(LocalDate.parse(raw, formatter))
            } catch (_: Exception) { raw }
            else -> raw
        }
    }

    override fun getObject(columnIndex: Int): Any? =
        getObject(columns[columnIndex - 1].name)

// Return a StringReader over the column’s text value
override fun getCharacterStream(columnIndex: Int): StringReader =
    getCharacterStream(columns[columnIndex - 1].name)

// Return a StringReader over the column’s text value (empty on NULL)
override fun getCharacterStream(columnLabel: String): StringReader {
    val s = getString(columnLabel)
    lastWasNull = s == null
    return StringReader(s ?: "")
}

    // Forward-only navigation and position checks
    override fun isBeforeFirst(): Boolean = currentIndex < 0

    override fun isAfterLast(): Boolean = currentIndex >= rows.size

    override fun isFirst(): Boolean = currentIndex == 0 && rows.isNotEmpty()

    override fun isLast(): Boolean = currentIndex == rows.size - 1 && rows.isNotEmpty()

    override fun beforeFirst() = forwardOnlyError()

    override fun afterLast() = forwardOnlyError()

    override fun first(): Boolean = forwardOnlyError()

    override fun last(): Boolean = forwardOnlyError()

    override fun getBigDecimal(columnIndex: Int): BigDecimal =
        getBigDecimal(columns[columnIndex - 1].name)

    override fun getBigDecimal(columnLabel: String?): BigDecimal {
        // Retrieve as string to handle NULL and conversion
        val raw = getString(columnLabel!!)
        if (lastWasNull) {
            // SQL NULL → return zero
            return BigDecimal.ZERO
        }
        return try {
            BigDecimal(raw)
        } catch (ex: Exception) {
            throw SQLException("Cannot convert value '$raw' in column '$columnLabel' to BigDecimal", ex)
        }
    }

    override fun getRow(): Int = if (currentIndex < 0) 0 else currentIndex + 1

    override fun absolute(row: Int): Boolean = forwardOnlyError()

    override fun relative(rows: Int): Boolean = forwardOnlyError()

    override fun previous(): Boolean = forwardOnlyError()

    override fun setFetchDirection(direction: Int) {
        if (direction != ResultSet.FETCH_FORWARD)
            throw SQLException("Only FETCH_FORWARD is supported")
    }

    override fun getFetchDirection(): Int = ResultSet.FETCH_FORWARD

    override fun setFetchSize(rows: Int) {
        if (rows < 0) throw SQLException("fetchSize must be >= 0")
        fetchSize = rows
        logger.info("Fetch size set to {}", rows)
    }

    override fun getFetchSize(): Int = fetchSize

    override fun getType(): Int = ResultSet.TYPE_FORWARD_ONLY//throw UnsupportedOperationException("Not implemented 56")

// The ResultSet is read-only
override fun getConcurrency(): Int = ResultSet.CONCUR_READ_ONLY
// This is a forward-only, read-only ResultSet; no row update tracking
override fun rowUpdated(): Boolean = false
override fun rowInserted(): Boolean = false
override fun rowDeleted(): Boolean = false
// ResultSet is read-only; updates are not supported
override fun updateNull(columnIndex: Int) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateBoolean(columnIndex: Int, x: Boolean) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateByte(columnIndex: Int, x: Byte) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateShort(columnIndex: Int, x: Short) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateInt(columnIndex: Int, x: Int) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateLong(columnIndex: Int, x: Long) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}

    // All update operations are unsupported for this read-only ResultSet
    override fun updateFloat(columnIndex: Int, x: Float) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateDouble(columnIndex: Int, x: Double) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateString(columnIndex: Int, x: String?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateBytes(columnIndex: Int, x: ByteArray?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateDate(columnIndex: Int, x: java.sql.Date?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateTime(columnIndex: Int, x: Time?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateTimestamp(columnIndex: Int, x: Timestamp?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateAsciiStream(columnIndex: Int, x: java.io.InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateBinaryStream(columnIndex: Int, x: java.io.InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateObject(columnIndex: Int, x: Any?) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateNull(columnLabel: String) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateBoolean(columnLabel: String, x: Boolean) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateByte(columnLabel: String, x: Byte) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateShort(columnLabel: String, x: Short) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateInt(columnLabel: String, x: Int) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateLong(columnLabel: String, x: Long) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateFloat(columnLabel: String, x: Float) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateDouble(columnLabel: String, x: Double) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateBigDecimal(columnLabel: String, x: BigDecimal?) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateString(columnLabel: String, x: String?) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateBytes(columnLabel: String, x: ByteArray?) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateDate(columnLabel: String?, x: java.sql.Date?) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateTime(columnLabel: String, x: Time?) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateTimestamp(columnLabel: String, x: Timestamp?) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}
override fun updateAsciiStream(columnLabel: String, x: java.io.InputStream?, length: Int) {
    throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
}

    // All update-stream operations are unsupported for this read-only ResultSet
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateAsciiStream(columnLabel: String?, x: InputStream?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateBinaryStream(columnLabel: String, x: InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateBinaryStream(columnLabel: String?, x: InputStream?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Int) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateCharacterStream(columnIndex: Int, x: Reader?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateCharacterStream(columnLabel: String?, reader: Reader?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }

    override fun updateObject(columnLabel: String, x: Any?, scaleOrLength: Int) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateObject(columnLabel: String, x: Any?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun insertRow() {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – row insertion is not supported")
    }
    override fun updateRow() {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – row updates are not supported")
    }
    override fun deleteRow() {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – row deletion is not supported")
    }
    override fun refreshRow() {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – row refresh is not supported")
    }
    override fun cancelRowUpdates() {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – row update cancellation is not supported")
    }
    override fun moveToInsertRow() {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – moving to insert row is not supported")
    }
    override fun moveToCurrentRow() {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – moving to current row is not supported")
    }
    override fun getStatement(): Statement {
        return statement ?: throw SQLException("Statement not available for this ResultSet")
    }
    override fun getRef(columnIndex: Int): Ref {
        throw SQLFeatureNotSupportedException("Ref types are not supported in XmlResultSet")
    }

    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?): Any? {
        throw SQLFeatureNotSupportedException("getObject with type map is not supported")
    }
    override fun getObject(columnLabel: String?, map: MutableMap<String, Class<*>>?): Any? {
        throw SQLFeatureNotSupportedException("getObject with type map is not supported")
    }

    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>?): T {
        if (type == null) throw SQLException("Type must not be null")
        // Delegate to the existing untyped getObject
        val raw: Any? = getObject(columnIndex)
        if (raw == null) {
            // match JDBC spec: return null for SQL NULL
            @Suppress("UNCHECKED_CAST")
            return null as T
        }
        if (!type.isInstance(raw)) {
            throw SQLException("Cannot convert value '$raw' (${raw.javaClass.name}) to ${type.name}")
        }
        return type.cast(raw)
    }
    override fun <T : Any?> getObject(columnLabel: String?, type: Class<T>?): T {
        if (columnLabel == null) throw SQLException("Column label must not be null")
        if (type == null) throw SQLException("Type must not be null")
        // Delegate to existing untyped getObject by label
        val raw: Any? = getObject(columnLabel)
        if (raw == null) {
            @Suppress("UNCHECKED_CAST")
            return null as T
        }
        if (!type.isInstance(raw)) {
            throw SQLException("Cannot convert value '$raw' (${raw.javaClass.name}) to ${type.name}")
        }
        return type.cast(raw)
    }


    override fun findColumn(columnLabel: String?): Int {
        columnLabel ?: throw SQLException("Column label cannot be null")
        return colInfo[columnLabel.lowercase()]?.index1
            ?: throw SQLException("Column $columnLabel not found")
    }

    // REF types are not supported in this read-only ResultSet
    override fun getRef(columnLabel: String): Ref =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – REF not supported")

    // BLOB/CLOB not supported
    override fun getBlob(columnIndex: Int): Blob =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – BLOB not supported")
    override fun getBlob(columnLabel: String): Blob =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – BLOB not supported")
    override fun getClob(columnIndex: Int): Clob =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – CLOB not supported")
    override fun getClob(columnLabel: String): Clob =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – CLOB not supported")

    // ARRAY not supported
    override fun getArray(columnIndex: Int): java.sql.Array =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – ARRAY not supported")
    override fun getArray(columnLabel: String): java.sql.Array =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – ARRAY not supported")

    // Calendar overloads for Date/Time/Timestamp
    override fun getDate(columnIndex: Int, cal: Calendar?): java.sql.Date =
        getDate(columnIndex)
    override fun getDate(columnLabel: String, cal: Calendar?): java.sql.Date =
        getDate(columnLabel)
    override fun getTime(columnIndex: Int, cal: Calendar?): Time =
        getTime(columnIndex)
    override fun getTime(columnLabel: String, cal: Calendar?): Time =
        getTime(columnLabel)
    override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp =
        getTimestamp(columnIndex)
    override fun getTimestamp(columnLabel: String, cal: Calendar?): Timestamp =
        getTimestamp(columnLabel)

    // Parse string into URL or throw SQLException on failure
    override fun getURL(columnIndex: Int): URL {
        val s = getString(columns[columnIndex - 1].name)
        if (lastWasNull) return URL("")
        return try {
            URL(s)
        } catch (ex: MalformedURLException) {
            throw SQLException("Cannot convert value '$s' to URL", ex)
        }
    }
    override fun getURL(columnLabel: String): URL {
        val s = getString(columnLabel)
        if (lastWasNull) return URL("")
        return try {
            URL(s)
        } catch (ex: MalformedURLException) {
            throw SQLException("Cannot convert value '$s' to URL", ex)
        }
    }

    // All update and row-related methods for BLOB/CLOB/Array/Ref and row ops throw SQLFeatureNotSupportedException
    override fun updateRef(columnIndex: Int, x: Ref?) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    override fun updateRef(columnLabel: String?, x: Ref?) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    override fun updateBlob(columnIndex: Int, x: Blob?) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    override fun updateBlob(columnLabel: String?, x: Blob?) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    override fun updateBlob(columnLabel: String, inputStream: InputStream?, length: Long) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    override fun updateBlob(columnLabel: String, inputStream: InputStream?) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    override fun updateClob(columnIndex: Int, x: Clob?) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    override fun updateClob(columnLabel: String?, x: Clob?) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    override fun updateClob(columnLabel: String?, reader: Reader?, length: Long) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    override fun updateClob(columnIndex: Int, reader: Reader?) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    override fun updateClob(columnLabel: String?, reader: Reader?) =
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")

    // ARRAY updates are not supported on this read-only ResultSet
    override fun updateArray(columnIndex: Int, x: java.sql.Array?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateArray(columnLabel: String?, x: java.sql.Array?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    // RowId types not supported
    override fun getRowId(columnIndex: Int): RowId =
        throw SQLFeatureNotSupportedException("RowId not supported by XmlResultSet")
    override fun getRowId(columnLabel: String?): RowId =
        throw SQLFeatureNotSupportedException("RowId not supported by XmlResultSet")
    // RowId updates are not supported on this read-only ResultSet
    override fun updateRowId(columnIndex: Int, x: RowId?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateRowId(columnLabel: String?, x: RowId?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    // Cursor holdability: closes on commit by default
    override fun getHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT
    override fun isClosed(): Boolean = closed
    // NString updates are not supported on this read-only ResultSet
    override fun updateNString(columnIndex: Int, nString: String?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateNString(columnLabel: String?, nString: String?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    // NClob updates are not supported on this read-only ResultSet
    override fun updateNClob(columnIndex: Int, nClob: NClob?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateNClob(columnLabel: String?, nClob: NClob?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    // NClob stream updates are not supported
    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateNClob(columnIndex: Int, reader: Reader?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    override fun updateNClob(columnLabel: String?, reader: Reader?) {
        throw SQLFeatureNotSupportedException("XmlResultSet is read-only – update operations are not supported")
    }
    // NClob retrieval is not supported in this driver
    override fun getNClob(columnIndex: Int): NClob =
        throw SQLFeatureNotSupportedException("NClob not supported by XmlResultSet")
    override fun getNClob(columnLabel: String?): NClob =
        throw SQLFeatureNotSupportedException("NClob not supported by XmlResultSet")

    // SQLXML retrieval and update not supported
    override fun getSQLXML(columnIndex: Int): SQLXML =
        throw SQLFeatureNotSupportedException("SQLXML not supported by XmlResultSet")
    override fun getSQLXML(columnLabel: String?): SQLXML =
        throw SQLFeatureNotSupportedException("SQLXML not supported by XmlResultSet")
    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only – SQLXML updates not supported")
    }
    override fun updateSQLXML(columnLabel: String?, xmlObject: SQLXML?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only – SQLXML updates not supported")
    }

    // NString is treated the same as String, but must return a non-null String
    override fun getNString(columnIndex: Int): String {
        val s = getString(columnIndex)
        if (lastWasNull) return ""
        return s!!
    }
    override fun getNString(columnLabel: String?): String {
        val s = getString(columnLabel!!)
        if (lastWasNull) return ""
        return s!!
    }

    // NCharacterStream is treated like getCharacterStream
    override fun getNCharacterStream(columnIndex: Int): StringReader =
        getCharacterStream(columnIndex)
    override fun getNCharacterStream(columnLabel: String?): StringReader =
        getCharacterStream(columnLabel!!)

    // NCharacterStream updates are not supported in this read-only ResultSet
    override fun updateNCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only – update operations are not supported")
    }
    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only – update operations are not supported")
    }
    override fun updateNCharacterStream(columnIndex: Int, x: Reader?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only – update operations are not supported")
    }
    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?) {
        throw SQLFeatureNotSupportedException("ResultSet is read-only – update operations are not supported")
    }

    /**
     * Unwraps this object to the given interface if supported.
     */
    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        if (iface != null && iface.isAssignableFrom(this::class.java)) {
            @Suppress("UNCHECKED_CAST")
            return this as T
        }
        throw SQLFeatureNotSupportedException("XmlResultSet cannot unwrap to ${iface?.name}")
    }

    /**
     * Returns true if this object wraps an implementation of the given interface.
     */
    override fun isWrapperFor(iface: Class<*>?): Boolean =
        iface != null && iface.isAssignableFrom(this::class.java)
}