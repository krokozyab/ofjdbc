package my.jdbc.wsdl_driver

import java.io.InputStream
import java.io.Reader
import java.io.StringReader
import java.io.Closeable
import java.math.BigDecimal
import java.sql.*
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.*
import org.slf4j.LoggerFactory

data class ColumnMetadata(val name: String, val type: Int, val typeName: String)

/**
 * @param rows the result rows (will be cleared after close)
 * @param resource optional underlying resource (InputStream, Reader, etc.) that should be closed with this ResultSet
 * @param columns column metadata; if not provided, inferred from rows
 */
class XmlResultSet(
    rows: List<Map<String, String>>,
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
    private val rows: MutableList<Map<String, String>> = rows.toMutableList()
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

    private fun currentRow(): Map<String, String> {
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

    //override fun getInt(columnLabel: String): Int =
    //    getString(columnLabel)?.toIntOrNull() ?: throw SQLException("Cannot convert value to int")
    // Instead of throwing an exception if conversion fails, return 0.
    override fun getInt(columnLabel: String): Int {
        val str = getString(columnLabel)   // sets lastWasNull
        if (lastWasNull) return 0
        return str!!.toIntOrNull()
            ?: throw SQLException("Cannot convert value '$str' in column '$columnLabel' to INT")
    }
    //override fun getObject(columnLabel: String): Any? = getString(columnLabel)

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
    override fun getByte(columnIndex: Int): Byte = throw UnsupportedOperationException("Not implemented 4")
    override fun getByte(columnLabel: String?): Byte = throw UnsupportedOperationException("Not implemented 5")
    override fun getShort(columnIndex: Int): Short = throw UnsupportedOperationException("Not implemented 6")
    override fun getShort(columnLabel: String?): Short = throw UnsupportedOperationException("Not implemented 7")
    //override fun getInt(columnIndex: Int): Int = throw UnsupportedOperationException("Not implemented 8")
    override fun getInt(columnIndex: Int): Int =
        getInt(columns[columnIndex - 1].name)
    //override fun getLong(columnIndex: Int): Long = throw UnsupportedOperationException("Not implemented 9")
    //override fun getLong(columnLabel: String?): Long = throw UnsupportedOperationException("Not implemented 10")

    override fun getLong(columnLabel: String): Long {
        val str = getString(columnLabel)
        if (lastWasNull) return 0L
        return str!!.toLongOrNull()
            ?: throw SQLException("Cannot convert value '$str' in column '$columnLabel' to LONG")
    }

    override fun getLong(columnIndex: Int): Long =
        getLong(columns[columnIndex - 1].name)


    override fun getFloat(columnIndex: Int): Float = throw UnsupportedOperationException("Not implemented 11")
    override fun getFloat(columnLabel: String?): Float = throw UnsupportedOperationException("Not implemented 12")
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
        TODO("Not yet implemented 59")
    }

    @Deprecated("Deprecated in Java")
    override fun getBigDecimal(columnLabel: String, scale: Int): BigDecimal = throw UnsupportedOperationException("Not implemented 15")
    override fun getBytes(columnIndex: Int): ByteArray = throw UnsupportedOperationException("Not implemented 16")
    override fun getBytes(columnLabel: String?): ByteArray = throw UnsupportedOperationException("Not implemented 17")
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
    override fun getTime(columnIndex: Int): Time = throw UnsupportedOperationException("Not implemented 20")
    override fun getTime(columnLabel: String?): Time = throw UnsupportedOperationException("Not implemented 21")
    override fun getTimestamp(columnIndex: Int): Timestamp = throw UnsupportedOperationException("Not implemented 22")
    override fun getTimestamp(columnLabel: String?): Timestamp = throw UnsupportedOperationException("Not implemented 23")
    override fun getAsciiStream(columnIndex: Int): java.io.InputStream = throw UnsupportedOperationException("Not implemented 24")
    override fun getAsciiStream(columnLabel: String?): java.io.InputStream = throw UnsupportedOperationException("Not implemented 25")
    @Deprecated("Deprecated in Java")
    override fun getUnicodeStream(columnIndex: Int): java.io.InputStream = throw UnsupportedOperationException("Not implemented 26")
    @Deprecated("Deprecated in Java")
    override fun getUnicodeStream(columnLabel: String?): java.io.InputStream = throw UnsupportedOperationException("Not implemented 27")
    override fun getBinaryStream(columnIndex: Int): java.io.InputStream = throw UnsupportedOperationException("Not implemented 28")
    override fun getBinaryStream(columnLabel: String?): java.io.InputStream = throw UnsupportedOperationException("Not implemented 29")
    //override fun getWarnings(): SQLWarning? = throw UnsupportedOperationException("Not implemented 30")
    override fun getWarnings(): SQLWarning? = warnings
    //override fun clearWarnings() = throw UnsupportedOperationException("Not implemented 31")
    override fun clearWarnings() {
        warnings = null
    }
    override fun getCursorName(): String = throw UnsupportedOperationException("Not implemented 32")
    //override fun getObject(columnLabel: String): Any? = getString(columnLabel)
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

    // Overloads that take a mapping parameter delegate to the basic methods.
    /*override fun getObject(columnLabel: String, map: MutableMap<String, Class<*>>): Any? {
        return getObject(columnLabel)
    }

    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>): Any? {
        return getObject(columnIndex)
    }
    */
    //override fun findColumn(columnLabel: String): Int = throw UnsupportedOperationException("Not implemented 35")
    override fun getCharacterStream(columnIndex: Int): StringReader = throw UnsupportedOperationException("Not implemented 36")
    override fun getCharacterStream(columnLabel: String): StringReader = throw UnsupportedOperationException("Not implemented 37")
    // Forward-only navigation and position checks
    override fun isBeforeFirst(): Boolean = currentIndex < 0
    override fun isAfterLast(): Boolean = currentIndex >= rows.size
    override fun isFirst(): Boolean = currentIndex == 0 && rows.isNotEmpty()
    override fun isLast(): Boolean = currentIndex == rows.size - 1 && rows.isNotEmpty()
    override fun beforeFirst() = forwardOnlyError()
    override fun afterLast() = forwardOnlyError()
    override fun first(): Boolean = forwardOnlyError()
    override fun last(): Boolean = forwardOnlyError()
    override fun getBigDecimal(columnIndex: Int): BigDecimal = throw UnsupportedOperationException("Not implemented 46")
    override fun getBigDecimal(columnLabel: String?): BigDecimal = throw UnsupportedOperationException("Not implemented 47")
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
    override fun getConcurrency(): Int = throw UnsupportedOperationException("Not implemented 57")
    override fun rowUpdated(): Boolean = throw UnsupportedOperationException("Not implemented 58")
    override fun rowInserted(): Boolean = throw UnsupportedOperationException("Not implemented 59")
    override fun rowDeleted(): Boolean = throw UnsupportedOperationException("Not implemented 60")
    override fun updateNull(columnIndex: Int) = throw UnsupportedOperationException("Not implemented 61")
    override fun updateBoolean(columnIndex: Int, x: Boolean) = throw UnsupportedOperationException("Not implemented 62")
    override fun updateByte(columnIndex: Int, x: Byte) = throw UnsupportedOperationException("Not implemented 63")
    override fun updateShort(columnIndex: Int, x: Short) = throw UnsupportedOperationException("Not implemented 64")
    override fun updateInt(columnIndex: Int, x: Int) = throw UnsupportedOperationException("Not implemented 65")
    override fun updateLong(columnIndex: Int, x: Long) = throw UnsupportedOperationException("Not implemented 66")
    override fun updateFloat(columnIndex: Int, x: Float) = throw UnsupportedOperationException("Not implemented 67")
    override fun updateDouble(columnIndex: Int, x: Double) = throw UnsupportedOperationException("Not implemented 68")
    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) = throw UnsupportedOperationException("Not implemented 69")
    override fun updateString(columnIndex: Int, x: String?) = throw UnsupportedOperationException("Not implemented 70")
    override fun updateBytes(columnIndex: Int, x: ByteArray?) = throw UnsupportedOperationException("Not implemented 71")
    override fun updateDate(columnIndex: Int, x: java.sql.Date?) = throw UnsupportedOperationException("Not implemented 72")
    override fun updateTime(columnIndex: Int, x: Time?) = throw UnsupportedOperationException("Not implemented 73")
    override fun updateTimestamp(columnIndex: Int, x: Timestamp?) = throw UnsupportedOperationException("Not implemented 74")
    override fun updateAsciiStream(columnIndex: Int, x: java.io.InputStream?, length: Int) = throw UnsupportedOperationException("Not implemented 75")
    override fun updateBinaryStream(columnIndex: Int, x: java.io.InputStream?, length: Int) = throw UnsupportedOperationException("Not implemented 76")
    //override fun updateCharacterStream(columnIndex: Int, x: Reader?) = throw UnsupportedOperationException("Not implemented 77")
    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) = throw UnsupportedOperationException("Not implemented 78")
    override fun updateObject(columnIndex: Int, x: Any?) = throw UnsupportedOperationException("Not implemented 79")
    override fun updateNull(columnLabel: String) = throw UnsupportedOperationException("Not implemented 80")
    override fun updateBoolean(columnLabel: String, x: Boolean) = throw UnsupportedOperationException("Not implemented 81")
    override fun updateByte(columnLabel: String, x: Byte) = throw UnsupportedOperationException("Not implemented 82")
    override fun updateShort(columnLabel: String, x: Short) = throw UnsupportedOperationException("Not implemented 83")
    override fun updateInt(columnLabel: String, x: Int) = throw UnsupportedOperationException("Not implemented 84")
    override fun updateLong(columnLabel: String, x: Long) = throw UnsupportedOperationException("Not implemented 85")
    override fun updateFloat(columnLabel: String, x: Float) = throw UnsupportedOperationException("Not implemented 86")
    override fun updateDouble(columnLabel: String, x: Double) = throw UnsupportedOperationException("Not implemented 87")
    override fun updateBigDecimal(columnLabel: String, x: BigDecimal?) = throw UnsupportedOperationException("Not implemented 88")
    override fun updateString(columnLabel: String, x: String?) = throw UnsupportedOperationException("Not implemented 89")
    override fun updateBytes(columnLabel: String, x: ByteArray?) = throw UnsupportedOperationException("Not implemented 90")
    override fun updateDate(columnLabel: String?, x: java.sql.Date?) = throw UnsupportedOperationException("Not implemented 91")
    override fun updateTime(columnLabel: String, x: Time?) = throw UnsupportedOperationException("Not implemented 92")
    override fun updateTimestamp(columnLabel: String, x: Timestamp?) = throw UnsupportedOperationException("Not implemented 93")
    override fun updateAsciiStream(columnLabel: String, x: java.io.InputStream?, length: Int) = throw UnsupportedOperationException("Not implemented 94")
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) {
        TODO("Not yet implemented 60")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Long) {
        TODO("Not yet implemented 61")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) {
        TODO("Not yet implemented 62")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?) {
        TODO("Not yet implemented 63")
    }

    override fun updateBinaryStream(columnLabel: String, x: java.io.InputStream?, length: Int) = throw UnsupportedOperationException("Not implemented 95")
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) {
        TODO("Not yet implemented 64")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Long) {
        TODO("Not yet implemented 65")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) {
        TODO("Not yet implemented 66")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?) {
        TODO("Not yet implemented 67")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) {
        TODO("Not yet implemented 68")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Int) {
        TODO("Not yet implemented 69")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        TODO("Not yet implemented 70")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented 71")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?) {
        TODO("Not yet implemented 72")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented 73")
    }

    //override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Int) = throw UnsupportedOperationException("Not implemented 96")
    override fun updateObject(columnLabel: String, x: Any?, scaleOrLength: Int) = throw UnsupportedOperationException("Not implemented 97")
    override fun updateObject(columnLabel: String, x: Any?) = throw UnsupportedOperationException("Not implemented 98")
    override fun insertRow() = throw UnsupportedOperationException("Not implemented 99")
    override fun updateRow() = throw UnsupportedOperationException("Not implemented 100")
    override fun deleteRow() = throw UnsupportedOperationException("Not implemented 101")
    override fun refreshRow() = throw UnsupportedOperationException("Not implemented 102")
    override fun cancelRowUpdates() = throw UnsupportedOperationException("Not implemented 103")
    override fun moveToInsertRow() = throw UnsupportedOperationException("Not implemented 104")
    override fun moveToCurrentRow() = throw UnsupportedOperationException("Not implemented 105")
    //override fun getStatement(): Statement = throw UnsupportedOperationException("Not implemented 106")
    override fun getStatement(): Statement {
        return statement ?: throw SQLException("Statement not available for this ResultSet")
    }
    override fun getRef(columnIndex: Int): Ref {
        TODO("Not yet implemented 74")
    }

    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?): Any = throw UnsupportedOperationException("Not implemented 107")
    override fun getObject(columnLabel: String?, map: MutableMap<String, Class<*>>?): Any {
        TODO("Not yet implemented 75")
    }

    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>?): T = throw UnsupportedOperationException("Not implemented 108")
    override fun <T : Any?> getObject(columnLabel: String?, type: Class<T>?): T = throw UnsupportedOperationException("Not implemented 109")
    override fun findColumn(columnLabel: String?): Int {
        columnLabel ?: throw SQLException("Column label cannot be null")
        return colInfo[columnLabel.lowercase()]?.index1
            ?: throw SQLException("Column $columnLabel not found")
    }

    //override fun findColumn(columnLabel: String?): Int = throw UnsupportedOperationException("Not implemented 110")
    override fun getRef(columnLabel: String): Ref = throw UnsupportedOperationException("Not implemented 111")
    override fun getBlob(columnIndex: Int): Blob {
        TODO("Not yet implemented 77")
    }

    override fun getBlob(columnLabel: String): Blob = throw UnsupportedOperationException("Not implemented 112")
    override fun getClob(columnIndex: Int): Clob {
        TODO("Not yet implemented 78")
    }

    override fun getClob(columnLabel: String): Clob = throw UnsupportedOperationException("Not implemented 113")
    override fun getArray(columnIndex: Int): java.sql.Array {
        TODO("Not yet implemented 79")
    }

    override fun getArray(columnLabel: String): java.sql.Array = throw UnsupportedOperationException("Not implemented 114")
    override fun getDate(columnIndex: Int, cal: Calendar?): java.sql.Date = throw UnsupportedOperationException("Not implemented 115")
    override fun getDate(columnLabel: String, cal: Calendar?): java.sql.Date = throw UnsupportedOperationException("Not implemented 116")
    override fun getTime(columnIndex: Int, cal: Calendar?): Time = throw UnsupportedOperationException("Not implemented 117")
    override fun getTime(columnLabel: String, cal: Calendar?): Time = throw UnsupportedOperationException("Not implemented 118")
    override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp = throw UnsupportedOperationException("Not implemented 119")
    override fun getTimestamp(columnLabel: String, cal: Calendar?): Timestamp = throw UnsupportedOperationException("Not implemented 120")
    override fun getURL(columnIndex: Int): java.net.URL = throw UnsupportedOperationException("Not implemented 121")
    override fun getURL(columnLabel: String): java.net.URL = throw UnsupportedOperationException("Not implemented 122")
    override fun updateRef(columnIndex: Int, x: Ref?) = throw UnsupportedOperationException("Not implemented 123")
    override fun updateRef(columnLabel: String?, x: Ref?) = throw UnsupportedOperationException("Not implemented 124")
    override fun updateBlob(columnIndex: Int, x: Blob?) = throw UnsupportedOperationException("Not implemented 125")
    override fun updateBlob(columnLabel: String?, x: Blob?) = throw UnsupportedOperationException("Not implemented 126")
    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) = throw UnsupportedOperationException("Not implemented 127")
    override fun updateBlob(columnLabel: String, inputStream: InputStream?, length: Long) = throw UnsupportedOperationException("Not implemented 128")
    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) = throw UnsupportedOperationException("Not implemented 129")
    override fun updateBlob(columnLabel: String, inputStream: InputStream?) = throw UnsupportedOperationException("Not implemented 130")
    override fun updateClob(columnIndex: Int, x: Clob?) = throw UnsupportedOperationException("Not implemented 131")
    override fun updateClob(columnLabel: String?, x: Clob?) = throw UnsupportedOperationException("Not implemented 132")
    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) {
        TODO("Not yet implemented 80")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented 81")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?) {
        TODO("Not yet implemented 82")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented 83")
    }

    //override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) = throw UnsupportedOperationException("Not implemented 133")
    //override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) = throw UnsupportedOperationException("Not implemented 134")
    //override fun updateClob(columnIndex: Int, reader: Reader?) = throw UnsupportedOperationException("Not implemented 135")
    //override fun updateClob(columnLabel: String?, reader: Reader?) = throw UnsupportedOperationException("Not implemented 136")
    override fun updateArray(columnIndex: Int, x: java.sql.Array?) = throw UnsupportedOperationException("Not implemented 137")
    override fun updateArray(columnLabel: String?, x: java.sql.Array?) = throw UnsupportedOperationException("Not implemented 138")
    override fun getRowId(columnIndex: Int): RowId = throw UnsupportedOperationException("Not implemented 139")
    override fun getRowId(columnLabel: String?): RowId = throw UnsupportedOperationException("Not implemented 140")
    override fun updateRowId(columnIndex: Int, x: RowId?) = throw UnsupportedOperationException("Not implemented 141")
    override fun updateRowId(columnLabel: String?, x: RowId?) = throw UnsupportedOperationException("Not implemented 142")
    override fun getHoldability(): Int = throw UnsupportedOperationException("Not implemented 143")
    override fun isClosed(): Boolean = closed
    override fun updateNString(columnIndex: Int, nString: String?) = throw UnsupportedOperationException("Not implemented 144")
    override fun updateNString(columnLabel: String?, nString: String?) = throw UnsupportedOperationException("Not implemented 145")
    override fun updateNClob(columnIndex: Int, nClob: NClob?) = throw UnsupportedOperationException("Not implemented 146")
    override fun updateNClob(columnLabel: String?, nClob: NClob?) = throw UnsupportedOperationException("Not implemented 147")
    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) {
        TODO("Not yet implemented 84")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented 85")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?) {
        TODO("Not yet implemented 86")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented 87")
    }

    //override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) = throw UnsupportedOperationException("Not implemented 148")
    //override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) = throw UnsupportedOperationException("Not implemented 149")
    //override fun updateNClob(columnLabel: String?, reader: Reader?) = throw UnsupportedOperationException("Not implemented 150")
    //override fun updateNClob(columnIndex: Int, reader: Reader?) = throw UnsupportedOperationException("Not implemented 151")
    override fun getNClob(columnIndex: Int): NClob = throw UnsupportedOperationException("Not implemented 152")
    override fun getNClob(columnLabel: String?): NClob = throw UnsupportedOperationException("Not implemented 153")
    override fun getSQLXML(columnIndex: Int): SQLXML = throw UnsupportedOperationException("Not implemented 154")
    override fun getSQLXML(columnLabel: String?): SQLXML = throw UnsupportedOperationException("Not implemented 155")
    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML?) = throw UnsupportedOperationException("Not implemented 156")
    override fun updateSQLXML(columnLabel: String?, xmlObject: SQLXML?) = throw UnsupportedOperationException("Not implemented 157")
    override fun getNString(columnIndex: Int): String = throw UnsupportedOperationException("Not implemented 158")
    override fun getNString(columnLabel: String?): String = throw UnsupportedOperationException("Not implemented 159")
    override fun getNCharacterStream(columnIndex: Int): StringReader = throw UnsupportedOperationException("Not implemented 160")
    override fun getNCharacterStream(columnLabel: String?): StringReader = throw UnsupportedOperationException("Not implemented 161")
    override fun updateNCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        TODO("Not yet implemented 88")
    }

    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented 89")
    }

    override fun updateNCharacterStream(columnIndex: Int, x: Reader?) {
        TODO("Not yet implemented 90")
    }

    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented 91")
    }

    //override fun updateNCharacterStream(columnIndex: Int, x: Reader?) = throw UnsupportedOperationException("Not implemented 162")
    //override fun updateNCharacterStream(columnLabel: String?, reader: StringReader?, length: Long) = throw UnsupportedOperationException("Not implemented 163")
    //override fun updateNCharacterStream(columnIndex: Int, x: StringReader?) = throw UnsupportedOperationException("Not implemented 164")
    //override fun updateNCharacterStream(columnLabel: String?, reader: StringReader?) = throw UnsupportedOperationException("Not implemented 165")
    override fun <T : Any?> unwrap(iface: Class<T>?): T = throw UnsupportedOperationException("Not implemented 166")
    override fun isWrapperFor(iface: Class<*>?): Boolean = throw UnsupportedOperationException("Not implemented 167")
}