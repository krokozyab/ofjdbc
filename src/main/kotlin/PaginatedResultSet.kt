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
    private val fetchSize: Int,
    private val logger: Logger
) : ResultSet {

    // Accumulated rows (each row is a map of column names (lowercase) to values)
    private val rows: MutableList<Map<String, String>> = mutableListOf()
    private var currentIndex = -1
    private var currentOffset = 0
    // Indicates whether the last fetch returned a full page (and therefore more rows might exist).
    private var lastPageFull = true

    init {
        // Fetch the first page upon initialization.
        fetchNextPage()
    }

    /**
     * Rewrite a SELECT SQL query to include Oracle‑style OFFSET/FETCH pagination.
     */
    private fun rewriteQueryForPagination(originalSql: String, offset: Int, fetchSize: Int): String {
        if (fetchSize <= 0) return originalSql.trim()
        val trimmed = originalSql.trim().uppercase()
        if (!trimmed.startsWith("SELECT") || trimmed.contains("FETCH")) return originalSql
        // Basic check to ensure ORDER BY is placed correctly
        val orderByIndex = trimmed.indexOf("ORDER BY")
        return if (orderByIndex != -1) {
            val orderByClause = originalSql.substring(orderByIndex)
            originalSql.substring(0, orderByIndex) + " OFFSET $offset ROWS FETCH NEXT $fetchSize ROWS ONLY " + orderByClause
        } else {
            "$originalSql OFFSET $offset ROWS FETCH NEXT $fetchSize ROWS ONLY"
        }
    }

    /**
     * Fetch the next page of rows from the server and append them to [rows].
     */
    private fun fetchNextPage() {
        // Only fetch if the previous page was full (i.e. there might be more rows).
        if (!lastPageFull) return

        val effectiveSql = rewriteQueryForPagination(originalSql, currentOffset, fetchSize)
        logger.info("Fetching page: SQL='{}'", effectiveSql)
        val responseXml = sendSqlViaWsdl(wsdlEndpoint, effectiveSql, username, password, reportPath)
        val doc: Document = parseXml(responseXml)
        var nodeList: NodeList = doc.getElementsByTagName("ROW")
        if (nodeList.length == 0) {
            val resultNodes: NodeList = doc.getElementsByTagName("RESULT")
            if (resultNodes.length > 0) {
                val resultText = resultNodes.item(0).textContent.trim()
                val unescapedXml = StringEscapeUtils.unescapeXml(resultText)
                val rowDoc: Document = parseXml(unescapedXml)
                nodeList = rowDoc.getElementsByTagName("ROW")
            }
        }
        logger.info("Fetched {} rows.", nodeList.length)
        // Parse rows from the NodeList.
        val newRows = mutableListOf<Map<String, String>>()
        for (i in 0 until nodeList.length) {
            val node = nodeList.item(i)
            if (node.nodeType == Node.ELEMENT_NODE) {
                val rowMap = mutableMapOf<String, String>()
                val children = node.childNodes
                for (j in 0 until children.length) {
                    val child = children.item(j)
                    if (child.nodeType == Node.ELEMENT_NODE) {
                        rowMap[child.nodeName.lowercase()] = child.textContent.trim()
                    }
                }
                newRows.add(rowMap)
            }
        }
        // Update state.
        rows.addAll(newRows)
        // If the number of rows fetched is less than fetchSize, then no more rows exist.
        lastPageFull = newRows.size == fetchSize
        currentOffset += newRows.size
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

    override fun getString(columnLabel: String): String? =
        currentRow()[columnLabel.lowercase()]

    override fun getString(columnIndex: Int): String? {
        val meta = metaData
        val colName = meta.getColumnName(columnIndex)
        return getString(colName)
    }

    override fun getInt(columnLabel: String): Int =
        getString(columnLabel)?.toIntOrNull() ?: throw SQLException("Cannot convert value to int")

    override fun getInt(columnIndex: Int): Int =
        getString(columnIndex)?.toIntOrNull() ?: throw SQLException("Cannot convert value to int")

    // For simplicity, we implement getObject() as getString() here.
    //override fun getObject(columnLabel: String): Any? = getString(columnLabel)
    override fun getObject(columnLabel: String): Any? {
        val value = getString(columnLabel)
        return when {
            value == null -> null
            value.matches(Regex("^-?\\d+$")) -> value.toInt() // Integer
            value.matches(Regex("^-?\\d+\\.\\d+$")) -> value.toDouble() // Double
            value.matches(Regex("^-?\\d+\\.?\\d*[Ee][+-]?\\d+$")) -> value.toDouble()// Exponential Notation
            else -> value // String
        }
    }
    override fun getObject(columnIndex: Int): Any? = getString(columnIndex)

    override fun getMetaData(): ResultSetMetaData {
        // Use the keys from the first row (if any) as columns.
        val columns = if (rows.isNotEmpty()) rows[0].keys.toList() else emptyList()
        return object : ResultSetMetaData {
            override fun getColumnCount(): Int = columns.size
            override fun getColumnName(column: Int): String = columns[column - 1]
            override fun getColumnLabel(column: Int): String = getColumnName(column)
            override fun isAutoIncrement(column: Int): Boolean = false
            override fun isCaseSensitive(column: Int): Boolean = true
            override fun isSearchable(column: Int): Boolean = false
            override fun isCurrency(column: Int): Boolean = false
            override fun isNullable(column: Int): Int = ResultSetMetaData.columnNullable
            override fun isSigned(column: Int): Boolean = false
            override fun getColumnDisplaySize(column: Int): Int = 50
            override fun getColumnType(column: Int): Int = Types.VARCHAR
            override fun getColumnTypeName(column: Int): String = "VARCHAR"
            override fun getPrecision(column: Int): Int = 0
            override fun getScale(column: Int): Int = 0
            override fun getSchemaName(column: Int): String = ""
            override fun getTableName(column: Int): String = ""
            override fun getCatalogName(column: Int): String = ""
            override fun isReadOnly(column: Int): Boolean = true
            override fun isWritable(column: Int): Boolean = false
            override fun isDefinitelyWritable(column: Int): Boolean = false
            override fun getColumnClassName(column: Int): String = "java.lang.String"
            override fun <T : Any?> unwrap(iface: Class<T>?): T =
                throw SQLFeatureNotSupportedException("Not implemented 1")
            override fun isWrapperFor(iface: Class<*>?): Boolean = false
        }
    }

    override fun wasNull(): Boolean = false
    override fun close() { /* no-op */ }

    // --- Stub methods ---
    // (For brevity, the remaining methods throw UnsupportedOperationException.)
    override fun getBoolean(columnIndex: Int): Boolean = throw UnsupportedOperationException("Not implemented 2")
    override fun getBoolean(columnLabel: String?): Boolean = throw UnsupportedOperationException("Not implemented 3")
    override fun getByte(columnIndex: Int): Byte = throw UnsupportedOperationException("Not implemented 4")
    override fun getByte(columnLabel: String?): Byte = throw UnsupportedOperationException("Not implemented 5")
    override fun getShort(columnIndex: Int): Short = throw UnsupportedOperationException("Not implemented 6")
    override fun getShort(columnLabel: String?): Short = throw UnsupportedOperationException("Not implemented 7")
    //override fun getLong(columnIndex: Int): Long = throw UnsupportedOperationException("Not implemented 8")
    //override fun getLong(columnLabel: String?): Long = throw UnsupportedOperationException("Not implemented 9")
    override fun getLong(columnLabel: String): Long {
        val value = getString(columnLabel)
        return if (!value.isNullOrBlank()) {
            value.toLongOrNull() ?: 0L
        } else {
            0L
        }
    }

    override fun getLong(columnIndex: Int): Long {
        val meta = metaData
        val colName = meta.getColumnName(columnIndex)
        return getLong(colName)
    }

    //override fun getFloat(columnIndex: Int): Float = throw UnsupportedOperationException("Not implemented 10")
    //override fun getFloat(columnLabel: String?): Float = throw UnsupportedOperationException("Not implemented 11")
    override fun getFloat(columnLabel: String): Float =
        getString(columnLabel)?.toFloatOrNull() ?: throw SQLException("Cannot convert value to float")

    override fun getFloat(columnIndex: Int): Float {
        val meta = metaData
        val colName = meta.getColumnName(columnIndex)
        return getFloat(colName)
    }
    override fun getDouble(columnIndex: Int): Double = throw UnsupportedOperationException("Not implemented 12")
    override fun getDouble(columnLabel: String?): Double = throw UnsupportedOperationException("Not implemented 13")
    @Deprecated("Deprecated in Java")
    override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal = throw UnsupportedOperationException("Not implemented 14")
    @Deprecated("Deprecated in Java")
    override fun getBigDecimal(columnLabel: String, scale: Int): BigDecimal = throw UnsupportedOperationException("Not implemented 15")
    override fun getBytes(columnIndex: Int): ByteArray = throw UnsupportedOperationException("Not implemented 16")
    override fun getBytes(columnLabel: String?): ByteArray = throw UnsupportedOperationException("Not implemented 17")
    override fun getDate(columnIndex: Int): java.sql.Date = throw UnsupportedOperationException("Not implemented 18")
    override fun getDate(columnLabel: String?): java.sql.Date = throw UnsupportedOperationException("Not implemented 19")
    override fun getDate(columnIndex: Int, cal: Calendar?): Date {
        TODO("Not yet implemented")
    }

    override fun getDate(columnLabel: String?, cal: Calendar?): Date {
        TODO("Not yet implemented")
    }

    override fun getTime(columnIndex: Int): java.sql.Time = throw UnsupportedOperationException("Not implemented 20")
    override fun getTime(columnLabel: String?): java.sql.Time = throw UnsupportedOperationException("Not implemented 21")
    override fun getTime(columnIndex: Int, cal: Calendar?): Time {
        TODO("Not yet implemented")
    }

    override fun getTime(columnLabel: String?, cal: Calendar?): Time {
        TODO("Not yet implemented")
    }

    override fun getTimestamp(columnIndex: Int): java.sql.Timestamp = throw UnsupportedOperationException("Not implemented 22")
    override fun getTimestamp(columnLabel: String?): java.sql.Timestamp = throw UnsupportedOperationException("Not implemented 23")
    override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp {
        TODO("Not yet implemented")
    }

    override fun getTimestamp(columnLabel: String?, cal: Calendar?): Timestamp {
        TODO("Not yet implemented")
    }

    override fun getAsciiStream(columnIndex: Int): java.io.InputStream = throw UnsupportedOperationException("Not implemented 24")
    override fun getAsciiStream(columnLabel: String?): java.io.InputStream = throw UnsupportedOperationException("Not implemented 25")
    override fun getUnicodeStream(columnIndex: Int): java.io.InputStream = throw UnsupportedOperationException("Not implemented 26")
    override fun getUnicodeStream(columnLabel: String?): java.io.InputStream = throw UnsupportedOperationException("Not implemented 27")
    override fun getBinaryStream(columnIndex: Int): java.io.InputStream = throw UnsupportedOperationException("Not implemented 28")
    override fun getBinaryStream(columnLabel: String?): java.io.InputStream = throw UnsupportedOperationException("Not implemented 29")
    override fun getWarnings(): java.sql.SQLWarning? = null
    override fun clearWarnings() { /* no warnings to clear */ }
    override fun getCursorName(): String = throw UnsupportedOperationException("Not implemented 30")
    override fun getObject(columnLabel: String, map: MutableMap<String, Class<*>>?): Any = throw UnsupportedOperationException("Not implemented 31")
    //override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?): Any = throw UnsupportedOperationException("Not implemented 32")
    //override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>?): T = throw UnsupportedOperationException("Not implemented 33")
    //override fun <T : Any?> getObject(columnLabel: String, type: Class<T>?): T = throw UnsupportedOperationException("Not implemented 34")
    override fun getCharacterStream(columnIndex: Int): java.io.Reader = throw UnsupportedOperationException("Not implemented 35")
    override fun getCharacterStream(columnLabel: String): java.io.Reader = throw UnsupportedOperationException("Not implemented 36")
    override fun isBeforeFirst(): Boolean = throw UnsupportedOperationException("Not implemented 37")
    override fun isAfterLast(): Boolean = throw UnsupportedOperationException("Not implemented 38")
    override fun isFirst(): Boolean = throw UnsupportedOperationException("Not implemented 39")
    override fun isLast(): Boolean = throw UnsupportedOperationException("Not implemented 40")
    override fun beforeFirst() = throw UnsupportedOperationException("Not implemented 41")
    override fun afterLast() = throw UnsupportedOperationException("Not implemented 42")
    override fun first(): Boolean = throw UnsupportedOperationException("Not implemented 43")
    override fun last(): Boolean = throw UnsupportedOperationException("Not implemented 44")
    override fun getBigDecimal(columnIndex: Int): BigDecimal = throw UnsupportedOperationException("Not implemented 45")
    override fun getBigDecimal(columnLabel: String?): BigDecimal = throw UnsupportedOperationException("Not implemented 46")
    override fun getRow(): Int = throw UnsupportedOperationException("Not implemented 47")
    override fun absolute(row: Int): Boolean = throw UnsupportedOperationException("Not implemented 48")
    override fun relative(rows: Int): Boolean = throw UnsupportedOperationException("Not implemented 49")
    override fun previous(): Boolean = throw UnsupportedOperationException("Not implemented 50")
    override fun setFetchDirection(direction: Int) = throw UnsupportedOperationException("Not implemented 51")
    override fun getFetchDirection(): Int = throw UnsupportedOperationException("Not implemented 52")
    override fun setFetchSize(rows: Int) = throw UnsupportedOperationException("Not implemented 53")
    override fun getFetchSize(): Int = throw UnsupportedOperationException("Not implemented 54")
    override fun getType(): Int = ResultSet.TYPE_FORWARD_ONLY
    override fun getConcurrency(): Int = throw UnsupportedOperationException("Not implemented 55")
    override fun rowUpdated(): Boolean = throw UnsupportedOperationException("Not implemented 56")
    override fun rowInserted(): Boolean = throw UnsupportedOperationException("Not implemented 57")
    override fun rowDeleted(): Boolean = throw UnsupportedOperationException("Not implemented 58")
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
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented")
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
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?) {
        TODO("Not yet implemented")
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
        TODO("Not yet implemented")
    }

    override fun getBlob(columnIndex: Int): Blob {
        TODO("Not yet implemented")
    }

    override fun getBlob(columnLabel: String?): Blob {
        TODO("Not yet implemented")
    }

    override fun getClob(columnIndex: Int): Clob {
        TODO("Not yet implemented")
    }

    override fun getClob(columnLabel: String?): Clob {
        TODO("Not yet implemented")
    }

    override fun getArray(columnIndex: Int): Array {
        TODO("Not yet implemented")
    }

    override fun getArray(columnLabel: String?): Array {
        TODO("Not yet implemented")
    }

    override fun getURL(columnIndex: Int): URL {
        TODO("Not yet implemented")
    }

    override fun getURL(columnLabel: String?): URL {
        TODO("Not yet implemented")
    }

    override fun updateRef(columnIndex: Int, x: Ref?) {
        TODO("Not yet implemented")
    }

    override fun updateRef(columnLabel: String?, x: Ref?) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnIndex: Int, x: Blob?) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnLabel: String?, x: Blob?) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnIndex: Int, x: Clob?) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnLabel: String?, x: Clob?) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateArray(columnIndex: Int, x: Array?) {
        TODO("Not yet implemented")
    }

    override fun updateArray(columnLabel: String?, x: Array?) {
        TODO("Not yet implemented")
    }

    override fun getRowId(columnIndex: Int): RowId {
        TODO("Not yet implemented")
    }

    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?): Any = throw UnsupportedOperationException("Not implemented 103")
    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>?): T = throw UnsupportedOperationException("Not implemented 104")
    override fun <T : Any?> getObject(columnLabel: String, type: Class<T>?): T = throw UnsupportedOperationException("Not implemented 105")
    override fun findColumn(columnLabel: String?): Int = throw UnsupportedOperationException("Not implemented 106")
    override fun getRowId(columnLabel: String): java.sql.RowId = throw UnsupportedOperationException("Not implemented 107")
    override fun updateRowId(columnIndex: Int, x: java.sql.RowId?) = throw UnsupportedOperationException("Not implemented 108")
    override fun updateRowId(columnLabel: String, x: java.sql.RowId?) = throw UnsupportedOperationException("Not implemented 109")
    override fun getHoldability(): Int = throw UnsupportedOperationException("Not implemented 110")
    override fun isClosed(): Boolean = false
    override fun updateNString(columnIndex: Int, nString: String?) = throw UnsupportedOperationException("Not implemented 111")
    override fun updateNString(columnLabel: String?, nString: String?) = throw UnsupportedOperationException("Not implemented 112")
    override fun updateNClob(columnIndex: Int, nClob: java.sql.NClob?) = throw UnsupportedOperationException("Not implemented 113")
    override fun updateNClob(columnLabel: String?, nClob: java.sql.NClob?) = throw UnsupportedOperationException("Not implemented 114")
    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun getNClob(columnIndex: Int): java.sql.NClob = throw UnsupportedOperationException("Not implemented 115")
    override fun getNClob(columnLabel: String?): java.sql.NClob = throw UnsupportedOperationException("Not implemented 116")
    override fun getSQLXML(columnIndex: Int): java.sql.SQLXML = throw UnsupportedOperationException("Not implemented 117")
    override fun getSQLXML(columnLabel: String?): java.sql.SQLXML = throw UnsupportedOperationException("Not implemented 118")
    override fun updateSQLXML(columnIndex: Int, xmlObject: java.sql.SQLXML?) = throw UnsupportedOperationException("Not implemented 119")
    override fun updateSQLXML(columnLabel: String?, xmlObject: java.sql.SQLXML?) = throw UnsupportedOperationException("Not implemented 120")
    override fun getNString(columnIndex: Int): String = throw UnsupportedOperationException("Not implemented 121")
    override fun getNString(columnLabel: String?): String = throw UnsupportedOperationException("Not implemented 122")
    override fun getNCharacterStream(columnIndex: Int): java.io.Reader = throw UnsupportedOperationException("Not implemented 123")
    override fun getNCharacterStream(columnLabel: String?): java.io.Reader = throw UnsupportedOperationException("Not implemented 124")
    override fun updateNCharacterStream(columnIndex: Int, x: java.io.Reader?, length: Long) = throw UnsupportedOperationException("Not implemented 125")
    override fun updateNCharacterStream(columnLabel: String?, reader: java.io.Reader?, length: Long) = throw UnsupportedOperationException("Not implemented 126")
    override fun updateNCharacterStream(columnIndex: Int, x: java.io.Reader?) = throw UnsupportedOperationException("Not implemented 127")
    override fun updateNCharacterStream(columnLabel: String?, reader: java.io.Reader?) = throw UnsupportedOperationException("Not implemented 128")
    override fun <T : Any?> unwrap(iface: Class<T>?): T = throw UnsupportedOperationException("Not implemented 129")
    override fun isWrapperFor(iface: Class<*>?): Boolean = throw UnsupportedOperationException("Not implemented 130")
}
