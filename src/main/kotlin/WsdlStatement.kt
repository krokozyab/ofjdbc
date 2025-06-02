
package my.jdbc.wsdl_driver

import my.jdbc.wsdl_driver.XmlResultSet

import org.slf4j.LoggerFactory
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.Blob
import java.sql.Clob
import java.sql.Connection
import java.sql.Date
import java.sql.NClob
import java.sql.Ref
import java.sql.ResultSet
import java.sql.SQLWarning
import java.sql.Statement
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLException
import java.util.regex.Pattern
import java.util.regex.Matcher
import java.sql.ResultSetMetaData
import java.sql.RowId
import java.sql.SQLXML
import java.sql.Time
import java.sql.Timestamp
import java.sql.Types
import java.util.Calendar

open class WsdlStatement(
    private val wsdlEndpoint: String,
    private val username: String,
    private val password: String,
    private val reportPath: String
) : Statement {

    // Field to store the last ResultSet produced.
    private var lastResultSet: ResultSet? = null

    /** Tracks whether the statement has been closed. */
    private var closed = false

    /** Chain of SQLWarning objects, if any were generated. */
    private var warnings: SQLWarning? = null

    // Field to store the fetch size (page size).
    private var fetchSize: Int = 500

    /** Timeout in seconds for query execution. */
    private var queryTimeout: Int = 0

    /** Tracks the fetch direction for result sets. */
    private var fetchDirection: Int = ResultSet.FETCH_FORWARD


    private val logger = LoggerFactory.getLogger(WsdlStatement::class.java)


    override fun executeQuery(sql: String): ResultSet {
        // reset warnings for this execution
        warnings = null

        val trimmed = sql.trimStart()

        /* --------------------------------------------------------------------
           1.  DBeaver “row‑count” helper patterns
               a)  SELECT COUNT(*) FROM (SELECT * FROM <table>)  dbvrcnt [OFFSET …]
               b)  SELECT COUNT(*) FROM (SELECT COUNT(*) FROM <table>) dbvrcnt
           -------------------------------------------------------------------- */
        val outerCountRegex = Regex(
            """(?is)^\s*SELECT\s+COUNT\(\*\)\s+FROM\s+\(\s*SELECT\s+\*\s+FROM\s+([^\s\)]+)\s*\)\s+dbvrcnt\b"""
        )
        val nestedCountRegex = Regex(
            """(?is)^\s*SELECT\s+COUNT\(\*\)\s+FROM\s+\(\s*SELECT\s+COUNT\(\*\)\s+FROM\s+([^\s\)]+)\s*\)\s+dbvrcnt\b"""
        )

        // (a) SELECT COUNT(*) FROM (SELECT * FROM table) dbvrcnt …
        outerCountRegex.find(trimmed)?.let { m ->
            val table = m.groupValues[1]
            val countSql = "SELECT COUNT(*) FROM $table"
            val xml = sendSqlViaWsdl(wsdlEndpoint, countSql, username, password, reportPath)
            //logger.info("XML snippet:\n{}", xml.take(1024))
            val cnt = parseSingleCountFromXml(xml)
            //logger.info("Row count for [{}] = {}", table, cnt)
            return CountResultSet(cnt)
        }

        // (b) SELECT COUNT(*) FROM (SELECT COUNT(*) FROM table) dbvrcnt
        nestedCountRegex.find(trimmed)?.let { m ->
            val table = m.groupValues[1]
            val countSql = "SELECT COUNT(*) FROM $table"
            val xml = sendSqlViaWsdl(wsdlEndpoint, countSql, username, password, reportPath)
            //logger.info("XML snippet:\n{}", xml.take(1024))
            val cnt = parseSingleCountFromXml(xml)
            //logger.info("Row count for [{}] = {}", table, cnt)
            return CountResultSet(cnt)
        }

        /* --------------------------------------------------------------------
           2.  Plain COUNT(*) …   (strip trailing pagination if present)
           -------------------------------------------------------------------- */
        val plainCountRegex = Regex("""(?is)^\s*SELECT\s+COUNT\(\*\)""")
        if (plainCountRegex.containsMatchIn(trimmed)) {
            val cleanedSql = trimmed.replace(
                Regex("""(?is)\s+OFFSET\s+\d+\s+ROWS\s+FETCH\s+NEXT\s+\d+\s+ROWS\s+ONLY\s*$"""),
                ""
            ).trim()
            val xml = sendSqlViaWsdl(wsdlEndpoint, cleanedSql, username, password, reportPath)
            //logger.info("XML snippet:\n{}", xml.take(1024))
            val cnt = parseSingleCountFromXml(xml)
            //logger.info("Row count query executed -> {}", cnt)
            return CountResultSet(cnt)
        }

        /* --------------------------------------------------------------------
           3.  Everything else → normal paginated ResultSet
           -------------------------------------------------------------------- */
        val paginatedRs = PaginatedResultSet(
            originalSql = sql,
            wsdlEndpoint = wsdlEndpoint,
            username = username,
            password = password,
            reportPath = reportPath,
            fetchSize = fetchSize,
            logger = logger
        )
        lastResultSet = paginatedRs
        return paginatedRs
    }

    /* ---------------------------------------------------------
       Helper: extract COUNT value from the XML.  Handles both
       1) literal  <COUNT...>123</COUNT...>
       2) escaped &lt;COUNT...&gt;123&lt;/COUNT...&gt;
       --------------------------------------------------------- */
    private fun parseSingleCountFromXml(xml: String): Long {
        // 1️⃣ Try literal form first
        val literal = Pattern.compile(
            "<\\s*COUNT[^>]*>(\\d+)</\\s*COUNT[^>]*>",
            Pattern.CASE_INSENSITIVE
        ).matcher(xml)
        if (literal.find()) return literal.group(1).toLong()

        // 2️⃣ Fallback: encoded &lt;COUNT...&gt;123&lt;/COUNT...&gt;
        // Handles BI Publisher escaping: &lt;COUNT...&gt;123&lt;/COUNT...&gt;
        val encoded = Pattern.compile(
            "&lt;\\s*COUNT[^>]*&gt;(\\d+)&lt;/\\s*COUNT[^>]*&gt;",
            Pattern.CASE_INSENSITIVE
        ).matcher(xml)
        return if (encoded.find()) encoded.group(1).toLong() else 0L
    }

    /* ---------------------------------------------------------
       Tiny one‑row ResultSet returned for COUNT queries
       --------------------------------------------------------- */
    private inner class CountResultSet(private val countValue: Long) : ResultSet {

        private var seen = false

        override fun next(): Boolean {
            return if (!seen) { seen = true; true } else false
        }

        override fun getLong(columnIndex: Int): Long {
            if (columnIndex != 1) throw SQLException("Invalid column index")
            return countValue
        }
        override fun getLong(columnLabel: String?): Long = countValue
        override fun getInt(columnIndex: Int): Int = countValue.toInt()
        override fun getString(columnIndex: Int): String = countValue.toString()
        override fun getInt(columnLabel: String?): Int = countValue.toInt()
        override fun getString(columnLabel: String?): String = countValue.toString()
        override fun getObject(columnIndex: Int): Any = countValue
        override fun getObject(columnLabel: String?): Any = countValue
        override fun findColumn(columnLabel: String?): Int =
            if (columnLabel.equals("COUNT", true)) 1 else throw SQLException("No such column")

        override fun getCharacterStream(columnIndex: Int): Reader? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getCharacterStream(columnLabel: String?): Reader? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getMetaData(): ResultSetMetaData = object : ResultSetMetaData {
            override fun getColumnCount(): Int = 1
            override fun getColumnName(column: Int): String = "COUNT"
            override fun getColumnLabel(column: Int): String = "COUNT"
            override fun getColumnType(column: Int): Int = Types.BIGINT
            override fun getColumnTypeName(column: Int): String = "BIGINT"
            override fun getColumnClassName(column: Int): String = "java.lang.Long"
            override fun isNullable(column: Int): Int = ResultSetMetaData.columnNoNulls
            override fun isSigned(column: Int): Boolean = true
            override fun isCaseSensitive(column: Int): Boolean = false
            override fun isReadOnly(column: Int): Boolean = true
            override fun isAutoIncrement(column: Int): Boolean = false
            override fun getPrecision(column: Int): Int = 0
            override fun getScale(column: Int): Int = 0
            override fun getColumnDisplaySize(column: Int): Int = 20
            override fun getSchemaName(column: Int): String = ""
            override fun getTableName(column: Int): String = ""
            override fun getCatalogName(column: Int): String = ""
            override fun isCurrency(column: Int): Boolean = false
            override fun isSearchable(column: Int): Boolean = false
            override fun isDefinitelyWritable(column: Int): Boolean = false
            override fun isWritable(column: Int): Boolean = false
            override fun <T> unwrap(iface: Class<T>?): T = throw SQLException("Not a wrapper")
            override fun isWrapperFor(iface: Class<*>?): Boolean = false
        }

        override fun close() { /* no‑op */ }
        override fun wasNull(): Boolean = false

        /* All other ResultSet methods are unsupported for this tiny RS */
        override fun <T> unwrap(iface: Class<T>?): T = throw SQLFeatureNotSupportedException("Not supported")
        override fun isWrapperFor(iface: Class<*>?): Boolean = false
        override fun getBoolean(columnIndex: Int) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getBoolean(columnLabel: String?) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getByte(columnIndex: Int) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getByte(columnLabel: String?) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getShort(columnIndex: Int) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getShort(columnLabel: String?) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getFloat(columnIndex: Int) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getFloat(columnLabel: String?) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getDouble(columnIndex: Int) = throw SQLFeatureNotSupportedException("Not supported")
        @Deprecated("Deprecated in Java")
        override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getDouble(columnLabel: String?) = throw SQLFeatureNotSupportedException("Not supported")
        @Deprecated("Deprecated in Java")
        override fun getBigDecimal(columnLabel: String?, scale: Int): BigDecimal? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getBigDecimal(columnIndex: Int) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getBigDecimal(columnLabel: String?) = throw SQLFeatureNotSupportedException("Not supported")
        override fun isBeforeFirst(): Boolean {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun isAfterLast(): Boolean {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun isFirst(): Boolean {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun isLast(): Boolean {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun beforeFirst() {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun afterLast() {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun first(): Boolean {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun last(): Boolean {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getRow(): Int {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun absolute(row: Int): Boolean {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun relative(rows: Int): Boolean {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun previous(): Boolean {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun setFetchDirection(direction: Int) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getFetchDirection(): Int {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun setFetchSize(rows: Int) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getFetchSize(): Int {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getType(): Int {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getConcurrency(): Int {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun rowUpdated(): Boolean {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun rowInserted(): Boolean {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun rowDeleted(): Boolean {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNull(columnIndex: Int) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBoolean(columnIndex: Int, x: Boolean) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateByte(columnIndex: Int, x: Byte) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateShort(columnIndex: Int, x: Short) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateInt(columnIndex: Int, x: Int) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateLong(columnIndex: Int, x: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateFloat(columnIndex: Int, x: Float) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateDouble(columnIndex: Int, x: Double) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateString(columnIndex: Int, x: String?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBytes(columnIndex: Int, x: ByteArray?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateDate(columnIndex: Int, x: Date?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateTime(columnIndex: Int, x: Time?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateTimestamp(columnIndex: Int, x: Timestamp?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Int) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Int) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateObject(columnIndex: Int, x: Any?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNull(columnLabel: String?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBoolean(columnLabel: String?, x: Boolean) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateByte(columnLabel: String?, x: Byte) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateShort(columnLabel: String?, x: Short) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateInt(columnLabel: String?, x: Int) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateLong(columnLabel: String?, x: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateFloat(columnLabel: String?, x: Float) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateDouble(columnLabel: String?, x: Double) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBigDecimal(columnLabel: String?, x: BigDecimal?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateString(columnLabel: String?, x: String?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBytes(columnLabel: String?, x: ByteArray?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateDate(columnLabel: String?, x: Date?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateTime(columnLabel: String?, x: Time?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateTimestamp(columnLabel: String?, x: Timestamp?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Int) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Int) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Int) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateObject(columnLabel: String?, x: Any?, scaleOrLength: Int) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateObject(columnLabel: String?, x: Any?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun insertRow() {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateRow() {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun deleteRow() {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun refreshRow() {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun cancelRowUpdates() {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun moveToInsertRow() {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun moveToCurrentRow() {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getStatement(): Statement? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getObject(
            columnIndex: Int,
            map: Map<String?, Class<*>?>?
        ): Any? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getRef(columnIndex: Int): Ref? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getBlob(columnIndex: Int): Blob? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getClob(columnIndex: Int): Clob? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getArray(columnIndex: Int): java.sql.Array? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getObject(
            columnLabel: String?,
            map: Map<String?, Class<*>?>?
        ): Any? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getRef(columnLabel: String?): Ref? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getBlob(columnLabel: String?): Blob? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getClob(columnLabel: String?): Clob? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getArray(columnLabel: String?): java.sql.Array? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getDate(columnIndex: Int, cal: Calendar?): Date? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getDate(columnLabel: String?, cal: Calendar?): Date? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getTime(columnIndex: Int, cal: Calendar?): Time? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getTime(columnLabel: String?, cal: Calendar?): Time? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getTimestamp(columnLabel: String?, cal: Calendar?): Timestamp? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getURL(columnIndex: Int): URL? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getURL(columnLabel: String?): URL? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateRef(columnIndex: Int, x: Ref?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateRef(columnLabel: String?, x: Ref?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBlob(columnIndex: Int, x: Blob?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBlob(columnLabel: String?, x: Blob?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateClob(columnIndex: Int, x: Clob?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateClob(columnLabel: String?, x: Clob?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateArray(columnIndex: Int, x: java.sql.Array?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateArray(columnLabel: String?, x: java.sql.Array?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getRowId(columnIndex: Int): RowId? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getRowId(columnLabel: String?): RowId? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateRowId(columnIndex: Int, x: RowId?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateRowId(columnLabel: String?, x: RowId?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getHoldability(): Int {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun isClosed(): Boolean {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNString(columnIndex: Int, nString: String?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNString(columnLabel: String?, nString: String?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNClob(columnIndex: Int, nClob: NClob?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNClob(columnLabel: String?, nClob: NClob?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getNClob(columnIndex: Int): NClob? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getNClob(columnLabel: String?): NClob? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getSQLXML(columnIndex: Int): SQLXML? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getSQLXML(columnLabel: String?): SQLXML? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateSQLXML(columnLabel: String?, xmlObject: SQLXML?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getNString(columnIndex: Int): String? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getNString(columnLabel: String?): String? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getNCharacterStream(columnIndex: Int): Reader? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getNCharacterStream(columnLabel: String?): Reader? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBlob(columnLabel: String?, inputStream: InputStream?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateClob(columnLabel: String?, reader: Reader?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNCharacterStream(columnIndex: Int, x: Reader?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNCharacterStream(columnLabel: String?, reader: Reader?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateAsciiStream(columnIndex: Int, x: InputStream?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBinaryStream(columnIndex: Int, x: InputStream?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateCharacterStream(columnIndex: Int, x: Reader?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateAsciiStream(columnLabel: String?, x: InputStream?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBinaryStream(columnLabel: String?, x: InputStream?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateCharacterStream(columnLabel: String?, reader: Reader?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBlob(columnIndex: Int, inputStream: InputStream?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateBlob(columnLabel: String?, inputStream: InputStream?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateClob(columnIndex: Int, reader: Reader?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateClob(columnLabel: String?, reader: Reader?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNClob(columnIndex: Int, reader: Reader?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun updateNClob(columnLabel: String?, reader: Reader?) {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun <T : Any?> getObject(columnIndex: Int, type: Class<T?>?): T? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun <T : Any?> getObject(columnLabel: String?, type: Class<T?>?): T? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getBytes(columnIndex: Int) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getBytes(columnLabel: String?) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getDate(columnIndex: Int) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getDate(columnLabel: String?) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getTime(columnIndex: Int) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getTime(columnLabel: String?) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getTimestamp(columnIndex: Int) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getAsciiStream(columnIndex: Int): InputStream? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        @Deprecated("Deprecated in Java")
        override fun getUnicodeStream(columnIndex: Int): InputStream? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getBinaryStream(columnIndex: Int): InputStream? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getTimestamp(columnLabel: String?) = throw SQLFeatureNotSupportedException("Not supported")
        override fun getAsciiStream(columnLabel: String?): InputStream? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        @Deprecated("Deprecated in Java")
        override fun getUnicodeStream(columnLabel: String?): InputStream? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getBinaryStream(columnLabel: String?): InputStream? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getWarnings(): SQLWarning? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun clearWarnings() {
            throw SQLFeatureNotSupportedException("Not supported")
        }

        override fun getCursorName(): String? {
            throw SQLFeatureNotSupportedException("Not supported")
        }

    }

    override fun close() {
        if (!closed) {
            lastResultSet?.close()
            closed = true
            logger.info("Statement closed")
        }
    }

    // --- Stub implementations ---
    override fun setMaxRows(max: Int) { /* no-op */ }

    /**
     * Escape processing (JDBC {fn ...} escapes) is not supported.
     * This is a no-op to satisfy the Statement API.
     */
    override fun setEscapeProcessing(enable: Boolean) {
        // No escape processing support; ignore the setting.
        addWarning(SQLWarning("setEscapeProcessing($enable) called but not supported"))
        logger.info("Escape processing flag ignored: {}", enable)
    }

    override fun getMaxRows(): Int = 0

    override fun execute(sql: String): Boolean {
        // reset warnings for this execution
        warnings = null
        lastResultSet = executeQuery(sql)
        return true
    }

    // Statement does not support updates in this read-only driver
    override fun executeUpdate(sql: String): Int =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )

    override fun getWarnings(): SQLWarning? = warnings

    override fun clearWarnings() {
        warnings = null
    }

    /**
     * Named cursors are not supported in this read-only driver.
     * This method is a no-op but records a warning.
     */
    override fun setCursorName(name: String?) {
        addWarning(SQLWarning("setCursorName($name) called but not supported"))
        logger.info("setCursorName ignored: {}", name)
    }

    /**
     * Sets the number of seconds the driver will wait for a query to complete.
     * No actual cancellation is implemented, but the value is tracked.
     */
    override fun setQueryTimeout(seconds: Int) {
        validateOpen()
        queryTimeout = seconds
        logger.info("Query timeout set to {} seconds", seconds)
    }

    /**
     * Retrieves the current query timeout value in seconds.
     */
    override fun getQueryTimeout(): Int = queryTimeout

    /**
     * Statement cancellation is not supported in this driver.
     */
    override fun cancel() {
        addWarning(SQLWarning("cancel() called but not supported"))
        logger.info("Cancel request ignored")
    }

    override fun getResultSet(): ResultSet? = lastResultSet

    override fun getUpdateCount(): Int = -1

    /**
     * Since this driver only returns a single ResultSet, always close the current one
     * and signal that there are no further results.
     */
    override fun getMoreResults(): Boolean {
        validateOpen()
        lastResultSet?.close()
        lastResultSet = null
        return false
    }

    override fun setFetchDirection(direction: Int) {
        validateOpen()
        if (direction != ResultSet.FETCH_FORWARD) {
            throw SQLFeatureNotSupportedException(
                "WsdlStatement only supports FETCH_FORWARD direction"
            )
        }
        fetchDirection = direction
        logger.info("Fetch direction set to FETCH_FORWARD")
    }

    override fun getFetchDirection(): Int {
        validateOpen()
        return fetchDirection
    }
    override fun setFetchSize(rows: Int) {
        fetchSize = rows
        logger.info("Fetch size set to {}", rows)
    }
    override fun getFetchSize(): Int = fetchSize

// The ResultSet is read-only
override fun getResultSetConcurrency(): Int = ResultSet.CONCUR_READ_ONLY

// The ResultSet is forward-only
override fun getResultSetType(): Int = ResultSet.TYPE_FORWARD_ONLY

// Batch updates are not supported in this read-only driver
override fun addBatch(sql: String) =
    throw SQLFeatureNotSupportedException(
        "WsdlStatement is read-only – batch updates not supported"
    )

// Batch updates are not supported; clearBatch is a no-op
override fun clearBatch() =
    throw SQLFeatureNotSupportedException(
        "WsdlStatement is read-only – batch updates not supported"
    )

// Batch execution is not supported in this read-only driver
override fun executeBatch(): IntArray =
    throw SQLFeatureNotSupportedException(
        "WsdlStatement is read-only – batch updates not supported"
    )

// Retrieving the connection is not supported by this Statement
override fun getConnection(): Connection =
    throw SQLFeatureNotSupportedException(
        "WsdlStatement cannot return the underlying Connection"
    )
    /**
     * No additional result sets; always return false.
     */
    override fun getMoreResults(current: Int): Boolean {
        validateOpen()
        lastResultSet?.close()
        lastResultSet = null
        return false
    }
    // No generated keys for read-only statements; return an empty result set.
    override fun getGeneratedKeys(): ResultSet = XmlResultSet(emptyList())
    // Updates not supported in read-only driver
    override fun executeUpdate(sql: String, autoGeneratedKeys: Int): Int =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )
    // Updates not supported in read-only driver
    override fun executeUpdate(sql: String, columnIndexes: IntArray): Int =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )
    // Updates not supported in read-only driver
    override fun executeUpdate(sql: String, columnNames: Array<out String>?): Int =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )
    // No maximum field size enforced; return 0
    override fun getMaxFieldSize(): Int = 0
    // Field size limit not supported; ignore
    override fun setMaxFieldSize(max: Int) { /* no-op */ }
    // Only SELECT statements are supported
    override fun execute(sql: String, autoGeneratedKeys: Int): Boolean =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )
    // Only SELECT statements are supported
    override fun execute(sql: String, columnIndexes: IntArray): Boolean =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )
    // Only SELECT statements are supported
    override fun execute(sql: String, columnNames: Array<out String>?): Boolean =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )
    // The driver keeps cursors open across commit
    override fun getResultSetHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT



    /** Throws SQLException if the statement is closed. */
    private fun validateOpen() {
        if (closed) throw SQLException("Statement is closed")
    }

    /**
     * Add a non-fatal SQLWarning to the warning chain.
     */
    protected fun addWarning(warning: SQLWarning) {
        if (warnings == null) {
            warnings = warning
        } else {
            warnings!!.setNextWarning(warning)
        }
    }

    override fun isClosed(): Boolean = closed

/**
 * This driver does not support statement pooling; ignore this setting.
 */
override fun setPoolable(poolable: Boolean) {
    addWarning(SQLWarning("setPoolable($poolable) called but not supported"))
    logger.info("setPoolable ignored: {}", poolable)
}
/**
 * Always false: pooling is not supported.
 */
override fun isPoolable(): Boolean = false
/**
 * closeOnCompletion is not supported; ignore it.
 */
override fun closeOnCompletion() {
    addWarning(SQLWarning("closeOnCompletion() called but not supported"))
    logger.info("closeOnCompletion ignored")
}
/**
 * Returns false: closeOnCompletion not supported.
 */
override fun isCloseOnCompletion(): Boolean = false
/**
 * Allows unwrapping to the driver’s statement class.
 */
override fun <T : Any?> unwrap(iface: Class<T>): T =
    if (iface.isAssignableFrom(javaClass)) iface.cast(this)
    else throw SQLFeatureNotSupportedException(
        "WsdlStatement cannot unwrap to ${iface.name}"
    )
/**
 * Indicates whether this is a wrapper for the given interface.
 */
override fun isWrapperFor(iface: Class<*>): Boolean =
    iface.isAssignableFrom(javaClass)
}
