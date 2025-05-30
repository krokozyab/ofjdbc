package my.jdbc.wsdl_driver

import org.slf4j.LoggerFactory
import java.sql.SQLException
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.util.*

/**
 * A minimal PreparedStatement implementation with parameter binding.
 * It uses a simple substitution mechanism to replace each '?' in the SQL
 * with the corresponding parameter value.
 */
class WsdlPreparedStatement(
    private val sql: String,
    wsdlEndpoint: String,
    username: String,
    password: String,
    reportPath: String
) : WsdlStatement(wsdlEndpoint, username, password, reportPath), PreparedStatement {

    private val logger = LoggerFactory.getLogger(WsdlPreparedStatement::class.java)
    // Cache for ResultSetMetaData to avoid repeated calls
    private var cachedMeta: ResultSetMetaData? = null
    // Store parameters by their 1-indexed position.
    private val parameters = mutableMapOf<Int, Any?>()

    /**
     * Ensures the JDBC parameter index is 1-based and valid.
     */
    private fun validate(index: Int) {
        if (index < 1) {
            throw SQLException("Parameter index must be >= 1: got $index")
        }
    }

    override fun setFetchSize(rows: Int) = super.setFetchSize(rows)

    override fun getFetchSize(): Int     = super.getFetchSize()

    // Build final SQL string by substituting each '?' with its parameter value.
    private fun buildSql(): String {
        var finalSql = sql
        // Count number of '?' in the SQL.
        val paramCount = sql.count { it == '?' }
        for (i in 1..paramCount) {
            val paramValue = parameters[i] ?: "NULL"
            val formattedValue = when (paramValue) {
                is String -> "'${paramValue.replace("'", "''")}'"
                is Boolean -> if (paramValue) "1" else "0"
                else -> paramValue.toString()
            }
            // Replace the first occurrence of '?' with the formatted value.
            finalSql = finalSql.replaceFirst("?", formattedValue)
        }
        return finalSql
    }

    override fun clearParameters() {
        parameters.clear()
    }

    override fun setNull(parameterIndex: Int, sqlType: Int) {
        parameters[parameterIndex] = null
    }

    override fun setNull(parameterIndex: Int, sqlType: Int, typeName: String?) {
        parameters[parameterIndex] = null
    }

    override fun setBoolean(parameterIndex: Int, x: Boolean) {
        parameters[parameterIndex] = x
    }

    override fun setByte(parameterIndex: Int, x: Byte) {
        parameters[parameterIndex] = x
    }

    override fun setShort(parameterIndex: Int, x: Short) {
        parameters[parameterIndex] = x
    }

    override fun setInt(parameterIndex: Int, x: Int) {
        parameters[parameterIndex] = x
    }

    override fun setLong(parameterIndex: Int, x: Long) {
        parameters[parameterIndex] = x
    }

    override fun setFloat(parameterIndex: Int, x: Float) {
        parameters[parameterIndex] = x
    }

    override fun setDouble(parameterIndex: Int, x: Double) {
        parameters[parameterIndex] = x
    }

    override fun setBigDecimal(parameterIndex: Int, x: BigDecimal?) {
        parameters[parameterIndex] = x
    }

    override fun setString(parameterIndex: Int, x: String?) {
        parameters[parameterIndex] = x
    }

    override fun setBytes(parameterIndex: Int, x: ByteArray?) {
        parameters[parameterIndex] = x
    }

    override fun setDate(parameterIndex: Int, x: Date?) {
        parameters[parameterIndex] = x
    }

    override fun setDate(parameterIndex: Int, x: Date?, cal: Calendar?) {
        parameters[parameterIndex] = x
    }

    override fun setTime(parameterIndex: Int, x: Time?) {
        parameters[parameterIndex] = x
    }

    override fun setTime(parameterIndex: Int, x: Time?, cal: Calendar?) {
        parameters[parameterIndex] = x
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?) {
        parameters[parameterIndex] = x
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?, cal: Calendar?) {
        parameters[parameterIndex] = x
    }

    // For stream parameters, we do not support binding in this minimal implementation.
    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Int) =
        throw UnsupportedOperationException("Stream parameter binding is not supported")

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Long) =
        throw UnsupportedOperationException("Stream parameter binding is not supported")

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?) =
        throw UnsupportedOperationException("Stream parameter binding is not supported")

    // Implementations for other stream-related methods (binary, character, etc.) are omitted.
    @Deprecated("Deprecated in Java",
        ReplaceWith("throw UnsupportedOperationException(\"Parameter binding is not supported\")")
    )
    override fun setUnicodeStream(parameterIndex: Int, x: InputStream?, length: Int) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Int) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Long) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setObject(parameterIndex: Int, x: Any?) {
        parameters[parameterIndex] = x
    }

    override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int) {
        parameters[parameterIndex] = x
    }

    override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int, scaleOrLength: Int) {
        parameters[parameterIndex] = x
    }

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Int) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Long) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setRef(parameterIndex: Int, x: Ref?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setBlob(parameterIndex: Int, x: Blob?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setBlob(parameterIndex: Int, inputStream: InputStream?, length: Long) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setBlob(parameterIndex: Int, inputStream: InputStream?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setClob(parameterIndex: Int, x: Clob?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setClob(parameterIndex: Int, reader: Reader?, length: Long) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setClob(parameterIndex: Int, reader: Reader?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setArray(parameterIndex: Int, x: Array?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun getMetaData(): ResultSetMetaData {
        // Return cached metadata if already retrieved
        cachedMeta?.let { return it }

        // Build the final SQL with bound parameters
        val finalSql = buildSql()
        logger.info("Retrieving metadata for prepared SQL: {}", finalSql)

        // Fetch zero rows to obtain metadata without data transfer
        val metaQuery = "$finalSql WHERE 1=0"
        super.executeQuery(metaQuery).use { rs ->
            val md = rs.metaData
            cachedMeta = md
            return md
        }
    }
    override fun setURL(parameterIndex: Int, x: URL?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun getParameterMetaData(): ParameterMetaData =
        throw SQLFeatureNotSupportedException("Parameter metadata is not supported")

    override fun setRowId(parameterIndex: Int, x: RowId?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setNString(parameterIndex: Int, value: String?) {
        parameters[parameterIndex] = value
    }

    override fun setNCharacterStream(parameterIndex: Int, value: Reader?, length: Long) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setNCharacterStream(parameterIndex: Int, value: Reader?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setNClob(parameterIndex: Int, value: NClob?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setNClob(parameterIndex: Int, reader: Reader?, length: Long) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    override fun setNClob(parameterIndex: Int, reader: Reader?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    /**
     * Binds a SQLXML parameter by extracting its string content.
     */
    override fun setSQLXML(parameterIndex: Int, xmlObject: SQLXML?) {
        validate(parameterIndex)
        // Store the XML content as a string for substitution
        parameters[parameterIndex] = xmlObject?.getString()
    }

    // Override the execute methods to use parameter binding.
    override fun executeQuery(): ResultSet {
        val finalSql = buildSql()
        logger.info("Executing prepared query: {}", finalSql)
        return super.executeQuery(finalSql)
    }

    // PreparedStatement does not support updates in this read-only driver
    override fun executeUpdate(): Int =
        throw SQLFeatureNotSupportedException(
            "WsdlPreparedStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )

    override fun execute(): Boolean {
        val finalSql = buildSql()
        logger.info("Executing prepared query: {}", finalSql)
        return super.execute(finalSql)
    }

    // Batch execution is not supported in this read-only PreparedStatement.
    override fun addBatch() =
        throw SQLFeatureNotSupportedException(
            "WsdlPreparedStatement is read-only – batch updates not supported"
        )

}
