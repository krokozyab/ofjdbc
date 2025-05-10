package my.jdbc.wsdl_driver

import org.slf4j.LoggerFactory
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
    // Store parameters by their 1-indexed position.
    private val parameters = mutableMapOf<Int, Any?>()

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
        TODO("Not yet implemented 56")
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

    override fun setSQLXML(parameterIndex: Int, xmlObject: SQLXML?) =
        throw UnsupportedOperationException("Parameter binding is not supported")

    // Override the execute methods to use parameter binding.
    override fun executeQuery(): ResultSet {
        val finalSql = buildSql()
        logger.info("Executing prepared query: {}", finalSql)
        return super.executeQuery(finalSql)
    }

    override fun executeUpdate(): Int {
        TODO("Not yet implemented 57")
    }

    override fun execute(): Boolean {
        val finalSql = buildSql()
        logger.info("Executing prepared query: {}", finalSql)
        return super.execute(finalSql)
    }

    override fun addBatch() {
        TODO("Not yet implemented 58")
    }
}
