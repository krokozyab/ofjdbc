package my.jdbc.wsdl_driver

import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.DriverPropertyInfo
import java.util.*
import java.util.logging.Logger

import java.sql.ResultSetMetaData
import java.sql.ResultSetMetaData.columnNullable
import java.sql.SQLFeatureNotSupportedException
import java.sql.Types

class WsdlDriver : Driver {
    var wsdlEndpoint: String = ""
    var reportPath: String = ""

    private val logger = LoggerFactory.getLogger(WsdlDriver::class.java)

    override fun connect(url: String?, info: Properties?): Connection? {
        if (url == null || info == null) return null
        val parts: List<String> = url.split(":")
        val user = info.getProperty("user")
        val pass = info.getProperty("password")
        wsdlEndpoint = "https:" + parts[2]
        reportPath = parts.getOrElse(3) { "/Custom/Financials/RP_ARB.xdo" }
        if (url.startsWith("jdbc:wsdl://")) {
            logger.info("Connecting to WSDL-based database with user: $user")
            return WsdlConnection(wsdlEndpoint, user, pass, reportPath)
        }
        return null
    }

    override fun acceptsURL(url: String?): Boolean =
        url?.startsWith("jdbc:wsdl://") ?: false

    override fun getMajorVersion(): Int = 1
    override fun getMinorVersion(): Int = 0
    override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> = arrayOf()
    override fun jdbcCompliant(): Boolean = false
    override fun getParentLogger(): Logger = Logger.getLogger("WsdlDriver")

    companion object {
        private val logger = LoggerFactory.getLogger(WsdlStatement::class.java)
        init {
            try {
                DriverManager.registerDriver(WsdlDriver())
            } catch (e: Exception) {
                logger.error("Error registering driver: ${e.message}")
            }
        }
    }
}

class DefaultResultSetMetaData(private val columns: List<String>) : ResultSetMetaData {
    override fun getColumnCount(): Int = columns.size
    override fun getColumnName(column: Int): String = columns[column - 1]
    override fun getColumnLabel(column: Int): String = getColumnName(column)
    override fun isAutoIncrement(column: Int): Boolean = false
    override fun isCaseSensitive(column: Int): Boolean = true
    override fun isSearchable(column: Int): Boolean = true
    override fun isCurrency(column: Int): Boolean = false
    override fun isNullable(column: Int): Int = columnNullable
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
    override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLFeatureNotSupportedException()
    override fun isWrapperFor(iface: Class<*>?): Boolean = false
}