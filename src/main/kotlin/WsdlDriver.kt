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
        if (url == null) return null
        if (!url.startsWith("jdbc:wsdl://")) return null
        val props = info ?: Properties()
        val parts: List<String> = url.split(":")
        val user = props.getProperty("user") ?: ""
        val pass = props.getProperty("password") ?: ""
        wsdlEndpoint = "https:" + parts[2]
        // Strip any query-style parameters (e.g. ?WSDL:/path&oauthProviderClass=...&authType=...) from the report path.
        reportPath = parts.getOrElse(3) { "/Custom/Financials/RP_ARB.xdo" }.substringBefore("&")

        // Optional OAuth: instantiate the configured provider via reflection and register an
        // OAuth authenticator for this endpoint. Core request logic stays untouched.
        val oauthProviderClass = extractUrlParam(url, "oauthProviderClass")
            ?: props.getProperty("oauthProviderClass")
        if (!oauthProviderClass.isNullOrBlank()) {
            try {
                val instance = Class.forName(oauthProviderClass.trim())
                    .getDeclaredConstructor(String::class.java, String::class.java, String::class.java)
                    .newInstance(wsdlEndpoint, user, pass)
                val provider = instance as? OAuthProvider
                    ?: throw java.sql.SQLException(
                        "Class '$oauthProviderClass' does not implement my.jdbc.wsdl_driver.OAuthProvider"
                    )
                AuthenticatorRegistry.register(wsdlEndpoint, OAuthAuthenticator(provider))
                logger.info("Registered OAuth provider '{}' for endpoint {}", oauthProviderClass, wsdlEndpoint)
            } catch (e: java.sql.SQLException) {
                throw e
            } catch (e: Exception) {
                throw java.sql.SQLException("Failed to initialize OAuth provider '$oauthProviderClass': ${e.message}", e)
            }
        }

        logger.info("Connecting to WSDL-based database with user: $user")
        return WsdlConnection(wsdlEndpoint, user, pass, reportPath)
    }

    /** Extracts a query parameter value from URL. Only supports '&' as separator (standard URL format).
     *  Example: `?WSDL:/path&oauthProviderClass=value&authType=BROWSER` → extracts `value` for `oauthProviderClass`
     */
    private fun extractUrlParam(url: String, name: String): String? =
        Regex("&" + Regex.escape(name) + "=([^&]+)").find(url)?.groupValues?.get(1)

    override fun acceptsURL(url: String?): Boolean =
        url?.startsWith("jdbc:wsdl://") ?: false

    override fun getMajorVersion(): Int = 1

    override fun getMinorVersion(): Int = 1

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

    /** Ensures the JDBC column index is 1‑based and inside the list size. */
    private fun check(column: Int) {
        if (column < 1 || column > columns.size) {
            throw java.sql.SQLException("Column index $column out of range 1..${columns.size}")
        }
    }

    override fun getColumnCount(): Int = columns.size

    override fun getColumnName(column: Int): String { check(column); return columns[column - 1] }

    override fun getColumnLabel(column: Int): String = getColumnName(column)

    override fun isAutoIncrement(column: Int): Boolean { check(column); return false }

    override fun isCaseSensitive(column: Int): Boolean { check(column); return true }

    override fun isSearchable(column: Int): Boolean { check(column); return true }

    override fun isCurrency(column: Int): Boolean { check(column); return false }

    override fun isNullable(column: Int): Int { check(column); return columnNullable }

    override fun isSigned(column: Int): Boolean { check(column); return false }

    override fun getColumnDisplaySize(column: Int): Int { check(column); return 50 }

    override fun getColumnType(column: Int): Int { check(column); return Types.VARCHAR }

    override fun getColumnTypeName(column: Int): String { check(column); return "VARCHAR" }

    override fun getPrecision(column: Int): Int { check(column); return 0 }

    override fun getScale(column: Int): Int { check(column); return 0 }

    override fun getSchemaName(column: Int): String { check(column); return "" }

    override fun getTableName(column: Int): String { check(column); return "" }

    override fun getCatalogName(column: Int): String { check(column); return "" }

    override fun isReadOnly(column: Int): Boolean { check(column); return true }

    override fun isWritable(column: Int): Boolean { check(column); return false }

    override fun isDefinitelyWritable(column: Int): Boolean { check(column); return false }

    override fun getColumnClassName(column: Int): String { check(column); return "java.lang.String" }

    override fun <T : Any?> unwrap(iface: Class<T>): T =
        if (iface.isAssignableFrom(javaClass)) iface.cast(this)
        else throw SQLFeatureNotSupportedException(
            "DefaultResultSetMetaData cannot unwrap to ${iface.name}"
        )

    override fun isWrapperFor(iface: Class<*>?): Boolean =
        iface?.isAssignableFrom(javaClass) == true
}