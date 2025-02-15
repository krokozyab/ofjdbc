package my.jdbc.wsdl_driver

import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.DriverPropertyInfo
import java.util.*
import java.util.logging.Logger

class WsdlDriver : Driver {
    var wsdlEndpoint: String = ""
    var reportPath: String = ""

    // Use SLF4J for logging.
    private val logger = LoggerFactory.getLogger(WsdlStatement::class.java)

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
        // Use SLF4J for logging.
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