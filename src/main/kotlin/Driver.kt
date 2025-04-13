package my.jdbc.wsdl_driver

import java.sql.DriverManager
import org.slf4j.Logger
import org.slf4j.LoggerFactory

private val wsdlLogger: Logger = LoggerFactory.getLogger("my.jdbc.wsdl_driver")

fun registerDriver() {
    // The WsdlDriver will parse the connection string (URL) to extract wsdlEndpoint and reportPath.
    val driver = WsdlDriver()
    DriverManager.registerDriver(driver)
    wsdlLogger.info("WSDL JDBC Driver registered successfully.")
}
