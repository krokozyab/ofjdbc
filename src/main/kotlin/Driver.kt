package my.jdbc.wsdl_driver

import java.sql.*

fun registerDriver() {
    // The WsdlDriver will parse the connection string (URL) to extract wsdlEndpoint and reportPath.
    val driver = WsdlDriver()
    DriverManager.registerDriver(driver)
    println("WSDL JDBC Driver registered successfully.")
}
