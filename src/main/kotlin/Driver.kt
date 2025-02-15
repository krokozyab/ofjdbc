package my.jdbc.wsdl_driver

import java.sql.*

/*
fun registerDriver(wsdlEndpoint: String, reportPath: String = "/Custom/Financials/RP_ARB.xdo") {
    val driver = WsdlDriver().apply {
        this.wsdlEndpoint = wsdlEndpoint
        this.reportPath = reportPath
    }
    DriverManager.registerDriver(driver)
    println("WSDL JDBC Driver registered successfully with report path: $reportPath")
}
*/
fun registerDriver() {
    // The WsdlDriver will parse the connection string (URL) to extract wsdlEndpoint and reportPath.
    val driver = WsdlDriver()
    DriverManager.registerDriver(driver)
    println("WSDL JDBC Driver registered successfully.")
}
