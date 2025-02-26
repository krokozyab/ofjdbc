package my.application

import java.io.File
import java.net.URLClassLoader
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.DriverPropertyInfo
import java.util.*

/**
 * A wrapper for a JDBC driver loaded in a separate class loader.
 * This class delegates all calls to the underlying driver.
 */
class DriverShim(private val driver: Driver) : Driver {
    override fun acceptsURL(url: String?): Boolean = driver.acceptsURL(url)
    override fun connect(url: String?, info: Properties?): Connection? = driver.connect(url, info)
    override fun getMajorVersion(): Int = driver.majorVersion
    override fun getMinorVersion(): Int = driver.minorVersion
    override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> =
        driver.getPropertyInfo(url, info)
    override fun jdbcCompliant(): Boolean = driver.jdbcCompliant()
    override fun getParentLogger() = driver.parentLogger
}

/**
 * Dynamically loads a JDBC driver JAR file and returns a DriverShim wrapping the driver.
 *
 * @param jarPath the absolute path to the driver JAR file.
 * @param driverClassName the fully qualified class name of the driver (e.g., "my.jdbc.wsdl_driver.WsdlDriver")
 */
fun loadDriverJar(jarPath: String, driverClassName: String): Driver {
    val jarFile = File(jarPath)
    if (!jarFile.exists()) {
        throw IllegalArgumentException("Jar file not found: $jarPath")
    }
    // Create a URLClassLoader for the driver JAR.
    val loader = URLClassLoader(arrayOf(jarFile.toURI().toURL()), Thread.currentThread().contextClassLoader)
    // Load the driver class using the provided loader.
    val driverClass = Class.forName(driverClassName, true, loader)
    val driverInstance = driverClass.getDeclaredConstructor().newInstance() as Driver
    return DriverShim(driverInstance)
}

fun main() {
    try {
        // Specify the path to your driver JAR and the fully qualified driver class name.
        val driverJarPath = "C:\\path-to-jar-file\\orfujdbc-1.0-SNAPSHOT.jar"
        val driverClassName = "my.jdbc.wsdl_driver.WsdlDriver"

        // Dynamically load the driver and register it with DriverManager.
        val driver = loadDriverJar(driverJarPath, driverClassName)
        DriverManager.registerDriver(driver)
        println("Driver class $driverClassName registered successfully.")

        // Define your JDBC URL, username, and password.
        val jdbcUrl = "jdbc:wsdl://you-server.oraclecloud.com/xmlpserver/services/ExternalReportWSSService?WSDL:/Custom/Financials/RP_ARB.xdo"
        val username = "xxx" // Never ever put creds in the code, nobody hires you :) use env vars, secrets managers etc
        val password = "xxx"

        // Obtain a connection.
        val connection = DriverManager.getConnection(jdbcUrl, username, password)
        println("Connection established: $connection")

        // Base SQL statement (without pagination clauses).
        val baseSql = "select gcc.CODE_COMBINATION_ID, gcc.SEGMENT1, gcc.SEGMENT4 from GL_CODE_COMBINATIONS gcc WHERE gcc.ACCOUNT_TYPE = ?"
        // Set the number of rows to fetch per page.
        val fetchSize = 1000
        var offset = 0
        var totalCount = 0

        // Loop to fetch pages until a page returns less than fetchSize rows.
        while (true) {
            // Append OFFSET/FETCH clause to the SQL query.
            val paginatedSql = "$baseSql OFFSET $offset ROWS FETCH NEXT $fetchSize ROWS ONLY"
            val pstmt = connection.prepareStatement(paginatedSql)
            pstmt.setString(1, "E") // Bind the parameter value.
            val rs = pstmt.executeQuery()

            var count = 0
            while (rs.next()) {
                val ccid = rs.getString("code_combination_id")
                val segment1 = rs.getString("segment1")
                val segment4 = rs.getString("segment4")
                println("CCID: $ccid, Segment1: $segment1, Segment4: $segment4")
                count++
            }
            rs.close()
            pstmt.close()

            if (count < fetchSize) {
                // No more rows to fetch.
                totalCount += count
                break
            }
            offset += fetchSize
            totalCount += count
        }
        println("Total records: $totalCount")
        connection.close()
    } catch (ex: Exception) {
        ex.printStackTrace()
    }
}
