package my.jdbc.wsdl_driver

import org.slf4j.LoggerFactory
import java.sql.*
import java.util.*

class WsdlConnection(
    val wsdlEndpoint: String,
    val username: String,
    val password: String,
    val reportPath: String
) : Connection {
    // For a minimal implementation, we assume the connection is always read-only.
    private var readOnly: Boolean = true
    private var autoCommit: Boolean = false

    companion object {
        private val logger = LoggerFactory.getLogger(WsdlConnection::class.java)
    }

    override fun createStatement(): Statement =
        WsdlStatement(wsdlEndpoint, username, password, reportPath)
    override fun close() { logger.info("Connection closed") }
    override fun isClosed(): Boolean = false

    // Implement getMetaData() by returning a minimal DatabaseMetaData.
    override fun getMetaData(): DatabaseMetaData = WsdlDatabaseMetaData(this)

    override fun prepareStatement(sql: String?): PreparedStatement = throw UnsupportedOperationException("Not implemented 207")
    override fun prepareCall(sql: String?): CallableStatement = throw UnsupportedOperationException("Not implemented 208")
    override fun nativeSQL(sql: String?): String = throw UnsupportedOperationException("Not implemented 209")
    //override fun setAutoCommit(autoCommit: Boolean) = throw UnsupportedOperationException("Not implemented 210")
    // Implement setAutoCommit as a no-op that simply stores the value.
    override fun setAutoCommit(autoCommit: Boolean) {
        this.autoCommit = autoCommit
        // Optionally log that this is a no-op for read-only connections.
        logger.info("setAutoCommit($autoCommit) called. (No transactional changes supported.)")
    }
    override fun getAutoCommit(): Boolean =  autoCommit//throw UnsupportedOperationException("Not implemented 211")
    override fun commit() {
        // This connection is read-only and does not support transactions,
        // so commit is a no-op.
        logger.info("Commit called, but this connection is read-only. No action taken.")
    }
    override fun rollback()
    {
        // This driver is read-only and does not support transactional changes,
        // so rollback is a no-op.
        logger.info("Rollback called, but this connection is read-only. No action taken.")
    }
    override fun setReadOnly(readOnly: Boolean) {this.readOnly = readOnly}// throw UnsupportedOperationException("Not implemented 214")
    override fun isReadOnly(): Boolean = this.readOnly //throw UnsupportedOperationException("Not implemented 215")
    override fun setCatalog(catalog: String?) = throw UnsupportedOperationException("Not implemented 216")
    //override fun getCatalog(): String? = throw UnsupportedOperationException("Not implemented 217")
    override fun getCatalog(): String? = ""
    override fun setTransactionIsolation(level: Int) = throw UnsupportedOperationException("Not implemented 218")
    override fun getTransactionIsolation(): Int = throw UnsupportedOperationException("Not implemented 219")
    //override fun getWarnings(): SQLWarning? = throw UnsupportedOperationException("Not implemented 220")
    override fun getWarnings(): SQLWarning? = null
    //override fun clearWarnings() = throw UnsupportedOperationException("Not implemented 221")
    override fun clearWarnings() { /* no warnings to clear */ }
    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement = throw UnsupportedOperationException("Not implemented 222")
    override fun prepareStatement(sql: String?, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement = throw UnsupportedOperationException("Not implemented 223")
    override fun prepareCall(sql: String?, resultSetType: Int, resultSetConcurrency: Int): CallableStatement = throw UnsupportedOperationException("Not implemented 224")
    override fun getTypeMap(): MutableMap<String, Class<*>> = throw UnsupportedOperationException("Not implemented 225")
    override fun setTypeMap(map: MutableMap<String, Class<*>>?) = throw UnsupportedOperationException("Not implemented 226")
    override fun setHoldability(holdability: Int) = throw UnsupportedOperationException("Not implemented 227")
    override fun getHoldability(): Int = throw UnsupportedOperationException("Not implemented 228")
    override fun setSavepoint(): Savepoint = throw UnsupportedOperationException("Not implemented 229")
    override fun setSavepoint(name: String?): Savepoint = throw UnsupportedOperationException("Not implemented 230")
    override fun rollback(savepoint: Savepoint?) = throw UnsupportedOperationException("Not implemented 231")
    override fun releaseSavepoint(savepoint: Savepoint?) = throw UnsupportedOperationException("Not implemented 232")
    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement = throw UnsupportedOperationException("Not implemented 233")
    override fun prepareStatement(sql: String?, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): PreparedStatement = throw UnsupportedOperationException("Not implemented 234")
    override fun prepareCall(sql: String?, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): CallableStatement = throw UnsupportedOperationException("Not implemented 235")
    override fun prepareStatement(sql: String?, autoGeneratedKeys: Int): PreparedStatement = throw UnsupportedOperationException("Not implemented 236")
    override fun prepareStatement(sql: String?, columnIndexes: IntArray?): PreparedStatement = throw UnsupportedOperationException("Not implemented 237")
    override fun prepareStatement(sql: String?, columnNames: Array<out String>?): PreparedStatement = throw UnsupportedOperationException("Not implemented 238")
    override fun createClob(): Clob = throw UnsupportedOperationException("Not implemented 239")
    override fun createBlob(): Blob = throw UnsupportedOperationException("Not implemented 240")
    override fun createNClob(): NClob = throw UnsupportedOperationException("Not implemented 241")
    override fun createSQLXML(): SQLXML = throw UnsupportedOperationException("Not implemented 242")
    override fun isValid(timeout: Int): Boolean = throw UnsupportedOperationException("Not implemented 243")
    override fun setClientInfo(name: String?, value: String?) = throw UnsupportedOperationException("Not implemented 244")
    override fun setClientInfo(properties: Properties?) = throw UnsupportedOperationException("Not implemented 245")
    override fun getClientInfo(name: String?): String = throw UnsupportedOperationException("Not implemented 246")
    override fun getClientInfo(): Properties = throw UnsupportedOperationException("Not implemented 247")
    override fun createArrayOf(typeName: String?, elements: Array<out Any>?): java.sql.Array = throw UnsupportedOperationException("Not implemented 248")
    override fun createStruct(typeName: String?, attributes: Array<out Any>?): Struct = throw UnsupportedOperationException("Not implemented 249")
    override fun setSchema(schema: String?) = throw UnsupportedOperationException("Not implemented 250")
    //override fun getSchema(): String = throw UnsupportedOperationException("Not implemented 251")
    override fun getSchema(): String = username
    override fun abort(executor: java.util.concurrent.Executor?) = throw UnsupportedOperationException("Not implemented 252")
    override fun setNetworkTimeout(executor: java.util.concurrent.Executor?, milliseconds: Int) = throw UnsupportedOperationException("Not implemented 253")
    override fun getNetworkTimeout(): Int = throw UnsupportedOperationException("Not implemented 254")
    override fun <T : Any?> unwrap(iface: Class<T>): T = throw UnsupportedOperationException("Not implemented 255")
    override fun isWrapperFor(iface: Class<*>): Boolean = throw UnsupportedOperationException("Not implemented 256")
}