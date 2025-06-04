package my.jdbc.wsdl_driver

import org.slf4j.LoggerFactory
import java.sql.*
import java.sql.ResultSet
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLClientInfoException
import java.sql.ClientInfoStatus
import java.util.*

class WsdlConnection(
    val wsdlEndpoint: String,
    val username: String,
    val password: String,
    val reportPath: String
) : Connection {
    init {
        require(wsdlEndpoint.isNotBlank()) { "wsdlEndpoint must not be blank" }
        require(reportPath.isNotBlank())   { "reportPath must not be blank" }
    }
    // For a minimal implementation, we assume the connection is always read-only.
    private var readOnly: Boolean = true
    /** JDBC default is autoCommit = true; driver is read‑only so value is advisory only. */
    private var autoCommit: Boolean = true
    private var currentSchema: String? = "FUSION"
    /** Network timeout requested by caller (ms); driver ignores it but returns the same value. */
    private var networkTimeoutMillis: Int = 0
    /** Cached client‑info properties (per JDBC 4.0) */
    private val clientInfoProps: Properties = Properties()

    companion object {
        private val logger = LoggerFactory.getLogger(WsdlConnection::class.java)
    }

    @Volatile
    private var closed: Boolean = false

    override fun createStatement(): Statement =
        WsdlStatement(wsdlEndpoint, username, password, reportPath)

    override fun close() {
        if (closed) return          // already closed – do nothing
        closed = true
        logger.info("Connection closed")
        //LocalMetadataCache.close()
    }

    override fun isClosed(): Boolean = closed

    // Implement getMetaData() by returning a minimal DatabaseMetaData.
    override fun getMetaData(): DatabaseMetaData = WsdlDatabaseMetaData(this)

    override fun prepareStatement(sql: String?): PreparedStatement {
        if (sql == null) {
            throw SQLException("SQL query cannot be null")
        }
        return WsdlPreparedStatement(sql, wsdlEndpoint, username, password, reportPath)
    }

    override fun prepareCall(sql: String?): CallableStatement {
        if (sql == null) {
            throw SQLException("SQL text cannot be null")
        }
        throw SQLFeatureNotSupportedException("Callable statements are not supported by this driver")
    }

    override fun nativeSQL(sql: String?): String {
        if (sql == null) {
            throw SQLException("SQL text cannot be null")
        }
        return sql
    }

    override fun setAutoCommit(autoCommit: Boolean) {
        this.autoCommit = autoCommit
        if (logger.isDebugEnabled)
            logger.debug("setAutoCommit({}) called – read‑only connection, no effect.", autoCommit)
    }

    override fun getAutoCommit(): Boolean =  autoCommit

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

    override fun setReadOnly(readOnly: Boolean) {
        if (!readOnly) {
            throw SQLFeatureNotSupportedException("This driver supports read‑only connections only")
        }
        // caller asked for read‑only → already true; nothing else to do
        this.readOnly = true
    }

    override fun isReadOnly(): Boolean = this.readOnly

    override fun setCatalog(catalog: String?) {
        if (catalog == null) {
            throw SQLException("Catalog cannot be null")
        }
        throw SQLFeatureNotSupportedException("Catalogs are not supported by this driver")
    }

    override fun getCatalog(): String? = null    // driver has no catalog concept

    override fun setTransactionIsolation(level: Int) {
        if (level != Connection.TRANSACTION_READ_COMMITTED) {
            throw SQLFeatureNotSupportedException("Only TRANSACTION_READ_COMMITTED is supported")
        }
        // no-op, since this driver is read-only and already uses READ_COMMITTED
    }

    override fun getTransactionIsolation(): Int = Connection.TRANSACTION_READ_COMMITTED

    override fun getWarnings(): SQLWarning? = null

    override fun clearWarnings() { /* no warnings to clear */ }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement {
        // Only forward-only, read-only result sets are supported.
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY and CONCUR_READ_ONLY are supported")
        }
        return createStatement()
    }

    override fun prepareStatement(sql: String?, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement {
        if (sql == null) {
            throw SQLException("SQL cannot be null")
        }
        // You may log the requested types if needed.
        logger.info("Preparing statement: {} (type={}, concurrency={})", sql, resultSetType, resultSetConcurrency)
        return WsdlPreparedStatement(sql, wsdlEndpoint, username, password, reportPath)
    }

    override fun prepareCall(
        sql: String?,
        resultSetType: Int,
        resultSetConcurrency: Int
    ): CallableStatement =
        throw SQLFeatureNotSupportedException("Stored procedures are not supported")

    override fun getTypeMap(): MutableMap<String, Class<*>> {
        // Return an empty map since custom type mappings are not supported
        return HashMap()
    }

    override fun setTypeMap(map: MutableMap<String, Class<*>>?) {
        if (map == null) {
            throw SQLException("Type map cannot be null")
        }
        throw SQLFeatureNotSupportedException("Custom type mappings are not supported by this driver")
    }

    /** Forward‑only, read‑only driver: only HOLD_CURSORS_OVER_COMMIT is supported. */
    override fun setHoldability(holdability: Int) {
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw SQLFeatureNotSupportedException(
                "Only ResultSet.HOLD_CURSORS_OVER_COMMIT is supported"
            )
        }
        // nothing else to do; value is accepted
    }

    override fun getHoldability(): Int = ResultSet.HOLD_CURSORS_OVER_COMMIT

    // --- Savepoint operations are not supported in this read‑only driver ---
    override fun setSavepoint(): Savepoint {
        throw SQLFeatureNotSupportedException("Savepoints are not supported by this read‑only driver")
    }

    override fun setSavepoint(name: String?): Savepoint {
        throw SQLFeatureNotSupportedException("Savepoints are not supported by this read‑only driver")
    }

    override fun rollback(savepoint: Savepoint?) {
        throw SQLFeatureNotSupportedException("Savepoints are not supported by this read‑only driver")
    }

    override fun releaseSavepoint(savepoint: Savepoint?) {
        throw SQLFeatureNotSupportedException("Savepoints are not supported by this read‑only driver")
    }

    // --- Statement / PreparedStatement creation with explicit holdability ---
    override fun createStatement(
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): Statement {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY ||
            resultSetConcurrency != ResultSet.CONCUR_READ_ONLY ||
            resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw SQLFeatureNotSupportedException(
                "Only TYPE_FORWARD_ONLY / CONCUR_READ_ONLY / HOLD_CURSORS_OVER_COMMIT are supported"
            )
        }
        return createStatement()
    }

    override fun prepareStatement(
        sql: String?,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): PreparedStatement {
        if (sql == null) {
            throw SQLException("SQL cannot be null")
        }
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY ||
            resultSetConcurrency != ResultSet.CONCUR_READ_ONLY ||
            resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw SQLFeatureNotSupportedException(
                "Only TYPE_FORWARD_ONLY / CONCUR_READ_ONLY / HOLD_CURSORS_OVER_COMMIT are supported"
            )
        }
        return WsdlPreparedStatement(sql, wsdlEndpoint, username, password, reportPath)
    }

    override fun prepareCall(
        sql: String?,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): CallableStatement {
        if (sql == null) {
            throw SQLException("SQL text cannot be null")
        }
        throw SQLFeatureNotSupportedException("Callable statements are not supported by this driver")
    }

    // --- PreparedStatement variants for auto‑generated keys ---
    override fun prepareStatement(sql: String?, autoGeneratedKeys: Int): PreparedStatement {
        if (sql == null) {
            throw SQLException("SQL cannot be null")
        }
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw SQLFeatureNotSupportedException("Auto‑generated keys are not supported by this driver")
        }
        return WsdlPreparedStatement(sql, wsdlEndpoint, username, password, reportPath)
    }

    override fun prepareStatement(sql: String?, columnIndexes: IntArray?): PreparedStatement {
        if (sql == null) {
            throw SQLException("SQL cannot be null")
        }
        throw SQLFeatureNotSupportedException("Auto‑generated keys are not supported by this driver")
    }

    override fun prepareStatement(sql: String?, columnNames: Array<out String>?): PreparedStatement {
        if (sql == null) {
            throw SQLException("SQL cannot be null")
        }
        throw SQLFeatureNotSupportedException("Auto‑generated keys are not supported by this driver")
    }

    // --- Large object creation (unsupported) ---
    override fun createClob(): Clob {
        throw SQLFeatureNotSupportedException("CLOBs are not supported by this driver")
    }

    override fun createBlob(): Blob {
        throw SQLFeatureNotSupportedException("BLOBs are not supported by this driver")
    }

    override fun createNClob(): NClob {
        throw SQLFeatureNotSupportedException("NCLOBs are not supported by this driver")
    }

    override fun createSQLXML(): SQLXML {
        throw SQLFeatureNotSupportedException("SQLXML is not supported by this driver")
    }

    override fun isValid(timeout: Int): Boolean = !isClosed()

    // --- Client‑info properties implementation ---
    override fun setClientInfo(name: String?, value: String?) {
        if (name == null) {
            throw SQLClientInfoException("Client‑info name cannot be null", null as? Map<String, ClientInfoStatus>)
        }
        if (value == null) {
            clientInfoProps.remove(name)
        } else {
            clientInfoProps[name] = value
        }
    }

    override fun setClientInfo(properties: Properties?) {
        if (properties == null) {
            throw SQLClientInfoException("Properties cannot be null", null as? Map<String, ClientInfoStatus>)
        }
        clientInfoProps.clear()
        clientInfoProps.putAll(properties)
    }

    override fun getClientInfo(name: String?): String {
        if (name == null) {
            throw SQLException("Client‑info name cannot be null")
        }
        return clientInfoProps.getProperty(name)
    }

    override fun getClientInfo(): Properties {
        // Return a defensive copy
        val copy = Properties()
        copy.putAll(clientInfoProps)
        return copy
    }

    // --- Advanced SQL types (unsupported) ---
    override fun createArrayOf(typeName: String?, elements: Array<out Any>?): java.sql.Array {
        throw SQLFeatureNotSupportedException("Array types are not supported by this driver")
    }

    override fun createStruct(typeName: String?, attributes: Array<out Any>?): Struct {
        throw SQLFeatureNotSupportedException("Struct types are not supported by this driver")
    }

    override fun setSchema(schema: String?) {
        currentSchema = schema
        logger.info("Schema set to: {}", schema)
    }

    override fun getSchema(): String = currentSchema ?: "FUSION"

    override fun abort(executor: java.util.concurrent.Executor?) {
        if (executor == null) {
            throw SQLException("Executor cannot be null")
        }
        // If already closed, nothing to do.
        if (isClosed()) return

        // Execute the close asynchronously using the caller‑provided executor.
        executor.execute {
            try {
                close()
            } catch (e: SQLException) {
                // Log and swallow – connection is being aborted anyway.
                logger.warn("Error while aborting connection", e)
            }
        }
    }

    override fun setNetworkTimeout(executor: java.util.concurrent.Executor?, milliseconds: Int) {
        // The WSDL driver is stateless / HTTP-based; we don't enforce socket timeouts here.
        // Store the value so that getNetworkTimeout() can echo it, as the spec expects.
        this.networkTimeoutMillis = milliseconds
        if (logger.isDebugEnabled)
            logger.debug("setNetworkTimeout({}, {}) called – no enforcement in driver", executor, milliseconds)
    }

    override fun getNetworkTimeout(): Int = networkTimeoutMillis

    override fun <T> unwrap(iface: Class<T>): T {
        if (iface.isInstance(this)) {
            @Suppress("UNCHECKED_CAST")
            return this as T
        }
        throw SQLException("Not a wrapper for ${iface.name}")
    }

    override fun isWrapperFor(iface: Class<*>): Boolean =
        iface.isInstance(this)
}