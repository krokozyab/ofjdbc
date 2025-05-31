package my.jdbc.wsdl_driver

import my.jdbc.wsdl_driver.XmlResultSet

import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLWarning
import java.sql.Statement
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLException

open class WsdlStatement(
    private val wsdlEndpoint: String,
    private val username: String,
    private val password: String,
    private val reportPath: String
) : Statement {

    // Field to store the last ResultSet produced.
    private var lastResultSet: ResultSet? = null

    /** Tracks whether the statement has been closed. */
    private var closed = false

    /** Chain of SQLWarning objects, if any were generated. */
    private var warnings: SQLWarning? = null

    // Field to store the fetch size (page size).
    private var fetchSize: Int = 500

    /** Timeout in seconds for query execution. */
    private var queryTimeout: Int = 0

    /** Tracks the fetch direction for result sets. */
    private var fetchDirection: Int = ResultSet.FETCH_FORWARD


    private val logger = LoggerFactory.getLogger(WsdlStatement::class.java)


    override fun executeQuery(sql: String): ResultSet {
        // reset warnings for this execution
        warnings = null
        // Instead of fetching just one page, return a PaginatedResultSet
        val paginatedRs = PaginatedResultSet(
            originalSql = sql,
            wsdlEndpoint = wsdlEndpoint,
            username = username,
            password = password,
            reportPath = reportPath,
            fetchSize = fetchSize,
            logger = logger
        )
        lastResultSet = paginatedRs
        return paginatedRs
    }

    override fun close() {
        if (!closed) {
            lastResultSet?.close()
            closed = true
            logger.info("Statement closed")
        }
    }

    // --- Stub implementations ---
    override fun setMaxRows(max: Int) { /* no-op */ }

    /**
     * Escape processing (JDBC {fn ...} escapes) is not supported.
     * This is a no-op to satisfy the Statement API.
     */
    override fun setEscapeProcessing(enable: Boolean) {
        // No escape processing support; ignore the setting.
        addWarning(SQLWarning("setEscapeProcessing($enable) called but not supported"))
        logger.info("Escape processing flag ignored: {}", enable)
    }

    override fun getMaxRows(): Int = 0

    override fun execute(sql: String): Boolean {
        // reset warnings for this execution
        warnings = null
        lastResultSet = executeQuery(sql)
        return true
    }

    // Statement does not support updates in this read-only driver
    override fun executeUpdate(sql: String): Int =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )

    override fun getWarnings(): SQLWarning? = warnings

    override fun clearWarnings() {
        warnings = null
    }

    /**
     * Named cursors are not supported in this read-only driver.
     * This method is a no-op but records a warning.
     */
    override fun setCursorName(name: String?) {
        addWarning(SQLWarning("setCursorName($name) called but not supported"))
        logger.info("setCursorName ignored: {}", name)
    }

    /**
     * Sets the number of seconds the driver will wait for a query to complete.
     * No actual cancellation is implemented, but the value is tracked.
     */
    override fun setQueryTimeout(seconds: Int) {
        validateOpen()
        queryTimeout = seconds
        logger.info("Query timeout set to {} seconds", seconds)
    }

    /**
     * Retrieves the current query timeout value in seconds.
     */
    override fun getQueryTimeout(): Int = queryTimeout

    /**
     * Statement cancellation is not supported in this driver.
     */
    override fun cancel() {
        addWarning(SQLWarning("cancel() called but not supported"))
        logger.info("Cancel request ignored")
    }

    override fun getResultSet(): ResultSet? = lastResultSet

    override fun getUpdateCount(): Int = -1

    /**
     * Since this driver only returns a single ResultSet, always close the current one
     * and signal that there are no further results.
     */
    override fun getMoreResults(): Boolean {
        validateOpen()
        lastResultSet?.close()
        lastResultSet = null
        return false
    }

    override fun setFetchDirection(direction: Int) {
        validateOpen()
        if (direction != ResultSet.FETCH_FORWARD) {
            throw SQLFeatureNotSupportedException(
                "WsdlStatement only supports FETCH_FORWARD direction"
            )
        }
        fetchDirection = direction
        logger.info("Fetch direction set to FETCH_FORWARD")
    }

    override fun getFetchDirection(): Int {
        validateOpen()
        return fetchDirection
    }
    override fun setFetchSize(rows: Int) {
        fetchSize = rows
        logger.info("Fetch size set to {}", rows)
    }
    override fun getFetchSize(): Int = fetchSize

// The ResultSet is read-only
override fun getResultSetConcurrency(): Int = ResultSet.CONCUR_READ_ONLY

// The ResultSet is forward-only
override fun getResultSetType(): Int = ResultSet.TYPE_FORWARD_ONLY

// Batch updates are not supported in this read-only driver
override fun addBatch(sql: String) =
    throw SQLFeatureNotSupportedException(
        "WsdlStatement is read-only – batch updates not supported"
    )

// Batch updates are not supported; clearBatch is a no-op
override fun clearBatch() =
    throw SQLFeatureNotSupportedException(
        "WsdlStatement is read-only – batch updates not supported"
    )

// Batch execution is not supported in this read-only driver
override fun executeBatch(): IntArray =
    throw SQLFeatureNotSupportedException(
        "WsdlStatement is read-only – batch updates not supported"
    )

// Retrieving the connection is not supported by this Statement
override fun getConnection(): Connection =
    throw SQLFeatureNotSupportedException(
        "WsdlStatement cannot return the underlying Connection"
    )
    /**
     * No additional result sets; always return false.
     */
    override fun getMoreResults(current: Int): Boolean {
        validateOpen()
        lastResultSet?.close()
        lastResultSet = null
        return false
    }
    // No generated keys for read-only statements; return an empty result set.
    override fun getGeneratedKeys(): ResultSet = XmlResultSet(emptyList())
    // Updates not supported in read-only driver
    override fun executeUpdate(sql: String, autoGeneratedKeys: Int): Int =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )
    // Updates not supported in read-only driver
    override fun executeUpdate(sql: String, columnIndexes: IntArray): Int =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )
    // Updates not supported in read-only driver
    override fun executeUpdate(sql: String, columnNames: Array<out String>?): Int =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )
    // No maximum field size enforced; return 0
    override fun getMaxFieldSize(): Int = 0
    // Field size limit not supported; ignore
    override fun setMaxFieldSize(max: Int) { /* no-op */ }
    // Only SELECT statements are supported
    override fun execute(sql: String, autoGeneratedKeys: Int): Boolean =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )
    // Only SELECT statements are supported
    override fun execute(sql: String, columnIndexes: IntArray): Boolean =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )
    // Only SELECT statements are supported
    override fun execute(sql: String, columnNames: Array<out String>?): Boolean =
        throw SQLFeatureNotSupportedException(
            "WsdlStatement is read-only – UPDATE/INSERT/DELETE not supported"
        )
    // The driver keeps cursors open across commit
    override fun getResultSetHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT



    /** Throws SQLException if the statement is closed. */
    private fun validateOpen() {
        if (closed) throw SQLException("Statement is closed")
    }

    /**
     * Add a non-fatal SQLWarning to the warning chain.
     */
    protected fun addWarning(warning: SQLWarning) {
        if (warnings == null) {
            warnings = warning
        } else {
            warnings!!.setNextWarning(warning)
        }
    }

    override fun isClosed(): Boolean = closed

/**
 * This driver does not support statement pooling; ignore this setting.
 */
override fun setPoolable(poolable: Boolean) {
    addWarning(SQLWarning("setPoolable($poolable) called but not supported"))
    logger.info("setPoolable ignored: {}", poolable)
}
/**
 * Always false: pooling is not supported.
 */
override fun isPoolable(): Boolean = false
/**
 * closeOnCompletion is not supported; ignore it.
 */
override fun closeOnCompletion() {
    addWarning(SQLWarning("closeOnCompletion() called but not supported"))
    logger.info("closeOnCompletion ignored")
}
/**
 * Returns false: closeOnCompletion not supported.
 */
override fun isCloseOnCompletion(): Boolean = false
/**
 * Allows unwrapping to the driver’s statement class.
 */
override fun <T : Any?> unwrap(iface: Class<T>): T =
    if (iface.isAssignableFrom(javaClass)) iface.cast(this)
    else throw SQLFeatureNotSupportedException(
        "WsdlStatement cannot unwrap to ${iface.name}"
    )
/**
 * Indicates whether this is a wrapper for the given interface.
 */
override fun isWrapperFor(iface: Class<*>): Boolean =
    iface.isAssignableFrom(javaClass)
}
