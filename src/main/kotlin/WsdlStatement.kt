package my.jdbc.wsdl_driver

import org.apache.commons.text.StringEscapeUtils
import org.slf4j.LoggerFactory
import org.w3c.dom.Document
import org.w3c.dom.NodeList
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLWarning
import java.sql.Statement

class WsdlStatement(
    private val wsdlEndpoint: String,
    private val username: String,
    private val password: String,
    private val reportPath: String
) : Statement {

    // Field to store the last ResultSet produced.
    private var lastResultSet: ResultSet? = null

    // Field to store the fetch size (page size).
    private var fetchSize: Int = 50

    // Field to store the current offset for pagination.
    private var currentOffset: Int = 0

    // Use SLF4J for logging.
    private val logger = LoggerFactory.getLogger(WsdlStatement::class.java)

    /**
     * Rewrites a SELECT SQL query to include Oracle-style OFFSET/FETCH pagination.
     * If the query already contains an OFFSET clause or does not start with SELECT,
     * the query is returned unchanged.
     */
    private fun rewriteQueryForPagination(originalSql: String, offset: Int, fetchSize: Int): String {
        val trimmedSql = originalSql.trim()
        if (!trimmedSql.uppercase().startsWith("SELECT") || trimmedSql.uppercase().contains("FETCH")) {
            return originalSql
        }
        return "$originalSql OFFSET $offset ROWS FETCH NEXT $fetchSize ROWS ONLY"
    }

    override fun executeQuery(sql: String): ResultSet {
        // If a fetch size is set, rewrite the SQL query to limit the returned rows.
        val effectiveSql = if (fetchSize > 0) {
            rewriteQueryForPagination(sql, currentOffset, fetchSize)
        } else {
            sql
        }

        val responseXml = sendSqlViaWsdl(wsdlEndpoint, effectiveSql, username, password, reportPath)
        val doc: Document = parseXml(responseXml)

        // Try to extract <ROW> nodes from the response.
        var rowNodes: NodeList = doc.getElementsByTagName("ROW")
        if (rowNodes.length == 0) {
            val resultNodes: NodeList = doc.getElementsByTagName("RESULT")
            if (resultNodes.length > 0) {
                val resultText: String = resultNodes.item(0).textContent.trim()
                val unescapedXml: String = StringEscapeUtils.unescapeXml(resultText)
                val rowDoc: Document = parseXml(unescapedXml)
                rowNodes = rowDoc.getElementsByTagName("ROW")
            }
        }
        logger.info("Found {} <ROW> elements.", rowNodes.length)
        val rs = createResultSetFromRowNodes(rowNodes)
        lastResultSet = rs
        return rs
    }

    override fun close() {
        logger.info("Statement closed")
    }

    // --- Stub implementations ---

    override fun setMaxRows(max: Int) { /* no-op */ }
    override fun setEscapeProcessing(enable: Boolean) =
        throw UnsupportedOperationException("Not implemented 168")
    override fun getMaxRows(): Int = 0

    override fun execute(sql: String): Boolean {
        if (sql.trim().uppercase().startsWith("SELECT")) {
            lastResultSet = executeQuery(sql)
            return true
        } else {
            throw UnsupportedOperationException("Only SELECT queries are supported.")
        }
    }

    override fun executeUpdate(sql: String): Int =
        throw UnsupportedOperationException("executeUpdate not supported 170")

    override fun getWarnings(): SQLWarning? = null

    override fun clearWarnings() =
        throw UnsupportedOperationException("Not implemented 172")
    override fun setCursorName(name: String?) =
        throw UnsupportedOperationException("Not implemented 173")
    override fun setQueryTimeout(seconds: Int) =
        throw UnsupportedOperationException("Not implemented 174")
    override fun getQueryTimeout(): Int =
        throw UnsupportedOperationException("Not implemented 175")
    override fun cancel() =
        throw UnsupportedOperationException("Not implemented 176")
    override fun getResultSet(): ResultSet? = lastResultSet
    override fun getUpdateCount(): Int = -1
    override fun getMoreResults(): Boolean = false
    override fun setFetchDirection(direction: Int) =
        throw UnsupportedOperationException("Not implemented 180")
    override fun getFetchDirection(): Int =
        throw UnsupportedOperationException("Not implemented 181")
    override fun setFetchSize(rows: Int) {
        fetchSize = rows
        logger.info("Fetch size set to {}", rows)
    }
    override fun getFetchSize(): Int = fetchSize
    override fun getResultSetConcurrency(): Int =
        throw UnsupportedOperationException("Not implemented 184")
    override fun getResultSetType(): Int =
        throw UnsupportedOperationException("Not implemented 185")
    override fun addBatch(sql: String) =
        throw UnsupportedOperationException("Not implemented 186")
    override fun clearBatch() =
        throw UnsupportedOperationException("Not implemented 187")
    override fun executeBatch(): IntArray =
        throw UnsupportedOperationException("Not implemented 188")
    override fun getConnection(): Connection =
        throw UnsupportedOperationException("Not implemented 189")
    override fun getMoreResults(current: Int): Boolean =
        throw UnsupportedOperationException("Not implemented 190")
    override fun getGeneratedKeys(): ResultSet =
        throw UnsupportedOperationException("Not implemented 191")
    override fun executeUpdate(sql: String, autoGeneratedKeys: Int): Int =
        throw UnsupportedOperationException("Not implemented 192")
    override fun executeUpdate(sql: String, columnIndexes: IntArray): Int =
        throw UnsupportedOperationException("Not implemented 193")
    override fun executeUpdate(sql: String, columnNames: Array<out String>?): Int =
        throw UnsupportedOperationException("Not implemented 194")
    override fun getMaxFieldSize(): Int =
        throw UnsupportedOperationException("Not implemented 195")
    override fun setMaxFieldSize(max: Int) =
        throw UnsupportedOperationException("Not implemented 196")
    override fun execute(sql: String, autoGeneratedKeys: Int): Boolean =
        throw UnsupportedOperationException("Not implemented 197")
    override fun execute(sql: String, columnIndexes: IntArray): Boolean =
        throw UnsupportedOperationException("Not implemented 198")
    override fun execute(sql: String, columnNames: Array<out String>?): Boolean =
        throw UnsupportedOperationException("Not implemented 199")
    override fun getResultSetHoldability(): Int =
        throw UnsupportedOperationException("Not implemented 200")
    override fun isClosed(): Boolean = false
    override fun setPoolable(poolable: Boolean) =
        throw UnsupportedOperationException("Not implemented 201")
    override fun isPoolable(): Boolean =
        throw UnsupportedOperationException("Not implemented 202")
    override fun closeOnCompletion() =
        throw UnsupportedOperationException("Not implemented 203")
    override fun isCloseOnCompletion(): Boolean =
        throw UnsupportedOperationException("Not implemented 204")
    override fun <T : Any?> unwrap(iface: Class<T>): T =
        throw UnsupportedOperationException("Not implemented 205")
    override fun isWrapperFor(iface: Class<*>): Boolean =
        throw UnsupportedOperationException("Not implemented 206")
}
