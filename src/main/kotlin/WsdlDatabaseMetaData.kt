package my.jdbc.wsdl_driver

import org.apache.commons.text.StringEscapeUtils
import org.slf4j.LoggerFactory
import org.w3c.dom.Node
import java.sql.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Logger for the WSDL driver
 */
//private val logger = LoggerFactory.getLogger("WsdlDriver")

/**
 * Read-write lock for thread-safe access to the local DuckDB metadata cache.
 * Allows multiple concurrent reads but exclusive writes.
 */
private val cacheLock = ReentrantReadWriteLock()

object LocalMetadataCache {
    // Use a file path in the user's home directory with better error handling
    private val userHome = System.getProperty("user.home")
        ?: throw IllegalStateException("user.home system property not set")
    private val duckDbFilePath = "$userHome/metadata.db"

    val connection: Connection by lazy {
        try {
            logger.info("Initializing DuckDB metadata cache at: $duckDbFilePath")
            val conn = DriverManager.getConnection("jdbc:duckdb:$duckDbFilePath")

            // Set connection properties for better performance
            conn.autoCommit = false

            conn.createStatement().use { stmt ->
                // Create tables with better indexing and constraints
                stmt.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS SCHEMAS_CACHE (
                        TABLE_SCHEM VARCHAR PRIMARY KEY,
                        TABLE_CATALOG VARCHAR,
                        CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """.trimIndent())

                stmt.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS CACHED_TABLES (
                        TABLE_CAT VARCHAR,
                        TABLE_SCHEM VARCHAR NOT NULL,
                        TABLE_NAME VARCHAR NOT NULL,
                        TABLE_TYPE VARCHAR,
                        REMARKS VARCHAR,
                        TYPE_CAT VARCHAR,
                        TYPE_SCHEM VARCHAR,
                        TYPE_NAME VARCHAR,
                        SELF_REFERENCING_COL_NAME VARCHAR,
                        REF_GENERATION VARCHAR,
                        CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (TABLE_SCHEM, TABLE_NAME)
                    )
                """.trimIndent())

                stmt.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS CACHED_COLUMNS (
                        TABLE_CAT VARCHAR,
                        TABLE_SCHEM VARCHAR NOT NULL,
                        TABLE_NAME VARCHAR NOT NULL,
                        COLUMN_NAME VARCHAR NOT NULL,
                        DATA_TYPE VARCHAR,
                        TYPE_NAME VARCHAR,
                        COLUMN_SIZE VARCHAR,
                        DECIMAL_DIGITS INTEGER,
                        NUM_PREC_RADIX INTEGER,
                        NULLABLE INTEGER,
                        ORDINAL_POSITION INTEGER,
                        CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (TABLE_SCHEM, TABLE_NAME, COLUMN_NAME)
                    )
                """.trimIndent())

                stmt.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS CACHED_INDEXES (
                        TABLE_CAT VARCHAR,
                        TABLE_SCHEM VARCHAR NOT NULL,
                        TABLE_NAME VARCHAR NOT NULL,
                        NON_UNIQUE VARCHAR,
                        INDEX_QUALIFIER VARCHAR,
                        INDEX_NAME VARCHAR NOT NULL,
                        TYPE VARCHAR,
                        ORDINAL_POSITION INTEGER,
                        COLUMN_NAME VARCHAR NOT NULL,
                        ASC_OR_DESC VARCHAR,
                        CARDINALITY BIGINT,
                        PAGES INTEGER,
                        FILTER_CONDITION VARCHAR,
                        CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (TABLE_SCHEM, TABLE_NAME, INDEX_NAME, COLUMN_NAME)
                    )
                """.trimIndent())

                // Create indexes for better query performance
                stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_cached_tables_type ON CACHED_TABLES(TABLE_TYPE)")
                stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_cached_columns_table ON CACHED_COLUMNS(TABLE_SCHEM, TABLE_NAME)")
                stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_cached_indexes_table ON CACHED_INDEXES(TABLE_SCHEM, TABLE_NAME)")
            }

            conn.commit()
            logger.info("DuckDB metadata cache initialized successfully")
            conn
        } catch (e: Exception) {
            logger.error("Failed to initialize DuckDB metadata cache", e)
            throw SQLException("Failed to initialize metadata cache", e)
        }
    }

    fun clearExpiredCache(maxAgeHours: Int = 24) {
        try {
            cacheLock.write {
                connection.createStatement().use { stmt ->
                    val cutoffTime = "datetime('now', '-$maxAgeHours hours')"
                    val tables = arrayOf("CACHED_TABLES", "CACHED_COLUMNS", "CACHED_INDEXES", "SCHEMAS_CACHE")

                    for (table in tables) {
                        val deleted = stmt.executeUpdate("DELETE FROM $table WHERE CREATED_AT < $cutoffTime")
                        if (deleted > 0) {
                            logger.info("Cleared $deleted expired entries from $table")
                        }
                    }
                    connection.commit()
                }
            }
        } catch (e: Exception) {
            logger.error("Error clearing expired cache entries", e)
            try {
                connection.rollback()
            } catch (rollbackEx: Exception) {
                logger.error("Error rolling back cache cleanup transaction", rollbackEx)
            }
        }
    }

    fun clearAllCache() {
        try {
            cacheLock.write {
                connection.createStatement().use { stmt ->
                    val tables = arrayOf("CACHED_TABLES", "CACHED_COLUMNS", "CACHED_INDEXES", "SCHEMAS_CACHE")

                    for (table in tables) {
                        val deleted = stmt.executeUpdate("DELETE FROM $table")
                        logger.info("Cleared all $deleted entries from $table")
                    }
                    connection.commit()
                    logger.info("All cache cleared successfully")
                }
            }
        } catch (e: Exception) {
            logger.error("Error clearing all cache entries", e)
            try {
                connection.rollback()
            } catch (rollbackEx: Exception) {
                logger.error("Error rolling back cache clear transaction", rollbackEx)
            }
        }
    }

    fun close() {
        try {
            cacheLock.write {
                if (!connection.isClosed) {
                    logger.info("Closing DuckDB connection")
                    connection.close()
                }
            }
        } catch (ex: Exception) {
            logger.error("Error closing DuckDB connection", ex)
        }
    }
}

class WsdlDatabaseMetaData(private val connection: WsdlConnection) : DatabaseMetaData {

    companion object {
        private const val DEFAULT_SCHEMA = "FUSION"
        private const val CACHE_BATCH_SIZE = 1000
        /** Page size used when fetching metadata via pagination. */
        @JvmField
        var METADATA_FETCH_SIZE: Int = 1000
    }

    override fun getDatabaseProductName(): String = "Oracle Fusion JDBC Driver"
    override fun getDatabaseProductVersion(): String = "1.0"
    override fun getDriverName(): String = "WSDL Oracle Fusion Driver"
    override fun getDriverVersion(): String = "1.0"
    override fun getDriverMajorVersion(): Int = 1
    override fun getDriverMinorVersion(): Int = 0
    override fun usesLocalFiles(): Boolean = false
    override fun usesLocalFilePerTable(): Boolean = false
    override fun supportsMixedCaseIdentifiers(): Boolean = false
    override fun storesUpperCaseIdentifiers(): Boolean = true
    override fun storesLowerCaseIdentifiers(): Boolean = false
    override fun storesMixedCaseIdentifiers(): Boolean = false
    override fun supportsMixedCaseQuotedIdentifiers(): Boolean = true
    override fun storesUpperCaseQuotedIdentifiers(): Boolean = false
    override fun storesLowerCaseQuotedIdentifiers(): Boolean = false
    override fun storesMixedCaseQuotedIdentifiers(): Boolean = true
    override fun getIdentifierQuoteString(): String = "\""
    override fun getSQLKeywords(): String = ""
    override fun getURL(): String = connection.wsdlEndpoint
    override fun getUserName(): String = connection.username
    override fun isReadOnly(): Boolean = true
    override fun nullsAreSortedHigh(): Boolean = true
    override fun nullsAreSortedLow(): Boolean = false
    override fun nullsAreSortedAtStart(): Boolean = false
    override fun nullsAreSortedAtEnd(): Boolean = true
    override fun allProceduresAreCallable(): Boolean = false
    override fun allTablesAreSelectable(): Boolean = true

    // Enhanced getTables with better error handling and performance
    override fun getTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        types: Array<out String>?
    ): ResultSet {
        val requestedTypes: Set<String>? = types?.filterNotNull()
            ?.map { it.uppercase() }
            ?.toSet()
            ?.takeIf { it.isNotEmpty() }

        // Log the request for debugging
        logger.debug("getTables called - catalog: $catalog, schema: $schemaPattern, tablePattern: $tableNamePattern, types: ${types?.joinToString()}")

        // Try cache first
        val cachedRows = getCachedTables(requestedTypes)
        if (cachedRows.isNotEmpty()) {
            logger.debug("Returning ${cachedRows.size} tables from cache")
            return XmlResultSet(cachedRows)
        }

        // Fetch from remote
        return fetchAndCacheTables(requestedTypes)
    }

    private fun getCachedTables(requestedTypes: Set<String>?): List<Map<String, String>> {
        val cachedRows = mutableListOf<Map<String, String>>()

        try {
            val typeFilter = requestedTypes?.joinToString(
                prefix = " AND TABLE_TYPE IN ('",
                separator = "','",
                postfix = "')"
            ) ?: ""

            cacheLock.read {
                LocalMetadataCache.connection.createStatement().use { stmt ->
                    val query = """
                        SELECT * FROM CACHED_TABLES
                        WHERE 1=1$typeFilter
                        ORDER BY TABLE_TYPE, TABLE_NAME
                    """
                    logger.debug("Cache query: $query")

                    stmt.executeQuery(query).use { rs ->
                        val meta = rs.metaData
                        while (rs.next()) {
                            val row = mutableMapOf<String, String>()
                            for (i in 1..meta.columnCount) {
                                val colName = meta.getColumnName(i).lowercase()
                                if (colName != "created_at") { // Exclude internal timestamp
                                    row[colName] = rs.getString(i) ?: ""
                                }
                            }
                            cachedRows.add(row)
                        }
                    }
                }
            }

            logger.debug("Found ${cachedRows.size} cached tables")
            if (cachedRows.isNotEmpty()) {
                logger.debug("Sample cached entries: ${cachedRows.take(5).map { "${it["table_name"]} (${it["table_type"]})" }}")
            }

        } catch (ex: Exception) {
            logger.error("Error reading cached tables", ex)
        }

        return cachedRows
    }

    private fun fetchAndCacheTables(requestedTypes: Set<String>?): ResultSet {
        // Build the type filter - if no specific types requested, get all common types
        val defaultTypes = setOf("TABLE", "VIEW", "SYNONYM")
        val typesToQuery = requestedTypes ?: defaultTypes

        logger.info("Fetching tables for types: ${typesToQuery.joinToString()}")

        // Use a comprehensive query that gets all object types
        val sql = """
            SELECT DISTINCT
                NULL AS TABLE_CAT,
                owner AS TABLE_SCHEM,
                UPPER(object_name) AS TABLE_NAME,
                UPPER(object_type) AS TABLE_TYPE,
                NULL AS REMARKS,
                NULL AS TYPE_CAT,
                NULL AS TYPE_SCHEM,
                NULL AS TYPE_NAME,
                NULL AS SELF_REFERENCING_COL_NAME,
                NULL AS REF_GENERATION
            FROM all_objects
            WHERE owner = '$DEFAULT_SCHEMA'
              AND object_type IN ('TABLE', 'VIEW', 'SYNONYM')
              AND status = 'VALID'
        """.trimIndent()

        return try {
            logger.debug("Executing remote getTables query with pagination")
            val remoteRows = fetchAllPages(
                wsdlEndpoint = connection.wsdlEndpoint,
                sql = sql,
                username = connection.username,
                password = connection.password,
                reportPath = connection.reportPath,
                fetchSize = METADATA_FETCH_SIZE,
                parsePage = this::parseTablesResponse
            )

            // Cache the results
            cacheTableRows(remoteRows)

            val resultRows = filterTableRowsByType(remoteRows, requestedTypes)
            XmlResultSet(resultRows.map { it.mapKeys { k -> k.key.lowercase() } })

        } catch (e: Exception) {
            logger.error("Error fetching tables from remote service", e)
            throw SQLException("Failed to fetch table metadata", e)
        }
    }

    private fun parseTablesResponse(responseXml: String): List<Map<String, String>> {
        val doc = parseXml(responseXml)
        var rowNodes = doc.getElementsByTagName("ROW")

        if (rowNodes.length == 0) {
            val resultNodes = doc.getElementsByTagName("RESULT")
            if (resultNodes.length > 0) {
                val resultText = resultNodes.item(0).textContent.trim()
                val unescapedXml = StringEscapeUtils.unescapeXml(resultText)
                val rowDoc = parseXml(unescapedXml)
                rowNodes = rowDoc.getElementsByTagName("ROW")
            }
        }

        val rows = mutableListOf<Map<String, String>>()
        for (i in 0 until rowNodes.length) {
            val rowNode = rowNodes.item(i)
            if (rowNode.nodeType == Node.ELEMENT_NODE) {
                val rowMap = mutableMapOf<String, String>()
                val children = rowNode.childNodes
                for (j in 0 until children.length) {
                    val child = children.item(j)
                    if (child.nodeType == Node.ELEMENT_NODE) {
                        rowMap[child.nodeName.uppercase()] = child.textContent.trim()
                    }
                }
                rows.add(rowMap)
            }
        }
        return rows
    }

    private fun deduplicateTableRows(rows: List<Map<String, String>>): Collection<Map<String, String>> {
        fun typePriority(type: String): Int = when (type.uppercase()) {
            "TABLE" -> 1
            "VIEW" -> 2
            "MATERIALIZED VIEW" -> 2  // Treat materialized views same as views
            "SYNONYM" -> 3
            "GLOBAL TEMPORARY", "LOCAL TEMPORARY" -> 4
            else -> 5
        }

        return rows.groupBy { "${it["TABLE_SCHEM"]}|${it["TABLE_NAME"]}" }
            .map { (_, dupes) -> dupes.minBy { typePriority(it["TABLE_TYPE"] ?: "") } }
    }

    private fun cacheTableRows(rows: Collection<Map<String, String>>) {
        try {
            cacheLock.write {
                LocalMetadataCache.connection.prepareStatement("""
                    INSERT OR IGNORE INTO CACHED_TABLES 
                    (TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, TYPE_CAT, TYPE_SCHEM, TYPE_NAME, SELF_REFERENCING_COL_NAME, REF_GENERATION) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """).use { pstmt ->
                    //val rowsToCache = filterTableRowsByType(rows, requestedTypes)
                    //rowsToCache.forEachIndexed
                    rows.forEachIndexed { index, row ->
                        pstmt.setString(1, row["TABLE_CAT"] ?: "")
                        pstmt.setString(2, row["TABLE_SCHEM"] ?: "")
                        pstmt.setString(3, row["TABLE_NAME"] ?: "")
                        pstmt.setString(4, row["TABLE_TYPE"] ?: "")
                        pstmt.setString(5, row["REMARKS"] ?: "")
                        pstmt.setString(6, row["TYPE_CAT"] ?: "")
                        pstmt.setString(7, row["TYPE_SCHEM"] ?: "")
                        pstmt.setString(8, row["TYPE_NAME"] ?: "")
                        pstmt.setString(9, row["SELF_REFERENCING_COL_NAME"] ?: "")
                        pstmt.setString(10, row["REF_GENERATION"] ?: "")
                        pstmt.addBatch()

                        // Execute batch periodically to avoid memory issues
                        if ((index + 1) % CACHE_BATCH_SIZE == 0) {
                            pstmt.executeBatch()
                        }
                    }
                    pstmt.executeBatch() // Execute remaining
                    LocalMetadataCache.connection.commit()
                    logger.debug("Cached ${rows.size} table rows")
                }
            }
        } catch (ex: Exception) {
            logger.error("Error caching table metadata", ex)
            try {
                LocalMetadataCache.connection.rollback()
            } catch (rollbackEx: Exception) {
                logger.error("Error rolling back cache transaction", rollbackEx)
            }
        }
    }

    private fun filterTableRowsByType(rows: Collection<Map<String, String>>, requestedTypes: Set<String>?): Collection<Map<String, String>> {
        return if (requestedTypes == null) {
            rows
        } else {
            rows.filter { requestedTypes.contains(it["TABLE_TYPE"]?.uppercase()) }
        }
    }

    // Enhanced getColumns with similar improvements
    override fun getColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        val schema = schemaPattern ?: DEFAULT_SCHEMA

        // Try cache first
        val cachedRows = getCachedColumns(schema, tableNamePattern, columnNamePattern)
        if (cachedRows.isNotEmpty()) {
            logger.debug("Returning ${cachedRows.size} columns from cache")
            return XmlResultSet(cachedRows)
        }

        // Fetch from remote
        return fetchAndCacheColumns(schema, tableNamePattern, columnNamePattern)
    }

    private fun getCachedColumns(
        schema: String,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): List<Map<String, String?>> {
        val cachedRows = mutableListOf<Map<String, String?>>()

        try {
            val schemaCondition = " AND TABLE_SCHEM = '${schema.uppercase()}'"
            val tableCondition = if (!tableNamePattern.isNullOrBlank())
                " AND TABLE_NAME LIKE '${tableNamePattern.uppercase()}'" else ""
            val columnCondition = if (!columnNamePattern.isNullOrBlank())
                " AND COLUMN_NAME LIKE '${columnNamePattern.uppercase()}'" else ""

            val query = """
                SELECT DISTINCT
                    TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE, TYPE_NAME, 
                    COLUMN_SIZE, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE, ORDINAL_POSITION 
                FROM CACHED_COLUMNS
                WHERE 1=1 $schemaCondition $tableCondition $columnCondition
                ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
            """.trimIndent()

            cacheLock.read {
                LocalMetadataCache.connection.createStatement().use { stmt ->
                    stmt.executeQuery(query).use { rs ->
                        while (rs.next()) {
                            val row = mutableMapOf<String, String?>()
                            row["table_cat"] = rs.getString("TABLE_CAT") ?: ""
                            row["table_schem"] = rs.getString("TABLE_SCHEM") ?: ""
                            row["table_name"] = rs.getString("TABLE_NAME") ?: ""
                            row["column_name"] = rs.getString("COLUMN_NAME") ?: ""
                            row["data_type"] = rs.getString("DATA_TYPE") ?: ""
                            row["type_name"] = rs.getString("TYPE_NAME") ?: ""
                            row["column_size"] = rs.getString("COLUMN_SIZE") ?: ""
                            row["decimal_digits"] = rs.getString("DECIMAL_DIGITS") ?: "0"
                            row["num_prec_radix"] = rs.getString("NUM_PREC_RADIX")
                            row["nullable"] = rs.getString("NULLABLE") ?: ""
                            row["ordinal_position"] = rs.getString("ORDINAL_POSITION") ?: ""
                            cachedRows.add(row)
                        }
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error("Error reading columns from cache", ex)
        }

        return cachedRows
    }

    private fun fetchAndCacheColumns(
        schema: String,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        val schemaCondition = " AND owner = '${schema.uppercase()}'"
        val tableCondition = if (!tableNamePattern.isNullOrBlank())
            " AND table_name LIKE '${tableNamePattern.uppercase()}'" else ""
        val columnCondition = if (!columnNamePattern.isNullOrBlank())
            " AND column_name LIKE '${columnNamePattern.uppercase()}'" else ""

        val sql = """
            SELECT 
                NULL AS TABLE_CAT,
                owner AS TABLE_SCHEM,
                table_name AS TABLE_NAME,
                column_name AS COLUMN_NAME,
                ${Types.VARCHAR} AS DATA_TYPE,
                data_type AS TYPE_NAME,
                data_length AS COLUMN_SIZE,
                data_scale AS DECIMAL_DIGITS,
                CASE WHEN data_precision IS NULL AND data_scale IS NULL
                     THEN NULL ELSE 10 END AS NUM_PREC_RADIX,
                CASE WHEN nullable = 'Y' THEN 1 ELSE 0 END AS NULLABLE,
                column_id AS ORDINAL_POSITION
            FROM all_tab_columns
            WHERE 1=1 $schemaCondition $tableCondition $columnCondition
            ORDER BY owner, table_name, column_id
        """.trimIndent()

        return try {
            val remoteRows = fetchAllPages(
                wsdlEndpoint = connection.wsdlEndpoint,
                sql = sql,
                username = connection.username,
                password = connection.password,
                reportPath = connection.reportPath,
                fetchSize = METADATA_FETCH_SIZE,
                parsePage = this::parseColumnsResponse
            )
            val uniqueRows = deduplicateColumnRows(remoteRows)

            // Cache the results
            cacheColumnRows(uniqueRows)

            XmlResultSet(uniqueRows)
        } catch (e: Exception) {
            logger.error("Error fetching columns from remote service", e)
            throw SQLException("Failed to fetch column metadata", e)
        }
    }

    private fun parseColumnsResponse(responseXml: String): List<Map<String, String?>> {
        val doc = parseXml(responseXml)
        var rowNodes = doc.getElementsByTagName("ROW")

        if (rowNodes.length == 0) {
            val resultNodes = doc.getElementsByTagName("RESULT")
            if (resultNodes.length > 0) {
                val resultText = resultNodes.item(0).textContent.trim()
                val unescapedXml = StringEscapeUtils.unescapeXml(resultText)
                val rowDoc = parseXml(unescapedXml)
                rowNodes = rowDoc.getElementsByTagName("ROW")
            }
        }

        val rows = mutableListOf<Map<String, String?>>()
        for (i in 0 until rowNodes.length) {
            val rowNode = rowNodes.item(i)
            if (rowNode.nodeType == Node.ELEMENT_NODE) {
                val rowMap = mutableMapOf<String, String?>()
                val children = rowNode.childNodes
                for (j in 0 until children.length) {
                    val child = children.item(j)
                    if (child.nodeType == Node.ELEMENT_NODE) {
                        val text = child.textContent.trim()
                        rowMap[child.nodeName.lowercase()] = text.takeIf { it.isNotEmpty() }
                    }
                }
                rows.add(rowMap)
            }
        }
        return rows
    }

    private fun deduplicateColumnRows(rows: List<Map<String, String?>>): List<Map<String, String?>> {
        return rows.groupBy {
            val schema = it["table_schem"]?.uppercase() ?: ""
            val table = it["table_name"]?.uppercase() ?: ""
            val column = it["column_name"]?.replace("\"", "")?.uppercase() ?: ""
            "$schema|$table|$column"
        }.map { (_, dupes) -> dupes.first() }
    }

    private fun cacheColumnRows(rows: List<Map<String, String?>>) {
        try {
            cacheLock.write {
                LocalMetadataCache.connection.prepareStatement("""
                    INSERT OR IGNORE INTO CACHED_COLUMNS 
                    (TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_SIZE, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE, ORDINAL_POSITION)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """).use { pstmt ->
                    rows.forEachIndexed { index, row ->
                        pstmt.setString(1, row["table_cat"] ?: "")
                        pstmt.setString(2, row["table_schem"] ?: "")
                        pstmt.setString(3, row["table_name"] ?: "")
                        pstmt.setString(4, row["column_name"] ?: "")
                        pstmt.setString(5, row["data_type"] ?: "")
                        pstmt.setString(6, row["type_name"] ?: "")
                        pstmt.setString(7, row["column_size"] ?: "")
                        pstmt.setObject(8, row["decimal_digits"]?.takeIf { it.isNotBlank() }?.toIntOrNull())
                        pstmt.setObject(9, row["num_prec_radix"]?.takeIf { it.isNotBlank() }?.toIntOrNull())
                        pstmt.setString(10, row["nullable"] ?: "")
                        pstmt.setString(11, row["ordinal_position"] ?: "")
                        pstmt.addBatch()

                        if ((index + 1) % CACHE_BATCH_SIZE == 0) {
                            pstmt.executeBatch()
                        }
                    }
                    pstmt.executeBatch()
                    LocalMetadataCache.connection.commit()
                    logger.debug("Cached ${rows.size} column rows")
                }
            }
        } catch (ex: Exception) {
            logger.error("Error caching column metadata", ex)
            try {
                LocalMetadataCache.connection.rollback()
            } catch (rollbackEx: Exception) {
                logger.error("Error rolling back cache transaction", rollbackEx)
            }
        }
    }

    // Helper method to create empty result sets
    private fun createEmptyResultSet(): ResultSet {
        return XmlResultSet(emptyList())
    }

    // Remaining methods from original implementation...
    override fun getExtraNameCharacters(): String = ""
    override fun supportsAlterTableWithAddColumn(): Boolean = false
    override fun supportsAlterTableWithDropColumn(): Boolean = false
    override fun supportsColumnAliasing(): Boolean = true
    override fun nullPlusNonNullIsNull(): Boolean = true
    override fun supportsConvert(): Boolean = true
    override fun supportsConvert(fromType: Int, toType: Int): Boolean = true
    override fun supportsTableCorrelationNames(): Boolean = false
    override fun supportsDifferentTableCorrelationNames(): Boolean = false
    override fun supportsExpressionsInOrderBy(): Boolean = true
    override fun supportsOrderByUnrelated(): Boolean = true
    override fun supportsGroupBy(): Boolean = true
    override fun supportsGroupByUnrelated(): Boolean = true
    override fun supportsGroupByBeyondSelect(): Boolean = false
    override fun supportsLikeEscapeClause(): Boolean = true
    override fun supportsMultipleResultSets(): Boolean = false
    override fun supportsMultipleTransactions(): Boolean = false
    override fun supportsNonNullableColumns(): Boolean = true
    override fun supportsMinimumSQLGrammar(): Boolean = true
    override fun supportsCoreSQLGrammar(): Boolean = true
    override fun supportsExtendedSQLGrammar(): Boolean = false
    override fun supportsANSI92EntryLevelSQL(): Boolean = true
    override fun supportsANSI92IntermediateSQL(): Boolean = false
    override fun supportsANSI92FullSQL(): Boolean = false
    override fun supportsIntegrityEnhancementFacility(): Boolean = false
    override fun supportsOuterJoins(): Boolean = true
    override fun supportsFullOuterJoins(): Boolean = false
    override fun supportsLimitedOuterJoins(): Boolean = true
    override fun getSchemaTerm(): String = "SCHEMA"

    override fun getNumericFunctions(): String =
        "ABS,ACOS,ASIN,ATAN,ATAN2,CEIL,COS,COSH,DEGREES,EXP,FLOOR,LN,LOG,LOG10,MOD," +
                "POWER,RADIANS,ROUND,SIGN,SIN,SINH,SQRT,TAN,TANH,TRUNC"

    override fun getStringFunctions(): String =
        "ASCII,CHR,CONCAT,INITCAP,INSTR,LENGTH,LTRIM,LOWER,LPAD,RPAD," +
                "REPLACE,RTRIM,SOUNDEX,SUBSTR,TRANSLATE,TRIM,UPPER"

    override fun getSystemFunctions(): String =
        "USER,UID,SYSDATE,SYSTIMESTAMP,CURRENT_DATE,CURRENT_TIMESTAMP," +
                "LOCALTIMESTAMP,DBTIMEZONE,SESSIONTIMEZONE,SYS_GUID"

    override fun getTimeDateFunctions(): String =
        "CURRENT_DATE,CURRENT_TIMESTAMP,LOCALTIMESTAMP,LOCALTIME,SYSDATE,SYSTIMESTAMP," +
                "ADD_MONTHS,EXTRACT,LAST_DAY,NEXT_DAY,MONTHS_BETWEEN,ROUND,TRUNC,NEW_TIME," +
                "TZ_OFFSET,SESSIONTIMEZONE,DBTIMEZONE"

    override fun getSearchStringEscape(): String = "\\"
    override fun getProcedureTerm(): String = "PROCEDURE"
    override fun getCatalogTerm(): String = ""
    override fun isCatalogAtStart(): Boolean = false
    override fun getCatalogSeparator(): String = "."

    // Schema and catalog support methods
    override fun supportsSchemasInDataManipulation(): Boolean = false
    override fun supportsSchemasInProcedureCalls(): Boolean = false
    override fun supportsSchemasInTableDefinitions(): Boolean = false
    override fun supportsSchemasInIndexDefinitions(): Boolean = false
    override fun supportsSchemasInPrivilegeDefinitions(): Boolean = false
    override fun supportsCatalogsInDataManipulation(): Boolean = false
    override fun supportsCatalogsInProcedureCalls(): Boolean = false
    override fun supportsCatalogsInTableDefinitions(): Boolean = false
    override fun supportsCatalogsInIndexDefinitions(): Boolean = false
    override fun supportsCatalogsInPrivilegeDefinitions(): Boolean = false

    // Transaction and cursor support
    override fun supportsPositionedDelete(): Boolean = false
    override fun supportsPositionedUpdate(): Boolean = false
    override fun supportsSelectForUpdate(): Boolean = false
    override fun supportsStoredProcedures(): Boolean = false
    override fun supportsSubqueriesInComparisons(): Boolean = true
    override fun supportsSubqueriesInExists(): Boolean = true
    override fun supportsSubqueriesInIns(): Boolean = true
    override fun supportsSubqueriesInQuantifieds(): Boolean = true
    override fun supportsCorrelatedSubqueries(): Boolean = true
    override fun supportsUnion(): Boolean = true
    override fun supportsUnionAll(): Boolean = true
    override fun supportsOpenCursorsAcrossCommit(): Boolean = true
    override fun supportsOpenCursorsAcrossRollback(): Boolean = false
    override fun supportsOpenStatementsAcrossCommit(): Boolean = false
    override fun supportsOpenStatementsAcrossRollback(): Boolean = false

    // Limits and constraints
    override fun getMaxBinaryLiteralLength(): Int = 0
    override fun getMaxCharLiteralLength(): Int = 0
    override fun getMaxColumnNameLength(): Int = 0
    override fun getMaxColumnsInGroupBy(): Int = 0
    override fun getMaxColumnsInIndex(): Int = 0
    override fun getMaxColumnsInOrderBy(): Int = 0
    override fun getMaxColumnsInSelect(): Int = 0
    override fun getMaxColumnsInTable(): Int = 0
    override fun getMaxConnections(): Int = 0
    override fun getMaxCursorNameLength(): Int = 0
    override fun getMaxIndexLength(): Int = 0
    override fun getMaxSchemaNameLength(): Int = 0
    override fun getMaxProcedureNameLength(): Int = 0
    override fun getMaxCatalogNameLength(): Int = 0
    override fun getMaxRowSize(): Int = 0
    override fun doesMaxRowSizeIncludeBlobs(): Boolean = false
    override fun getMaxStatementLength(): Int = 0
    override fun getMaxStatements(): Int = 0
    override fun getMaxTableNameLength(): Int = 0
    override fun getMaxTablesInSelect(): Int = 0
    override fun getMaxUserNameLength(): Int = 0

    // Transaction support
    override fun getDefaultTransactionIsolation(): Int = Connection.TRANSACTION_NONE
    override fun supportsTransactions(): Boolean = false
    override fun supportsTransactionIsolationLevel(level: Int): Boolean = false
    override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean = false
    override fun supportsDataManipulationTransactionsOnly(): Boolean = false
    override fun dataDefinitionCausesTransactionCommit(): Boolean = false
    override fun dataDefinitionIgnoredInTransactions(): Boolean = false

    // Metadata methods that return empty result sets
    override fun getProcedures(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?
    ): ResultSet = createEmptyResultSet()

    override fun getProcedureColumns(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet = createEmptyResultSet()

    override fun getSchemas(): ResultSet = createEmptyResultSet()
    override fun getSchemas(catalog: String?, schemaPattern: String?): ResultSet = createEmptyResultSet()
    override fun getCatalogs(): ResultSet = createEmptyResultSet()

    override fun getTableTypes(): ResultSet {
        val typesList = listOf(
            mapOf("table_type" to "TABLE"),
            mapOf("table_type" to "VIEW"),
            mapOf("table_type" to "SYNONYM"),
            mapOf("table_type" to "MATERIALIZED VIEW"),
            mapOf("table_type" to "GLOBAL TEMPORARY"),
            mapOf("table_type" to "LOCAL TEMPORARY")
        )
        return XmlResultSet(typesList)
    }

    override fun getColumnPrivileges(
        catalog: String?, schema: String?, table: String?, columnNamePattern: String?
    ): ResultSet = createEmptyResultSet()

    override fun getTablePrivileges(
        catalog: String?, schemaPattern: String?, tableNamePattern: String?
    ): ResultSet = createEmptyResultSet()

    override fun getBestRowIdentifier(
        catalog: String?, schema: String?, table: String?, scope: Int, nullable: Boolean
    ): ResultSet = createEmptyResultSet()

    override fun getVersionColumns(
        catalog: String?, schema: String?, table: String?
    ): ResultSet = createEmptyResultSet()

    override fun getPrimaryKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        val schema = schema ?: DEFAULT_SCHEMA
        val sql = """
            SELECT 
                NULL AS TABLE_CAT,
                c.owner AS TABLE_SCHEM,
                c.table_name AS TABLE_NAME,
                cc.column_name AS COLUMN_NAME,
                cc.position AS KEY_SEQ,
                c.constraint_name AS PK_NAME
            FROM all_constraints c
            JOIN all_cons_columns cc 
                ON c.constraint_name = cc.constraint_name
                AND c.owner = cc.owner
                AND c.table_name = cc.table_name
            WHERE c.constraint_type = 'P'
            AND c.owner = '${schema.uppercase()}'
            ${if (!table.isNullOrBlank()) "AND c.table_name = '${table.uppercase()}'" else ""}
            ORDER BY c.owner, c.table_name, cc.position
        """.trimIndent()

        return try {
            val remoteRows = fetchAllPages(
                wsdlEndpoint = connection.wsdlEndpoint,
                sql = sql,
                username = connection.username,
                password = connection.password,
                reportPath = connection.reportPath,
                fetchSize = METADATA_FETCH_SIZE,
                parsePage = { xml ->
                    val doc = parseXml(xml)
                    val rowNodes = doc.getElementsByTagName("ROW")
                    val resultRows = mutableListOf<Map<String, String>>()
                    for (i in 0 until rowNodes.length) {
                        val rowNode = rowNodes.item(i)
                        if (rowNode.nodeType == Node.ELEMENT_NODE) {
                            val rowMap = mutableMapOf<String, String>()
                            val children = rowNode.childNodes
                            for (j in 0 until children.length) {
                                val child = children.item(j)
                                if (child.nodeType == Node.ELEMENT_NODE) {
                                    rowMap[child.nodeName.lowercase()] = child.textContent.trim()
                                }
                            }
                            resultRows.add(rowMap)
                        }
                    }
                    resultRows
                }
            )

            XmlResultSet(remoteRows)
        } catch (e: Exception) {
            logger.error("Error fetching primary keys", e)
            createEmptyResultSet()
        }
    }

    override fun getImportedKeys(
        catalog: String?, schema: String?, table: String?
    ): ResultSet = createEmptyResultSet()

    override fun getExportedKeys(
        catalog: String?, schema: String?, table: String?
    ): ResultSet = createEmptyResultSet()

    override fun getCrossReference(
        parentCatalog: String?,
        parentSchema: String?,
        parentTable: String?,
        foreignCatalog: String?,
        foreignSchema: String?,
        foreignTable: String?
    ): ResultSet = createEmptyResultSet()

    override fun getTypeInfo(): ResultSet {
        val types: List<Map<String, Any?>> = listOf(
            mapOf(
                "type_name" to "CHAR",
                "data_type" to Types.CHAR,
                "precision" to 2000,
                "literal_prefix" to "'",
                "literal_suffix" to "'",
                "create_params" to "length",
                "nullable" to DatabaseMetaData.typeNullable,
                "case_sensitive" to true,
                "searchable" to DatabaseMetaData.typeSearchable,
                "unsigned_attribute" to false,
                "fixed_prec_scale" to false,
                "auto_increment" to false,
                "local_type_name" to "CHAR",
                "minimum_scale" to 0,
                "maximum_scale" to 0,
                "sql_data_type" to 0,
                "sql_datetime_sub" to 0,
                "num_prec_radix" to 10
            ),
            mapOf(
                "type_name" to "VARCHAR2",
                "data_type" to Types.VARCHAR,
                "precision" to 4000,
                "literal_prefix" to "'",
                "literal_suffix" to "'",
                "create_params" to "length",
                "nullable" to DatabaseMetaData.typeNullable,
                "case_sensitive" to true,
                "searchable" to DatabaseMetaData.typeSearchable,
                "unsigned_attribute" to false,
                "fixed_prec_scale" to false,
                "auto_increment" to false,
                "local_type_name" to "VARCHAR2",
                "minimum_scale" to 0,
                "maximum_scale" to 0,
                "sql_data_type" to 0,
                "sql_datetime_sub" to 0,
                "num_prec_radix" to 10
            ),
            mapOf(
                "type_name" to "NUMBER",
                "data_type" to Types.NUMERIC,
                "precision" to 38,
                "literal_prefix" to null,
                "literal_suffix" to null,
                "create_params" to "precision,scale",
                "nullable" to DatabaseMetaData.typeNullable,
                "case_sensitive" to false,
                "searchable" to DatabaseMetaData.typeSearchable,
                "unsigned_attribute" to false,
                "fixed_prec_scale" to false,
                "auto_increment" to false,
                "local_type_name" to "NUMBER",
                "minimum_scale" to -84,
                "maximum_scale" to 127,
                "sql_data_type" to 0,
                "sql_datetime_sub" to 0,
                "num_prec_radix" to 10
            ),
            mapOf(
                "type_name" to "DATE",
                "data_type" to Types.DATE,
                "precision" to 7,
                "literal_prefix" to "'",
                "literal_suffix" to "'",
                "create_params" to null,
                "nullable" to DatabaseMetaData.typeNullable,
                "case_sensitive" to false,
                "searchable" to DatabaseMetaData.typeSearchable,
                "unsigned_attribute" to false,
                "fixed_prec_scale" to false,
                "auto_increment" to false,
                "local_type_name" to "DATE",
                "minimum_scale" to 0,
                "maximum_scale" to 0,
                "sql_data_type" to 0,
                "sql_datetime_sub" to 0,
                "num_prec_radix" to 10
            ),
            mapOf(
                "type_name" to "TIMESTAMP",
                "data_type" to Types.TIMESTAMP,
                "precision" to 11,
                "literal_prefix" to "'",
                "literal_suffix" to "'",
                "create_params" to "precision",
                "nullable" to DatabaseMetaData.typeNullable,
                "case_sensitive" to false,
                "searchable" to DatabaseMetaData.typeSearchable,
                "unsigned_attribute" to false,
                "fixed_prec_scale" to false,
                "auto_increment" to false,
                "local_type_name" to "TIMESTAMP",
                "minimum_scale" to 0,
                "maximum_scale" to 9,
                "sql_data_type" to 0,
                "sql_datetime_sub" to 0,
                "num_prec_radix" to 10
            ),
            mapOf(
                "type_name" to "CLOB",
                "data_type" to Types.CLOB,
                "precision" to Int.MAX_VALUE,
                "literal_prefix" to null,
                "literal_suffix" to null,
                "create_params" to null,
                "nullable" to DatabaseMetaData.typeNullable,
                "case_sensitive" to true,
                "searchable" to DatabaseMetaData.typePredNone,
                "unsigned_attribute" to false,
                "fixed_prec_scale" to false,
                "auto_increment" to false,
                "local_type_name" to "CLOB",
                "minimum_scale" to 0,
                "maximum_scale" to 0,
                "sql_data_type" to 0,
                "sql_datetime_sub" to 0,
                "num_prec_radix" to 10
            ),
            mapOf(
                "type_name" to "BLOB",
                "data_type" to Types.BLOB,
                "precision" to Int.MAX_VALUE,
                "literal_prefix" to null,
                "literal_suffix" to null,
                "create_params" to null,
                "nullable" to DatabaseMetaData.typeNullable,
                "case_sensitive" to false,
                "searchable" to DatabaseMetaData.typePredNone,
                "unsigned_attribute" to false,
                "fixed_prec_scale" to false,
                "auto_increment" to false,
                "local_type_name" to "BLOB",
                "minimum_scale" to 0,
                "maximum_scale" to 0,
                "sql_data_type" to 0,
                "sql_datetime_sub" to 0,
                "num_prec_radix" to 10
            )
        )

        val rows = types.map { row ->
            row.mapValues { it.value?.toString() ?: "" }
        }
        return XmlResultSet(rows)
    }

    override fun getIndexInfo(
        catalog: String?,
        schema: String?,
        table: String?,
        unique: Boolean,
        approximate: Boolean
    ): ResultSet {
        if (table.isNullOrBlank()) {
            return createEmptyResultSet()
        }

        val schema = schema ?: DEFAULT_SCHEMA
        val tableName = table.uppercase()

        // Try cache first
        val cachedRows = getCachedIndexInfo(schema, tableName, unique)
        if (cachedRows.isNotEmpty()) {
            logger.debug("Returning ${cachedRows.size} indexes from cache")
            return XmlResultSet(cachedRows)
        }

        // Fetch from remote
        return fetchAndCacheIndexInfo(schema, tableName, unique)
    }

    private fun getCachedIndexInfo(schema: String, tableName: String, unique: Boolean): List<Map<String, String>> {
        val cachedRows = mutableListOf<Map<String, String>>()

        try {
            val schemaCondition = "AND TABLE_SCHEM = '${schema.uppercase()}'"
            val tableCondition = "AND upper(TABLE_NAME) = '$tableName'"
            val uniqueCondition = if (unique) "AND NON_UNIQUE = '0'" else ""

            val query = """
                SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME,
                       NON_UNIQUE, INDEX_QUALIFIER, INDEX_NAME,
                       TYPE, ORDINAL_POSITION, COLUMN_NAME,
                       ASC_OR_DESC, CARDINALITY, PAGES, FILTER_CONDITION
                FROM CACHED_INDEXES
                WHERE 1=1 $schemaCondition $tableCondition $uniqueCondition
                ORDER BY INDEX_NAME, ORDINAL_POSITION
            """.trimIndent()

            cacheLock.read {
                LocalMetadataCache.connection.createStatement().use { stmt ->
                    stmt.executeQuery(query).use { rs ->
                        val meta = rs.metaData
                        while (rs.next()) {
                            val row = mutableMapOf<String, String>()
                            for (i in 1..meta.columnCount) {
                                val colName = meta.getColumnName(i).lowercase()
                                if (colName != "created_at") { // Exclude internal timestamp
                                    val raw = rs.getString(i)
                                    val normalised = if (raw.isNullOrBlank() &&
                                        (colName == "cardinality" || colName == "pages" || colName == "ordinal_position"))
                                        "0"
                                    else
                                        raw ?: ""
                                    row[colName] = normalised
                                }
                            }
                            cachedRows.add(row)
                        }
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error("Error reading index info from cache", ex)
        }

        return cachedRows
    }

    private fun fetchAndCacheIndexInfo(schema: String, tableName: String, unique: Boolean): ResultSet {
        val sql = """
            SELECT
              NULL AS TABLE_CAT,
              idx.owner AS TABLE_SCHEM,
              idx.table_name AS TABLE_NAME,
              CASE WHEN idx.uniqueness = 'UNIQUE' THEN '0' ELSE '1' END AS NON_UNIQUE,
              NULL AS INDEX_QUALIFIER,
              idx.index_name AS INDEX_NAME,
              '${DatabaseMetaData.tableIndexOther}' AS TYPE,
              ic.column_position AS ORDINAL_POSITION,
              ic.column_name AS COLUMN_NAME,
              CASE WHEN ic.descend = 'ASC' THEN 'A' WHEN ic.descend = 'DESC' THEN 'D' ELSE NULL END AS ASC_OR_DESC,
              NULL AS CARDINALITY,
              NULL AS PAGES,
              NULL AS FILTER_CONDITION
            FROM all_indexes idx
            JOIN all_ind_columns ic
              ON ic.index_owner = idx.owner
             AND ic.index_name  = idx.index_name
            WHERE idx.owner = '${schema.uppercase()}'
              AND upper(idx.table_name) = '$tableName'
              ${if (unique) "AND idx.uniqueness = 'UNIQUE'" else ""}
            ORDER BY idx.index_name, ic.column_position
        """.trimIndent()

        return try {
            logger.debug("Executing remote getIndexInfo query with pagination")
            val remoteRows = fetchAllPages(
                wsdlEndpoint = connection.wsdlEndpoint,
                sql = sql,
                username = connection.username,
                password = connection.password,
                reportPath = connection.reportPath,
                fetchSize = METADATA_FETCH_SIZE,
                parsePage = this::parseIndexResponse
            )

            // Cache the results
            cacheIndexRows(remoteRows)

            XmlResultSet(remoteRows)
        } catch (e: Exception) {
            logger.error("Error fetching index info from remote service", e)
            createEmptyResultSet()
        }
    }

    private fun parseIndexResponse(responseXml: String): List<Map<String, String>> {
        val doc = parseXml(responseXml)
        var rowNodes = doc.getElementsByTagName("ROW")

        if (rowNodes.length == 0) {
            val resultNodes = doc.getElementsByTagName("RESULT")
            if (resultNodes.length > 0) {
                val unescaped = StringEscapeUtils.unescapeXml(resultNodes.item(0).textContent.trim())
                rowNodes = parseXml(unescaped).getElementsByTagName("ROW")
            }
        }

        val rows = mutableListOf<Map<String, String>>()
        for (i in 0 until rowNodes.length) {
            val node = rowNodes.item(i)
            if (node.nodeType == Node.ELEMENT_NODE) {
                val map = mutableMapOf<String, String>()
                val children = node.childNodes
                for (j in 0 until children.length) {
                    val child = children.item(j)
                    if (child.nodeType == Node.ELEMENT_NODE) {
                        map[child.nodeName.lowercase()] = child.textContent.trim()
                    }
                }

                // Normalize numeric fields
                val keys = listOf("cardinality", "pages", "ordinal_position")
                keys.forEach { key ->
                    if (map[key].isNullOrBlank()) {
                        map[key] = "0"
                    }
                }

                rows.add(map)
            }
        }
        return rows
    }

    private fun cacheIndexRows(rows: List<Map<String, String>>) {
        try {
            cacheLock.write {
                LocalMetadataCache.connection.prepareStatement("""
                    INSERT OR IGNORE INTO CACHED_INDEXES
                    (TABLE_CAT, TABLE_SCHEM, TABLE_NAME, NON_UNIQUE, INDEX_QUALIFIER, INDEX_NAME,
                     TYPE, ORDINAL_POSITION, COLUMN_NAME, ASC_OR_DESC, CARDINALITY, PAGES, FILTER_CONDITION)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """).use { pstmt ->
                    rows.forEachIndexed { index, row ->
                        pstmt.setString(1, row["table_cat"] ?: "")
                        pstmt.setString(2, row["table_schem"] ?: "")
                        pstmt.setString(3, row["table_name"] ?: "")
                        pstmt.setString(4, row["non_unique"] ?: "")
                        pstmt.setString(5, row["index_qualifier"] ?: "")
                        pstmt.setString(6, row["index_name"] ?: "")
                        pstmt.setString(7, row["type"] ?: "")
                        pstmt.setObject(8, row["ordinal_position"]?.takeIf { it.isNotBlank() }?.toIntOrNull())
                        pstmt.setString(9, row["column_name"] ?: "")
                        pstmt.setString(10, row["asc_or_desc"] ?: "")
                        pstmt.setObject(11, row["cardinality"]?.takeIf { it.isNotBlank() }?.toLongOrNull())
                        pstmt.setObject(12, row["pages"]?.takeIf { it.isNotBlank() }?.toIntOrNull())
                        pstmt.setString(13, row["filter_condition"] ?: "")
                        pstmt.addBatch()

                        if ((index + 1) % CACHE_BATCH_SIZE == 0) {
                            pstmt.executeBatch()
                        }
                    }
                    pstmt.executeBatch()
                    LocalMetadataCache.connection.commit()
                    logger.debug("Cached ${rows.size} index rows")
                }
            }
        } catch (ex: Exception) {
            logger.error("Error caching index metadata", ex)
            try {
                LocalMetadataCache.connection.rollback()
            } catch (rollbackEx: Exception) {
                logger.error("Error rolling back cache transaction", rollbackEx)
            }
        }
    }

    // ResultSet support methods
    override fun supportsResultSetType(type: Int): Boolean = type == ResultSet.TYPE_FORWARD_ONLY
    override fun supportsResultSetConcurrency(type: Int, concurrency: Int): Boolean =
        type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY
    override fun ownUpdatesAreVisible(type: Int): Boolean = false
    override fun ownDeletesAreVisible(type: Int): Boolean = false
    override fun ownInsertsAreVisible(type: Int): Boolean = false
    override fun othersUpdatesAreVisible(type: Int): Boolean = false
    override fun othersDeletesAreVisible(type: Int): Boolean = false
    override fun othersInsertsAreVisible(type: Int): Boolean = false
    override fun updatesAreDetected(type: Int): Boolean = false
    override fun deletesAreDetected(type: Int): Boolean = false
    override fun insertsAreDetected(type: Int): Boolean = false
    override fun supportsBatchUpdates(): Boolean = false

    // UDT and advanced features
    override fun getUDTs(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        types: IntArray?
    ): ResultSet = createEmptyResultSet()

    override fun getConnection(): Connection = throw SQLFeatureNotSupportedException(
        "WsdlDatabaseMetaData.getConnection(): use the existing Connection instance"
    )

    override fun supportsSavepoints(): Boolean = false
    override fun supportsNamedParameters(): Boolean = false
    override fun supportsMultipleOpenResults(): Boolean = false
    override fun supportsGetGeneratedKeys(): Boolean = false
    override fun getSuperTypes(catalog: String?, schemaPattern: String?, typeNamePattern: String?): ResultSet = createEmptyResultSet()
    override fun getSuperTables(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet = createEmptyResultSet()
    override fun getAttributes(catalog: String?, schemaPattern: String?, typeNamePattern: String?, attributeNamePattern: String?): ResultSet = createEmptyResultSet()
    override fun supportsResultSetHoldability(holdability: Int): Boolean = holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT
    override fun getResultSetHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT
    override fun getDatabaseMajorVersion(): Int = 1
    override fun getDatabaseMinorVersion(): Int = 0
    override fun getJDBCMajorVersion(): Int = 4
    override fun getJDBCMinorVersion(): Int = 2
    override fun getSQLStateType(): Int = DatabaseMetaData.sqlStateSQL99
    override fun locatorsUpdateCopy(): Boolean = false
    override fun supportsStatementPooling(): Boolean = false
    override fun getRowIdLifetime(): RowIdLifetime = RowIdLifetime.ROWID_UNSUPPORTED
    override fun supportsStoredFunctionsUsingCallSyntax(): Boolean = false
    override fun autoCommitFailureClosesAllResultSets(): Boolean = false
    override fun getClientInfoProperties(): ResultSet = createEmptyResultSet()
    override fun getFunctions(catalog: String?, schemaPattern: String?, functionNamePattern: String?): ResultSet = createEmptyResultSet()
    override fun getFunctionColumns(catalog: String?, schemaPattern: String?, functionNamePattern: String?, columnNamePattern: String?): ResultSet = createEmptyResultSet()
    override fun getPseudoColumns(catalog: String?, schemaPattern: String?, tableNamePattern: String?, columnNamePattern: String?): ResultSet = createEmptyResultSet()
    override fun generatedKeyAlwaysReturned(): Boolean = false

    override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLFeatureNotSupportedException(
        "WsdlDatabaseMetaData.unwrap(): cannot unwrap to ${iface?.name}"
    )
    override fun isWrapperFor(iface: Class<*>?): Boolean = false
}