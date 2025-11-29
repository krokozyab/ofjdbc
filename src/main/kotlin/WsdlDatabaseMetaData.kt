package my.jdbc.wsdl_driver

import org.apache.commons.text.StringEscapeUtils
import org.w3c.dom.Node
import java.sql.*


object LocalMetadataCache {
    // Use a file path in the user's home directory.
    private val userHome = System.getProperty("user.home")
    private val ofjdbcDir = "$userHome/.ofjdbc"
    private val duckDbFilePath = "$ofjdbcDir/metadata.db"
    
    @Volatile
    private var _connection: Connection? = null
    private val connectionLock = Any()
    private var shutdownHookRegistered = false

    val connection: Connection
        get() {
            return _connection ?: synchronized(connectionLock) {
                _connection ?: createConnection().also { 
                    _connection = it
                    registerShutdownHook()
                }
            }
        }
    
    private fun createConnection(): Connection {
        java.io.File(ofjdbcDir).mkdirs()
        logger.info("Using DuckDB file: $duckDbFilePath")
        val conn = DriverManager.getConnection("jdbc:duckdb:$duckDbFilePath")
        conn.autoCommit = true
        conn.createStatement().use { stmt ->
            stmt.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS SCHEMAS_CACHE (
                    TABLE_SCHEM VARCHAR,
                    TABLE_CATALOG VARCHAR
                )
                """.trimIndent()
            )
            stmt.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS CACHED_TABLES (
                    TABLE_CAT VARCHAR,
                    TABLE_SCHEM VARCHAR,
                    TABLE_NAME VARCHAR,
                    TABLE_TYPE VARCHAR,
                    REMARKS VARCHAR,
                    TYPE_CAT VARCHAR,
                    TYPE_SCHEM VARCHAR,
                    TYPE_NAME VARCHAR,
                    SELF_REFERENCING_COL_NAME VARCHAR,
                    REF_GENERATION VARCHAR,
                    TABLE_ID VARCHAR,
                    PRIMARY KEY (TABLE_SCHEM, TABLE_NAME)
                )
                """.trimIndent()
            )
            stmt.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS CACHED_COLUMNS (
                    TABLE_CAT VARCHAR,
                    TABLE_SCHEM VARCHAR,
                    TABLE_NAME VARCHAR,
                    COLUMN_NAME VARCHAR,
                    DATA_TYPE VARCHAR,
                    TYPE_NAME VARCHAR,
                    COLUMN_SIZE VARCHAR,
                    DECIMAL_DIGITS INTEGER,
                    NUM_PREC_RADIX INTEGER,
                    NULLABLE INTEGER,
                    ORDINAL_POSITION INTEGER,
                    REMARKS VARCHAR, 
                    PRIMARY KEY (TABLE_SCHEM, TABLE_NAME, COLUMN_NAME)
                )
                """.trimIndent()
            )
            stmt.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS CACHED_INDEXES (
                    TABLE_CAT VARCHAR,
                    TABLE_SCHEM VARCHAR,
                    TABLE_NAME VARCHAR,
                    NON_UNIQUE VARCHAR,
                    INDEX_QUALIFIER VARCHAR,
                    INDEX_NAME VARCHAR,
                    TYPE VARCHAR,
                    ORDINAL_POSITION INTEGER,
                    COLUMN_NAME VARCHAR,
                    ASC_OR_DESC VARCHAR,
                    CARDINALITY BIGINT,
                    PAGES INTEGER,
                    FILTER_CONDITION VARCHAR,
                    PRIMARY KEY (TABLE_SCHEM, TABLE_NAME, INDEX_NAME, COLUMN_NAME)
                )
                """.trimIndent()
            )
            stmt.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS CACHED_FOREIGN_KEYS (
                    PKTABLE_CAT VARCHAR,
                    PKTABLE_SCHEM VARCHAR,
                    PKTABLE_NAME VARCHAR,
                    PKCOLUMN_NAME VARCHAR,
                    FKTABLE_CAT VARCHAR,
                    FKTABLE_SCHEM VARCHAR,
                    FKTABLE_NAME VARCHAR,
                    FKCOLUMN_NAME VARCHAR,
                    KEY_SEQ INTEGER,
                    UPDATE_RULE INTEGER,
                    DELETE_RULE INTEGER,
                    FK_NAME VARCHAR,
                    PK_NAME VARCHAR,
                    DEFERRABILITY INTEGER,
                    PRIMARY KEY (FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, KEY_SEQ)
                )
                """.trimIndent()
            )
            stmt.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS CACHED_PROCEDURES (
                    PROCEDURE_CAT VARCHAR,
                    PROCEDURE_SCHEM VARCHAR,
                    PROCEDURE_NAME VARCHAR,
                    REMARKS VARCHAR,
                    PROCEDURE_TYPE INTEGER,
                    SPECIFIC_NAME VARCHAR,
                    PRIMARY KEY (PROCEDURE_SCHEM, PROCEDURE_NAME)
                )
                """.trimIndent()
            )
            stmt.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS CACHED_FUNCTIONS (
                    FUNCTION_CAT VARCHAR,
                    FUNCTION_SCHEM VARCHAR,
                    FUNCTION_NAME VARCHAR,
                    REMARKS VARCHAR,
                    FUNCTION_TYPE INTEGER,
                    SPECIFIC_NAME VARCHAR,
                    PRIMARY KEY (FUNCTION_SCHEM, FUNCTION_NAME)
                )
                """.trimIndent()
            )
            stmt.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS CACHED_UDTS (
                    TYPE_CAT VARCHAR,
                    TYPE_SCHEM VARCHAR,
                    TYPE_NAME VARCHAR,
                    CLASS_NAME VARCHAR,
                    DATA_TYPE INTEGER,
                    REMARKS VARCHAR,
                    BASE_TYPE INTEGER,
                    PRIMARY KEY (TYPE_SCHEM, TYPE_NAME)
                )
                """.trimIndent()
            )
        }
        return conn
    }
    
    private fun registerShutdownHook() {
        if (!shutdownHookRegistered) {
            Runtime.getRuntime().addShutdownHook(Thread {
                logger.info("Shutting down LocalMetadataCache")
                close()
            })
            shutdownHookRegistered = true
        }
    }
    
    fun executeBatch(
        sql: String,
        rows: List<Map<String, String>>,
        parameterSetter: (PreparedStatement, Map<String, String>) -> Unit,
        batchSize: Int = 1000
    ) {
        if (rows.isEmpty()) return
        
        connection.prepareStatement(sql).use { pstmt ->
            var count = 0
            for (row in rows) {
                try {
                    parameterSetter(pstmt, row)
                    pstmt.addBatch()
                    count++
                    
                    if (count % batchSize == 0) {
                        pstmt.executeBatch()
                        pstmt.clearBatch()
                    }
                } catch (ex: SQLException) {
                    logger.warn("Error adding row to batch: ${ex.message}")
                }
            }
            
            // Execute remaining batch
            if (count % batchSize != 0) {
                pstmt.executeBatch()
            }
        }
    }

    fun close() {
        synchronized(connectionLock) {
            _connection?.let { conn ->
                try {
                    if (!conn.isClosed) {
                        logger.info("Closing DuckDB connection.")
                        conn.close()
                    }
                } catch (ex: Exception) {
                    logger.error("Error closing DuckDB connection: ${ex.message}")
                } finally {
                    _connection = null
                }
            }
        }
    }
}



class WsdlDatabaseMetaData(private val connection: WsdlConnection) : DatabaseMetaData {

    override fun getDatabaseProductName(): String = "Oracle Fusion JDBC Driver"

    override fun getDatabaseProductVersion(): String = "1.0"

    override fun getDriverName(): String = "sergey.rudenko.ba@gmail.com"

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

    // For Oracle, the identifier quote string is a double quote.
    override fun getIdentifierQuoteString(): String = "\""

    override fun getSQLKeywords(): String {
        // Return an empty string since there are no additional non-SQL92 keywords to report.
        return ""
    }

    override fun getURL(): String = connection.wsdlEndpoint

    override fun getUserName(): String = connection.username

    override fun isReadOnly(): Boolean = true

    override fun nullsAreSortedHigh(): Boolean = true

    override fun nullsAreSortedLow(): Boolean = false

    override fun nullsAreSortedAtStart(): Boolean = false

    override fun nullsAreSortedAtEnd(): Boolean = true

    // All other methods throw SQLFeatureNotSupportedException with sequential numbers starting at 257.
    override fun allProceduresAreCallable(): Boolean = false

    override fun allTablesAreSelectable(): Boolean = true

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

    // Oracle supports a broad set of numeric functions; return the most common ones
    // so client tools (e.g. SQL IDEs) can offer proper code‑completion.
    override fun getNumericFunctions(): String =
        "ABS,ACOS,ASIN,ATAN,ATAN2,CEIL,COS,COSH,DEGREES,EXP,FLOOR,LN,LOG,LOG10,MOD," +
        "POWER,RADIANS,ROUND,SIGN,SIN,SINH,SQRT,TAN,TANH,TRUNC"

    // Common Oracle string functions returned for JDBC code‑completion
    override fun getStringFunctions(): String =
        "ASCII,CHR,CONCAT,INITCAP,INSTR,LENGTH,LTRIM,LOWER,LPAD,RPAD," +
        "REPLACE,RTRIM,SOUNDEX,SUBSTR,TRANSLATE,TRIM,UPPER"

    // Oracle system functions (environment / date‑time) for JDBC code‑completion
    override fun getSystemFunctions(): String =
        "USER,UID,SYSDATE,SYSTIMESTAMP,CURRENT_DATE,CURRENT_TIMESTAMP," +
        "LOCALTIMESTAMP,DBTIMEZONE,SESSIONTIMEZONE,SYS_GUID"

    // Oracle date/time functions for JDBC code‑completion
    override fun getTimeDateFunctions(): String =
        "CURRENT_DATE,CURRENT_TIMESTAMP,LOCALTIMESTAMP,LOCALTIME,SYSDATE,SYSTIMESTAMP," +
        "ADD_MONTHS,EXTRACT,LAST_DAY,NEXT_DAY,MONTHS_BETWEEN,ROUND,TRUNC,NEW_TIME," +
        "TZ_OFFSET,SESSIONTIMEZONE,DBTIMEZONE"

    override fun getSearchStringEscape(): String = "\\"

    // In Oracle, the standard term for stored procedures is "PROCEDURE"
    override fun getProcedureTerm(): String = "PROCEDURE"

    // In Oracle, the concept of a catalog isn’t used
    override fun getCatalogTerm(): String = ""

    override fun isCatalogAtStart(): Boolean = false

    override fun getCatalogSeparator(): String = "."

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

    // Oracle retains open cursors after a commit, so clients can continue fetching.
    override fun supportsOpenCursorsAcrossCommit(): Boolean = true

    override fun supportsOpenCursorsAcrossRollback(): Boolean = false

    override fun supportsOpenStatementsAcrossCommit(): Boolean = false

    override fun supportsOpenStatementsAcrossRollback(): Boolean = false

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

    override fun getDefaultTransactionIsolation(): Int = Connection.TRANSACTION_NONE

    override fun supportsTransactions(): Boolean = false

    override fun supportsTransactionIsolationLevel(level: Int): Boolean = false

    override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean = false

    override fun supportsDataManipulationTransactionsOnly(): Boolean = false

    override fun dataDefinitionCausesTransactionCommit(): Boolean = false

    override fun dataDefinitionIgnoredInTransactions(): Boolean = false

    override fun getProcedures(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?
    ): ResultSet {
        val localConn = LocalMetadataCache.connection
        val defSchema = "FUSION"
        
        // Try cache first
        val cachedRows = mutableListOf<Map<String, String>>()
        try {
            val schemaCondLocal = if (!schemaPattern.isNullOrBlank()) "AND PROCEDURE_SCHEM LIKE '${schemaPattern.uppercase()}'" else "AND PROCEDURE_SCHEM = '$defSchema'"
            val nameCondLocal = if (!procedureNamePattern.isNullOrBlank()) "AND PROCEDURE_NAME LIKE '${procedureNamePattern.uppercase()}'" else ""
            
            localConn.createStatement().use { stmt ->
                stmt.executeQuery(
                    """
                    SELECT PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, REMARKS, PROCEDURE_TYPE, SPECIFIC_NAME
                    FROM CACHED_PROCEDURES
                    WHERE 1=1 $schemaCondLocal $nameCondLocal
                    ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME
                    """.trimIndent()
                ).use { rs ->
                    val meta = rs.metaData
                    while (rs.next()) {
                        val row = mutableMapOf<String, String>()
                        for (i in 1..meta.columnCount) {
                            row[meta.getColumnName(i).lowercase()] = rs.getString(i) ?: ""
                        }
                        cachedRows.add(row)
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error("Error reading cached procedures metadata: ${ex.message}")
        }
        
        if (cachedRows.isNotEmpty()) {
            logger.info("Returning ${cachedRows.size} procedures from local cache.")
            return XmlResultSet(cachedRows)
        }
        
        // Fetch from remote
        val schemaCondRemote = if (!schemaPattern.isNullOrBlank()) "AND owner LIKE '${schemaPattern.uppercase()}'" else "AND owner = '$defSchema'"
        val nameCondRemote = if (!procedureNamePattern.isNullOrBlank()) "AND object_name LIKE '${procedureNamePattern.uppercase()}'" else ""
        
        val sql = """
            SELECT 
                NULL AS PROCEDURE_CAT,
                owner AS PROCEDURE_SCHEM,
                object_name AS PROCEDURE_NAME,
                NULL AS REMARKS,
                CASE 
                    WHEN procedure_name IS NULL THEN 1
                    ELSE 2
                END AS PROCEDURE_TYPE,
                object_name AS SPECIFIC_NAME
            FROM all_procedures
            WHERE 1=1 $schemaCondRemote $nameCondRemote
            ORDER BY owner, object_name
        """.trimIndent()
        
        logger.info("Executing remote getProcedures SQL: {}", sql)
        val responseXml = sendSqlViaWsdl(
            connection.wsdlEndpoint,
            sql,
            connection.username,
            connection.password,
            connection.reportPath
        )
        val remoteRows = parseRows(responseXml)

        
        // Cache the results
        try {
            localConn.prepareStatement(
                """
                INSERT OR IGNORE INTO CACHED_PROCEDURES 
                (PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, REMARKS, PROCEDURE_TYPE, SPECIFIC_NAME) 
                VALUES (?, ?, ?, ?, ?, ?)
                """.trimIndent()
            ).use { pstmt ->
                for (row in remoteRows) {
                    pstmt.setString(1, row["PROCEDURE_CAT"] ?: "")
                    pstmt.setString(2, row["PROCEDURE_SCHEM"] ?: "")
                    pstmt.setString(3, row["PROCEDURE_NAME"] ?: "")
                    pstmt.setString(4, row["REMARKS"] ?: "")
                    pstmt.setObject(5, row["PROCEDURE_TYPE"]?.toIntOrNull())
                    pstmt.setString(6, row["SPECIFIC_NAME"] ?: "")
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
                logger.info("Saved remote procedures metadata into local cache.")
            }
        } catch (ex: Exception) {
            logger.error("Error saving remote procedures metadata to local cache: {}", ex.message)
        }
        
        return XmlResultSet(remoteRows.map { it.mapKeys { k -> k.key.lowercase() } })
    }

    override fun getProcedureColumns(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet = createEmptyResultSet()

    // Implement getTables for Oracle
    override fun getTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        types: Array<out String>?
    ): ResultSet {
        val localConn = LocalMetadataCache.connection

        // Normalise requested table‐types (if any) to UPPER‑CASE so we can
        // compare against Oracle’s OBJECT_TYPE values.
        val requestedTypes: Set<String>? =
            types?.filterNotNull()
                 ?.map { it.uppercase() }
                 ?.toSet()
                 ?.takeIf { it.isNotEmpty() }

        //The only schema we need
        val defSchema = "FUSION"

        // First, attempt to read from the local cache.
        val cachedRows = mutableListOf<Map<String, String>>()
        try {
            val typeFilterSql =
                requestedTypes?.joinToString(prefix = " AND TABLE_TYPE IN ('", separator = "','", postfix = "')")
                    ?: ""
            localConn.createStatement().use { stmt ->
                stmt.executeQuery(
                    """
                    SELECT * FROM CACHED_TABLES
                    WHERE 1=1$typeFilterSql
                    ORDER BY TABLE_NAME
                    """.trimIndent()
                ).use { rs ->
                    val meta = rs.metaData
                    while (rs.next()) {
                        val row = mutableMapOf<String, String>()
                        for (i in 1..meta.columnCount) {
                            row[meta.getColumnName(i).lowercase()] = rs.getString(i) ?: ""
                        }
                        cachedRows.add(row)
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error("Error reading cached tables metadata: ${ex.message}")
        }
        if (cachedRows.isNotEmpty()) {
            logger.info("Returning ${cachedRows.size} rows from local cache.")
            return XmlResultSet(cachedRows)
        }

        val typeListSql = requestedTypes
            ?.joinToString(prefix = "('", separator = "','", postfix = "')")
            ?: "('TABLE','VIEW')" // todo need to investigate where other is needed -  ,'SYNONYM','GLOBAL TEMPORARY','LOCAL TEMPORARY')"

        val baseSql = """
            /*+ FIRST_ROWS(1000) */
                SELECT
                  CAST(NULL AS VARCHAR2(1))          AS TABLE_CAT,
                  'FUSION'                           AS TABLE_SCHEM,
                    t.table_name,
                    t.table_type,
                  t.description                      AS REMARKS,
                  CAST(NULL AS VARCHAR2(1))          AS TYPE_CAT,
                  CAST(NULL AS VARCHAR2(1))          AS TYPE_SCHEM,
                  CAST(NULL AS VARCHAR2(1))          AS TYPE_NAME,
                  CAST(NULL AS VARCHAR2(1))          AS SELF_REFERENCING_COL_NAME,
                  CAST(NULL AS VARCHAR2(1))          AS REF_GENERATION,
                  t.table_id                         AS TABLE_ID
                FROM (
                  SELECT 
                         view_id   AS table_id,
                         view_name AS table_name,
                         'VIEW'    AS table_type,
                          description
                  FROM   FND_VIEWS
                  UNION ALL
                  SELECT 
                         table_id,
                         table_name,
                         'TABLE'   AS table_type,
                         description
                  FROM   FND_TABLES
                ) t
                WHERE t.table_type IN $typeListSql
                  --AND t.table_name IS NOT NULL 
                  --AND TRIM(t.table_name) != ''
                  ORDER BY t.table_type, t.table_name
        """.trimIndent()

        logger.info("Executing paginated getTables query")
        val remoteRows = executeWithPagination(
            connection.wsdlEndpoint,
            baseSql,
            connection.username,
            connection.password,
            connection.reportPath
        )

        /*
         * DBeaver complains about “Duplicate object name … in cache SimpleObjectCache”.
         * This happens because the same TABLE_NAME can be returned multiple times with
         * different OBJECT_TYPEs (TABLE, VIEW, SYNONYM).
         *
         * We keep a single entry per (TABLE_SCHEM,TABLE_NAME) and choose a *preferred*
         * OBJECT_TYPE using the following precedence:
         *      1) TABLE
         *      2) VIEW
         *      3) SYNONYM
         * Any additional duplicates are discarded.
         */
        fun typePriority(t: String): Int = when (t.uppercase()) {
            "TABLE"            -> 1
            "VIEW"             -> 2
            "SYNONYM"          -> 3
            else               -> 4
        }

        val uniqueRows: Collection<Map<String, String>> =
            remoteRows
                // group by schema+table
                .groupBy { "${it["TABLE_SCHEM"]}|${it["TABLE_NAME"]}" }
                .map { (_, dupes) -> dupes.minBy { typePriority(it["TABLE_TYPE"] ?: "") } }

        // ---------------------------------------------------------
        // 4) Cache the unique rows into DuckDB & build result set
        // ---------------------------------------------------------
        try {
            localConn.prepareStatement(
                """
            INSERT OR IGNORE INTO CACHED_TABLES 
            (TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, TYPE_CAT, TYPE_SCHEM, TYPE_NAME, SELF_REFERENCING_COL_NAME, REF_GENERATION, TABLE_ID) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent()
            ).use { pstmt ->
                val rowsToCache = if (requestedTypes == null)
                    uniqueRows
                else
                    uniqueRows.filter { requestedTypes.contains(it["TABLE_TYPE"]?.uppercase()) }
                for (row in rowsToCache) {
                    val tableCat   = row["TABLE_CAT"] ?: ""
                    val tableSchem = row["TABLE_SCHEM"] ?: ""
                    val tableName  = row["TABLE_NAME"] ?: ""
                    val tableType  = row["TABLE_TYPE"] ?: ""
                    val remarks    = row["REMARKS"] ?: ""
                    val typeCat    = row["TYPE_CAT"] ?: ""
                    val typeSchem  = row["TYPE_SCHEM"] ?: ""
                    val typeName   = row["TYPE_NAME"] ?: ""
                    val selfRefCol = row["SELF_REFERENCING_COL_NAME"] ?: ""
                    val refGeneration = row["REF_GENERATION"] ?: ""
                    val tableId = row["TABLE_ID"] ?: ""
                    pstmt.setString(1, tableCat)
                    pstmt.setString(2, tableSchem)
                    pstmt.setString(3, tableName)
                    pstmt.setString(4, tableType)
                    pstmt.setString(5, remarks)
                    pstmt.setString(6, typeCat)
                    pstmt.setString(7, typeSchem)
                    pstmt.setString(8, typeName)
                    pstmt.setString(9, selfRefCol)
                    pstmt.setString(10, refGeneration)
                    pstmt.setString(11, tableId)
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
                logger.info("Saved remote tables metadata into local cache.")
            }
        } catch (ex: Exception) {
            logger.error("Error saving remote tables metadata to local cache: {}", ex.message)
        }

        // Return the unique rows to DBeaver, lowercasing keys for XmlResultSet.
        val resultRows = if (requestedTypes == null)
            uniqueRows
        else
            uniqueRows.filter { requestedTypes.contains(it["TABLE_TYPE"]?.uppercase()) }

        return XmlResultSet(resultRows.map { it.mapKeys { k -> k.key.lowercase() } })
    }

    override fun getSchemas(): ResultSet = createEmptyResultSet()

    override fun getSchemas(catalog: String?, schemaPattern: String?): ResultSet = createEmptyResultSet()

    override fun getCatalogs(): ResultSet = createEmptyResultSet()

    // Oracle-supported table types for JDBC metadata
    override fun getTableTypes(): ResultSet {
        val typesList = listOf(
            mapOf("TABLE_TYPE" to "TABLE"),
            mapOf("TABLE_TYPE" to "VIEW"),
            mapOf("TABLE_TYPE" to "SYNONYM"),
            mapOf("TABLE_TYPE" to "GLOBAL TEMPORARY"),
            mapOf("TABLE_TYPE" to "LOCAL TEMPORARY")
        )
        return XmlResultSet(typesList)
    }

    override fun getColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        val localConn = LocalMetadataCache.connection
        // The only schema we need
        val defSchema = "FUSION"

        // Always limit metadata to the single supported schema (FUSION).
        // This avoids duplicate‑object warnings when DBeaver requests columns
        // without specifying a schema, because Oracle otherwise returns
        // every accessible schema.
        val schemaCondLocal  = " AND TABLE_SCHEM = '$defSchema'"
        val tableCondLocal = if (!tableNamePattern.isNullOrBlank()) " AND TABLE_NAME LIKE '${tableNamePattern.uppercase()}'" else ""
        val columnCondLocal = if (!columnNamePattern.isNullOrBlank()) " AND COLUMN_NAME LIKE '${columnNamePattern.uppercase()}'" else ""
        val localQuery = """
        SELECT DISTINCT
            TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE, TYPE_NAME, 
            COLUMN_SIZE, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE, ORDINAL_POSITION, REMARKS 
        FROM CACHED_COLUMNS
        --WHERE 1=1 $schemaCondLocal $tableCondLocal $columnCondLocal
        WHERE TABLE_NAME = '$tableNamePattern'
        ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
    """.trimIndent()

        // Try reading from the local cache.
        try {
            localConn.createStatement().use { stmt ->
                stmt.executeQuery(localQuery).use { rs ->
                    val localRows = mutableListOf<Map<String, String?>>()
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
                        row["num_prec_radix"] = rs.getString("NUM_PREC_RADIX") ?: "0"
                        row["nullable"] = rs.getString("NULLABLE") ?: ""
                        row["ordinal_position"] = rs.getString("ORDINAL_POSITION") ?: ""
                        row["remarks"] = rs.getString("REMARKS") ?: ""
                        localRows.add(row)
                    }
                    if (localRows.isNotEmpty()) {
                        logger.info("Returning columns from local cache with ${localRows.size} rows.")
                        return XmlResultSet(localRows)
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error("Error reading columns from local metadata cache: ${ex.message}")
        }

        // If local cache is empty, query the remote service.
        // We don't filter by t.owner / t.table_name remotely because FND_COLUMNS exposes table via TABLE_ID.
        // Instead, if caller supplied a tableNamePattern we resolve matching TABLE_IDs from the local
        // CACHED_TABLES cache and restrict the remote query by c.table_id IN (...).
        val columnCondRemote = if (!columnNamePattern.isNullOrBlank()) " AND c.column_name LIKE '${columnNamePattern.uppercase()}'" else ""

        var tableIdCondRemote = ""
        try {
            val ids = mutableListOf<String>()
            val likePattern = tableNamePattern?.uppercase()
            val schemaFilterLocal = if (!schemaPattern.isNullOrBlank()) " AND TABLE_SCHEM = '${schemaPattern.uppercase()}'" else ""
            localConn.createStatement().use { stmt ->
                stmt.executeQuery(
                    """
                    SELECT TABLE_ID FROM CACHED_TABLES
                    WHERE UPPER(TABLE_NAME) LIKE '$likePattern' $schemaFilterLocal
                    """.trimIndent()
                ).use { rs ->
                    while (rs.next()) {
                        val id = rs.getString(1)
                        if (!id.isNullOrBlank()) ids.add(id)
                    }
                }
            }
            if (ids.isNotEmpty()) {
                // Use numeric list without quotes - TABLE_IDs are stored as strings but represent numbers
                tableIdCondRemote = " AND c.table_id IN (${ids.joinToString(separator = ",")})"
            } else {
                logger.info("No TABLE_IDs found in CACHED_TABLES for pattern '$likePattern'; remote query will not be restricted by TABLE_ID.")
            }
        } catch (ex: Exception) {
            logger.error("Error resolving TABLE_IDs from CACHED_TABLES: ${ex.message}")
        }

        var sql =""
        if (!tableNamePattern.isNullOrBlank() && (tableNamePattern.trim().uppercase().endsWith("_V") || tableNamePattern.trim().uppercase().endsWith("_VL"))) {
            // views branch
            val tableCondRemote = if (!tableNamePattern.isNullOrBlank()) " AND table_name LIKE '${tableNamePattern.uppercase()}'" else ""
            val schemaCondRemote  = " AND owner = '$defSchema'"

            sql = """
                SELECT 
                    NULL AS TABLE_CAT,
                    owner AS TABLE_SCHEM,
                    table_name AS TABLE_NAME,
                    column_name AS COLUMN_NAME,
                    ${java.sql.Types.VARCHAR} AS DATA_TYPE,
                    data_type AS TYPE_NAME,
                    data_length AS COLUMN_SIZE,
                    data_precision AS DECIMAL_DIGITS,
                    data_scale AS NUM_PREC_RADIX,
                    CASE WHEN nullable = 'Y' THEN 1 ELSE 0 END AS NULLABLE,
                    column_id AS ORDINAL_POSITION
                FROM all_tab_columns
                WHERE 1=1 $schemaCondRemote $tableCondRemote 
                ORDER BY owner, table_name, column_id
            """.trimIndent()
        } else
        {
            // tables branch
            sql = """
                SELECT
                    NULL AS TABLE_CAT,
                    'FUSION' AS TABLE_SCHEM,
                    '$tableNamePattern' AS TABLE_NAME,
                    c.user_column_name AS COLUMN_NAME,
                    ${java.sql.Types.VARCHAR} AS DATA_TYPE,
                    COALESCE(c.column_type, c.domain_code) AS TYPE_NAME,
                    c.width AS COLUMN_SIZE,
                    c."SCALE" AS DECIMAL_DIGITS,
                    CASE WHEN c."PRECISION" IS NOT NULL THEN 10 ELSE NULL END AS NUM_PREC_RADIX,
                    CASE WHEN c.null_allowed_flag = 'Y' THEN 1 ELSE 0 END AS NULLABLE,
                    COALESCE(c.column_sequence, c.column_id) AS ORDINAL_POSITION,
                    c.description AS REMARKS
                FROM FND_COLUMNS c
                WHERE 1=1 $tableIdCondRemote 
                --$columnCondRemote
                ORDER BY  COALESCE(c.column_sequence, c.column_id)
            """.trimIndent()
        }




        val responseXml = sendSqlViaWsdl(
            connection.wsdlEndpoint,
            sql,
            connection.username,
            connection.password,
            connection.reportPath
        )
        val remoteRows = parseRows(responseXml, true)


        // --- Deduplicate rows across quoted / un‑quoted column names -------------
        // Some columns are returned twice: once as plain UPPERCASE and once quoted
        // (e.g. LANGUAGE  vs  "LANGUAGE").  We collapse those variants here.
        val uniqueRows: List<Map<String, String>> =
            remoteRows
                .groupBy {
                    val schem = it["table_schem"]?.uppercase() ?: ""
                    val tbl   = it["table_name"]?.uppercase() ?: ""
                    val col   = it["column_name"]?.replace("\"", "")?.uppercase() ?: ""
                    "$schem|$tbl|$col"
                }
                .map { (_, dupes) -> dupes.first() }

        // Save the remote metadata into the local cache.
        try {
            localConn.prepareStatement(
                """
            INSERT OR IGNORE INTO CACHED_COLUMNS 
            (TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_SIZE, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE, ORDINAL_POSITION, REMARKS)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent()
            ).use { pstmt ->
                for (row in uniqueRows) {
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
                    pstmt.setString(12, row["remarks"] ?: "")
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
                logger.info("Saved remote columns into local cache (${uniqueRows.size} rows).")
            }
        } catch (ex: Exception) {
            logger.error("Error saving remote metadata to local cache: ${ex.message}")
        }
        return XmlResultSet(uniqueRows)
    }

    // No column-level privileges available via WSDL; return an empty result set.
    override fun getColumnPrivileges(
        catalog: String?, schema: String?, table: String?, columnNamePattern: String?
    ): ResultSet = createEmptyResultSet()

    // No table-level privileges available via WSDL; return an empty result set.
    override fun getTablePrivileges(
        catalog: String?, schemaPattern: String?, tableNamePattern: String?
    ): ResultSet = createEmptyResultSet()

    // Best row identifier not supported; return an empty result set.
    override fun getBestRowIdentifier(
        catalog: String?, schema: String?, table: String?, scope: Int, nullable: Boolean
    ): ResultSet = createEmptyResultSet()

    // Version columns (optimistic locking) not supported; return an empty result set.
    override fun getVersionColumns(
        catalog: String?, schema: String?, table: String?
    ): ResultSet = createEmptyResultSet()

    override fun getPrimaryKeys(catalog: String?, schema: String?, table: String?): ResultSet {
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
        ${if (!schema.isNullOrBlank()) "AND c.owner LIKE '${schema.uppercase()}'" else ""}
        ${if (!table.isNullOrBlank()) "AND c.table_name LIKE '${table.uppercase()}'" else ""}
        ORDER BY c.owner, c.table_name, cc.position
    """.trimIndent()
        val responseXml = sendSqlViaWsdl(
            connection.wsdlEndpoint,
            sql,
            connection.username,
            connection.password,
            connection.reportPath
        )

        val remoteRows = parseRows(responseXml, true)


        return XmlResultSet(remoteRows)
    }

    // Imported foreign keys are foreign keys that reference this table (this table is the primary key table)
    override fun getImportedKeys(
        catalog: String?, schema: String?, table: String?
    ): ResultSet {
        if (table.isNullOrBlank()) {
            return createEmptyResultSet()
        }
        
        val localConn = LocalMetadataCache.connection
        val defSchema = (schema ?: "FUSION").uppercase()
        val tableName = table.uppercase()
        
        // Try cache first - look for foreign keys where this table is the primary key table
        val cachedRows = mutableListOf<Map<String, String>>()
        try {
            val schemaCondLocal = if (!schema.isNullOrBlank()) "AND PKTABLE_SCHEM = '$defSchema'" else ""
            val tableCondLocal = "AND UPPER(PKTABLE_NAME) = '$tableName'"
            
            localConn.createStatement().use { stmt ->
                stmt.executeQuery(
                    """
                    SELECT PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, PKCOLUMN_NAME,
                           FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FKCOLUMN_NAME,
                           KEY_SEQ, UPDATE_RULE, DELETE_RULE, FK_NAME, PK_NAME, DEFERRABILITY
                    FROM CACHED_FOREIGN_KEYS
                    WHERE 1=1 $schemaCondLocal $tableCondLocal
                    ORDER BY FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, KEY_SEQ
                    """.trimIndent()
                ).use { rs ->
                    val meta = rs.metaData
                    while (rs.next()) {
                        val row = mutableMapOf<String, String>()
                        for (i in 1..meta.columnCount) {
                            row[meta.getColumnName(i).lowercase()] = rs.getString(i) ?: ""
                        }
                        cachedRows.add(row)
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error("Error reading cached imported keys metadata: ${ex.message}")
        }
        
        if (cachedRows.isNotEmpty()) {
            logger.info("Returning ${cachedRows.size} imported keys from local cache.")
            return XmlResultSet(cachedRows)
        }
        
        // If cache is empty, delegate to getForeignKeys logic but filter for this table as PK table
        val schemaCondRemote = if (!schema.isNullOrBlank()) "AND r.owner = '$defSchema'" else "AND r.owner = 'FUSION'"
        val tableCondRemote = "AND UPPER(r.table_name) = '$tableName'"
        
        val finalSql = """
            SELECT 
                NULL AS PKTABLE_CAT,
                r.owner AS PKTABLE_SCHEM,
                r.table_name AS PKTABLE_NAME,
                rc.column_name AS PKCOLUMN_NAME,
                NULL AS FKTABLE_CAT,
                c.owner AS FKTABLE_SCHEM,
                c.table_name AS FKTABLE_NAME,
                cc.column_name AS FKCOLUMN_NAME,
                cc.position AS KEY_SEQ,
                CASE c.delete_rule 
                    WHEN 'CASCADE' THEN 0
                    WHEN 'SET NULL' THEN 2
                    ELSE 1 
                END AS UPDATE_RULE,
                CASE c.delete_rule 
                    WHEN 'CASCADE' THEN 0
                    WHEN 'SET NULL' THEN 2
                    ELSE 1 
                END AS DELETE_RULE,
                c.constraint_name AS FK_NAME,
                r.constraint_name AS PK_NAME,
                7 AS DEFERRABILITY
            FROM all_constraints c
            JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name AND c.owner = cc.owner
            JOIN all_constraints r ON c.r_constraint_name = r.constraint_name AND c.r_owner = r.owner
            JOIN all_cons_columns rc ON r.constraint_name = rc.constraint_name AND r.owner = rc.owner
            WHERE c.constraint_type = 'R'
            AND cc.position = rc.position
            $schemaCondRemote $tableCondRemote
            ORDER BY c.owner, c.table_name, c.constraint_name, cc.position
        """.trimIndent()
        
        logger.info("Executing remote getImportedKeys SQL: {}", finalSql)
        val responseXml = sendSqlViaWsdl(
            connection.wsdlEndpoint,
            finalSql,
            connection.username,
            connection.password,
            connection.reportPath
        )
        val remoteRows = parseRows(responseXml)

        
        // Cache the results (same cache table as getForeignKeys)
        try {
            localConn.prepareStatement(
                """
                INSERT OR IGNORE INTO CACHED_FOREIGN_KEYS 
                (PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, PKCOLUMN_NAME, FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FKCOLUMN_NAME, KEY_SEQ, UPDATE_RULE, DELETE_RULE, FK_NAME, PK_NAME, DEFERRABILITY) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent()
            ).use { pstmt ->
                for (row in remoteRows) {
                    pstmt.setString(1, row["PKTABLE_CAT"] ?: "")
                    pstmt.setString(2, row["PKTABLE_SCHEM"] ?: "")
                    pstmt.setString(3, row["PKTABLE_NAME"] ?: "")
                    pstmt.setString(4, row["PKCOLUMN_NAME"] ?: "")
                    pstmt.setString(5, row["FKTABLE_CAT"] ?: "")
                    pstmt.setString(6, row["FKTABLE_SCHEM"] ?: "")
                    pstmt.setString(7, row["FKTABLE_NAME"] ?: "")
                    pstmt.setString(8, row["FKCOLUMN_NAME"] ?: "")
                    pstmt.setObject(9, row["KEY_SEQ"]?.toIntOrNull())
                    pstmt.setObject(10, row["UPDATE_RULE"]?.toIntOrNull())
                    pstmt.setObject(11, row["DELETE_RULE"]?.toIntOrNull())
                    pstmt.setString(12, row["FK_NAME"] ?: "")
                    pstmt.setString(13, row["PK_NAME"] ?: "")
                    pstmt.setObject(14, row["DEFERRABILITY"]?.toIntOrNull())
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
                logger.info("Saved remote imported keys metadata into local cache.")
            }
        } catch (ex: Exception) {
            logger.error("Error saving remote imported keys metadata to local cache: {}", ex.message)
        }
        
        return XmlResultSet(remoteRows.map { it.mapKeys { k -> k.key.lowercase() } })
    }

    // Exported foreign keys are foreign keys from this table (this table is the foreign key table)
    override fun getExportedKeys(
        catalog: String?, schema: String?, table: String?
    ): ResultSet {
        // For exported keys, this table is the foreign key table, so delegate to getForeignKeys
        return getForeignKeys(catalog, schema, table)
    }

    // Note: JDBC DatabaseMetaData doesn't have getForeignKeys method, but we implement it for internal use
    fun getForeignKeys(
        catalog: String?,
        schema: String?,
        table: String?
    ): ResultSet {
        if (table.isNullOrBlank()) {
            return createEmptyResultSet()
        }
        
        val localConn = LocalMetadataCache.connection
        val defSchema = (schema ?: "FUSION").uppercase()
        val tableName = table.uppercase()
        
        // Try cache first
        val cachedRows = mutableListOf<Map<String, String>>()
        try {
            val schemaCondLocal = if (!schema.isNullOrBlank()) "AND FKTABLE_SCHEM = '$defSchema'" else ""
            val tableCondLocal = "AND UPPER(FKTABLE_NAME) = '$tableName'"
            
            localConn.createStatement().use { stmt ->
                stmt.executeQuery(
                    """
                    SELECT PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, PKCOLUMN_NAME,
                           FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FKCOLUMN_NAME,
                           KEY_SEQ, UPDATE_RULE, DELETE_RULE, FK_NAME, PK_NAME, DEFERRABILITY
                    FROM CACHED_FOREIGN_KEYS
                    WHERE 1=1 $schemaCondLocal $tableCondLocal
                    ORDER BY FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, KEY_SEQ
                    """.trimIndent()
                ).use { rs ->
                    val meta = rs.metaData
                    while (rs.next()) {
                        val row = mutableMapOf<String, String>()
                        for (i in 1..meta.columnCount) {
                            row[meta.getColumnName(i).lowercase()] = rs.getString(i) ?: ""
                        }
                        cachedRows.add(row)
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error("Error reading cached foreign keys metadata: ${ex.message}")
        }
        
        if (cachedRows.isNotEmpty()) {
            logger.info("Returning ${cachedRows.size} foreign keys from local cache.")
            return XmlResultSet(cachedRows)
        }
        
        // Fetch from remote
        val schemaCondRemote = if (!schema.isNullOrBlank()) "AND c.owner = '$defSchema'" else "AND c.owner = 'FUSION'"
        val tableCondRemote = "AND UPPER(c.table_name) = '$tableName'"
        
        val finalSql = """
            SELECT 
                NULL AS PKTABLE_CAT,
                r.owner AS PKTABLE_SCHEM,
                r.table_name AS PKTABLE_NAME,
                rc.column_name AS PKCOLUMN_NAME,
                NULL AS FKTABLE_CAT,
                c.owner AS FKTABLE_SCHEM,
                c.table_name AS FKTABLE_NAME,
                cc.column_name AS FKCOLUMN_NAME,
                cc.position AS KEY_SEQ,
                CASE c.delete_rule 
                    WHEN 'CASCADE' THEN 0
                    WHEN 'SET NULL' THEN 2
                    ELSE 1 
                END AS UPDATE_RULE,
                CASE c.delete_rule 
                    WHEN 'CASCADE' THEN 0
                    WHEN 'SET NULL' THEN 2
                    ELSE 1 
                END AS DELETE_RULE,
                c.constraint_name AS FK_NAME,
                r.constraint_name AS PK_NAME,
                7 AS DEFERRABILITY
            FROM all_constraints c
            JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name AND c.owner = cc.owner
            JOIN all_constraints r ON c.r_constraint_name = r.constraint_name AND c.r_owner = r.owner
            JOIN all_cons_columns rc ON r.constraint_name = rc.constraint_name AND r.owner = rc.owner
            WHERE c.constraint_type = 'R'
            AND cc.position = rc.position
            $schemaCondRemote $tableCondRemote
            ORDER BY c.owner, c.table_name, c.constraint_name, cc.position
        """.trimIndent()
        
        logger.info("Executing remote getForeignKeys SQL: {}", finalSql)
        val responseXml = sendSqlViaWsdl(
            connection.wsdlEndpoint,
            finalSql,
            connection.username,
            connection.password,
            connection.reportPath
        )
        val remoteRows = parseRows(responseXml)

        
        // Cache the results
        try {
            localConn.prepareStatement(
                """
                INSERT OR IGNORE INTO CACHED_FOREIGN_KEYS 
                (PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, PKCOLUMN_NAME, FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FKCOLUMN_NAME, KEY_SEQ, UPDATE_RULE, DELETE_RULE, FK_NAME, PK_NAME, DEFERRABILITY) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent()
            ).use { pstmt ->
                for (row in remoteRows) {
                    pstmt.setString(1, row["PKTABLE_CAT"] ?: "")
                    pstmt.setString(2, row["PKTABLE_SCHEM"] ?: "")
                    pstmt.setString(3, row["PKTABLE_NAME"] ?: "")
                    pstmt.setString(4, row["PKCOLUMN_NAME"] ?: "")
                    pstmt.setString(5, row["FKTABLE_CAT"] ?: "")
                    pstmt.setString(6, row["FKTABLE_SCHEM"] ?: "")
                    pstmt.setString(7, row["FKTABLE_NAME"] ?: "")
                    pstmt.setString(8, row["FKCOLUMN_NAME"] ?: "")
                    pstmt.setObject(9, row["KEY_SEQ"]?.toIntOrNull())
                    pstmt.setObject(10, row["UPDATE_RULE"]?.toIntOrNull())
                    pstmt.setObject(11, row["DELETE_RULE"]?.toIntOrNull())
                    pstmt.setString(12, row["FK_NAME"] ?: "")
                    pstmt.setString(13, row["PK_NAME"] ?: "")
                    pstmt.setObject(14, row["DEFERRABILITY"]?.toIntOrNull())
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
                logger.info("Saved remote foreign keys metadata into local cache.")
            }
        } catch (ex: Exception) {
            logger.error("Error saving remote foreign keys metadata to local cache: {}", ex.message)
        }
        
        // Return the results with lowercase keys for XmlResultSet
        return XmlResultSet(remoteRows.map { it.mapKeys { k -> k.key.lowercase() } })
    }

    // Cross-reference (foreign key relationship) metadata not available via WSDL; return an empty result set.
    override fun getCrossReference(
        parentCatalog: String?,
        parentSchema: String?,
        parentTable: String?,
        foreignCatalog: String?,
        foreignSchema: String?,
        foreignTable: String?
    ): ResultSet = createEmptyResultSet()

    override fun getTypeInfo(): ResultSet {
        // Expanded Oracle type list (common scalar + LOB)
        val types: List<Map<String, Any?>> = listOf(
            mapOf(
                "TYPE_NAME" to "CHAR",
                "DATA_TYPE" to Types.CHAR,
                "PRECISION" to 2000,
                "LITERAL_PREFIX" to "'",
                "LITERAL_SUFFIX" to "'",
                "CREATE_PARAMS" to "length",
                "NULLABLE" to DatabaseMetaData.typeNullable,
                "CASE_SENSITIVE" to true,
                "SEARCHABLE" to DatabaseMetaData.typeSearchable,
                "UNSIGNED_ATTRIBUTE" to false,
                "FIXED_PREC_SCALE" to false,
                "AUTO_INCREMENT" to false,
                "LOCAL_TYPE_NAME" to "CHAR",
                "MINIMUM_SCALE" to 0,
                "MAXIMUM_SCALE" to 0,
                "SQL_DATA_TYPE" to 0,
                "SQL_DATETIME_SUB" to 0,
                "NUM_PREC_RADIX" to 10
            ),
            mapOf(
                "TYPE_NAME" to "NCHAR",
                "DATA_TYPE" to Types.NCHAR,
                "PRECISION" to 2000,
                "LITERAL_PREFIX" to "'",
                "LITERAL_SUFFIX" to "'",
                "CREATE_PARAMS" to "length",
                "NULLABLE" to DatabaseMetaData.typeNullable,
                "CASE_SENSITIVE" to true,
                "SEARCHABLE" to DatabaseMetaData.typeSearchable,
                "UNSIGNED_ATTRIBUTE" to false,
                "FIXED_PREC_SCALE" to false,
                "AUTO_INCREMENT" to false,
                "LOCAL_TYPE_NAME" to "NCHAR",
                "MINIMUM_SCALE" to 0,
                "MAXIMUM_SCALE" to 0,
                "SQL_DATA_TYPE" to 0,
                "SQL_DATETIME_SUB" to 0,
                "NUM_PREC_RADIX" to 10
            ),
            mapOf(
                "TYPE_NAME" to "VARCHAR2",
                "DATA_TYPE" to Types.VARCHAR,
                "PRECISION" to 4000,
                "LITERAL_PREFIX" to "'",
                "LITERAL_SUFFIX" to "'",
                "CREATE_PARAMS" to "length",
                "NULLABLE" to DatabaseMetaData.typeNullable,
                "CASE_SENSITIVE" to true,
                "SEARCHABLE" to DatabaseMetaData.typeSearchable,
                "UNSIGNED_ATTRIBUTE" to false,
                "FIXED_PREC_SCALE" to false,
                "AUTO_INCREMENT" to false,
                "LOCAL_TYPE_NAME" to "VARCHAR2",
                "MINIMUM_SCALE" to 0,
                "MAXIMUM_SCALE" to 0,
                "SQL_DATA_TYPE" to 0,
                "SQL_DATETIME_SUB" to 0,
                "NUM_PREC_RADIX" to 10
            ),
            mapOf(
                "TYPE_NAME" to "NVARCHAR2",
                "DATA_TYPE" to Types.NVARCHAR,
                "PRECISION" to 4000,
                "LITERAL_PREFIX" to "'",
                "LITERAL_SUFFIX" to "'",
                "CREATE_PARAMS" to "length",
                "NULLABLE" to DatabaseMetaData.typeNullable,
                "CASE_SENSITIVE" to true,
                "SEARCHABLE" to DatabaseMetaData.typeSearchable,
                "UNSIGNED_ATTRIBUTE" to false,
                "FIXED_PREC_SCALE" to false,
                "AUTO_INCREMENT" to false,
                "LOCAL_TYPE_NAME" to "NVARCHAR2",
                "MINIMUM_SCALE" to 0,
                "MAXIMUM_SCALE" to 0,
                "SQL_DATA_TYPE" to 0,
                "SQL_DATETIME_SUB" to 0,
                "NUM_PREC_RADIX" to 10
            ),
            mapOf(
                "TYPE_NAME" to "NUMBER",
                "DATA_TYPE" to Types.NUMERIC,
                "PRECISION" to 38,
                "LITERAL_PREFIX" to null,
                "LITERAL_SUFFIX" to null,
                "CREATE_PARAMS" to "precision,scale",
                "NULLABLE" to DatabaseMetaData.typeNullable,
                "CASE_SENSITIVE" to false,
                "SEARCHABLE" to DatabaseMetaData.typeSearchable,
                "UNSIGNED_ATTRIBUTE" to false,
                "FIXED_PREC_SCALE" to false,
                "AUTO_INCREMENT" to false,
                "LOCAL_TYPE_NAME" to "NUMBER",
                "MINIMUM_SCALE" to -84,
                "MAXIMUM_SCALE" to 127,
                "SQL_DATA_TYPE" to 0,
                "SQL_DATETIME_SUB" to 0,
                "NUM_PREC_RADIX" to 10
            ),
            mapOf(
                "TYPE_NAME" to "FLOAT",
                "DATA_TYPE" to Types.FLOAT,
                "PRECISION" to 126,
                "LITERAL_PREFIX" to null,
                "LITERAL_SUFFIX" to null,
                "CREATE_PARAMS" to "precision",
                "NULLABLE" to DatabaseMetaData.typeNullable,
                "CASE_SENSITIVE" to false,
                "SEARCHABLE" to DatabaseMetaData.typeSearchable,
                "UNSIGNED_ATTRIBUTE" to false,
                "FIXED_PREC_SCALE" to false,
                "AUTO_INCREMENT" to false,
                "LOCAL_TYPE_NAME" to "FLOAT",
                "MINIMUM_SCALE" to 0,
                "MAXIMUM_SCALE" to 0,
                "SQL_DATA_TYPE" to 0,
                "SQL_DATETIME_SUB" to 0,
                "NUM_PREC_RADIX" to 2
            ),
            mapOf(
                "TYPE_NAME" to "DATE",
                "DATA_TYPE" to Types.DATE,
                "PRECISION" to 7,
                "LITERAL_PREFIX" to "'",
                "LITERAL_SUFFIX" to "'",
                "CREATE_PARAMS" to null,
                "NULLABLE" to DatabaseMetaData.typeNullable,
                "CASE_SENSITIVE" to false,
                "SEARCHABLE" to DatabaseMetaData.typeSearchable,
                "UNSIGNED_ATTRIBUTE" to false,
                "FIXED_PREC_SCALE" to false,
                "AUTO_INCREMENT" to false,
                "LOCAL_TYPE_NAME" to "DATE",
                "MINIMUM_SCALE" to 0,
                "MAXIMUM_SCALE" to 0,
                "SQL_DATA_TYPE" to 0,
                "SQL_DATETIME_SUB" to 0,
                "NUM_PREC_RADIX" to 10
            ),
            mapOf(
                "TYPE_NAME" to "TIMESTAMP",
                "DATA_TYPE" to Types.TIMESTAMP,
                "PRECISION" to 11,
                "LITERAL_PREFIX" to "'",
                "LITERAL_SUFFIX" to "'",
                "CREATE_PARAMS" to "precision",
                "NULLABLE" to DatabaseMetaData.typeNullable,
                "CASE_SENSITIVE" to false,
                "SEARCHABLE" to DatabaseMetaData.typeSearchable,
                "UNSIGNED_ATTRIBUTE" to false,
                "FIXED_PREC_SCALE" to false,
                "AUTO_INCREMENT" to false,
                "LOCAL_TYPE_NAME" to "TIMESTAMP",
                "MINIMUM_SCALE" to 0,
                "MAXIMUM_SCALE" to 9,
                "SQL_DATA_TYPE" to 0,
                "SQL_DATETIME_SUB" to 0,
                "NUM_PREC_RADIX" to 10
            ),
            mapOf(
                "TYPE_NAME" to "RAW",
                "DATA_TYPE" to Types.BINARY,
                "PRECISION" to 2000,
                "LITERAL_PREFIX" to "HEXTORAW('",
                "LITERAL_SUFFIX" to "')",
                "CREATE_PARAMS" to "length",
                "NULLABLE" to DatabaseMetaData.typeNullable,
                "CASE_SENSITIVE" to false,
                "SEARCHABLE" to DatabaseMetaData.typePredNone,
                "UNSIGNED_ATTRIBUTE" to false,
                "FIXED_PREC_SCALE" to false,
                "AUTO_INCREMENT" to false,
                "LOCAL_TYPE_NAME" to "RAW",
                "MINIMUM_SCALE" to 0,
                "MAXIMUM_SCALE" to 0,
                "SQL_DATA_TYPE" to 0,
                "SQL_DATETIME_SUB" to 0,
                "NUM_PREC_RADIX" to 10
            ),
            mapOf(
                "TYPE_NAME" to "BLOB",
                "DATA_TYPE" to Types.BLOB,
                "PRECISION" to Int.MAX_VALUE,
                "LITERAL_PREFIX" to null,
                "LITERAL_SUFFIX" to null,
                "CREATE_PARAMS" to null,
                "NULLABLE" to DatabaseMetaData.typeNullable,
                "CASE_SENSITIVE" to false,
                "SEARCHABLE" to DatabaseMetaData.typePredNone,
                "UNSIGNED_ATTRIBUTE" to false,
                "FIXED_PREC_SCALE" to false,
                "AUTO_INCREMENT" to false,
                "LOCAL_TYPE_NAME" to "BLOB",
                "MINIMUM_SCALE" to 0,
                "MAXIMUM_SCALE" to 0,
                "SQL_DATA_TYPE" to 0,
                "SQL_DATETIME_SUB" to 0,
                "NUM_PREC_RADIX" to 10
            ),
            mapOf(
                "TYPE_NAME" to "CLOB",
                "DATA_TYPE" to Types.CLOB,
                "PRECISION" to Int.MAX_VALUE,
                "LITERAL_PREFIX" to null,
                "LITERAL_SUFFIX" to null,
                "CREATE_PARAMS" to null,
                "NULLABLE" to DatabaseMetaData.typeNullable,
                "CASE_SENSITIVE" to true,
                "SEARCHABLE" to DatabaseMetaData.typePredNone,
                "UNSIGNED_ATTRIBUTE" to false,
                "FIXED_PREC_SCALE" to false,
                "AUTO_INCREMENT" to false,
                "LOCAL_TYPE_NAME" to "CLOB",
                "MINIMUM_SCALE" to 0,
                "MAXIMUM_SCALE" to 0,
                "SQL_DATA_TYPE" to 0,
                "SQL_DATETIME_SUB" to 0,
                "NUM_PREC_RADIX" to 10
            )
        )

        // Convert to lowercase‑key string map for XmlResultSet
        val rows = types.map { row ->
            row.mapKeys { it.key.lowercase() }
               .mapValues { it.value?.toString() ?: "" }
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
        val localConn = LocalMetadataCache.connection
        val defSchema = (schema ?: "FUSION").uppercase()
        val tableName = table.uppercase()

        // Standard JDBC columns for getIndexInfo
        val schemaCondLocal = if (!schema.isNullOrBlank()) "AND TABLE_SCHEM = '$defSchema'" else ""
        val tableCondLocal  = "AND upper(TABLE_NAME) = '$tableName'"
        val uniqueCondLocal = if (unique) "AND NON_UNIQUE = '0'" else ""
        val localSql = """
            SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME,
                   NON_UNIQUE, INDEX_QUALIFIER, INDEX_NAME,
                   TYPE, ORDINAL_POSITION, COLUMN_NAME,
                   ASC_OR_DESC, CARDINALITY, PAGES, FILTER_CONDITION
            FROM CACHED_INDEXES
            WHERE 1=1 $schemaCondLocal $tableCondLocal $uniqueCondLocal
            ORDER BY INDEX_NAME, ORDINAL_POSITION
        """.trimIndent()

        try {
            localConn.createStatement().use { stmt ->
                stmt.executeQuery(localSql).use { rs ->
                    val rows = mutableListOf<Map<String, String>>()
                    val meta = rs.metaData
                    while (rs.next()) {
                        val row = mutableMapOf<String, String>()
                        for (i in 1..meta.columnCount) {
                            val colName = meta.getColumnName(i).lowercase()
                            val raw      = rs.getString(i)
                            // For numeric columns (BIGINT/INTEGER) DuckDB returns null;
                            // DBeaver expects "0" or a valid integer, not an empty string.
                            val normalised = if (raw.isNullOrBlank() &&
                                (colName == "cardinality" || colName == "pages" || colName == "ordinal_position"))
                                "0"
                            else
                                raw ?: ""
                            row[colName] = normalised
                        }
                        rows.add(row)
                    }
                    if (rows.isNotEmpty()) {
                        logger.info("Returning ${rows.size} indexes from local cache.")
                        return XmlResultSet(rows)
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error("Error reading index info from local cache: ${ex.message}")
        }

        // Build remote SQL for standard JDBC columns
        val remoteSql = """
            SELECT
              NULL AS TABLE_CAT,
              idx.owner AS TABLE_SCHEM,
              idx.table_name AS TABLE_NAME,
              CASE WHEN idx.uniqueness = 'UNIQUE' THEN '0' ELSE '1' END AS NON_UNIQUE,
              NULL AS INDEX_QUALIFIER,
              idx.index_name AS INDEX_NAME,
              CASE 
                WHEN idx.index_type = 'NORMAL' THEN '${DatabaseMetaData.tableIndexOther}'
                WHEN idx.index_type = 'BITMAP' THEN '${DatabaseMetaData.tableIndexOther}'
                ELSE '${DatabaseMetaData.tableIndexOther}'
              END AS TYPE,
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
            WHERE idx.owner = '$defSchema'
              AND upper(idx.table_name) = '$tableName'
              ${if (unique) "AND idx.uniqueness = 'UNIQUE'" else ""}
            ORDER BY idx.index_name, ic.column_position
        """.trimIndent()
        logger.info("Executing remote getIndexInfo SQL: {}", remoteSql)
        val responseXml = sendSqlViaWsdl(
            connection.wsdlEndpoint,
            remoteSql,
            connection.username,
            connection.password,
            connection.reportPath
        )
        val remoteRows = parseRows(responseXml, true)

        // Insert into cache with all 13 columns
        try {
            localConn.prepareStatement(
                """
                INSERT OR IGNORE INTO CACHED_INDEXES
                (TABLE_CAT, TABLE_SCHEM, TABLE_NAME, NON_UNIQUE, INDEX_QUALIFIER, INDEX_NAME,
                 TYPE, ORDINAL_POSITION, COLUMN_NAME, ASC_OR_DESC, CARDINALITY, PAGES, FILTER_CONDITION)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent()
            ).use { pstmt ->
                for (row in remoteRows) {
                    pstmt.setString(1, row["table_cat"] ?: "")
                    pstmt.setString(2, row["table_schem"] ?: "")
                    pstmt.setString(3, row["table_name"] ?: "")
                    pstmt.setString(4, row["non_unique"] ?: "")
                    pstmt.setString(5, row["index_qualifier"] ?: "")
                    pstmt.setString(6, row["index_name"] ?: "")
                    pstmt.setString(7, row["type"] ?: "")
                    // ORDINAL_POSITION (INTEGER)
                    pstmt.setObject(
                        8,
                        row["ordinal_position"]?.takeIf { it.isNotBlank() }?.toIntOrNull()
                    )
                    pstmt.setString(9, row["column_name"] ?: "")
                    pstmt.setString(10, row["asc_or_desc"] ?: "")
                    pstmt.setObject(
                        11,
                        row["cardinality"]?.takeIf { it.isNotBlank() }?.toLongOrNull()
                    )
                    // PAGES (INTEGER)
                    pstmt.setObject(
                        12,
                        row["pages"]?.takeIf { it.isNotBlank() }?.toIntOrNull()
                    )
                    pstmt.setString(13, row["filter_condition"] ?: "")
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
                logger.info("Saved remote index info into local cache (${remoteRows.size} rows).")
            }
        } catch (ex: Exception) {
            logger.error("Error saving index info to local cache: ${ex.message}")
        }

        // Only return maps with standard JDBC keys, normalising blank/null numeric columns to "0"
        val jdbcRows = remoteRows.map { row ->
            val keys = listOf(
                "table_cat", "table_schem", "table_name", "non_unique", "index_qualifier", "index_name",
                "type", "ordinal_position", "column_name", "asc_or_desc", "cardinality", "pages", "filter_condition"
            )
            keys.associateWith { key ->
                val v = row[key] ?: ""
                if (v.isBlank() &&
                    (key == "cardinality" || key == "pages" || key == "ordinal_position"))
                    "0"
                else
                    v
            }
        }
        return XmlResultSet(jdbcRows)
    }

    private fun executeWithPagination(
        endpoint: String,
        baseSql: String,
        username: String,
        password: String,
        reportPath: String,
        pageSize: Int = 2000
    ): List<Map<String, String>> {
        val allRows = mutableListOf<Map<String, String>>()
        var offset = 0
        var hasMoreData = true

        while (hasMoreData) {
            // Add ROWNUM-based pagination to the SQL
            val paginatedSql = """
            SELECT * FROM (
                SELECT ROWNUM as rn, sub.* FROM (
                    $baseSql
                ) sub
                WHERE ROWNUM <= ${offset + pageSize}
            )
            WHERE rn > $offset
        """.trimIndent()

            logger.info("Executing paginated query (offset: $offset, limit: $pageSize)")
            val responseXml = sendSqlViaWsdl(endpoint, paginatedSql, username, password, reportPath)
            val remoteRows = parseRows(responseXml)

            val pageRows = remoteRows.map { row ->
                val m = mutableMapOf<String, String>()
                for ((k,v) in row) {
                    if (k.uppercase() != "RN") {
                        m[k.uppercase()] = (v ?: "").trim()
                    }
                }
                m
            }

            allRows.addAll(pageRows)

            // Check if we got a full page - if not, we're done
            hasMoreData = pageRows.size == pageSize
            offset += pageRows.size

            logger.info("Retrieved ${'$'}{pageRows.size} rows in this page, total so far: ${'$'}{allRows.size}")
        }

        logger.info("Pagination complete. Total rows retrieved: ${allRows.size}")
        return allRows
    }

    override fun supportsResultSetType(type: Int): Boolean = type == ResultSet.TYPE_FORWARD_ONLY

    override fun supportsResultSetConcurrency(type: Int, concurrency: Int): Boolean = type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY

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

    override fun getUDTs(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        types: IntArray?
    ): ResultSet {
        val localConn = LocalMetadataCache.connection
        val defSchema = "FUSION"
        
        // Try cache first
        val cachedRows = mutableListOf<Map<String, String>>()
        try {
            val schemaCondLocal = if (!schemaPattern.isNullOrBlank()) "AND TYPE_SCHEM LIKE '${schemaPattern.uppercase()}'" else "AND TYPE_SCHEM = '$defSchema'"
            val nameCondLocal = if (!typeNamePattern.isNullOrBlank()) "AND TYPE_NAME LIKE '${typeNamePattern.uppercase()}'" else ""
            val typeCondLocal = if (types != null && types.isNotEmpty()) {
                val typeList = types.joinToString(",")
                "AND DATA_TYPE IN ($typeList)"
            } else ""
            
            localConn.createStatement().use { stmt ->
                stmt.executeQuery(
                    """
                    SELECT TYPE_CAT, TYPE_SCHEM, TYPE_NAME, CLASS_NAME, DATA_TYPE, REMARKS, BASE_TYPE
                    FROM CACHED_UDTS
                    WHERE 1=1 $schemaCondLocal $nameCondLocal $typeCondLocal
                    ORDER BY TYPE_SCHEM, TYPE_NAME
                    """.trimIndent()
                ).use { rs ->
                    val meta = rs.metaData
                    while (rs.next()) {
                        val row = mutableMapOf<String, String>()
                        for (i in 1..meta.columnCount) {
                            row[meta.getColumnName(i).lowercase()] = rs.getString(i) ?: ""
                        }
                        cachedRows.add(row)
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error("Error reading cached UDTs metadata: ${ex.message}")
        }
        
        if (cachedRows.isNotEmpty()) {
            logger.info("Returning ${cachedRows.size} UDTs from local cache.")
            return XmlResultSet(cachedRows)
        }
        
        // Fetch from remote
        val schemaCondRemote = if (!schemaPattern.isNullOrBlank()) "AND owner LIKE '${schemaPattern.uppercase()}'" else "AND owner = '$defSchema'"
        val nameCondRemote = if (!typeNamePattern.isNullOrBlank()) "AND type_name LIKE '${typeNamePattern.uppercase()}'" else ""
        
        val sql = """
            SELECT 
                NULL AS TYPE_CAT,
                owner AS TYPE_SCHEM,
                type_name AS TYPE_NAME,
                NULL AS CLASS_NAME,
                CASE 
                    WHEN typecode = 'OBJECT' THEN 2000
                    WHEN typecode = 'COLLECTION' THEN 2003
                    ELSE 2000
                END AS DATA_TYPE,
                NULL AS REMARKS,
                NULL AS BASE_TYPE
            FROM all_types
            WHERE predefined = 'NO'
            $schemaCondRemote $nameCondRemote
            ORDER BY owner, type_name
        """.trimIndent()
        
        logger.info("Executing remote getUDTs SQL: {}", sql)
        val responseXml = sendSqlViaWsdl(
            connection.wsdlEndpoint,
            sql,
            connection.username,
            connection.password,
            connection.reportPath
        )
        val remoteRows = parseRows(responseXml)

        
        // Cache the results
        try {
            localConn.prepareStatement(
                """
                INSERT OR IGNORE INTO CACHED_UDTS 
                (TYPE_CAT, TYPE_SCHEM, TYPE_NAME, CLASS_NAME, DATA_TYPE, REMARKS, BASE_TYPE) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """.trimIndent()
            ).use { pstmt ->
                for (row in remoteRows) {
                    pstmt.setString(1, row["TYPE_CAT"] ?: "")
                    pstmt.setString(2, row["TYPE_SCHEM"] ?: "")
                    pstmt.setString(3, row["TYPE_NAME"] ?: "")
                    pstmt.setString(4, row["CLASS_NAME"] ?: "")
                    pstmt.setObject(5, row["DATA_TYPE"]?.toIntOrNull())
                    pstmt.setString(6, row["REMARKS"] ?: "")
                    pstmt.setObject(7, row["BASE_TYPE"]?.toIntOrNull())
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
                logger.info("Saved remote UDTs metadata into local cache.")
            }
        } catch (ex: Exception) {
            logger.error("Error saving remote UDTs metadata to local cache: {}", ex.message)
        }
        
        return XmlResultSet(remoteRows.map { it.mapKeys { k -> k.key.lowercase() } })
    }

    override fun getConnection(): Connection =
        throw SQLFeatureNotSupportedException(
            "WsdlDatabaseMetaData.getConnection(): use the existing Connection instance"
        )

    override fun supportsSavepoints(): Boolean = false

    override fun supportsNamedParameters(): Boolean = false

    override fun supportsMultipleOpenResults(): Boolean = false

    override fun supportsGetGeneratedKeys(): Boolean = false

    override fun getSuperTypes(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?
    ): ResultSet = createEmptyResultSet()

    // Oracle does not support table inheritance; return empty set.
    override fun getSuperTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?
    ): ResultSet = createEmptyResultSet()

    override fun getAttributes(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        attributeNamePattern: String?
    ): ResultSet = createEmptyResultSet()

    override fun supportsResultSetHoldability(holdability: Int): Boolean =
        holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT

    override fun getResultSetHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT


    override fun getDatabaseMajorVersion(): Int {
        // Return a static major version for Oracle Fusion.
        return 1
    }

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

    override fun getFunctions(
        catalog: String?, schemaPattern: String?, functionNamePattern: String?
    ): ResultSet {
        val localConn = LocalMetadataCache.connection
        val defSchema = "FUSION"
        
        // Try cache first
        val cachedRows = mutableListOf<Map<String, String>>()
        try {
            val schemaCondLocal = if (!schemaPattern.isNullOrBlank()) "AND FUNCTION_SCHEM LIKE '${schemaPattern.uppercase()}'" else "AND FUNCTION_SCHEM = '$defSchema'"
            val nameCondLocal = if (!functionNamePattern.isNullOrBlank()) "AND FUNCTION_NAME LIKE '${functionNamePattern.uppercase()}'" else ""
            
            localConn.createStatement().use { stmt ->
                stmt.executeQuery(
                    """
                    SELECT FUNCTION_CAT, FUNCTION_SCHEM, FUNCTION_NAME, REMARKS, FUNCTION_TYPE, SPECIFIC_NAME
                    FROM CACHED_FUNCTIONS
                    WHERE 1=1 $schemaCondLocal $nameCondLocal
                    ORDER BY FUNCTION_SCHEM, FUNCTION_NAME
                    """.trimIndent()
                ).use { rs ->
                    val meta = rs.metaData
                    while (rs.next()) {
                        val row = mutableMapOf<String, String>()
                        for (i in 1..meta.columnCount) {
                            row[meta.getColumnName(i).lowercase()] = rs.getString(i) ?: ""
                        }
                        cachedRows.add(row)
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error("Error reading cached functions metadata: ${ex.message}")
        }
        
        if (cachedRows.isNotEmpty()) {
            logger.info("Returning ${cachedRows.size} functions from local cache.")
            return XmlResultSet(cachedRows)
        }
        
        // Fetch from remote - functions are procedures with procedure_name NOT NULL
        val schemaCondRemote = if (!schemaPattern.isNullOrBlank()) "AND owner LIKE '${schemaPattern.uppercase()}'" else "AND owner = '$defSchema'"
        val nameCondRemote = if (!functionNamePattern.isNullOrBlank()) "AND object_name LIKE '${functionNamePattern.uppercase()}'" else ""
        
        val sql = """
            SELECT 
                NULL AS FUNCTION_CAT,
                owner AS FUNCTION_SCHEM,
                object_name AS FUNCTION_NAME,
                NULL AS REMARKS,
                2 AS FUNCTION_TYPE,
                object_name AS SPECIFIC_NAME
            FROM all_procedures
            WHERE procedure_name IS NOT NULL
            $schemaCondRemote $nameCondRemote
            ORDER BY owner, object_name
        """.trimIndent()
        
        logger.info("Executing remote getFunctions SQL: {}", sql)
        val responseXml = sendSqlViaWsdl(
            connection.wsdlEndpoint,
            sql,
            connection.username,
            connection.password,
            connection.reportPath
        )
        val remoteRows = parseRows(responseXml)

        
        // Cache the results
        try {
            localConn.prepareStatement(
                """
                INSERT OR IGNORE INTO CACHED_FUNCTIONS 
                (FUNCTION_CAT, FUNCTION_SCHEM, FUNCTION_NAME, REMARKS, FUNCTION_TYPE, SPECIFIC_NAME) 
                VALUES (?, ?, ?, ?, ?, ?)
                """.trimIndent()
            ).use { pstmt ->
                for (row in remoteRows) {
                    pstmt.setString(1, row["FUNCTION_CAT"] ?: "")
                    pstmt.setString(2, row["FUNCTION_SCHEM"] ?: "")
                    pstmt.setString(3, row["FUNCTION_NAME"] ?: "")
                    pstmt.setString(4, row["REMARKS"] ?: "")
                    pstmt.setObject(5, row["FUNCTION_TYPE"]?.toIntOrNull())
                    pstmt.setString(6, row["SPECIFIC_NAME"] ?: "")
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
                logger.info("Saved remote functions metadata into local cache.")
            }
        } catch (ex: Exception) {
            logger.error("Error saving remote functions metadata to local cache: {}", ex.message)
        }
        
        return XmlResultSet(remoteRows.map { it.mapKeys { k -> k.key.lowercase() } })
    }

    override fun getFunctionColumns(
        catalog: String?, schemaPattern: String?,
        functionNamePattern: String?, columnNamePattern: String?
    ): ResultSet = createEmptyResultSet()

    override fun getPseudoColumns(
        catalog: String?, schemaPattern: String?,
        tableNamePattern: String?, columnNamePattern: String?
    ): ResultSet = createEmptyResultSet()

    override fun generatedKeyAlwaysReturned(): Boolean = false

    override fun <T : Any?> unwrap(iface: Class<T>?): T =
        throw SQLFeatureNotSupportedException(
            "WsdlDatabaseMetaData.unwrap(): cannot unwrap to ${iface?.name}"
        )
    override fun isWrapperFor(iface: Class<*>?): Boolean = false

}
