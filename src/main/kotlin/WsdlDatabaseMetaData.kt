package my.jdbc.wsdl_driver

import org.apache.commons.text.StringEscapeUtils
import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import java.sql.*


object LocalMetadataCache {
    // Use a file path in the user's home directory.
    private val userHome = System.getProperty("user.home")
    private val duckDbFilePath = "$userHome/metadata.db"

    val connection: Connection by lazy {
        logger.info("Using DuckDB file: $duckDbFilePath")
        val conn = DriverManager.getConnection("jdbc:duckdb:$duckDbFilePath")
        conn.createStatement().use { stmt ->
            stmt.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS SCHEMAS_CACHE (
                    TABLE_SCHEM VARCHAR PRIMARY KEY,
                    TABLE_CATALOG VARCHAR
                )
                """.trimIndent()
            )
            stmt.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS CACHED_TABLES (
                    TABLE_CAT  VARCHAR,
                    TABLE_SCHEM VARCHAR,
                    TABLE_NAME  VARCHAR,
                    TABLE_TYPE  VARCHAR,
                    REMARKS VARCHAR,
                    TYPE_CAT VARCHAR,
                    TYPE_SCHEM VARCHAR,
                    TYPE_NAME VARCHAR,
                    SELF_REFERENCING_COL_NAME VARCHAR,
                    REF_GENERATION VARCHAR,
                    PRIMARY KEY (TABLE_SCHEM, TABLE_NAME)
                )
                """.trimIndent()
            )
            stmt.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS CACHED_COLUMNS (
                    TABLE_CAT  VARCHAR,
                    TABLE_SCHEM VARCHAR,
                    TABLE_NAME  VARCHAR,
                    COLUMN_NAME VARCHAR,
                    ORDINAL_POSITION VARCHAR,
                    DATA_TYPE VARCHAR,
                    TYPE_NAME VARCHAR,
                    COLUMN_SIZE VARCHAR,
                    DECIMAL_DIGITS VARCHAR,
                    NUM_PREC_RADIX VARCHAR,
                    NULLABLE VARCHAR,
                    PRIMARY KEY (TABLE_SCHEM, TABLE_NAME, COLUMN_NAME)
                )
                """.trimIndent()
            )
            stmt.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS CACHED_INDEXES (
                    TABLE_SCHEM VARCHAR,
                    TABLE_NAME  VARCHAR,
                    INDEX_NAME  VARCHAR,
                    ORDINAL_POSITION VARCHAR,
                    PRIMARY KEY (TABLE_SCHEM, TABLE_NAME, INDEX_NAME, ORDINAL_POSITION)
                )
                """.trimIndent()
            )
        }
        conn
    }

    fun close() {
        try {
            if (!connection.isClosed) {
                logger.info("Closing DuckDB connection.")
                connection.close()
            }
        } catch (ex: Exception) {
            logger.error("Error closing DuckDB connection: ${ex.message}")
        }
    }
}



class WsdlDatabaseMetaData(private val connection: WsdlConnection) : DatabaseMetaData {
    /**
     * Resolve which Oracle schema to query.
     *
     * 0. If the connection itself has an explicitly set schema, honour it
     * 1. If the caller passed an explicit pattern – use that.
     * 2. If the JDBC username *looks* like a real Oracle schema (doesn’t contain '@'),
     *    use the username.
     * 3. Otherwise fall back to the default Fusion schema.
     */
    private fun resolveSchema(pattern: String?): String {
        // 0. If the connection itself has an explicitly set schema, honour it
        (connection as? WsdlConnection)?.let { conn ->
            val connSchema = conn.getSchema()
            if (!connSchema.isNullOrBlank()) return connSchema.uppercase()
        }
        if (!pattern.isNullOrBlank()) return pattern.uppercase()

        val login = connection.username
        return if (!login.isNullOrBlank() && !login.contains('@'))
            login.uppercase()
        else
            "FUSION"
    }
    override fun getDatabaseProductName(): String = "Oracle Fusion JDBC Driver"
    override fun getDatabaseProductVersion(): String = "1.0"
    override fun getDriverName(): String = "sergey.rudenko.ba@gmail.com"
    override fun getDriverVersion(): String = "1.0"
    override fun getDriverMajorVersion(): Int = 1
    override fun getDriverMinorVersion(): Int = 0


    override fun usesLocalFiles(): Boolean {
        TODO("Not yet implemented 52")
    }

    override fun usesLocalFilePerTable(): Boolean {
        TODO("Not yet implemented 53")
    }

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

    override fun nullsAreSortedAtEnd(): Boolean = false

    // All other methods throw SQLFeatureNotSupportedException with sequential numbers starting at 257.
    override fun allProceduresAreCallable(): Boolean = false //throw SQLFeatureNotSupportedException("Not implemented 257")
    override fun allTablesAreSelectable(): Boolean = true //throw SQLFeatureNotSupportedException("Not implemented 258")
    //override fun getExtraNameCharacters(): String = throw SQLFeatureNotSupportedException("Not implemented 259")
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

    override fun getSchemaTerm(): String {
        //TODO("Not yet implemented 54")
        return "FUSION"
    }

    override fun getNumericFunctions(): String = "ABS,ACOS,ASIN,ATAN,ATAN2,CEIL,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MOD,POWER,ROUND,SIN,SQRT,TAN"
    override fun getStringFunctions(): String = "LCASE,UPPER,INITCAP,LTRIM,RTRIM,CONCAT,SUBSTR,INSTR,REPLACE,LPAD,RPAD"
    override fun getSystemFunctions(): String = "USER,SYSDATE,CURRENT_DATE,SYS_GUID()"
    override fun getTimeDateFunctions(): String = "NOW,CURRENT_DATE,CURRENT_TIMESTAMP,SYSDATE,SYSTIMESTAMP,LOCALTIMESTAMP,SESSIONTIMEZONE,DBTIMEZONE"
    //override fun getSchemaTerm(): String = throw SQLFeatureNotSupportedException("Not implemented 265")
    override fun getSearchStringEscape(): String = "\\"
    //override fun getProcedureTerm(): String = throw SQLFeatureNotSupportedException("Not implemented 266")
    // In Oracle, the standard term for stored procedures is "PROCEDURE"
    override fun getProcedureTerm(): String = "PROCEDURE"
    //override fun getCatalogTerm(): String = throw SQLFeatureNotSupportedException("Not implemented 267")
    // In Oracle, the concept of a catalog isn’t used
    override fun getCatalogTerm(): String = ""
    //override fun isCatalogAtStart(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 268")
    override fun isCatalogAtStart(): Boolean = false
    //override fun getCatalogSeparator(): String = throw SQLFeatureNotSupportedException("Not implemented 269")
    override fun getCatalogSeparator(): String = "."
    override fun supportsSchemasInDataManipulation(): Boolean = true
    override fun supportsSchemasInProcedureCalls(): Boolean = true
    override fun supportsSchemasInTableDefinitions(): Boolean = true
    override fun supportsSchemasInIndexDefinitions(): Boolean = true
    override fun supportsSchemasInPrivilegeDefinitions(): Boolean = true
    override fun supportsCatalogsInDataManipulation(): Boolean = false
    override fun supportsCatalogsInProcedureCalls(): Boolean = false
    override fun supportsCatalogsInTableDefinitions(): Boolean = false
    override fun supportsCatalogsInIndexDefinitions(): Boolean = false
    override fun supportsCatalogsInPrivilegeDefinitions(): Boolean = false

    override fun supportsPositionedDelete(): Boolean = false //throw SQLFeatureNotSupportedException("Not implemented 270")
    override fun supportsPositionedUpdate(): Boolean = false //throw SQLFeatureNotSupportedException("Not implemented 271")
    override fun supportsSelectForUpdate(): Boolean = false //throw SQLFeatureNotSupportedException("Not implemented 272")
    override fun supportsStoredProcedures(): Boolean = false
    override fun supportsSubqueriesInComparisons(): Boolean = true //throw SQLFeatureNotSupportedException("Not implemented 273")
    override fun supportsSubqueriesInExists(): Boolean = true //throw SQLFeatureNotSupportedException("Not implemented 274")
    override fun supportsSubqueriesInIns(): Boolean = true //throw SQLFeatureNotSupportedException("Not implemented 275")
    override fun supportsSubqueriesInQuantifieds(): Boolean = true //throw SQLFeatureNotSupportedException("Not implemented 276")
    override fun supportsCorrelatedSubqueries(): Boolean = true
    override fun supportsUnion(): Boolean = true //throw SQLFeatureNotSupportedException("Not implemented 278")
    override fun supportsUnionAll(): Boolean = true //throw SQLFeatureNotSupportedException("Not implemented 279")
    override fun supportsOpenCursorsAcrossCommit(): Boolean = false //throw SQLFeatureNotSupportedException("Not implemented 280")
    override fun supportsOpenCursorsAcrossRollback(): Boolean = false //throw SQLFeatureNotSupportedException("Not implemented 281")
    override fun supportsOpenStatementsAcrossCommit(): Boolean = false //throw SQLFeatureNotSupportedException("Not implemented 282")
    override fun supportsOpenStatementsAcrossRollback(): Boolean = false //throw SQLFeatureNotSupportedException("Not implemented 283")

    override fun getMaxBinaryLiteralLength(): Int = 0 //throw SQLFeatureNotSupportedException("Not implemented 284")
    override fun getMaxCharLiteralLength(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 285")
    override fun getMaxColumnNameLength(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 286")
    override fun getMaxColumnsInGroupBy(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 287")
    override fun getMaxColumnsInIndex(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 288")
    override fun getMaxColumnsInOrderBy(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 289")
    override fun getMaxColumnsInSelect(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 290")
    override fun getMaxColumnsInTable(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 291")
    override fun getMaxConnections(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 292")
    override fun getMaxCursorNameLength(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 293")
    override fun getMaxIndexLength(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 294")
    override fun getMaxSchemaNameLength(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 295")
    override fun getMaxProcedureNameLength(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 296")
    override fun getMaxCatalogNameLength(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 297")
    override fun getMaxRowSize(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 298")
    override fun doesMaxRowSizeIncludeBlobs(): Boolean = false //throw SQLFeatureNotSupportedException("Not implemented 299")
    override fun getMaxStatementLength(): Int = 0 // throw SQLFeatureNotSupportedException("Not implemented 300")
    override fun getMaxStatements(): Int = 0 //  throw SQLFeatureNotSupportedException("Not implemented 301")
    override fun getMaxTableNameLength(): Int = 0 //  throw SQLFeatureNotSupportedException("Not implemented 302")
    override fun getMaxTablesInSelect(): Int = 0 //  throw SQLFeatureNotSupportedException("Not implemented 303")
    override fun getMaxUserNameLength(): Int = 0 //  throw SQLFeatureNotSupportedException("Not implemented 304")
    override fun getDefaultTransactionIsolation(): Int = Connection.TRANSACTION_NONE //throw SQLFeatureNotSupportedException("Not implemented 305")
    override fun supportsTransactions(): Boolean = false
    override fun supportsTransactionIsolationLevel(level: Int): Boolean = false
    override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean = false
    override fun supportsDataManipulationTransactionsOnly(): Boolean = false
    override fun dataDefinitionCausesTransactionCommit(): Boolean = false
    override fun dataDefinitionIgnoredInTransactions(): Boolean = false
//    override fun getProcedures(catalog: String?, schemaPattern: String?, procedureNamePattern: String?): ResultSet =
//        throw SQLFeatureNotSupportedException("Not implemented 306")
    override fun getProcedures(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?
    ): ResultSet {
        // Oracle Fusion exposes no PL/SQL procedures; return an empty set so
        // callers don't fail on SQLFeatureNotSupportedException.
        return createEmptyResultSet()
    }


    override fun getProcedureColumns(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet = throw SQLFeatureNotSupportedException("Not implemented 307")

    // Implement getTables for Oracle with schema pattern support and cache uniqueness.
    override fun getTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        types: Array<out String>?
    ): ResultSet {
        val localConn = LocalMetadataCache.connection
        val cachedRows = mutableListOf<Map<String, String>>()

        val defSchema = resolveSchema(schemaPattern)
        val schemaCondLocal = if (!schemaPattern.isNullOrBlank())
            " AND TABLE_SCHEM LIKE '${schemaPattern.uppercase()}'"
        else
            " AND TABLE_SCHEM = '$defSchema'"
        val tableCondLocal   = if (!tableNamePattern.isNullOrBlank()) " AND TABLE_NAME LIKE '${tableNamePattern.uppercase()}'" else ""
        val typesList = types?.map { it.uppercase() } ?: listOf("TABLE", "VIEW")
        val typeCondLocal = if (typesList.isNotEmpty()) {
            " AND TABLE_TYPE IN (${typesList.joinToString(",") { "'$it'" }})"
        } else ""

        // First, attempt to read from the local cache.
        try {
            localConn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT * FROM CACHED_TABLES WHERE 1=1$schemaCondLocal$tableCondLocal$typeCondLocal ORDER BY TABLE_NAME").use { rs ->
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

        // If the cache is empty, build the remote query.
        // If the caller did not specify a schema pattern, restrict to the resolved default
        val schemaCondRemote = if (!schemaPattern.isNullOrBlank())
            " AND owner LIKE '${schemaPattern.uppercase()}'"
        else
            " AND owner = '$defSchema'"
        val tableCondRemote  = if (!tableNamePattern.isNullOrBlank()) " AND upper(object_name) LIKE '${tableNamePattern.uppercase()}'" else ""
        val typeCondRemote   = if (typesList.isNotEmpty()) {
            " AND object_type IN (${typesList.joinToString(",") { "'$it'" }})"
        } else " AND object_type IN ('TABLE', 'VIEW')"
        val remoteSql = """
            SELECT DISTINCT
                NULL AS TABLE_CAT,
                owner AS TABLE_SCHEM,
                upper(object_name) AS TABLE_NAME,
                object_type AS TABLE_TYPE,
                NULL AS REMARKS,
                NULL AS TYPE_CAT,
                NULL AS TYPE_SCHEM,
                NULL AS TYPE_NAME,
                NULL AS SELF_REFERENCING_COL_NAME,
                NULL AS REF_GENERATION
            FROM all_objects
            WHERE 1=1$schemaCondRemote$tableCondRemote$typeCondRemote
            ORDER BY TABLE_SCHEM, TABLE_NAME
        """.trimIndent()

        logger.info("Executing remote getTables SQL: {}", remoteSql)
        val responseXml = sendSqlViaWsdl(
            connection.wsdlEndpoint,
            remoteSql,
            connection.username,
            connection.password,
            connection.reportPath
        )
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

        // Cache the remote metadata into the local DuckDB cache.
        try {
            localConn.prepareStatement(
                """
                INSERT OR IGNORE INTO CACHED_TABLES 
                (TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, TYPE_CAT, TYPE_SCHEM, TYPE_NAME, SELF_REFERENCING_COL_NAME, REF_GENERATION) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent()
            ).use { pstmt ->
                for (i in 0 until rowNodes.length) {
                    val rowNode = rowNodes.item(i)
                    var tableCat = ""
                    var tableSchem = ""
                    var tableName = ""
                    var tableType = ""
                    var remarks = ""
                    var typeCat = ""
                    var typeSchem = ""
                    var typeName = ""
                    var selfRefCol = ""
                    var refGeneration = ""
                    val children = rowNode.childNodes
                    for (j in 0 until children.length) {
                        val child = children.item(j)
                        if (child.nodeType == Node.ELEMENT_NODE) {
                            when (child.nodeName.uppercase()) {
                                "TABLE_CAT" -> tableCat = child.textContent.trim()
                                "TABLE_SCHEM" -> tableSchem = child.textContent.trim()
                                "TABLE_NAME" -> tableName = child.textContent.trim()
                                "TABLE_TYPE" -> tableType = child.textContent.trim()
                                "REMARKS" -> remarks = child.textContent.trim()
                                "TYPE_CAT" -> typeCat = child.textContent.trim()
                                "TYPE_SCHEM" -> typeSchem = child.textContent.trim()
                                "TYPE_NAME" -> typeName = child.textContent.trim()
                                "SELF_REFERENCING_COL_NAME" -> selfRefCol = child.textContent.trim()
                                "REF_GENERATION" -> refGeneration = child.textContent.trim()
                            }
                        }
                    }
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
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
                logger.info("Saved remote tables metadata into local cache.")
            }
        } catch (ex: Exception) {
            logger.error("Error saving remote tables metadata to local cache: {}", ex.message)
        }

        // Now re-read the local cache.
        val newCachedRows = mutableListOf<Map<String, String>>()
        try {
            localConn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT * FROM CACHED_TABLES WHERE 1=1$schemaCondLocal$tableCondLocal$typeCondLocal ORDER BY TABLE_NAME").use { rs ->
                    val meta = rs.metaData
                    while (rs.next()) {
                        val row = mutableMapOf<String, String>()
                        for (i in 1..meta.columnCount) {
                            row[meta.getColumnName(i).lowercase()] = rs.getString(i) ?: ""
                        }
                        newCachedRows.add(row)
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error("Error reading cached tables metadata: {}", ex.message)
        }
        return XmlResultSet(newCachedRows)
    }


    // Implement getSchemas() with DuckDB cache and remote fallback
    override fun getSchemas(): ResultSet {
        val localConn = LocalMetadataCache.connection
        val cachedRows = mutableListOf<Map<String, String>>()
        try {
            localConn.createStatement().use { stmt ->
                stmt.executeQuery(
                    "SELECT TABLE_SCHEM, TABLE_CATALOG FROM SCHEMAS_CACHE ORDER BY TABLE_SCHEM"
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
            logger.error("Error reading local metadata cache: ${ex.message}")
        }

        if (cachedRows.isNotEmpty()) {
            logger.info("Returning {} schemas from local cache.", cachedRows.size)
            return XmlResultSet(cachedRows)
        }

        // If no cached data is found, query the remote service.
        val schemaPattern: String? = null // no filter for getSchemas()
        val remoteSql = buildString {
            append("SELECT username AS TABLE_SCHEM, '' AS TABLE_CATALOG FROM all_users WHERE 1=1 ")
            if (!schemaPattern.isNullOrBlank()) {
                append("AND username LIKE '${schemaPattern.uppercase()}' ")
            }
            append("ORDER BY username")
        }
        val responseXml = sendSqlViaWsdl(
            connection.wsdlEndpoint,
            remoteSql,
            connection.username,
            connection.password,
            connection.reportPath
        )
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

        // Save the remote schemas into the local cache, avoiding duplicates.
        try {
            localConn.prepareStatement(
                "INSERT OR IGNORE INTO SCHEMAS_CACHE (TABLE_SCHEM, TABLE_CATALOG) VALUES (?, ?)"
            ).use { pstmt ->
                for (i in 0 until rowNodes.length) {
                    val rowNode = rowNodes.item(i)
                    var tableSchem = ""
                    var tableCatalog = ""
                    val children = rowNode.childNodes
                    for (j in 0 until children.length) {
                        val child = children.item(j)
                        if (child.nodeType == Node.ELEMENT_NODE) {
                            when (child.nodeName.uppercase()) {
                                "TABLE_SCHEM" -> tableSchem = child.textContent.trim()
                                "TABLE_CATALOG" -> tableCatalog = child.textContent.trim()
                            }
                        }
                    }
                    pstmt.setString(1, tableSchem)
                    pstmt.setString(2, tableCatalog)
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
                logger.info("Saved remote schemas into local cache.")
            }
        } catch (ex: Exception) {
            logger.error("Error saving remote metadata to local cache: ${ex.message}")
        }
        // Always re-read the cache and return those rows.
        val rereadRows = mutableListOf<Map<String, String>>()
        try {
            localConn.createStatement().use { stmt ->
                stmt.executeQuery(
                    "SELECT TABLE_SCHEM, TABLE_CATALOG FROM SCHEMAS_CACHE ORDER BY TABLE_SCHEM"
                ).use { rs ->
                    val meta = rs.metaData
                    while (rs.next()) {
                        val row = mutableMapOf<String, String>()
                        for (i in 1..meta.columnCount) {
                            row[meta.getColumnName(i).lowercase()] = rs.getString(i) ?: ""
                        }
                        rereadRows.add(row)
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error("Error reading local metadata cache after remote: ${ex.message}")
        }
        return XmlResultSet(rereadRows)
    }




    override fun getSchemas(catalog: String?, schemaPattern: String?): ResultSet {
        TODO("Not yet implemented 55")
    }

    //override fun getCatalogs(): ResultSet = throw SQLFeatureNotSupportedException("Not implemented 310")
    override fun getCatalogs(): ResultSet = createEmptyResultSet()
    //override fun getTableTypes(): ResultSet = throw SQLFeatureNotSupportedException("Not implemented 311")
    override fun getTableTypes(): ResultSet {
        val typesList = listOf(
            mapOf("TABLE_TYPE" to "TABLE"),
            mapOf("TABLE_TYPE" to "VIEW")
        )
        return XmlResultSet(typesList)
    }

    //    override fun getColumns(catalog: String?, schemaPattern: String?, tableNamePattern: String?, columnNamePattern: String?): ResultSet =
//        throw SQLFeatureNotSupportedException("Not implemented 312")
    override fun getColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        val localConn = LocalMetadataCache.connection
        val defSchema = resolveSchema(schemaPattern)
        val schemaCondLocal = if (!schemaPattern.isNullOrBlank())
            " AND TABLE_SCHEM LIKE '${schemaPattern.uppercase()}'"
        else
            " AND TABLE_SCHEM = '$defSchema'"
        val tableCondLocal   = if (!tableNamePattern.isNullOrBlank()) " AND TABLE_NAME LIKE '${tableNamePattern.uppercase()}'" else ""
        val columnCondLocal  = if (!columnNamePattern.isNullOrBlank()) " AND COLUMN_NAME LIKE '${columnNamePattern.uppercase()}'" else ""
        val localQuery = """
            SELECT 
                TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE, TYPE_NAME, 
                COLUMN_SIZE, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE, ORDINAL_POSITION 
            FROM CACHED_COLUMNS
            WHERE 1=1 $schemaCondLocal $tableCondLocal $columnCondLocal
            ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
        """.trimIndent()

        // Try reading from the local cache.
        try {
            localConn.createStatement().use { stmt ->
                stmt.executeQuery(localQuery).use { rs ->
                    val localRows = mutableListOf<Map<String, String>>()
                    while (rs.next()) {
                        val row = mutableMapOf<String, String>()
                        row["table_cat"] = rs.getString("TABLE_CAT") ?: ""
                        row["table_schem"] = rs.getString("TABLE_SCHEM") ?: ""
                        row["table_name"] = rs.getString("TABLE_NAME") ?: ""
                        row["column_name"] = rs.getString("COLUMN_NAME") ?: ""
                        row["data_type"] = rs.getString("DATA_TYPE") ?: ""
                        row["type_name"] = rs.getString("TYPE_NAME") ?: ""
                        row["column_size"] = rs.getString("COLUMN_SIZE") ?: ""
                        row["decimal_digits"] = rs.getString("DECIMAL_DIGITS") ?: ""
                        row["num_prec_radix"] = rs.getString("NUM_PREC_RADIX") ?: ""
                        row["nullable"] = rs.getString("NULLABLE") ?: ""
                        row["ordinal_position"] = rs.getString("ORDINAL_POSITION") ?: ""
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
        val schemaCondRemote = if (!schemaPattern.isNullOrBlank())
            " AND owner LIKE '${schemaPattern.uppercase()}'"
        else
            " AND owner = '$defSchema'"
        val tableCondRemote   = if (!tableNamePattern.isNullOrBlank()) " AND table_name LIKE '${tableNamePattern.uppercase()}'" else ""
        val columnCondRemote  = if (!columnNamePattern.isNullOrBlank()) " AND column_name LIKE '${columnNamePattern.uppercase()}'" else ""
        val sql = """
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
            WHERE 1=1 $schemaCondRemote $tableCondRemote $columnCondRemote
            ORDER BY owner, table_name, column_id
        """.trimIndent()

        val responseXml = sendSqlViaWsdl(
            connection.wsdlEndpoint,
            sql,
            connection.username,
            connection.password,
            connection.reportPath
        )
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

        val remoteRows = mutableListOf<Map<String, String>>()
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
                remoteRows.add(rowMap)
            }
        }

        // Save the remote metadata into the local cache, avoiding duplicates.
        try {
            localConn.prepareStatement(
                """
                INSERT OR IGNORE INTO CACHED_COLUMNS 
                (TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_SIZE, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE, ORDINAL_POSITION)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent()
            ).use { pstmt ->
                for (row in remoteRows) {
                    pstmt.setString(1, row["table_cat"] ?: "")
                    pstmt.setString(2, row["table_schem"] ?: "")
                    pstmt.setString(3, row["table_name"] ?: "")
                    pstmt.setString(4, row["column_name"] ?: "")
                    pstmt.setString(5, row["data_type"] ?: "")
                    pstmt.setString(6, row["type_name"] ?: "")
                    pstmt.setString(7, row["column_size"] ?: "")
                    pstmt.setString(8, row["decimal_digits"] ?: "")
                    pstmt.setString(9, row["num_prec_radix"] ?: "")
                    pstmt.setString(10, row["nullable"] ?: "")
                    pstmt.setString(11, row["ordinal_position"] ?: "")
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
                logger.info("Saved remote columns into local cache (${remoteRows.size} rows).")
            }
        } catch (ex: Exception) {
            logger.error("Error saving remote metadata to local cache: ${ex.message}")
        }
        return XmlResultSet(remoteRows)
    }



    override fun getColumnPrivileges(catalog: String?, schema: String?, table: String?, columnNamePattern: String?): ResultSet =
        throw SQLFeatureNotSupportedException("Not implemented 313")
    override fun getTablePrivileges(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet =
        throw SQLFeatureNotSupportedException("Not implemented 314")
    override fun getBestRowIdentifier(catalog: String?, schema: String?, table: String?, scope: Int, nullable: Boolean): ResultSet =
        throw SQLFeatureNotSupportedException("Not implemented 315")
    override fun getVersionColumns(catalog: String?, schema: String?, table: String?): ResultSet =
        throw SQLFeatureNotSupportedException("Not implemented 316")
    //override fun getPrimaryKeys(catalog: String?, schema: String?, table: String?): ResultSet =
    //    throw SQLFeatureNotSupportedException("Not implemented 317")
    /*override fun getPrimaryKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        // This minimal driver does not support retrieving primary keys.
        // Returning an empty ResultSet.
        return createEmptyResultSet()
    }*/
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

        val doc = parseXml(responseXml)
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

        return XmlResultSet(resultRows)
    }
    override fun getImportedKeys(catalog: String?, schema: String?, table: String?): ResultSet =
        throw SQLFeatureNotSupportedException("Not implemented 318")
    override fun getExportedKeys(catalog: String?, schema: String?, table: String?): ResultSet =
        throw SQLFeatureNotSupportedException("Not implemented 319")

    override fun getCrossReference(
        parentCatalog: String?,
        parentSchema: String?,
        parentTable: String?,
        foreignCatalog: String?,
        foreignSchema: String?,
        foreignTable: String?
    ): ResultSet = throw SQLFeatureNotSupportedException("Not implemented 320")
    //override fun getTypeInfo(): ResultSet = throw SQLFeatureNotSupportedException("Not implemented 321")
    override fun getTypeInfo(): ResultSet {
        // For Oracle Fusion, we provide type information for several common Oracle data types.
        val typesList: List<Map<String, Any?>> = listOf(
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
                "TYPE_NAME" to "NUMBER",
                "DATA_TYPE" to Types.NUMERIC,
                "PRECISION" to 38,
                "LITERAL_PREFIX" to null,
                "LITERAL_SUFFIX" to null,
                "CREATE_PARAMS" to null,
                "NULLABLE" to DatabaseMetaData.typeNullable,
                "CASE_SENSITIVE" to false,
                "SEARCHABLE" to DatabaseMetaData.typeSearchable,
                "UNSIGNED_ATTRIBUTE" to false,
                "FIXED_PREC_SCALE" to false,
                "AUTO_INCREMENT" to false,
                "LOCAL_TYPE_NAME" to "NUMBER",
                "MINIMUM_SCALE" to 0,
                "MAXIMUM_SCALE" to 38,
                "SQL_DATA_TYPE" to 0,
                "SQL_DATETIME_SUB" to 0,
                "NUM_PREC_RADIX" to 10
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
                "CREATE_PARAMS" to null,
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
            )
        )

        // Convert each value to a String (using empty string for null) so our minimal XmlResultSet works.
        val rowsAsString = typesList.map { row ->
            row.mapValues { (_, v) -> v?.toString() ?: "" }
        }
        return XmlResultSet(rowsAsString)
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
        val defSchema = resolveSchema(schema)
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
                            row[meta.getColumnName(i).lowercase()] = rs.getString(i) ?: ""
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
        val doc = parseXml(responseXml)
        var rowNodes = doc.getElementsByTagName("ROW")
        if (rowNodes.length == 0) {
            val resultNodes = doc.getElementsByTagName("RESULT")
            if (resultNodes.length > 0) {
                val unescaped = StringEscapeUtils.unescapeXml(resultNodes.item(0).textContent.trim())
                rowNodes = parseXml(unescaped).getElementsByTagName("ROW")
            }
        }

        // Map XML nodes to lowercase-keyed map for standard JDBC columns
        val remoteRows = mutableListOf<Map<String, String>>()
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
                remoteRows.add(map)
            }
        }
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
                    pstmt.setString(8, row["ordinal_position"] ?: "")
                    pstmt.setString(9, row["column_name"] ?: "")
                    pstmt.setString(10, row["asc_or_desc"] ?: "")
                    pstmt.setString(11, row["cardinality"] ?: "")
                    pstmt.setString(12, row["pages"] ?: "")
                    pstmt.setString(13, row["filter_condition"] ?: "")
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
                logger.info("Saved remote index info into local cache (${remoteRows.size} rows).")
            }
        } catch (ex: Exception) {
            logger.error("Error saving index info to local cache: ${ex.message}")
        }

        // Only return maps with standard JDBC keys
        val jdbcRows = remoteRows.map { row ->
            val keys = listOf(
                "table_cat", "table_schem", "table_name", "non_unique", "index_qualifier", "index_name",
                "type", "ordinal_position", "column_name", "asc_or_desc", "cardinality", "pages", "filter_condition"
            )
            keys.associateWith { row[it] ?: "" }
        }
        return XmlResultSet(jdbcRows)
    }



    override fun supportsResultSetType(type: Int): Boolean = false
    override fun supportsResultSetConcurrency(type: Int, concurrency: Int): Boolean = false
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
        // Return an empty result set for UDT queries.
        return createEmptyResultSet()
    }


    override fun getConnection(): Connection {
        throw SQLFeatureNotSupportedException("Not implemented 326")
    }

    override fun supportsSavepoints(): Boolean = false
    override fun supportsNamedParameters(): Boolean = false
    override fun supportsMultipleOpenResults(): Boolean = false
    override fun supportsGetGeneratedKeys(): Boolean = false

    override fun getSuperTypes(catalog: String?, schemaPattern: String?, typeNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("Not implemented 327")
    }

    override fun getSuperTables(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("Not implemented 328")
    }

    override fun getAttributes(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        attributeNamePattern: String?
    ): ResultSet {
        throw SQLFeatureNotSupportedException("Not implemented 329")
    }

    override fun supportsResultSetHoldability(holdability: Int): Boolean = false


    override fun getResultSetHoldability(): Int {
        throw SQLFeatureNotSupportedException("Not implemented 331")
    }


    override fun getDatabaseMajorVersion(): Int {
        // Return a static major version for Oracle Fusion.
        return 1
    }

    override fun getDatabaseMinorVersion(): Int = 0


    override fun getJDBCMajorVersion(): Int = 4

    override fun getJDBCMinorVersion(): Int = 2

    //override fun getSQLStateType(): Int = throw SQLFeatureNotSupportedException("Not implemented 323")
    override fun getSQLStateType(): Int = DatabaseMetaData.sqlStateSQL99
    override fun locatorsUpdateCopy(): Boolean = false //throw SQLFeatureNotSupportedException("Not implemented 324")
    override fun supportsStatementPooling(): Boolean = false //throw SQLFeatureNotSupportedException("Not implemented 325")
    override fun getRowIdLifetime(): RowIdLifetime {
        return RowIdLifetime.ROWID_VALID_FOREVER
    }

    override fun supportsStoredFunctionsUsingCallSyntax(): Boolean = false
    override fun autoCommitFailureClosesAllResultSets(): Boolean = false

    override fun getClientInfoProperties(): ResultSet {
        throw SQLFeatureNotSupportedException("Not implemented 332")
    }

    override fun getFunctions(catalog: String?, schemaPattern: String?, functionNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("Not implemented 333")
    }

    override fun getFunctionColumns(
        catalog: String?,
        schemaPattern: String?,
        functionNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        throw SQLFeatureNotSupportedException("Not implemented 334")
    }

    override fun getPseudoColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        throw SQLFeatureNotSupportedException("Not implemented 335")
    }

    override fun generatedKeyAlwaysReturned(): Boolean = false

    override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLFeatureNotSupportedException("Not implemented 336")
    override fun isWrapperFor(iface: Class<*>?): Boolean = false


}

