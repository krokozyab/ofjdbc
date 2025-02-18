package my.jdbc.wsdl_driver

import org.apache.commons.text.StringEscapeUtils
import org.w3c.dom.Document
import org.w3c.dom.NodeList
import java.sql.*

class WsdlDatabaseMetaData(private val connection: WsdlConnection) : DatabaseMetaData {
    override fun getDatabaseProductName(): String = "Oracle Fusion JDBC Driver"
    override fun getDatabaseProductVersion(): String = "1.0"
    override fun getDriverName(): String = "sergey.rudenko.ba@gmail.com"
    override fun getDriverVersion(): String = "1.0"
    override fun getDriverMajorVersion(): Int = 1
    override fun getDriverMinorVersion(): Int = 0


    override fun usesLocalFiles(): Boolean {
        TODO("Not yet implemented")
    }

    override fun usesLocalFilePerTable(): Boolean {
        TODO("Not yet implemented")
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
    override fun supportsAlterTableWithAddColumn(): Boolean = true
    override fun supportsAlterTableWithDropColumn(): Boolean =true
    override fun supportsColumnAliasing(): Boolean = true
    override fun nullPlusNonNullIsNull(): Boolean = true
    override fun supportsConvert(): Boolean = false
    override fun supportsConvert(fromType: Int, toType: Int): Boolean = false
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
        TODO("Not yet implemented")
    }

    override fun getNumericFunctions(): String = "ABS,ACOS,ASIN,ATAN,ATAN2,CEIL,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MOD,POWER,ROUND,SIN,SQRT,TAN"
    override fun getStringFunctions(): String = "LCASE,UPPER,INITCAP,LTRIM,RTRIM,CONCAT,SUBSTR,INSTR,REPLACE,LPAD,RPAD"
    override fun getSystemFunctions(): String = "USER,SYSDATE,CURRENT_DATE"
    override fun getTimeDateFunctions(): String = "NOW,CURDATE,CURTIME,SYSDATE"
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
    override fun supportsSchemasInIndexDefinitions(): Boolean = false
    override fun supportsSchemasInPrivilegeDefinitions(): Boolean = false
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
    override fun getProcedures(catalog: String?, schemaPattern: String?, procedureNamePattern: String?): ResultSet =
        throw SQLFeatureNotSupportedException("Not implemented 306")
    override fun getProcedureColumns(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet = throw SQLFeatureNotSupportedException("Not implemented 307")

    //override fun getTables(catalog: String?, schemaPattern: String?, tableNamePattern: String?, types: Array<out String>?): ResultSet =
    //    throw SQLFeatureNotSupportedException("Not implemented 308")

    // Implement getTables for Oracle
    override fun getTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        types: Array<out String>?
    ): ResultSet {
        // Build WHERE conditions based on provided parameters
        val schemaCond = if (!schemaPattern.isNullOrEmpty()) " AND owner LIKE '$schemaPattern'" else ""
        val tableCond = if (!tableNamePattern.isNullOrEmpty()) " AND table_name LIKE '$tableNamePattern'" else ""
        val viewCond = if (!tableNamePattern.isNullOrEmpty()) " AND view_name LIKE '$tableNamePattern'" else ""
        // Determine which types to include (default both TABLE and VIEW)
        val typesList = types?.map { it.uppercase() } ?: listOf("TABLE", "VIEW")
        val queries = mutableListOf<String>()
        if (typesList.isEmpty() || typesList.contains("TABLE")) {
            queries.add(
                "SELECT null AS TABLE_CAT, owner AS TABLE_SCHEM, table_name AS TABLE_NAME, 'TABLE' AS TABLE_TYPE, " +
                        "null AS REMARKS, null AS TYPE_CAT, null AS TYPE_SCHEM, null AS TYPE_NAME, " +
                        "null AS SELF_REFERENCING_COL_NAME, null AS REF_GENERATION " +
                        "FROM all_tables WHERE 1=1$schemaCond$tableCond"
            )
        }
        if (typesList.isEmpty() || typesList.contains("VIEW")) {
            queries.add(
                "SELECT null AS TABLE_CAT, owner AS TABLE_SCHEM, view_name AS TABLE_NAME, 'VIEW' AS TABLE_TYPE, " +
                        "null AS REMARKS, null AS TYPE_CAT, null AS TYPE_SCHEM, null AS TYPE_NAME, " +
                        "null AS SELF_REFERENCING_COL_NAME, null AS REF_GENERATION " +
                        "FROM all_views WHERE 1=1$schemaCond$viewCond"
            )
        }
        if (queries.isEmpty()) return createEmptyResultSet()
        val finalSql = queries.joinToString(" UNION ALL ")
        val responseXml = sendSqlViaWsdl(connection.wsdlEndpoint, finalSql, connection.username, connection.password, connection.reportPath)
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
        return createResultSetFromRowNodes(rowNodes)
    }






    //override fun getSchemas(): ResultSet = throw SQLFeatureNotSupportedException("Not implemented 309")
    override fun getSchemas(): ResultSet {
        // Query ALL_USERS to list all schemas (usernames)
        val sql = "SELECT username AS TABLE_SCHEM, '' AS TABLE_CATALOG FROM all_users ORDER BY username"
        // Send the SQL query via the WSDL service using the connection parameters.
        val responseXml = sendSqlViaWsdl(
            connection.wsdlEndpoint,
            sql,
            connection.username,
            connection.password,
            connection.reportPath
        )
        val doc = parseXml(responseXml)
        var rowNodes: NodeList = doc.getElementsByTagName("ROW")
        if (rowNodes.length == 0) {
            val resultNodes: NodeList = doc.getElementsByTagName("RESULT")
            if (resultNodes.length > 0) {
                val resultText: String = resultNodes.item(0).textContent.trim()
                val unescapedXml: String = org.apache.commons.text.StringEscapeUtils.unescapeXml(resultText)
                val rowDoc: Document = parseXml(unescapedXml)
                rowNodes = rowDoc.getElementsByTagName("ROW")
            }
        }
        return createResultSetFromRowNodes(rowNodes)
    }

    override fun getSchemas(catalog: String?, schemaPattern: String?): ResultSet {
        TODO("Not yet implemented")
    }

    //override fun getCatalogs(): ResultSet = throw SQLFeatureNotSupportedException("Not implemented 310")
    override fun getCatalogs(): ResultSet = createEmptyResultSet()
    override fun getTableTypes(): ResultSet = throw SQLFeatureNotSupportedException("Not implemented 311")
//    override fun getColumns(catalog: String?, schemaPattern: String?, tableNamePattern: String?, columnNamePattern: String?): ResultSet =
//        throw SQLFeatureNotSupportedException("Not implemented 312")
override fun getColumns(
    catalog: String?,
    schemaPattern: String?,
    tableNamePattern: String?,
    columnNamePattern: String?
): ResultSet {
    // Build Oracle filter conditions.
    // In Oracle, the "schema" is the OWNER and the table name is stored in TABLE_NAME.
    val schemaCond = if (!schemaPattern.isNullOrBlank()) " AND owner LIKE '${schemaPattern.uppercase()}'" else ""
    val tableCond = if (!tableNamePattern.isNullOrBlank()) " AND table_name LIKE '${tableNamePattern.uppercase()}'" else ""
    val columnCond = if (!columnNamePattern.isNullOrBlank()) " AND column_name LIKE '${columnNamePattern.uppercase()}'" else ""

    // Minimal set of columns that a JDBC driver is expected to return:
    // TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE, TYPE_NAME,
    // COLUMN_SIZE, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE, ORDINAL_POSITION, etc.
    // For this minimal implementation we will select a subset.
    val sql = """
        SELECT 
            NULL AS TABLE_CAT,
            owner AS TABLE_SCHEM,
            table_name AS TABLE_NAME,
            column_name AS COLUMN_NAME,
            /* For simplicity, we use a constant for DATA_TYPE (VARCHAR) */
            ${java.sql.Types.VARCHAR} AS DATA_TYPE,
            data_type AS TYPE_NAME,
            data_length AS COLUMN_SIZE,
            data_precision AS DECIMAL_DIGITS,
            data_scale AS NUM_PREC_RADIX,
            CASE WHEN nullable = 'Y' THEN 1 ELSE 0 END AS NULLABLE,
            column_id AS ORDINAL_POSITION
        FROM all_tab_columns
        WHERE 1=1 $schemaCond $tableCond $columnCond
        ORDER BY owner, table_name, column_id
    """.trimIndent()

    // Execute the query via our helper (this sends the SQL via WSDL and returns an XML response).
    val responseXml = sendSqlViaWsdl(connection.wsdlEndpoint, sql, connection.username, connection.password, connection.reportPath)
    val doc = parseXml(responseXml)
    var rowNodes = doc.getElementsByTagName("ROW")
    if (rowNodes.length == 0) {
        val resultNodes = doc.getElementsByTagName("RESULT")
        if (resultNodes.length > 0) {
            val resultText = resultNodes.item(0).textContent.trim()
            val unescapedXml = org.apache.commons.text.StringEscapeUtils.unescapeXml(resultText)
            val rowDoc = parseXml(unescapedXml)
            rowNodes = rowDoc.getElementsByTagName("ROW")
        }
    }
    return createResultSetFromRowNodes(rowNodes)
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
    override fun getPrimaryKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        // This minimal driver does not support retrieving primary keys.
        // Returning an empty ResultSet.
        return createEmptyResultSet()
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

    override fun getIndexInfo(catalog: String?, schema: String?, table: String?, unique: Boolean, approximate: Boolean): ResultSet =
        throw SQLFeatureNotSupportedException("Not implemented 322")


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
        throw SQLFeatureNotSupportedException("Not implemented 325")
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

    override fun supportsResultSetHoldability(holdability: Int): Boolean {
        throw SQLFeatureNotSupportedException("Not implemented 330")
    }

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

