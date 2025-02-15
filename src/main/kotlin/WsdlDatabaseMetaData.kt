package my.jdbc.wsdl_driver

import org.apache.commons.text.StringEscapeUtils
import java.sql.*

class WsdlDatabaseMetaData(private val connection: WsdlConnection) : DatabaseMetaData {
    override fun getDatabaseProductName(): String = "Oracle Fusion WSDL JDBC Driver"
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

    override fun supportsMixedCaseIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun storesUpperCaseIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun storesLowerCaseIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun storesMixedCaseIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsMixedCaseQuotedIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun storesUpperCaseQuotedIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun storesLowerCaseQuotedIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun storesMixedCaseQuotedIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getIdentifierQuoteString(): String {
        TODO("Not yet implemented")
    }

    override fun getSQLKeywords(): String {
        TODO("Not yet implemented")
    }

    override fun getURL(): String = connection.wsdlEndpoint
    override fun getUserName(): String = connection.username
    override fun isReadOnly(): Boolean {
        TODO("Not yet implemented")
    }

    override fun nullsAreSortedHigh(): Boolean {
        TODO("Not yet implemented")
    }

    override fun nullsAreSortedLow(): Boolean {
        TODO("Not yet implemented")
    }

    override fun nullsAreSortedAtStart(): Boolean {
        TODO("Not yet implemented")
    }

    override fun nullsAreSortedAtEnd(): Boolean {
        TODO("Not yet implemented")
    }

    // All other methods throw SQLFeatureNotSupportedException with sequential numbers starting at 257.
    override fun allProceduresAreCallable(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 257")
    override fun allTablesAreSelectable(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 258")
    override fun getExtraNameCharacters(): String = throw SQLFeatureNotSupportedException("Not implemented 259")
    override fun supportsAlterTableWithAddColumn(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsAlterTableWithDropColumn(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsColumnAliasing(): Boolean {
        TODO("Not yet implemented")
    }

    override fun nullPlusNonNullIsNull(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsConvert(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsConvert(fromType: Int, toType: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsTableCorrelationNames(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsDifferentTableCorrelationNames(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsExpressionsInOrderBy(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsOrderByUnrelated(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsGroupBy(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsGroupByUnrelated(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsGroupByBeyondSelect(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsLikeEscapeClause(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsMultipleResultSets(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsMultipleTransactions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsNonNullableColumns(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsMinimumSQLGrammar(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsCoreSQLGrammar(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsExtendedSQLGrammar(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsANSI92EntryLevelSQL(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsANSI92IntermediateSQL(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsANSI92FullSQL(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsIntegrityEnhancementFacility(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsOuterJoins(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsFullOuterJoins(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsLimitedOuterJoins(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getNumericFunctions(): String = throw SQLFeatureNotSupportedException("Not implemented 260")
    override fun getStringFunctions(): String = throw SQLFeatureNotSupportedException("Not implemented 261")
    override fun getSystemFunctions(): String = throw SQLFeatureNotSupportedException("Not implemented 262")
    override fun getTimeDateFunctions(): String = throw SQLFeatureNotSupportedException("Not implemented 263")
    override fun getSearchStringEscape(): String = throw SQLFeatureNotSupportedException("Not implemented 264")
    override fun getSchemaTerm(): String = throw SQLFeatureNotSupportedException("Not implemented 265")
    override fun getProcedureTerm(): String = throw SQLFeatureNotSupportedException("Not implemented 266")
    override fun getCatalogTerm(): String = throw SQLFeatureNotSupportedException("Not implemented 267")
    override fun isCatalogAtStart(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 268")
    override fun getCatalogSeparator(): String = throw SQLFeatureNotSupportedException("Not implemented 269")
    override fun supportsSchemasInDataManipulation(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsSchemasInProcedureCalls(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsSchemasInTableDefinitions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsSchemasInIndexDefinitions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsSchemasInPrivilegeDefinitions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsCatalogsInDataManipulation(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsCatalogsInProcedureCalls(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsCatalogsInTableDefinitions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsCatalogsInIndexDefinitions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsCatalogsInPrivilegeDefinitions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsPositionedDelete(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 270")
    override fun supportsPositionedUpdate(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 271")
    override fun supportsSelectForUpdate(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 272")
    override fun supportsStoredProcedures(): Boolean = false
    override fun supportsSubqueriesInComparisons(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 273")
    override fun supportsSubqueriesInExists(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 274")
    override fun supportsSubqueriesInIns(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 275")
    override fun supportsSubqueriesInQuantifieds(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 276")
    override fun supportsCorrelatedSubqueries(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 277")
    override fun supportsUnion(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 278")
    override fun supportsUnionAll(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 279")
    override fun supportsOpenCursorsAcrossCommit(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 280")
    override fun supportsOpenCursorsAcrossRollback(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 281")
    override fun supportsOpenStatementsAcrossCommit(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 282")
    override fun supportsOpenStatementsAcrossRollback(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 283")
    override fun getMaxBinaryLiteralLength(): Int = throw SQLFeatureNotSupportedException("Not implemented 284")
    override fun getMaxCharLiteralLength(): Int = throw SQLFeatureNotSupportedException("Not implemented 285")
    override fun getMaxColumnNameLength(): Int = throw SQLFeatureNotSupportedException("Not implemented 286")
    override fun getMaxColumnsInGroupBy(): Int = throw SQLFeatureNotSupportedException("Not implemented 287")
    override fun getMaxColumnsInIndex(): Int = throw SQLFeatureNotSupportedException("Not implemented 288")
    override fun getMaxColumnsInOrderBy(): Int = throw SQLFeatureNotSupportedException("Not implemented 289")
    override fun getMaxColumnsInSelect(): Int = throw SQLFeatureNotSupportedException("Not implemented 290")
    override fun getMaxColumnsInTable(): Int = throw SQLFeatureNotSupportedException("Not implemented 291")
    override fun getMaxConnections(): Int = throw SQLFeatureNotSupportedException("Not implemented 292")
    override fun getMaxCursorNameLength(): Int = throw SQLFeatureNotSupportedException("Not implemented 293")
    override fun getMaxIndexLength(): Int = throw SQLFeatureNotSupportedException("Not implemented 294")
    override fun getMaxSchemaNameLength(): Int = throw SQLFeatureNotSupportedException("Not implemented 295")
    override fun getMaxProcedureNameLength(): Int = throw SQLFeatureNotSupportedException("Not implemented 296")
    override fun getMaxCatalogNameLength(): Int = throw SQLFeatureNotSupportedException("Not implemented 297")
    override fun getMaxRowSize(): Int = throw SQLFeatureNotSupportedException("Not implemented 298")
    override fun doesMaxRowSizeIncludeBlobs(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 299")
    override fun getMaxStatementLength(): Int = throw SQLFeatureNotSupportedException("Not implemented 300")
    override fun getMaxStatements(): Int = throw SQLFeatureNotSupportedException("Not implemented 301")
    override fun getMaxTableNameLength(): Int = throw SQLFeatureNotSupportedException("Not implemented 302")
    override fun getMaxTablesInSelect(): Int = throw SQLFeatureNotSupportedException("Not implemented 303")
    override fun getMaxUserNameLength(): Int = throw SQLFeatureNotSupportedException("Not implemented 304")
    override fun getDefaultTransactionIsolation(): Int = throw SQLFeatureNotSupportedException("Not implemented 305")
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
        // Use your sendSqlViaWsdl to run the query
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






    override fun getSchemas(): ResultSet = throw SQLFeatureNotSupportedException("Not implemented 309")
    override fun getSchemas(catalog: String?, schemaPattern: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getCatalogs(): ResultSet = throw SQLFeatureNotSupportedException("Not implemented 310")
    override fun getTableTypes(): ResultSet = throw SQLFeatureNotSupportedException("Not implemented 311")
//    override fun getColumns(catalog: String?, schemaPattern: String?, tableNamePattern: String?, columnNamePattern: String?): ResultSet =
//        throw SQLFeatureNotSupportedException("Not implemented 312")
    override fun getColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        // Minimal implementation: return an empty result set
        return createEmptyResultSet()
    }
    override fun getColumnPrivileges(catalog: String?, schema: String?, table: String?, columnNamePattern: String?): ResultSet =
        throw SQLFeatureNotSupportedException("Not implemented 313")
    override fun getTablePrivileges(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet =
        throw SQLFeatureNotSupportedException("Not implemented 314")
    override fun getBestRowIdentifier(catalog: String?, schema: String?, table: String?, scope: Int, nullable: Boolean): ResultSet =
        throw SQLFeatureNotSupportedException("Not implemented 315")
    override fun getVersionColumns(catalog: String?, schema: String?, table: String?): ResultSet =
        throw SQLFeatureNotSupportedException("Not implemented 316")
    override fun getPrimaryKeys(catalog: String?, schema: String?, table: String?): ResultSet =
        throw SQLFeatureNotSupportedException("Not implemented 317")
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
    override fun getTypeInfo(): ResultSet = throw SQLFeatureNotSupportedException("Not implemented 321")
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
    override fun supportsBatchUpdates(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getUDTs(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        types: IntArray?
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getConnection(): Connection {
        TODO("Not yet implemented")
    }

    override fun supportsSavepoints(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsNamedParameters(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsMultipleOpenResults(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsGetGeneratedKeys(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getSuperTypes(catalog: String?, schemaPattern: String?, typeNamePattern: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getSuperTables(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getAttributes(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        attributeNamePattern: String?
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun supportsResultSetHoldability(holdability: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun getResultSetHoldability(): Int {
        TODO("Not yet implemented")
    }

    override fun getDatabaseMajorVersion(): Int {
        TODO("Not yet implemented")
    }

    override fun getDatabaseMinorVersion(): Int {
        TODO("Not yet implemented")
    }

    override fun getJDBCMajorVersion(): Int {
        TODO("Not yet implemented")
    }

    override fun getJDBCMinorVersion(): Int {
        TODO("Not yet implemented")
    }

    override fun getSQLStateType(): Int = throw SQLFeatureNotSupportedException("Not implemented 323")
    override fun locatorsUpdateCopy(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 324")
    override fun supportsStatementPooling(): Boolean = throw SQLFeatureNotSupportedException("Not implemented 325")
    override fun getRowIdLifetime(): RowIdLifetime {
        TODO("Not yet implemented")
    }

    override fun supportsStoredFunctionsUsingCallSyntax(): Boolean {
        TODO("Not yet implemented")
    }

    override fun autoCommitFailureClosesAllResultSets(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getClientInfoProperties(): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getFunctions(catalog: String?, schemaPattern: String?, functionNamePattern: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getFunctionColumns(
        catalog: String?,
        schemaPattern: String?,
        functionNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getPseudoColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun generatedKeyAlwaysReturned(): Boolean {
        TODO("Not yet implemented")
    }

    override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLFeatureNotSupportedException("Not implemented 326")
    override fun isWrapperFor(iface: Class<*>?): Boolean = false
}