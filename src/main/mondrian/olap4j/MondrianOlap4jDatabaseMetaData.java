/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import mondrian.olap.MondrianServer;
import mondrian.xmla.XmlaUtil;

import org.olap4j.OlapDatabaseMetaData;
import org.olap4j.OlapException;
import org.olap4j.OlapConnection;
import org.olap4j.impl.NamedListImpl;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Implementation of {@link org.olap4j.OlapDatabaseMetaData}
 * for the Mondrian OLAP engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using {@link Factory#newDatabaseMetaData}.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since May 23, 2007
 */
abstract class MondrianOlap4jDatabaseMetaData implements OlapDatabaseMetaData {
    final MondrianOlap4jConnection olap4jConnection;
    final MondrianServer mondrianServer;

    // A mondrian instance contains only one catalog (and one schema).
    private final MondrianOlap4jCatalog olap4jCatalog =
        new MondrianOlap4jCatalog(this);

    MondrianOlap4jDatabaseMetaData(
        MondrianOlap4jConnection olap4jConnection)
    {
        this.olap4jConnection = olap4jConnection;
        mondrianServer =
            MondrianServer.forConnection(olap4jConnection.connection);
    }

    // helpers

    /**
     * Executes a metadata query and returns the result as a JDBC
     * {@link ResultSet}.
     *
     * @param methodName Name of the metadata request. Corresponds to the XMLA
     * method name, e.g. "MDSCHEMA_CUBES"
     *
     * @param patternValues Array of alternating parameter name and value
     * pairs. If the parameter value is null, it is ignored.
     *
     * @return Result set of metadata
     */
    private ResultSet getMetadata(
        String methodName,
        Object... patternValues)
    {
        Map<String, Object> restrictionMap =
            new HashMap<String, Object>();
        assert patternValues.length % 2 == 0;
        for (int i = 0; i < patternValues.length / 2; ++i) {
            final String key = (String) patternValues[i * 2];
            Object value = patternValues[i * 2 + 1];
            if (value != null) {
                if (value instanceof String) {
                    value = Collections.singletonList((String) value);
                }
                restrictionMap.put(key, value);
            }
        }
        XmlaUtil.MetadataRowset rowset =
            XmlaUtil.getMetadataRowset(
                olap4jConnection.connection,
                MondrianOlap4jConnection.LOCALDB_CATALOG_NAME,
                methodName,
                restrictionMap);
        return olap4jConnection.factory.newFixedResultSet(
            olap4jConnection, rowset.headerList, rowset.rowList);
    }

    private XmlaUtil.Wildcard wildcard(String pattern) {
        return pattern == null
            ? null
            : new XmlaUtil.Wildcard(pattern);
    }

    // package-protected
    NamedList<Catalog> getCatalogObjects() {
        NamedList<MondrianOlap4jCatalog> list =
            new NamedListImpl<MondrianOlap4jCatalog>();
        list.add(olap4jCatalog);
        return Olap4jUtil.cast(list);
    }

    // implement DatabaseMetaData

    public boolean allProceduresAreCallable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean allTablesAreSelectable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getURL() throws SQLException {
        return olap4jConnection.connection.getConnectString();
    }

    public String getUserName() throws SQLException {
        // mondrian does not support a user name property
        return null;
    }

    public boolean isReadOnly() throws SQLException {
        // all mondrian databases are read-only
        return true;
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean nullsAreSortedLow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getDatabaseProductName() throws SQLException {
        return mondrianServer.getVersion().getProductName();
    }

    public String getDatabaseProductVersion() throws SQLException {
        return mondrianServer.getVersion().getVersionString();
    }

    public String getDriverName() throws SQLException {
        return MondrianOlap4jDriver.NAME;
    }

    public String getDriverVersion() throws SQLException {
        return MondrianOlap4jDriver.VERSION;
    }

    public int getDriverMajorVersion() {
        return MondrianOlap4jDriver.MAJOR_VERSION;
    }

    public int getDriverMinorVersion() {
        return MondrianOlap4jDriver.MINOR_VERSION;
    }

    public boolean usesLocalFiles() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getIdentifierQuoteString() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getSQLKeywords() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getNumericFunctions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getStringFunctions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getSystemFunctions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getTimeDateFunctions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getSearchStringEscape() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getExtraNameCharacters() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsColumnAliasing() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsConvert() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsConvert(
        int fromType, int toType) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsGroupBy() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsOuterJoins() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getSchemaTerm() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getProcedureTerm() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getCatalogTerm() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isCatalogAtStart() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getCatalogSeparator() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsPositionedDelete() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsStoredProcedures() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsUnion() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsUnionAll() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxCharLiteralLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxColumnNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxColumnsInIndex() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxColumnsInOrderBy() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxColumnsInSelect() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxColumnsInTable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxConnections() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxCursorNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxIndexLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxSchemaNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxProcedureNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxCatalogNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxRowSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxStatementLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxStatements() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxTableNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxTablesInSelect() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxUserNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getDefaultTransactionIsolation() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsTransactions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getProcedures(
        String catalog,
        String schemaPattern,
        String procedureNamePattern) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getProcedureColumns(
        String catalog,
        String schemaPattern,
        String procedureNamePattern,
        String columnNamePattern) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getTables(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String types[]) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getSchemas() throws SQLException {
        if (false) {
            // Do not use DBSCHEMA_SCHEMATA: it has different columns than the
            // JDBC spec requires
            return getMetadata("DBSCHEMA_SCHEMATA");
        }
        List<String> headerList =
            Arrays.asList("TABLE_SCHEM", "TABLE_CAT");
        List<List<Object>> rowList = new ArrayList<List<Object>>();
        for (Schema schema : olap4jCatalog.getSchemas()) {
            rowList.add(
                Arrays.asList(
                    (Object) schema.getName(),
                    schema.getCatalog().getName()));
        }
        return olap4jConnection.factory.newFixedResultSet(
            olap4jConnection, headerList, rowList);
    }

    public ResultSet getCatalogs() throws SQLException {
        if (false) {
            // Do not use DBSCHEMA_CATALOGS: it has different columns than the
            // JDBC spec requires
            return getMetadata("DBSCHEMA_CATALOGS");
        }

        List<String> headerList =
            Arrays.asList("TABLE_CAT");
        List<List<Object>> rowList = 
            Collections.singletonList(
                Arrays.asList((Object) olap4jCatalog.getName()));
        return olap4jConnection.factory.newFixedResultSet(
            olap4jConnection, headerList, rowList);
    }

    public ResultSet getTableTypes() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getColumns(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String columnNamePattern) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getColumnPrivileges(
        String catalog,
        String schema,
        String table,
        String columnNamePattern) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getTablePrivileges(
        String catalog,
        String schemaPattern,
        String tableNamePattern) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getBestRowIdentifier(
        String catalog,
        String schema,
        String table,
        int scope,
        boolean nullable) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getVersionColumns(
        String catalog, String schema, String table) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getPrimaryKeys(
        String catalog, String schema, String table) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getImportedKeys(
        String catalog, String schema, String table) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getExportedKeys(
        String catalog, String schema, String table) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getCrossReference(
        String parentCatalog,
        String parentSchema,
        String parentTable,
        String foreignCatalog,
        String foreignSchema,
        String foreignTable) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getTypeInfo() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getIndexInfo(
        String catalog,
        String schema,
        String table,
        boolean unique,
        boolean approximate) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsResultSetType(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsResultSetConcurrency(
        int type, int concurrency) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean ownDeletesAreVisible(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean ownInsertsAreVisible(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean othersDeletesAreVisible(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean othersInsertsAreVisible(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean updatesAreDetected(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean deletesAreDetected(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean insertsAreDetected(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsBatchUpdates() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getUDTs(
        String catalog,
        String schemaPattern,
        String typeNamePattern,
        int[] types) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public OlapConnection getConnection() {
        return olap4jConnection;
    }

    public boolean supportsSavepoints() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsNamedParameters() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getSuperTypes(
        String catalog,
        String schemaPattern,
        String typeNamePattern) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getSuperTables(
        String catalog,
        String schemaPattern,
        String tableNamePattern) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getAttributes(
        String catalog,
        String schemaPattern,
        String typeNamePattern,
        String attributeNamePattern) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getDatabaseMajorVersion() throws SQLException {
        return mondrianServer.getVersion().getMajorVersion();
    }

    public int getDatabaseMinorVersion() throws SQLException {
        return mondrianServer.getVersion().getMajorVersion();
    }

    public int getJDBCMajorVersion() throws SQLException {
        // mondrian olap4j supports jdbc 4.0
        return 4;
    }

    public int getJDBCMinorVersion() throws SQLException {
        // mondrian olap4j supports jdbc 4.0
        return 0;
    }

    public int getSQLStateType() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsStatementPooling() throws SQLException {
        throw new UnsupportedOperationException();
    }

    // implement java.sql.Wrapper

    // straightforward implementation of unwrap and isWrapperFor, since this
    // class already implements the interface they most likely require:
    // DatabaseMetaData and OlapDatabaseMetaData

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw olap4jConnection.helper.createException(
            "does not implement '" + iface + "'");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    // implement OlapDatabaseMetaData

    public ResultSet getActions(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String actionNamePattern) throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_ACTIONS",
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "ACTION_NAME", wildcard(actionNamePattern));
    }

    public ResultSet getDatasources() throws OlapException {
        return getMetadata("DISCOVER_DATASOURCES");
    }

    public ResultSet getLiterals() throws OlapException {
        return getMetadata("DISCOVER_LITERALS");
    }

    public ResultSet getDatabaseProperties(
        String dataSourceName,
        String propertyNamePattern) throws OlapException
    {
        return getMetadata("DISCOVER_PROPERTIES");
    }

    public ResultSet getProperties(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String dimensionUniqueName,
        String hierarchyUniqueName,
        String levelUniqueName,
        String memberUniqueName,
        String propertyNamePattern) throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_PROPERTIES",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "DIMENSION_UNIQUE_NAME", dimensionUniqueName,
            "HIERARCHY_UNIQUE_NAME", hierarchyUniqueName,
            "LEVEL_UNIQUE_NAME", levelUniqueName,
            "MEMBER_UNIQUE_NAME", memberUniqueName,
            "PROPERTY_NAME", wildcard(propertyNamePattern));
    }

    public String getMdxKeywords() throws OlapException {
        StringBuilder buf = new StringBuilder();
        for (String keyword : mondrianServer.getKeywords()) {
            if (buf.length() > 0) {
                buf.append(',');
            }
            buf.append(keyword);
        }
        return buf.toString();
    }

    public ResultSet getCubes(
        String catalog,
        String schemaPattern,
        String cubeNamePattern)
        throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_CUBES",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern));
    }

    public ResultSet getDimensions(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String dimensionNamePattern)
        throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_DIMENSIONS",
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "DIMSENSION_NAME", wildcard(dimensionNamePattern));
    }

    public ResultSet getOlapFunctions(
        String functionNamePattern) throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_FUNCTIONS",
            "FUNCTION_NAME", wildcard(functionNamePattern));
    }

    public ResultSet getHierarchies(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String dimensionNamePattern,
        String hierarchyNamePattern) 
        throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_HIERARCHIES",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "DIMENSION_NAME", wildcard(dimensionNamePattern),
            "HIERARCHY_NAME", wildcard(hierarchyNamePattern));
    }

    public ResultSet getMeasures(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String measureNamePattern,
        String measureUniqueName) throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_MEASURES",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "MEASURE_NAME", wildcard(measureNamePattern),
            "MEASURE_UNIQUE_NAME", measureUniqueName);
    }

    public ResultSet getMembers(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String dimensionUniqueName,
        String hierarchyUniqueName,
        String levelUniqueName,
        String memberUniqueName,
        Set<Member.TreeOp> treeOps) throws OlapException
    {
        String treeOpString;
        if (treeOps != null) {
            int op = 0;
            for (Member.TreeOp treeOp : treeOps) {
                op |= treeOp.xmlaOrdinal();
            }
            treeOpString = String.valueOf(op);
        } else {
            treeOpString = null;
        }
        return getMetadata(
            "MDSCHEMA_MEMBERS",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "DIMENSION_UNIQUE_NAME", dimensionUniqueName,
            "HIERARCHY_UNIQUE_NAME", hierarchyUniqueName,
            "LEVEL_UNIQUE_NAME", levelUniqueName,
            "MEMBER_UNIQUE_NAME", memberUniqueName,
            "TREE_OP", treeOpString);
    }

    public ResultSet getLevels(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String dimensionUniqueName,
        String hierarchyUniqueName,
        String levelNamePattern) throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_LEVELS",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "DIMENSION_UNIQUE_NAME", dimensionUniqueName,
            "HIERARCHY_UNIQUE_NAME", hierarchyUniqueName,
            "LEVEL_NAME", wildcard(levelNamePattern));
    }

    public ResultSet getSets(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String setNamePattern) throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_SETS",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "SET_NAME", wildcard(setNamePattern));
    }
}

// End MondrianOlap4jDatabaseMetaData.java
