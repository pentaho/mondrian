/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Oct 3, 2002
*/

package mondrian.rolap.sql;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.ResultSet;

/**
 * <code>DelegatingDatabaseMetaData</code> implements {@link DatabaseMetaData}
 * (up to JDK 1.4) by delegating to an underlying database metadata object.
 *
 * @author jhyde
 * @since Oct 3, 2002
 * @version $Id$
 **/
public class DelegatingDatabaseMetaData implements DatabaseMetaData {
	protected DatabaseMetaData meta;
	public DelegatingDatabaseMetaData(DatabaseMetaData meta) {
		this.meta = meta;
	}

	// Below this point, implementations of DatabaseMetaData methods
	public boolean allProceduresAreCallable() throws SQLException {
		return meta.allProceduresAreCallable();
	}

	public boolean allTablesAreSelectable() throws SQLException {
		return meta.allTablesAreSelectable();
	}

	public boolean dataDefinitionCausesTransactionCommit()
			throws SQLException {
		return meta.dataDefinitionCausesTransactionCommit();
	}

	public boolean dataDefinitionIgnoredInTransactions()
			throws SQLException {
		return meta.dataDefinitionIgnoredInTransactions();
	}

	public boolean deletesAreDetected(int type) throws SQLException {
		return meta.deletesAreDetected(type);
	}

	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return meta.doesMaxRowSizeIncludeBlobs();
	}

	public ResultSet getAttributes(String catalog, String schemaPattern,
								   String typeNamePattern, String attributeNamePattern)
			throws SQLException {
		return meta.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
	}

	public ResultSet getBestRowIdentifier(String catalog, String schema,
										  String table, int scope, boolean nullable) throws SQLException {
		return meta.getBestRowIdentifier(catalog, schema, table, scope, nullable);
	}

	public ResultSet getCatalogs() throws SQLException {
		return meta.getCatalogs();
	}

	public String getCatalogSeparator() throws SQLException {
		return meta.getCatalogSeparator();
	}

	public String getCatalogTerm() throws SQLException {
		return meta.getCatalogTerm();
	}

	public ResultSet getColumnPrivileges(String catalog, String schema,
										 String table, String columnNamePattern) throws SQLException {
		return meta.getColumnPrivileges(catalog, schema, table, columnNamePattern);
	}

	public ResultSet getColumns(String catalog, String schemaPattern,
								String tableNamePattern, String columnNamePattern)
			throws SQLException {
		return meta.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}

	public java.sql.Connection getConnection() throws SQLException {
		return meta.getConnection();
	}

	public ResultSet getCrossReference(
			String primaryCatalog, String primarySchema, String primaryTable,
			String foreignCatalog, String foreignSchema, String foreignTable
			) throws SQLException {
		return meta.getCrossReference(primaryCatalog, primarySchema, primaryTable, foreignCatalog, foreignSchema, foreignTable);
	}

	public int getDatabaseMajorVersion() throws SQLException {
		return meta.getDatabaseMajorVersion();
	}

	public int getDatabaseMinorVersion() throws SQLException {
		return meta.getDatabaseMinorVersion();
	}

	public String getDatabaseProductName() throws SQLException {
		return meta.getDatabaseProductName();
	}

	public String getDatabaseProductVersion() throws SQLException {
		return meta.getDatabaseProductVersion();
	}

	public int getDefaultTransactionIsolation() throws SQLException {
		return meta.getDefaultTransactionIsolation();
	}

	public int getDriverMajorVersion() {
		return meta.getDriverMajorVersion();
	}

	public int getDriverMinorVersion() {
		return meta.getDriverMinorVersion();
	}

	public String getDriverName() throws SQLException {
		return meta.getDriverName();
	}

	public String getDriverVersion() throws SQLException {
		return meta.getDriverVersion();
	}

	public ResultSet getExportedKeys(String catalog, String schema,
									 String table) throws SQLException {
		return meta.getExportedKeys(catalog, schema, table);
	}

	public String getExtraNameCharacters() throws SQLException {
		return meta.getExtraNameCharacters();
	}

	public String getIdentifierQuoteString() throws SQLException {
		return meta.getIdentifierQuoteString();
	}

	public ResultSet getImportedKeys(String catalog, String schema,
									 String table) throws SQLException {
		return meta.getImportedKeys(catalog, schema, table);
	}

	public ResultSet getIndexInfo(String catalog, String schema, String table,
								  boolean unique, boolean approximate)
			throws SQLException {
		return meta.getIndexInfo(catalog, schema, table, unique, approximate);
	}

	public int getJDBCMajorVersion() throws SQLException {
		return meta.getJDBCMajorVersion();
	}

	public int getJDBCMinorVersion() throws SQLException {
		return meta.getJDBCMinorVersion();
	}

	public int getMaxBinaryLiteralLength() throws SQLException {
		return meta.getMaxBinaryLiteralLength();
	}

	public int getMaxCatalogNameLength() throws SQLException {
		return meta.getMaxCatalogNameLength();
	}

	public int getMaxCharLiteralLength() throws SQLException {
		return meta.getMaxCharLiteralLength();
	}

	public int getMaxColumnNameLength() throws SQLException {
		return meta.getMaxColumnNameLength();
	}

	public int getMaxColumnsInGroupBy() throws SQLException {
		return meta.getMaxColumnsInGroupBy();
	}

	public int getMaxColumnsInIndex() throws SQLException {
		return meta.getMaxColumnsInIndex();
	}

	public int getMaxColumnsInOrderBy() throws SQLException {
		return meta.getMaxColumnsInOrderBy();
	}

	public int getMaxColumnsInSelect() throws SQLException {
		return meta.getMaxColumnsInSelect();
	}

	public int getMaxColumnsInTable() throws SQLException {
		return meta.getMaxColumnsInTable();
	}

	public int getMaxConnections() throws SQLException {
		return meta.getMaxConnections();
	}

	public int getMaxCursorNameLength() throws SQLException {
		return meta.getMaxCursorNameLength();
	}

	public int getMaxIndexLength() throws SQLException {
		return meta.getMaxIndexLength();
	}

	public int getMaxProcedureNameLength() throws SQLException {
		return meta.getMaxProcedureNameLength();
	}

	public int getMaxRowSize() throws SQLException {
		return meta.getMaxRowSize();
	}

	public int getMaxSchemaNameLength() throws SQLException {
		return meta.getMaxSchemaNameLength();
	}

	public int getMaxStatementLength() throws SQLException {
		return meta.getMaxStatementLength();
	}

	public int getMaxStatements() throws SQLException {
		return meta.getMaxStatements();
	}

	public int getMaxTableNameLength() throws SQLException {
		return meta.getMaxTableNameLength();
	}

	public int getMaxTablesInSelect() throws SQLException {
		return meta.getMaxTablesInSelect();
	}

	public int getMaxUserNameLength() throws SQLException {
		return meta.getMaxUserNameLength();
	}

	public String getNumericFunctions() throws SQLException {
		return meta.getNumericFunctions();
	}

	public ResultSet getPrimaryKeys(String catalog, String schema,
									String table) throws SQLException {
		return meta.getPrimaryKeys(catalog, schema, table);
	}

	public ResultSet getProcedureColumns(String catalog,
										 String schemaPattern,
										 String procedureNamePattern,
										 String columnNamePattern) throws SQLException {
		return meta.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
	}

	public ResultSet getProcedures(String catalog, String schemaPattern,
								   String procedureNamePattern) throws SQLException {
		return meta.getProcedures(catalog, schemaPattern, procedureNamePattern);
	}

	public String getProcedureTerm() throws SQLException {
		return meta.getProcedureTerm();
	}

	public int getResultSetHoldability() throws SQLException {
		return meta.getResultSetHoldability();
	}

	public ResultSet getSchemas() throws SQLException {
		return meta.getSchemas();
	}

	public String getSchemaTerm() throws SQLException {
		return meta.getSchemaTerm();
	}

	public String getSearchStringEscape() throws SQLException {
		return meta.getSearchStringEscape();
	}

	public String getSQLKeywords() throws SQLException {
		return meta.getSQLKeywords();
	}

	public int getSQLStateType() throws SQLException {
		return meta.getSQLStateType();
	}

	public String getStringFunctions() throws SQLException {
		return meta.getStringFunctions();
	}

	public ResultSet getSuperTables(String catalog, String schemaPattern,
									String tableNamePattern) throws SQLException {
		return meta.getSuperTables(catalog, schemaPattern, tableNamePattern);
	}

	public ResultSet getSuperTypes(String catalog, String schemaPattern,
								   String typeNamePattern) throws SQLException {
		return meta.getSuperTypes(catalog, schemaPattern, typeNamePattern);
	}

	public String getSystemFunctions() throws SQLException {
		return meta.getSystemFunctions();
	}

	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
										String tableNamePattern) throws SQLException {
		return meta.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
	}

	public ResultSet getTables(String catalog, String schemaPattern,
							   String tableNamePattern, String types[]) throws SQLException {
		return meta.getTables(catalog, schemaPattern, tableNamePattern, types);
	}

	public ResultSet getTableTypes() throws SQLException {
		return meta.getTableTypes();
	}

	public String getTimeDateFunctions() throws SQLException {
		return meta.getTimeDateFunctions();
	}

	public ResultSet getTypeInfo() throws SQLException {
		return meta.getTypeInfo();
	}

	public ResultSet getUDTs(String catalog, String schemaPattern,
							 String typeNamePattern, int[] types)
			throws SQLException {
		return meta.getUDTs(catalog, schemaPattern, typeNamePattern, types);
	}

	public String getURL() throws SQLException {
		return meta.getURL();
	}

	public String getUserName() throws SQLException {
		return meta.getUserName();
	}

	public ResultSet getVersionColumns(String catalog, String schema,
									   String table) throws SQLException {
		return meta.getVersionColumns(catalog, schema, table);
	}

	public boolean insertsAreDetected(int type) throws SQLException {
		return meta.insertsAreDetected(type);
	}

	public boolean isCatalogAtStart() throws SQLException {
		return meta.isCatalogAtStart();
	}

	public boolean isReadOnly() throws SQLException {
		return meta.isReadOnly();
	}

	public boolean locatorsUpdateCopy() throws SQLException {
		return meta.locatorsUpdateCopy();
	}

	public boolean nullPlusNonNullIsNull() throws SQLException {
		return meta.nullPlusNonNullIsNull();
	}

	public boolean nullsAreSortedAtEnd() throws SQLException {
		return meta.nullsAreSortedAtEnd();
	}

	public boolean nullsAreSortedAtStart() throws SQLException {
		return meta.nullsAreSortedAtStart();
	}

	public boolean nullsAreSortedHigh() throws SQLException {
		return meta.nullsAreSortedHigh();
	}

	public boolean nullsAreSortedLow() throws SQLException {
		return meta.nullsAreSortedLow();
	}

	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return meta.othersDeletesAreVisible(type);
	}

	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return meta.othersInsertsAreVisible(type);
	}

	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return meta.othersUpdatesAreVisible(type);
	}

	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return meta.ownDeletesAreVisible(type);
	}

	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return meta.ownInsertsAreVisible(type);
	}

	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return meta.ownUpdatesAreVisible(type);
	}

	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return meta.storesLowerCaseIdentifiers();
	}

	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return meta.storesLowerCaseQuotedIdentifiers();
	}

	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return meta.storesMixedCaseIdentifiers();
	}

	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return meta.storesMixedCaseQuotedIdentifiers();
	}

	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return meta.storesUpperCaseIdentifiers();
	}

	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return meta.storesUpperCaseQuotedIdentifiers();
	}

	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return meta.supportsAlterTableWithAddColumn();
	}

	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return meta.supportsAlterTableWithDropColumn();
	}

	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return meta.supportsANSI92EntryLevelSQL();
	}

	public boolean supportsANSI92FullSQL() throws SQLException {
		return meta.supportsANSI92FullSQL();
	}

	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return meta.supportsANSI92IntermediateSQL();
	}

	public boolean supportsBatchUpdates() throws SQLException {
		return meta.supportsBatchUpdates();
	}

	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return meta.supportsCatalogsInDataManipulation();
	}

	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return meta.supportsCatalogsInIndexDefinitions();
	}

	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return meta.supportsCatalogsInPrivilegeDefinitions();
	}

	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return meta.supportsCatalogsInProcedureCalls();
	}

	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return meta.supportsCatalogsInTableDefinitions();
	}

	public boolean supportsColumnAliasing() throws SQLException {
		return meta.supportsColumnAliasing();
	}

	public boolean supportsConvert() throws SQLException {
		return meta.supportsConvert();
	}

	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		return meta.supportsConvert(fromType, toType);
	}

	public boolean supportsCoreSQLGrammar() throws SQLException {
		return meta.supportsCoreSQLGrammar();
	}

	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return meta.supportsCorrelatedSubqueries();
	}

	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException {
		return meta.supportsDataDefinitionAndDataManipulationTransactions();
	}

	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException {
		return meta.supportsDataManipulationTransactionsOnly();
	}

	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return meta.supportsDifferentTableCorrelationNames();
	}

	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return meta.supportsExpressionsInOrderBy();
	}

	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return meta.supportsExtendedSQLGrammar();
	}

	public boolean supportsFullOuterJoins() throws SQLException {
		return meta.supportsFullOuterJoins();
	}

	public boolean supportsGetGeneratedKeys() throws SQLException {
		return meta.supportsGetGeneratedKeys();
	}

	public boolean supportsGroupBy() throws SQLException {
		return meta.supportsGroupBy();
	}

	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return meta.supportsGroupByBeyondSelect();
	}

	public boolean supportsGroupByUnrelated() throws SQLException {
		return meta.supportsGroupByUnrelated();
	}

	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return meta.supportsIntegrityEnhancementFacility();
	}

	public boolean supportsLikeEscapeClause() throws SQLException {
		return meta.supportsLikeEscapeClause();
	}

	public boolean supportsLimitedOuterJoins() throws SQLException {
		return meta.supportsLimitedOuterJoins();
	}

	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return meta.supportsMinimumSQLGrammar();
	}

	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return meta.supportsMixedCaseIdentifiers();
	}

	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return meta.supportsMixedCaseQuotedIdentifiers();
	}

	public boolean supportsMultipleOpenResults() throws SQLException {
		return meta.supportsMultipleOpenResults();
	}

	public boolean supportsMultipleResultSets() throws SQLException {
		return meta.supportsMultipleResultSets();
	}

	public boolean supportsMultipleTransactions() throws SQLException {
		return meta.supportsMultipleTransactions();
	}

	public boolean supportsNamedParameters() throws SQLException {
		return meta.supportsNamedParameters();
	}

	public boolean supportsNonNullableColumns() throws SQLException {
		return meta.supportsNonNullableColumns();
	}

	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return meta.supportsOpenCursorsAcrossCommit();
	}

	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return meta.supportsOpenCursorsAcrossRollback();
	}

	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return meta.supportsOpenStatementsAcrossCommit();
	}

	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return meta.supportsOpenStatementsAcrossRollback();
	}

	public boolean supportsOrderByUnrelated() throws SQLException {
		return meta.supportsOrderByUnrelated();
	}

	public boolean supportsOuterJoins() throws SQLException {
		return meta.supportsOuterJoins();
	}

	public boolean supportsPositionedDelete() throws SQLException {
		return meta.supportsPositionedDelete();
	}

	public boolean supportsPositionedUpdate() throws SQLException {
		return meta.supportsPositionedUpdate();
	}

	public boolean supportsResultSetConcurrency(int type, int concurrency)
			throws SQLException {
		return meta.supportsResultSetConcurrency(type, concurrency);
	}

	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		return meta.supportsResultSetHoldability(holdability);
	}

	public boolean supportsResultSetType(int type) throws SQLException {
		return meta.supportsResultSetType(type);
	}

	public boolean supportsSavepoints() throws SQLException {
		return meta.supportsSavepoints();
	}

	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return meta.supportsSchemasInDataManipulation();
	}

	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return meta.supportsSchemasInIndexDefinitions();
	}

	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return meta.supportsSchemasInPrivilegeDefinitions();
	}

	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return meta.supportsSchemasInProcedureCalls();
	}

	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return meta.supportsSchemasInTableDefinitions();
	}

	public boolean supportsSelectForUpdate() throws SQLException {
		return meta.supportsSelectForUpdate();
	}

	public boolean supportsStatementPooling() throws SQLException {
		return meta.supportsStatementPooling();
	}

	public boolean supportsStoredProcedures() throws SQLException {
		return meta.supportsStoredProcedures();
	}

	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return meta.supportsSubqueriesInComparisons();
	}

	public boolean supportsSubqueriesInExists() throws SQLException {
		return meta.supportsSubqueriesInExists();
	}

	public boolean supportsSubqueriesInIns() throws SQLException {
		return meta.supportsSubqueriesInIns();
	}

	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return meta.supportsSubqueriesInQuantifieds();
	}

	public boolean supportsTableCorrelationNames() throws SQLException {
		return meta.supportsTableCorrelationNames();
	}

	public boolean supportsTransactionIsolationLevel(int level)
			throws SQLException {
		return meta.supportsTransactionIsolationLevel(level);
	}

	public boolean supportsTransactions() throws SQLException {
		return meta.supportsTransactions();
	}

	public boolean supportsUnion() throws SQLException {
		return meta.supportsUnion();
	}

	public boolean supportsUnionAll() throws SQLException {
		return meta.supportsUnionAll();
	}

	public boolean updatesAreDetected(int type) throws SQLException {
		return meta.updatesAreDetected(type);
	}

	public boolean usesLocalFilePerTable() throws SQLException {
		return meta.usesLocalFilePerTable();
	}

	public boolean usesLocalFiles() throws SQLException {
		return meta.usesLocalFiles();
	}
}

// End DelegatingDatabaseMetaData.java
