/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.rolap.SqlStatement;
import mondrian.spi.*;

import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Implementation of {@link mondrian.spi.Dialect} that delegates to an
 * underlying dialect.
 *
 * @author jhyde
 */
public class DelegatingDialect implements Dialect {
    protected final Dialect dialect;

    /**
     * Creates a DelegatingDialect.
     *
     * @param dialect Underlying dialect to which to delegate calls.
     */
    protected DelegatingDialect(Dialect dialect) {
        assert dialect != null;
        this.dialect = dialect;
    }

    public String toUpper(String expr) {
        return dialect.toUpper(expr);
    }

    public String caseWhenElse(String cond, String thenExpr, String elseExpr) {
        return dialect.caseWhenElse(cond, thenExpr, elseExpr);
    }

    public String quoteIdentifier(String val) {
        return dialect.quoteIdentifier(val);
    }

    public void quoteIdentifier(String val, StringBuilder buf) {
        dialect.quoteIdentifier(val, buf);
    }

    public String quoteIdentifier(String qual, String name) {
        return dialect.quoteIdentifier(qual, name);
    }

    public void quoteIdentifier(StringBuilder buf, String... names) {
        dialect.quoteIdentifier(buf, names);
    }

    public String getQuoteIdentifierString() {
        return dialect.getQuoteIdentifierString();
    }

    public void quoteStringLiteral(StringBuilder buf, String value) {
        dialect.quoteStringLiteral(buf, value);
    }

    public void quoteNumericLiteral(StringBuilder buf, Number value) {
        dialect.quoteNumericLiteral(buf, value);
    }

    public void quoteBooleanLiteral(StringBuilder buf, boolean value) {
        dialect.quoteBooleanLiteral(buf, value);
    }

    public void quoteDateLiteral(StringBuilder buf, Date value) {
        dialect.quoteDateLiteral(buf, value);
    }

    public void quoteTimeLiteral(StringBuilder buf, Time value) {
        dialect.quoteTimeLiteral(buf, value);
    }

    public void quoteTimestampLiteral(StringBuilder buf, Timestamp value) {
        dialect.quoteTimestampLiteral(buf, value);
    }

    public boolean requiresAliasForFromQuery() {
        return dialect.requiresAliasForFromQuery();
    }

    public boolean allowsAs() {
        return dialect.allowsAs();
    }

    public boolean allowsFromQuery() {
        return dialect.allowsFromQuery();
    }

    public boolean allowsCompoundCountDistinct() {
        return dialect.allowsCompoundCountDistinct();
    }

    public boolean allowsCountDistinct() {
        return dialect.allowsCountDistinct();
    }

    public boolean allowsMultipleCountDistinct() {
        return dialect.allowsMultipleCountDistinct();
    }

    public boolean allowsMultipleDistinctSqlMeasures() {
        return dialect.allowsMultipleDistinctSqlMeasures();
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return dialect.generateInline(columnNames, columnTypes, valueList);
    }

    public boolean needsExponent(Object value, String valueString) {
        return dialect.needsExponent(value, valueString);
    }

    public void quote(StringBuilder buf, Object value, Datatype datatype) {
        dialect.quote(buf, value, datatype);
    }

    public boolean allowsDdl() {
        return dialect.allowsDdl();
    }

    public String generateOrderItem(
        String expr,
        boolean nullable,
        boolean ascending,
        boolean collateNullsLast)
    {
        return dialect.generateOrderItem(
            expr, nullable, ascending, collateNullsLast);
    }

    public boolean supportsGroupByExpressions() {
        return dialect.supportsGroupByExpressions();
    }

    public boolean supportsGroupingSets() {
        return dialect.supportsGroupingSets();
    }

    public boolean supportsUnlimitedValueList() {
        return dialect.supportsUnlimitedValueList();
    }

    public boolean requiresGroupByAlias() {
        return dialect.requiresGroupByAlias();
    }

    public boolean requiresOrderByAlias() {
        return dialect.requiresOrderByAlias();
    }

    public boolean requiresHavingAlias() {
        return dialect.requiresHavingAlias();
    }

    public boolean allowsOrderByAlias() {
        return dialect.allowsOrderByAlias();
    }

    public boolean requiresUnionOrderByOrdinal() {
        return dialect.requiresUnionOrderByOrdinal();
    }

    public boolean requiresUnionOrderByExprToBeInSelectClause() {
        return dialect.requiresUnionOrderByExprToBeInSelectClause();
    }

    public boolean supportsMultiValueInExpr() {
        return dialect.supportsMultiValueInExpr();
    }

    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return dialect.supportsResultSetConcurrency(type, concurrency);
    }

    public int getMaxColumnNameLength() {
        return dialect.getMaxColumnNameLength();
    }

    public DatabaseProduct getDatabaseProduct() {
        return dialect.getDatabaseProduct();
    }

    public void appendHintsAfterFromClause(
        StringBuilder buf, Map<String, String> hints)
    {
        dialect.appendHintsAfterFromClause(buf, hints);
    }

    public boolean allowsDialectSharing() {
        return dialect.allowsDialectSharing();
    }

    public boolean allowsSelectNotInGroupBy() {
        return dialect.allowsSelectNotInGroupBy();
    }

    public boolean allowsJoinOn() {
        return dialect.allowsJoinOn();
    }

    public boolean allowsRegularExpressionInWhereClause() {
        return dialect.allowsRegularExpressionInWhereClause();
    }

    public String generateCountExpression(String exp) {
        return dialect.generateCountExpression(exp);
    }

    public String generateRegularExpression(String source, String javaRegExp) {
        return dialect.generateRegularExpression(source, javaRegExp);
    }

    public Datatype sqlTypeToDatatype(String typeName, int type) {
        return dialect.sqlTypeToDatatype(typeName, type);
    }

    public String datatypeToString(Datatype datatype, int precision, int scale)
    {
        return dialect.datatypeToString(datatype, precision, scale);
    }

    public List<StatisticsProvider> getStatisticsProviders() {
        return dialect.getStatisticsProviders();
    }

    public SqlStatement.Type getType(
        ResultSetMetaData metadata, int columnIndex) throws SQLException
    {
        return dialect.getType(metadata, columnIndex);
    }

    public boolean alwaysQuoteIdentifiers() {
        return dialect.alwaysQuoteIdentifiers();
    }

    public boolean needToQuote(String identifier) {
        return dialect.needToQuote(identifier);
    }

    public boolean hasSpecialChars(String identifier) {
        return dialect.hasSpecialChars(identifier);
    }

    public String rectifyCase(String identifier) {
        return dialect.rectifyCase(identifier);
    }

    public Dialect withQuoting(boolean alwaysQuoteIdentifiers) {
        return dialect.withQuoting(alwaysQuoteIdentifiers);
    }
}

// End DelegatingDialect.java
