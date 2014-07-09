/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2013 Pentaho and others
// All Rights Reserved.
 */
package mondrian.spi.impl;

import mondrian.olap.Util;
import mondrian.spi.StatisticsProvider;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Phoenix JDBC driver. Phoenix provides a SQL API on top of
 * HBase that targets low-latency queries without map-reduce.
 *
 * @author Benny Chow
 * @since Oct 31, 2013
 */
public class PhoenixDialect extends JdbcDialectImpl {

  public static final JdbcDialectFactory FACTORY = new JdbcDialectFactory(
      PhoenixDialect.class,
      DatabaseProduct.PHOENIX);

  public PhoenixDialect(Connection connection) throws SQLException {
    super(connection);
  }

  @Override
  public boolean allowsAs() {
    return true;
  }

  @Override
  public boolean allowsCompoundCountDistinct() {
    return false;
  }

  @Override
  public boolean allowsCountDistinct() {
    return false;
  }

  @Override
  public boolean allowsDdl() {
    return true;
  }

  @Override
  public boolean allowsDialectSharing() {
    return true;
  }

  @Override
  public boolean allowsFromQuery() {
    return false;
  }

  @Override
  public boolean allowsMultipleCountDistinct() {
    return false;
  }

  @Override
  public boolean allowsMultipleDistinctSqlMeasures() {
    return false;
  }

  @Override
  public boolean allowsOrderByAlias() {
    return false;
  }

  @Override
  public boolean allowsRegularExpressionInWhereClause() {
    return true;
  }

  @Override
  public boolean allowsSelectNotInGroupBy() {
    return true;
  }

  @Override
  public String getQuoteIdentifierString() {
    return "\"";
  }

  @Override
  public boolean requiresAliasForFromQuery() {
    return false;
  }

  @Override
  public boolean requiresHavingAlias() {
    return false;
  }

  @Override
  public boolean requiresOrderByAlias() {
    return false;
  }

  @Override
  public boolean requiresUnionOrderByExprToBeInSelectClause() {
    return false;
  }

  @Override
  public boolean requiresUnionOrderByOrdinal() {
    return false;
  }

  @Override
  public boolean supportsGroupByExpressions() {
    return true;
  }

  @Override
  public boolean supportsGroupingSets() {
    return false;
  }

  @Override
  public boolean supportsMultiValueInExpr() {
    return true;
  }

  @Override
  public boolean supportsUnlimitedValueList() {
    return true;
  }

  @Override
  public List<StatisticsProvider> getStatisticsProviders() {
    return new ArrayList<StatisticsProvider>();
  }

  protected void quoteDateLiteral(StringBuilder buf, String value, Date date) {
   // Phoenix accepts TO_DATE('2008-01-23') but not SQL:2003 format.
    buf.append("TO_DATE(");
    Util.singleQuoteString(value, buf);
    buf.append(", 'yyyy-MM-dd')");
  }

  public void quoteTimeLiteral(StringBuilder buf, Time value) {
    buf.append("TO_DATE(");
    Util.singleQuoteString(value.toString(), buf);
    buf.append(", 'HH:mm:ss')");
  }

  public void quoteTimestampLiteral(StringBuilder buf, Timestamp value) {
    buf.append("TO_DATE(");
    Util.singleQuoteString(value.toString(), buf);
    buf.append(", 'yyyy-MM-dd HH:mm:ss.SSS')");
  }

}

// End PhoenixDialect.java
