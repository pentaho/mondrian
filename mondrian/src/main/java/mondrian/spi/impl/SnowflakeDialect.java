/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2019 Hitachi Vantara..  All rights reserved.
 */
package mondrian.spi.impl;


import mondrian.olap.Util;
import mondrian.spi.DialectUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Stream;

public class SnowflakeDialect extends JdbcDialectImpl {

  private final String flagsRegexp = "^(\\(\\?([a-zA-Z]+)\\)).*$";
  private final Pattern flagsPattern = Pattern.compile( flagsRegexp );

  //Snowflake regex allowed inline modifiers
  //https://docs.snowflake.net/manuals/sql-reference/functions-regexp.html
  public enum RegexParameters {
    CASE_SENSITIVE( "c" ),
    CASE_INSENSITIVE( "i" ),
    MULTI_LINE( "m" ),
    //"e" allowed by snowflake but not compatible with RLIKE
    WILDCARD_MATCHES_NEWLINE( "s" );

    final String parameter;

    RegexParameters( String param ) {
      parameter = param;
    }
  }

  public static final JdbcDialectFactory FACTORY =
    new JdbcDialectFactory( SnowflakeDialect.class, DatabaseProduct.SNOWFLAKE );

  public SnowflakeDialect( Connection connection ) throws SQLException {
    super( connection );
  }

  @Override public String getQuoteIdentifierString() {
    return "\"";
  }

  @Override
  public String generateInline( List<String> columnNames, List<String> columnTypes, List<String[]> valueList ) {
    return generateInlineGeneric( columnNames, columnTypes, valueList, null, false );
  }

  @Override
  public void quoteStringLiteral( StringBuilder buf, String s ) {
    Util.singleQuoteString( s.replaceAll( "\\\\", "\\\\\\\\" ), buf );
  }

  @Override
  public boolean allowsOrderByAlias() {
    return true;
  }

  @Override
  public boolean allowsSelectNotInGroupBy() {
    return false;
  }

  /**
   * Requires order by alias, in some cases:
   *
   * For example: a select query that lists all the columns used in the ORDER_BY expression will succeed
   *
   * <code>SELECT "store_id", "unit_sales" FROM "sales_fact_1997" ORDER BY "store_id" + "unit_sales";</code>
   *
   * while a query that only has some of the columns used in the ORDER_BY expression will not:
   *
   * <code>SELECT "unit_sales" FROM "sales_fact_1997" ORDER BY "store_id" + "unit_sales";</code>
   *
   * @return true,
   *  in some cases snowflake requires expressions in the ORDER_BY clause to be in aliased in the SELECT clause
   */
  @Override
  public boolean requiresOrderByAlias() {
    return true;
  }

  @Override
  public boolean allowsRegularExpressionInWhereClause() {
    return true;
  }

  @Override
  public String generateRegularExpression( String source, String javaRegex ) {
    try {
      Pattern.compile( javaRegex );
    } catch ( PatternSyntaxException e ) {
      // Not a valid Java regex. Too risky to continue.
      return null;
    }

    javaRegex = DialectUtil.cleanUnicodeAwareCaseFlag( javaRegex );
    javaRegex = javaRegex.replace( "\\Q", "" );
    javaRegex = javaRegex.replace( "\\E", "" );

    final Matcher flagsMatcher = flagsPattern.matcher( javaRegex );

    //only use snowflake compatible parameters
    StringBuilder parameters = new StringBuilder();
    if ( flagsMatcher.matches() ) {
      final String flags = flagsMatcher.group( 2 );
      Stream.of( RegexParameters.values() )
        .map( rp -> rp.parameter )
        .filter( flags::contains )
        .forEach( parameters::append );
    }

    //remove the flags as flag parameters will be added to RLIKE
    if ( flagsMatcher.matches() ) {
      javaRegex =
        javaRegex.substring( 0, flagsMatcher.start( 1 ) )
          + javaRegex.substring( flagsMatcher.end( 1 ) );
    }

    final StringBuilder sb = new StringBuilder();
    sb.append( " RLIKE ( " );
    sb.append( source );
    sb.append( ", " );
    quoteStringLiteral( sb, javaRegex );
    if ( parameters.toString().length() > 0 ) {
      sb.append( ", " );
      quoteStringLiteral( sb, parameters.toString() );
    }
    sb.append( ")" );
    return sb.toString();
  }
}
