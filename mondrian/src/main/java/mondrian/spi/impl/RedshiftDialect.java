/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2020 Hitachi Vantara..  All rights reserved.
*/

package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import mondrian.olap.Util;
import mondrian.spi.DialectUtil;

/**
 * User: cboyden Date: 2/8/13
 */
public class RedshiftDialect extends PostgreSqlDialect {
  /**
   * Creates a RedshiftDialect.
   *
   * @param connection
   *          Connection
   */
  public RedshiftDialect( Connection connection ) throws SQLException {
    super( connection );
  }

  public static final JdbcDialectFactory FACTORY =
      new JdbcDialectFactory( RedshiftDialect.class, DatabaseProduct.POSTGRESQL ) {
        protected boolean acceptsConnection( Connection connection ) {
          return super.acceptsConnection( connection ) && isDatabase( DatabaseProduct.REDSHIFT, connection );
        }
      };

  public DatabaseProduct getDatabaseProduct() {
    return DatabaseProduct.REDSHIFT;
  }

  @Override
  public String generateInline( List<String> columnNames, List<String> columnTypes, List<String[]> valueList ) {
    return generateInlineGeneric( columnNames, columnTypes, valueList, null, false );
  }

  @Override
  public void quoteStringLiteral( StringBuilder buf, String value ) {
    // '\' to '\\'
    Util.singleQuoteString( value.replaceAll( "\\\\", "\\\\\\\\" ), buf );
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

    // We might have to use case-insensitive matching
    javaRegex = DialectUtil.cleanUnicodeAwareCaseFlag( javaRegex );
    StringBuilder mappedFlags = new StringBuilder();
    String[][] mapping = new String[][] { { "i", "i" } };
    javaRegex = extractEmbeddedFlags( javaRegex, mapping, mappedFlags );
    boolean caseSensitive = true;
    if ( mappedFlags.toString().contains( "i" ) ) {
      caseSensitive = false;
    }
    
    // Now build the string.
    final StringBuilder sb = new StringBuilder();
    // https://docs.aws.amazon.com/redshift/latest/dg/REGEXP_INSTR.html
    sb.append( "REGEXP_INSTR(" );
    sb.append( source );
    sb.append( "," );
    quoteStringLiteral( sb, javaRegex );
    sb.append( ",1,1,0," );
    sb.append( caseSensitive ? "'c'" : "'i'" );
    sb.append( ") > 0" );

    return sb.toString();
  }
}

// End RedshiftDialect.java
