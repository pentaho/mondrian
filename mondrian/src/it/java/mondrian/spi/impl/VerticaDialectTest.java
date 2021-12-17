/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2020-2021 Hitachi Vantara..  All rights reserved.
 */
package mondrian.spi.impl;

import junit.framework.TestCase;
import mondrian.spi.Dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VerticaDialectTest extends TestCase {
  private Connection connection = mock( Connection.class );
  private DatabaseMetaData metaData = mock( DatabaseMetaData.class );
  private VerticaDialect dialect;

  @Override
  protected void setUp() throws Exception {
    when( metaData.getDatabaseProductName() ).thenReturn( Dialect.DatabaseProduct.VERTICA.name() );
    when( connection.getMetaData() ).thenReturn( metaData );
    dialect = new VerticaDialect( connection );
  }

  public void testAllowsRegularExpressionInWhereClause() {
    assertTrue( dialect.allowsRegularExpressionInWhereClause() );
  }

  public void testGenerateRegularExpression_InvalidRegex() throws Exception {
    assertNull( "Invalid regex should be ignored", dialect.generateRegularExpression( "table.column", "(a" ) );
  }

  public void testGenerateRegularExpression_CaseInsensitive() throws Exception {
    String sql = dialect.generateRegularExpression( "table.column", "(?is)|(?u).*a.*" );
    assertEquals( " REGEXP_LIKE ( CAST (table.column AS VARCHAR), '.*a.*', 'in')", sql );
  }

  public void testGenerateRegularExpression_CaseSensitive() throws Exception {
    String sql = dialect.generateRegularExpression( "table.column", ".*a.*" );
    assertEquals( " REGEXP_LIKE ( CAST (table.column AS VARCHAR), '.*a.*')", sql );
  }
}
// End VerticaDialectTest.java
