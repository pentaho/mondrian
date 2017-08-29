/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2017 Pentaho Corporation.
// All rights reserved.
 */
package mondrian.spi.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import com.mysql.jdbc.Statement;

import junit.framework.TestCase;
import mondrian.spi.Dialect;

public class MySqlDialectTest extends TestCase {
  private Connection connection = mock( Connection.class );
  private DatabaseMetaData metaData = mock( DatabaseMetaData.class );
  Statement statmentMock = mock( Statement.class );
  private MySqlDialect dialect;

  @Override
  protected void setUp() throws Exception {
    when( metaData.getDatabaseProductName() ).thenReturn( Dialect.DatabaseProduct.MYSQL.name() );
    when( metaData.getDatabaseProductVersion() ).thenReturn( "5.0" );
    when( statmentMock.execute( any() ) ).thenReturn( false );
    when( connection.getMetaData() ).thenReturn( metaData );
    when( connection.createStatement() ).thenReturn( statmentMock );
    dialect = new MySqlDialect( connection );
  }

  public void testAllowsRegularExpressionInWhereClause() {
    assertTrue( dialect.allowsRegularExpressionInWhereClause() );
  }

  public void testGenerateRegularExpression_InvalidRegex() throws Exception {
    assertNull( "Invalid regex should be ignored", dialect.generateRegularExpression( "table.column", "(a" ) );
  }

  public void testGenerateRegularExpression_CaseInsensitive() throws Exception {
    String sql = dialect.generateRegularExpression( "table.column", "(?i)|(?u).*a.*" );
    assertEquals( "table.column IS NOT NULL AND UPPER(table.column) REGEXP '.*A.*'", sql );
  }

  public void testGenerateRegularExpression_CaseSensitive() throws Exception {
    String sql = dialect.generateRegularExpression( "table.column", ".*a.*" );
    assertEquals( "table.column IS NOT NULL AND table.column REGEXP '.*a.*'", sql );
  }
}
//End MySqlDialectTest.java
