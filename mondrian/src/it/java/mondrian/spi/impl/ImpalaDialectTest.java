/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2017 Hitachi Vantara.
// All rights reserved.
 */
package mondrian.spi.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import junit.framework.TestCase;
import mondrian.spi.Dialect;

/**
 * @author Andrey Khayrutdinov
 */
public class ImpalaDialectTest extends TestCase {
  private Connection connection = mock( Connection.class );
  private DatabaseMetaData metaData = mock( DatabaseMetaData.class );
  private ImpalaDialect impalaDialect;

  @Override
  protected void setUp() throws Exception {
    when( metaData.getDatabaseProductName() ).thenReturn( Dialect.DatabaseProduct.IMPALA.name() );
    when( connection.getMetaData() ).thenReturn( metaData );
    impalaDialect = new ImpalaDialect( connection );
  }

  public void testAllowsRegularExpressionInWhereClause() {
    assertTrue( impalaDialect.allowsRegularExpressionInWhereClause() );
  }

  public void testGenerateRegularExpression_InvalidRegex() throws Exception {
    assertNull( "Invalid regex should be ignored", impalaDialect.generateRegularExpression( "table.column", "(a" ) );
  }

  public void testGenerateRegularExpression_CaseInsensitive() throws Exception {
    String sql = impalaDialect.generateRegularExpression( "table.column", "(?i)|(?u).*a.*" );
    assertSqlWithRegex( false, sql, "'.*A.*'" );
  }

  public void testGenerateRegularExpression_CaseSensitive() throws Exception {
    String sql = impalaDialect.generateRegularExpression( "table.column", ".*1.*" );
    assertSqlWithRegex( true, sql, "'.*1.*'" );
  }

  private void assertSqlWithRegex( boolean isCaseSensitive, String sql, String quotedRegex ) throws Exception {
    assertNotNull( "Sql should be generated", sql );
    assertEquals( sql, isCaseSensitive, !sql.contains( "UPPER" ) );
    assertTrue( sql, sql.contains( "cast(table.column as string)" ) );
    assertTrue( sql, sql.contains( "REGEXP" ) );
    assertTrue( sql, sql.contains( quotedRegex ) );
  }
}
// End ImpalaDialectTest.java
