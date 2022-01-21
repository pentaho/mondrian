/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2021 Hitachi Vantara.
// All rights reserved.
 */
package mondrian.spi.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import mondrian.spi.Dialect;

public class OracleDialectTest{
  private Connection connection = mock( Connection.class );
  private DatabaseMetaData metaData = mock( DatabaseMetaData.class );
  Statement statmentMock = mock( Statement.class );
  private OracleDialect dialect;

  @BeforeEach
  public void setUp() throws Exception {
    when( metaData.getDatabaseProductName() ).thenReturn( Dialect.DatabaseProduct.ORACLE.name() );
    when( connection.getMetaData() ).thenReturn( metaData );
    dialect = new OracleDialect( connection );
  }

  @Test
  public void testAllowsRegularExpressionInWhereClause() {
    assertTrue( dialect.allowsRegularExpressionInWhereClause() );
  }

  @Test
  public void testGenerateRegularExpression_InvalidRegex() throws Exception {
    assertNull( dialect.generateRegularExpression( "table.column", "(a" ), "Invalid regex should be ignored" );
  }

  @Test
  public void testGenerateRegularExpression_CaseInsensitive() throws Exception {
    String sql = dialect.generateRegularExpression( "table.column", "(?i)|(?u).*a.*" );
    assertEquals( "table.column IS NOT NULL AND REGEXP_LIKE(table.column, '.*a.*', 'i')", sql );
  }

  @Test
  public void testGenerateRegularExpression_CaseSensitive() throws Exception {
    String sql = dialect.generateRegularExpression( "table.column", ".*a.*" );
    assertEquals( "table.column IS NOT NULL AND REGEXP_LIKE(table.column, '.*a.*', '')", sql );
  }
}
//End OracleDialectTest.java
