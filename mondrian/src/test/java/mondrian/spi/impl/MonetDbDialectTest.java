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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import mondrian.spi.Dialect;

public class MonetDbDialectTest{
  private Connection connection = mock( Connection.class );
  private DatabaseMetaData metaData = mock( DatabaseMetaData.class );

  private MonetDbDialect dialect;
  private static final String JUL_2017_SP1_DB_VERSION = "11.27.5";
  private static final String AUG_2011_SP2_DB_VERSION = "11.5.7";
  private static final String AUG_2011_SP1_DB_VERSION = "11.5.3";
  private static final String CURRENT_DB_VERSION = JUL_2017_SP1_DB_VERSION;

  @BeforeEach
  protected void setUp() throws Exception {
    when( metaData.getDatabaseProductName() ).thenReturn( Dialect.DatabaseProduct.MONETDB.name() );
    when( metaData.getDatabaseProductVersion() ).thenReturn( CURRENT_DB_VERSION );
    when( connection.getMetaData() ).thenReturn( metaData );
    dialect = new MonetDbDialect( connection );
  }

  @Test
  public void testAllowsCountDistinctFalse_BeforeAug2011SP2() throws Exception {
    // set up the version before Aug2011 SP2
    when( metaData.getDatabaseProductVersion() ).thenReturn( AUG_2011_SP1_DB_VERSION );
    dialect = new MonetDbDialect( connection );
    assertFalse( dialect.allowsCountDistinct() );
  }

  @Test
  public void testAllowsCountDistinctTrue_StartingFromAug2011SP2() throws Exception {
    // set up the version starting from Aug2011 SP2
    when( metaData.getDatabaseProductVersion() ).thenReturn( AUG_2011_SP2_DB_VERSION );
    dialect = new MonetDbDialect( connection );
    assertTrue( dialect.allowsCountDistinct() );
  }

  @Test
  public void testAllowsCountDistinct() throws Exception {
    assertTrue( dialect.allowsCountDistinct() );
  }

  @Test
  public void testAllowsCompoundCountDistinct() {
    assertFalse( dialect.allowsCompoundCountDistinct() );
  }

  @Test
  public void testAllowsMultipleCountDistinct() {
    assertFalse( dialect.allowsMultipleCountDistinct() );
  }
  
  @Test
  public void testAllowsMultipleDistinctSqlMeasures() {
    assertFalse( dialect.allowsMultipleDistinctSqlMeasures() );
  }

  @Test
  public void testAllowsCountDistinctWithOtherAggs() {
    assertFalse( dialect.allowsCountDistinctWithOtherAggs() );
  }

  @Test
  public void testRequiresAliasForFromQuery() {
    assertTrue( dialect.requiresAliasForFromQuery() );
  }

  @Test
  public void testSupportsGroupByExpressions() {
    assertFalse( dialect.supportsGroupByExpressions() );
  }

  @Test
  public void testRequiresHavingAlias() {
    assertFalse( dialect.requiresHavingAlias() );
  }

  @Test
  public void testRequiresGroupByAlias() {
    assertFalse( dialect.requiresGroupByAlias() );
  }

  @Test
  public void testRequiresOrderByAlias() {
    assertFalse( dialect.requiresOrderByAlias() );
  }

  @Test
  public void testAllowsOrderByAlias() {
    assertFalse( dialect.allowsOrderByAlias() );
  }

  @Test
  public void testCompareVersions_Equals() {
    assertEquals( 0, dialect.compareVersions( AUG_2011_SP2_DB_VERSION, AUG_2011_SP2_DB_VERSION ) );
  }

  @Test
  public void testCompareVersions_SubStringLeft_Less() {
    assertEquals( -1, dialect.compareVersions( "11.3", "11.3.28" ) );
  }

  @Test
  public void testCompareVersions_SubStringRight_More() {
    assertEquals( 1, dialect.compareVersions( "11.3.28", "11.3" ) );
  }

  @Test
  public void testCompareVersions_LessOnMajor() {
    assertEquals( -1, dialect.compareVersions( "3.3.28", "11.3.28" ) );
  }

  @Test
  public void testCompareVersions_MoreOnMajor() {
    assertEquals( 1, dialect.compareVersions( "10.3.28", "2.3.28" ) );
  }

  @Test
  public void testCompareVersions_LessOnMinor() {
    assertEquals( -1, dialect.compareVersions( "11.3.28", "11.11.28" ) );
  }

  @Test
  public void testCompareVersions_MoreOnMinor() {
    assertEquals( 1, dialect.compareVersions( "11.11.28", "11.3.28" ) );
  }

  @Test
  public void testCompareVersions_LessOnPatch() {
    assertEquals( -1, dialect.compareVersions( "11.10.3", "11.10.28" ) );
  }

  @Test
  public void testCompareVersions_MoreOnPatch() {
    assertEquals( 1, dialect.compareVersions( "11.10.28", "11.10.3" ) );
  }

  @Test
  public void testCompareVersions_OnlyMajor() {
    assertEquals( -1, dialect.compareVersions( "7", "10" ) );
  }

}
// End MonetDbDialectTest.java
