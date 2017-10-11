/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2017 Hitachi Vantara.
// All rights reserved.
 */
package mondrian.olap.fun;

import mondrian.olap.Category;
import mondrian.olap.Connection;
import mondrian.olap.Exp;
import mondrian.olap.MondrianException;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.type.StringType;
import mondrian.olap.type.Type;
import mondrian.test.FoodMartTestCase;

public class PropertiesFunctionTest extends FoodMartTestCase {

  private static final String TIME_MEMBER_CAPTION = "1997";
  private static final String TIME_WEEKLY_MEMBER_CAPTION = "All Time.Weeklys";
  private static final String STORE_MEMBER_CAPTION = "All Stores";
  private static final int[] ZERO_POS = new int[] { 0 };
  private Query query;
  private Result result;
  private Connection connection;
  private Exp resolvedFun;
  private static final StringType STRING_TYPE = new StringType();

  @Override
  protected void setUp() throws Exception {
    connection = getConnection();
    query = null;
    result = null;
    resolvedFun = null;
  }

  // The "Time" dimention in foodmart schema contains two hierarchies.
  // The first hierarchy doesn't have a name. By default, a hierarchy has the same name as its dimension, so the first
  // hierarchy is called "Time".
  // Below the tests for the "Time" hierarchy and dimention.
  public void testMemberCaptionPropertyOnTimeDimension() {
    verifyMemberCaptionPropertyFunction( "[Time].Properties('MEMBER_CAPTION')", Category.String, STRING_TYPE, TIME_MEMBER_CAPTION );
  }

  public void testCurrentMemberCaptionPropertyOnTimeDimension() {
    verifyMemberCaptionPropertyFunction( "[Time].CurrentMember.Properties('MEMBER_CAPTION')", Category.String, STRING_TYPE, TIME_MEMBER_CAPTION );
  }

  public void testMemberCaptionPropertyOnTimeHierarchy() {
    verifyMemberCaptionPropertyFunction( "[Time].[Time].Properties('MEMBER_CAPTION')", Category.String, STRING_TYPE, TIME_MEMBER_CAPTION );
  }

  public void testCurrentMemberCaptionPropertyOnTimeHierarchy() {
    verifyMemberCaptionPropertyFunction( "[Time].[Time].CurrentMember.Properties('MEMBER_CAPTION')", Category.String, STRING_TYPE, TIME_MEMBER_CAPTION );
  }

  public void testGenerateWithMemberCaptionPropertyOnTimeDimension() {
    verifyGenerateWithMemberCaptionPropertyFunction( "Generate([Time].CurrentMember, [Time].CurrentMember.Properties('MEMBER_CAPTION'))", Category.String, STRING_TYPE, TIME_MEMBER_CAPTION );
  }

  public void testGenerateWithMemberCaptionPropertyOnTimeHierarchy() {
    verifyGenerateWithMemberCaptionPropertyFunction( "Generate([Time].CurrentMember, [Time].[Time].CurrentMember.Properties('MEMBER_CAPTION'))", Category.String, STRING_TYPE, TIME_MEMBER_CAPTION );
  }

  // Below the tests for the "Time.Weekly" hierarchy.
  public void testMemberCaptionPropertyOnWeeklyHierarchy() {
    verifyMemberCaptionPropertyFunction( "[Time.Weekly].Properties('MEMBER_CAPTION')", Category.String, STRING_TYPE, TIME_WEEKLY_MEMBER_CAPTION );
  }

  public void testCurrentMemberCaptionPropertyOnWeeklyHierarchy() {
    verifyMemberCaptionPropertyFunction( "[Time.Weekly].CurrentMember.Properties('MEMBER_CAPTION')", Category.String, STRING_TYPE, TIME_WEEKLY_MEMBER_CAPTION );
  }

  public void testGenerateWithMemberCaptionPropertyOnWeeklyHierarchy() {
    verifyGenerateWithMemberCaptionPropertyFunction( "Generate([Time.Weekly].CurrentMember, [Time.Weekly].CurrentMember.Properties('MEMBER_CAPTION'))", Category.String, STRING_TYPE, TIME_WEEKLY_MEMBER_CAPTION );
  }

  // The "Store" dimention in foodmart schema contains only one hierarchy that has no name. So its name is "Store".
  // Below the tests for the "Store" hierarchy and dimention.
  public void testMemberCaptionPropertyOnStoreDimension() {
    verifyMemberCaptionPropertyFunction( "[Store].Properties('MEMBER_CAPTION')", Category.String, STRING_TYPE, STORE_MEMBER_CAPTION );
  }

  public void testCurrentMemberCaptionPropertyOnStoreDimension() {
    verifyMemberCaptionPropertyFunction( "[Store].CurrentMember.Properties('MEMBER_CAPTION')", Category.String, STRING_TYPE, STORE_MEMBER_CAPTION );
  }

  public void testMemberCaptionPropertyOnStoreHierarchy() {
    verifyMemberCaptionPropertyFunction( "[Store].[Store].Properties('MEMBER_CAPTION')", Category.String, STRING_TYPE, STORE_MEMBER_CAPTION );
  }

  public void testCurrentMemberCaptionPropertyOnStoreHierarchy() {
    verifyMemberCaptionPropertyFunction( "[Store].[Store].CurrentMember.Properties('MEMBER_CAPTION')", Category.String, STRING_TYPE, STORE_MEMBER_CAPTION );
  }

  public void testGenerateWithMemberCaptionPropertyOnStoreDimension() {
    verifyGenerateWithMemberCaptionPropertyFunction( "Generate([Store].CurrentMember, [Store].CurrentMember.Properties('MEMBER_CAPTION'))", Category.String, STRING_TYPE, STORE_MEMBER_CAPTION );
  }

  public void testGenerateWithMemberCaptionPropertyOnStoreHierarchy() {
    verifyGenerateWithMemberCaptionPropertyFunction( "Generate([Store].CurrentMember, [Store].[Store].CurrentMember.Properties('MEMBER_CAPTION'))", Category.String, STRING_TYPE, STORE_MEMBER_CAPTION );
  }

  private void verifyMemberCaptionPropertyFunction( String propertyQuery, int expectedCategory, Type expectedReturnType, String expectedResult ) {
    query = connection.parseQuery( generateQueryString( propertyQuery ) );
    assertNotNull( query );
    resolvedFun = query.getFormulas()[0].getExpression();

    assertNotNull( resolvedFun );
    assertEquals( expectedCategory, resolvedFun.getCategory() );
    assertEquals( expectedReturnType, resolvedFun.getType() );

    result = getConnection().execute( query );
    assertNotNull( result );
    assertEquals( expectedResult, result.getCell( ZERO_POS ).getFormattedValue() );
  }

  private void verifyGenerateWithMemberCaptionPropertyFunction( String functionQuery, int expectedCategory, Type expectedReturnType, String expectedResult ) {
    try {
      query = connection.parseQuery( generateQueryString( functionQuery ) );
    } catch ( MondrianException e ) {
      e.printStackTrace();
      fail( "No exception should be thrown but we have: " + e.getCause().getLocalizedMessage() );
    }
    assertNotNull( query );
    resolvedFun = query.getFormulas()[0].getExpression();

    assertNotNull( resolvedFun );
    assertEquals( expectedCategory, resolvedFun.getCategory() );
    assertEquals( expectedReturnType, resolvedFun.getType() );

    result = getConnection().execute( query );
    assertNotNull( result );
    assertEquals( expectedResult, result.getCell( ZERO_POS ).getFormattedValue() );
  }

  private static String generateQueryString( String exp ) {
    return "WITH MEMBER [Measures].[Foo] as " + exp + "SELECT {[Measures].[Foo]} ON COLUMNS from [Sales]";
  }

}
