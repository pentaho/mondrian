/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;

import junit.framework.TestCase;
import mondrian.olap.MondrianDef;

public class RolapUtilTest extends TestCase {

  private static final String FILTER_QUERY =
      "`TableAlias`.`promotion_id` = 112";
  private static final String FILTER_DIALECT = "mysql";
  private static final String TABLE_ALIAS = "TableAlias";
  private static final String RELATION_ALIAS = "RelationAlias";
  private static final String FACT_NAME = "order_fact";
  private MondrianDef.Relation fact;

  public void testMakeRolapStarKeyUnmodifiable() throws Exception {
    try {
      fact = new MondrianDef.Table(
          wrapStrSources(getFactTableWithSQLFilter()));
      List<String> polapStarKey = RolapUtil.makeRolapStarKey(FACT_NAME);
      assertNotNull(polapStarKey);
      polapStarKey.add("OneMore");
      fail(
          "It should not be allowed to change the rolap star key."
          + "UnsupportedOperationException expected but was not  been appeared.");
      } catch (UnsupportedOperationException e) {
      assertTrue(true);
    }
  }

  public void testMakeRolapStarKey_ByFactTableName() throws Exception {
    fact = new MondrianDef.Table(wrapStrSources(getFactTableWithSQLFilter()));
    List<String> polapStarKey = RolapUtil.makeRolapStarKey(FACT_NAME);
    assertNotNull(polapStarKey);
    assertEquals(1, polapStarKey.size());
    assertEquals(FACT_NAME, polapStarKey.get(0));
  }

  public void testMakeRolapStarKey_FactTableWithSQLFilter() throws Exception {
    fact = new MondrianDef.Table(wrapStrSources(getFactTableWithSQLFilter()));
    List<String> polapStarKey = RolapUtil.makeRolapStarKey(fact);
    assertNotNull(polapStarKey);
    assertEquals(3, polapStarKey.size());
    assertEquals(TABLE_ALIAS, polapStarKey.get(0));
    assertEquals(FILTER_DIALECT, polapStarKey.get(1));
    assertEquals(FILTER_QUERY, polapStarKey.get(2));
  }

  public void testMakeRolapStarKey_FactTableWithEmptyFilter()
      throws Exception {
    fact = new MondrianDef.Table(wrapStrSources(
        getFactTableWithEmptySQLFilter()));
    List<String> polapStarKey = RolapUtil.makeRolapStarKey(fact);
    assertNotNull(polapStarKey);
    assertEquals(1, polapStarKey.size());
    assertEquals(TABLE_ALIAS, polapStarKey.get(0));
  }

  public void testMakeRolapStarKey_FactTableWithoutSQLFilter()
      throws Exception {
    fact = new MondrianDef.Table(wrapStrSources(
        getFactTableWithoutSQLFilter()));
    List<String> polapStarKey = RolapUtil.makeRolapStarKey(fact);
    assertNotNull(polapStarKey);
    assertEquals(1, polapStarKey.size());
    assertEquals(TABLE_ALIAS, polapStarKey.get(0));
  }

  public void testMakeRolapStarKey_FactRelation() throws Exception {
    List<String> polapStarKey = RolapUtil.makeRolapStarKey(
        getFactRelationMock());
    assertNotNull(polapStarKey);
    assertEquals(1, polapStarKey.size());
    assertEquals(RELATION_ALIAS, polapStarKey.get(0));
  }

  private static String getFactTableWithSQLFilter() {
    String fact =
        "<Table name=\"sales_fact_1997\" alias=\"TableAlias\">\n"
        + " <SQL dialect=\"mysql\">\n"
        + "     `TableAlias`.`promotion_id` = 112\n"
        + " </SQL>\n"
        + "</Table>";
    return fact;
  }

  private static String getFactTableWithEmptySQLFilter() {
    String fact =
        "<Table name=\"sales_fact_1997\" alias=\"TableAlias\">\n"
        + " <SQL dialect=\"mysql\"/>\n"
        + "</Table>";
    return fact;
  }

  private static String getFactTableWithoutSQLFilter() {
    String fact =
        "<Table name=\"sales_fact_1997\" alias=\"TableAlias\">\n"
        + "</Table>";
    return fact;
  }

  private static MondrianDef.Relation getFactRelationMock() throws Exception {
    MondrianDef.Relation factMock = mock(MondrianDef.Relation.class);
    when(factMock.getAlias()).thenReturn(RELATION_ALIAS);
    return factMock;
  }

  private static DOMWrapper wrapStrSources(String resStr) throws XOMException {
    final Parser xmlParser = XOMUtil.createDefaultParser();
    final DOMWrapper def = xmlParser.parse(resStr);
    return def;
  }
}

// End RolapUtilTest.java
