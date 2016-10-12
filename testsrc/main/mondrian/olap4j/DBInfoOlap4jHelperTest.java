/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2016 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap4j;

import java.sql.Types;

import mondrian.olap.Level;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.spi.Dialect.Datatype;
import mondrian.test.FoodMartTestCase;

public class DBInfoOlap4jHelperTest extends FoodMartTestCase {

  public void testDBShemaName() {
    propSaver.set(
        MondrianProperties.instance().SsasCompatibleNaming, true);
    String mdxTime = "SELECT\n"
        + "   [Measures].[Unit Sales] ON COLUMNS,\n"
        + "   [Promotions].Members ON ROWS\n"
        + "FROM [Sales]";
    Result resultTime = executeQuery(mdxTime);
    Level lvl = resultTime.getAxes()[1].getPositions().get(0).get(0)
        .getLevel();
    assertEquals("FoodMart", DBInfoOlap4jHelper.getDBSchemaName(lvl));
    flushContextSchemaCache();
  }

  public void testDBTableName() {
    propSaver.set(
        MondrianProperties.instance().SsasCompatibleNaming, true);
    String mdxTime = "SELECT\n"
        + "   [Measures].[Unit Sales] ON COLUMNS,\n"
        + "   [Promotions].Members ON ROWS\n"
        + "FROM [Sales]";
    Result resultTime = executeQuery(mdxTime);
    Level lvl = resultTime.getAxes()[1].getPositions().get(0).get(0)
        .getLevel();
    assertEquals("promotion", DBInfoOlap4jHelper.getDBTableName(lvl));
    flushContextSchemaCache();
  }

  public void testDBColumnName() {
    propSaver.set(
        MondrianProperties.instance().SsasCompatibleNaming, true);
    String mdxTime = "SELECT\n"
        + "   [Measures].[Unit Sales] ON COLUMNS,\n"
        + "   [Promotions].Members ON ROWS\n"
        + "FROM [Sales]";
    Result resultTime = executeQuery(mdxTime);
    Level lvl = resultTime.getAxes()[1].getPositions().get(0).get(0)
        .getLevel();
    assertEquals("promotion_name", DBInfoOlap4jHelper.getDBColumnName(lvl));
    flushContextSchemaCache();
  }

  public void testDBFactForeignKey() {
    propSaver.set(
        MondrianProperties.instance().SsasCompatibleNaming, true);
    String mdxTime = "SELECT\n"
        + "   [Measures].[Unit Sales] ON COLUMNS,\n"
        + "   [Promotions].Members ON ROWS\n"
        + "FROM [Sales]";
    Result resultTime = executeQuery(mdxTime);
    Level lvl = resultTime.getAxes()[1].getPositions().get(0).get(0)
        .getLevel();
    assertEquals("promotion_id", DBInfoOlap4jHelper.getFactTableDBForeignKey(
        lvl));
    flushContextSchemaCache();
  }

  public void testDBDimensionTableName() {
    propSaver.set(
        MondrianProperties.instance().SsasCompatibleNaming, true);
    String mdxTime = "SELECT\n"
        + "   [Measures].[Unit Sales] ON COLUMNS,\n"
        + "   [Promotions].Members ON ROWS\n"
        + "FROM [Sales]";
    Result resultTime = executeQuery(mdxTime);
    Level lvl = resultTime.getAxes()[1].getPositions().get(0).get(0)
        .getLevel();
    assertEquals("promotion", DBInfoOlap4jHelper.getDimensionDBTable(lvl));
    flushContextSchemaCache();
  }

  public void testDBDimensionPrimaryKey() {
    propSaver.set(
        MondrianProperties.instance().SsasCompatibleNaming, true);
    String mdxTime = "SELECT\n"
        + "   [Measures].[Unit Sales] ON COLUMNS,\n"
        + "   [Promotions].Members ON ROWS\n"
        + "FROM [Sales]";
    Result resultTime = executeQuery(mdxTime);
    Level lvl = resultTime.getAxes()[1].getPositions().get(0).get(0)
        .getLevel();
    assertEquals("promotion_id", DBInfoOlap4jHelper.getDimensionDBPrimaryKey(
        lvl));
    flushContextSchemaCache();
  }

  public void testDBColumnDatatype() {
    propSaver.set(
        MondrianProperties.instance().SsasCompatibleNaming, true);
    String mdxTime = "SELECT\n"
        + "   [Measures].[Unit Sales] ON COLUMNS,\n"
        + "   [Promotions].Members ON ROWS\n"
        + "FROM [Sales]";
    Result resultTime = executeQuery(mdxTime);
    Level lvl = resultTime.getAxes()[1].getPositions().get(0).get(0)
        .getLevel();
    assertEquals(Datatype.String, DBInfoOlap4jHelper.getDBColumnType(lvl));

    mdxTime = "SELECT\n"
        + "   [Measures].[Unit Sales] ON COLUMNS,\n"
        + "   [Time].[Weekly].Members ON ROWS\n"
        + "FROM [Sales]";
    resultTime = executeQuery(mdxTime);
    lvl = resultTime.getAxes()[1].getPositions().get(0).get(0).getLevel();
    assertEquals(Datatype.Numeric, DBInfoOlap4jHelper.getDBColumnType(lvl));
    flushContextSchemaCache();
  }

  public void testDBColumnSQLDatatype() {
    propSaver.set(
        MondrianProperties.instance().SsasCompatibleNaming, true);
    String mdxTime = "SELECT\n"
        + "   [Measures].[Unit Sales] ON COLUMNS,\n"
        + "   [Promotions].Members ON ROWS\n"
        + "FROM [Sales]";
    Result resultTime = executeQuery(mdxTime);
    Level lvl = resultTime.getAxes()[1].getPositions().get(0).get(0)
        .getLevel();
    assertEquals(Types.VARCHAR, DBInfoOlap4jHelper.getDBColumnSQLType(
        getConnection(),lvl));

    mdxTime = "SELECT\n"
        + "   [Measures].[Unit Sales] ON COLUMNS,\n"
        + "   [Time].[Weekly].Members ON ROWS\n"
        + "FROM [Sales]";
    resultTime = executeQuery(mdxTime);
    lvl = resultTime.getAxes()[1].getPositions().get(0).get(0).getLevel();
    assertEquals(Types.SMALLINT, DBInfoOlap4jHelper.getDBColumnSQLType(
        getConnection(),lvl));
    flushContextSchemaCache();
  }

  private void flushContextSchemaCache() {
    getTestContext().flushSchemaCache();
  }

}
//End DBInfoOlap4jHelperTest.java'