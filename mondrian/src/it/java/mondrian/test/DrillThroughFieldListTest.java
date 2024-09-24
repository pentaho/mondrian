/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara
// All Rights Reserved.
//
*/
package mondrian.test;

import mondrian.olap.OlapElement;
import mondrian.olap.Result;
import mondrian.rolap.RolapCell;

import java.util.Arrays;
import java.util.List;

/**
 * Test drillthrought operation with specified field list.
 * If a field list was specified that means that
 * the MDX is a DRILLTHROUGH operation and
 * includes a RETURN clause.
 *
 * @author Yury_Bakhmutski
 * @since Nov 18, 2015
 */
public class DrillThroughFieldListTest extends FoodMartTestCase {
  private TestContext context = getTestContext();

  public DrillThroughFieldListTest() {
    super();
  }

  public DrillThroughFieldListTest(String name) {
    super(name);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    propSaver.set(propSaver.properties.GenerateFormattedSql, true);
  }

  public void testOneJoin() {
    String mdx = "SELECT\n"
        + "[Measures].[Unit Sales] ON COLUMNS,\n"
        + "[Time].[Quarter].[Q1] ON ROWS\n"
        + "FROM [Sales]";
    Result result = executeQuery(mdx);

    RolapCell rCell = (RolapCell)result.getCell(new int[] { 0, 0 });

    assertTrue(rCell.canDrillThrough());

    OlapElement returnMeasureAttribute = result.getAxes()[0]
        .getPositions().get(0).get(0);
    OlapElement returnLevelAttribute = result.getAxes()[1]
        .getPositions().get(0).get(0).getLevel();

    List<OlapElement> attributes = Arrays
        .asList(returnMeasureAttribute, returnLevelAttribute);

    String expectedSql;
    switch (getTestContext().getDialect().getDatabaseProduct()) {
    case MARIADB:
    case MYSQL:
        expectedSql =
            "select\n"
            + "    time_by_day.quarter as Quarter,\n"
            + "    sales_fact_1997.unit_sales as Unit Sales\n"
            + "from\n"
            + "    time_by_day as time_by_day,\n"
            + "    sales_fact_1997 as sales_fact_1997\n"
            + "where\n"
            + "    sales_fact_1997.time_id = time_by_day.time_id\n"
            + "and\n"
            + "    time_by_day.the_year = 1997\n"
            + "and\n"
            + "    time_by_day.quarter = 'Q1'\n"
            + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    Quarter ASC"
                : "    time_by_day.quarter ASC");
        break;
    case ORACLE:
        expectedSql =
            "select\n"
            + "    time_by_day.quarter as Quarter,\n"
            + "    sales_fact_1997.unit_sales as Unit Sales\n"
            + "from\n"
            + "    time_by_day time_by_day,\n"
            + "    sales_fact_1997 sales_fact_1997\n"
            + "where\n"
            + "    sales_fact_1997.time_id = time_by_day.time_id\n"
            + "and\n"
            + "    time_by_day.the_year = 1997\n"
            + "and\n"
            + "    time_by_day.quarter = 'Q1'\n"
            + "order by\n"
            + "    time_by_day.quarter ASC";
        break;
    default:
        return;
    }

    String actual = rCell.getDrillThroughSQL(attributes, true);
    int expectedRowsNumber = 21588;

    context.assertSqlEquals(expectedSql, actual, expectedRowsNumber);
  }

  public void testOneJoinTwoMeasures() {
    String mdx = "SELECT\n"
        + "{[Measures].[Unit Sales], [Measures].[Store Cost]} ON COLUMNS,\n"
        + "[Time].[Quarter].[Q1] ON ROWS\n"
        + "FROM [Sales]";
    Result result = executeQuery(mdx);

    RolapCell rCell = (RolapCell)result.getCell(new int[] { 0, 0 });

    assertTrue(rCell.canDrillThrough());

    OlapElement unitSalesAttribute = result.getAxes()[0]
        .getPositions().get(0).get(0);
    OlapElement storeCostAttribute = result.getAxes()[0]
        .getPositions().get(1).get(0);
    OlapElement quarterAttribute = result.getAxes()[1]
        .getPositions().get(0).get(0).getLevel();

    List<OlapElement> attributes = Arrays
        .asList(unitSalesAttribute, storeCostAttribute, quarterAttribute);

    String expectedSql;
    switch (getTestContext().getDialect().getDatabaseProduct()) {
    case MARIADB:
    case MYSQL:
        expectedSql =
            "select\n"
            + "    time_by_day.quarter as Quarter,\n"
            + "    sales_fact_1997.unit_sales as Unit Sales,\n"
            + "    sales_fact_1997.store_cost as Store Cost\n"
            + "from\n"
            + "    time_by_day as time_by_day,\n"
            + "    sales_fact_1997 as sales_fact_1997\n"
            + "where\n"
            + "    sales_fact_1997.time_id = time_by_day.time_id\n"
            + "and\n"
            + "    time_by_day.the_year = 1997\n"
            + "and\n"
            + "    time_by_day.quarter = 'Q1'\n"
            + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    Quarter ASC"
                : "    time_by_day.quarter ASC");
        break;
    case ORACLE:
        expectedSql =
            "select\n"
            + "    time_by_day.quarter as Quarter,\n"
            + "    sales_fact_1997.unit_sales as Unit Sales,\n"
            + "    sales_fact_1997.store_cost as Store Cost\n"
            + "from\n"
            + "    time_by_day time_by_day,\n"
            + "    sales_fact_1997 sales_fact_1997\n"
            + "where\n"
            + "    sales_fact_1997.time_id = time_by_day.time_id\n"
            + "and\n"
            + "    time_by_day.the_year = 1997\n"
            + "and\n"
            + "    time_by_day.quarter = 'Q1'\n"
            + "order by\n"
            + "    time_by_day.quarter ASC";
        break;
    default:
        return;
    }

    String actual = rCell.getDrillThroughSQL(attributes, true);
    int expectedRowsNumber = 21588;

    context.assertSqlEquals(expectedSql, actual, expectedRowsNumber);
  }

  public void testTwoJoins() {
    String mdx = "SELECT\n"
        + "{[Measures].[Unit Sales], [Measures].[Store Cost]} ON COLUMNS,\n"
        + "NONEMPTYCROSSJOIN({[Time].[Quarter].[Q1]},"
        + " {[Product].[Product Name].[Good Imported Beer]}) ON ROWS\n"
        + "FROM [Sales]";
    Result result = executeQuery(mdx);

    RolapCell rCell = (RolapCell)result.getCell(new int[] { 0, 0 });

    assertTrue(rCell.canDrillThrough());

    OlapElement unitSalesAttribute = result.getAxes()[0]
        .getPositions().get(0).get(0);
    OlapElement storeCostAttribute = result.getAxes()[0]
        .getPositions().get(1).get(0);
    OlapElement quarterAttribute = result.getAxes()[1]
        .getPositions().get(0).get(0).getLevel();

    List<OlapElement> attributes = Arrays
        .asList(unitSalesAttribute, storeCostAttribute, quarterAttribute);

    String expectedSql;
    switch (getTestContext().getDialect().getDatabaseProduct()) {
    case MARIADB:
    case MYSQL:
        expectedSql = "select\n"
            + "    time_by_day.quarter as Quarter,\n"
            + "    sales_fact_1997.unit_sales as Unit Sales,\n"
            + "    sales_fact_1997.store_cost as Store Cost\n"
            + "from\n"
            + "    time_by_day as time_by_day,\n"
            + "    sales_fact_1997 as sales_fact_1997,\n"
            + "    product as product\n"
            + "where\n"
            + "    sales_fact_1997.time_id = time_by_day.time_id\n"
            + "and\n"
            + "    time_by_day.the_year = 1997\n"
            + "and\n"
            + "    time_by_day.quarter = 'Q1'\n"
            + "and\n"
            + "    sales_fact_1997.product_id = product.product_id\n"
            + "and\n"
            + "    product.product_name = 'Good Imported Beer'\n"
            + "order by\n"
            + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    Quarter ASC"
                : "    time_by_day.quarter ASC");
        break;
    case ORACLE:
        expectedSql = "select\n"
            + "    time_by_day.quarter as Quarter,\n"
            + "    sales_fact_1997.unit_sales as Unit Sales,\n"
            + "    sales_fact_1997.store_cost as Store Cost\n"
            + "from\n"
            + "    time_by_day time_by_day,\n"
            + "    sales_fact_1997 sales_fact_1997,\n"
            + "    product product\n"
            + "where\n"
            + "    sales_fact_1997.time_id = time_by_day.time_id\n"
            + "and\n"
            + "    time_by_day.the_year = 1997\n"
            + "and\n"
            + "    time_by_day.quarter = 'Q1'\n"
            + "and\n"
            + "    sales_fact_1997.product_id = product.product_id\n"
            + "and\n"
            + "    product.product_name = 'Good Imported Beer'\n"
            + "order by\n"
            + "    time_by_day.quarter ASC";
        break;
    default:
        return;
    }

    String actual = rCell.getDrillThroughSQL(attributes, true);
    int expectedRowsNumber = 7;

    context.assertSqlEquals(expectedSql, actual, expectedRowsNumber);
  }

  public void testNoJoin() {
    String mdx = "SELECT\n"
        + "Measures.[Store Sqft] on COLUMNS\n"
        + "FROM [Store]";
    Result result = executeQuery(mdx);

    RolapCell rCell = (RolapCell)result.getCell(new int[] { 0 });

    assertTrue(rCell.canDrillThrough());

    OlapElement StoreSqftAttribute = result.getAxes()[0]
        .getPositions().get(0).get(0);

    List<OlapElement> attributes = Arrays
        .asList(StoreSqftAttribute);

    String expectedSql;
    switch (getTestContext().getDialect().getDatabaseProduct()) {
    case MARIADB:
    case MYSQL:
        expectedSql =
            "select\n"
            + "    store.store_sqft as Store Sqft\n"
            + "from\n"
            + "    store as store";
        break;
    case ORACLE:
        expectedSql =
            "select\n"
            + "    store.store_sqft as Store Sqft\n"
            + "from\n"
            + "    store store";
        break;
    default:
        return;
    }

    String actual = rCell.getDrillThroughSQL(attributes, true);
    int expectedRowsNumber = 25;

    context.assertSqlEquals(expectedSql, actual, expectedRowsNumber);
  }

  public void testVirtualCube() {
    String mdx = " SELECT\n"
        + " [Measures].[Unit Sales] ON COLUMNS\n"
        + " FROM [Warehouse and Sales]\n"
        + " WHERE [Gender].[F]";
    Result result = executeQuery(mdx);

    RolapCell rCell = (RolapCell)result.getCell(new int[] { 0 });

    assertTrue(rCell.canDrillThrough());

    OlapElement StoreSqftAttribute = result.getAxes()[0]
        .getPositions().get(0).get(0);

    List<OlapElement> attributes = Arrays
        .asList(StoreSqftAttribute);

    String expectedSql;
    switch (getTestContext().getDialect().getDatabaseProduct()) {
    case MARIADB:
    case MYSQL:
        expectedSql = "select\n"
            + "    sales_fact_1997.unit_sales as Unit Sales\n"
            + "from\n"
            + "    time_by_day as time_by_day,\n"
            + "    sales_fact_1997 as sales_fact_1997,\n"
            + "    customer as customer\n"
            + "where\n"
            + "    sales_fact_1997.time_id = time_by_day.time_id\n"
            + "and\n"
            + "    time_by_day.the_year = 1997\n"
            + "and\n"
            + "    sales_fact_1997.customer_id = customer.customer_id\n"
            + "and\n"
            + "    customer.gender = 'F'";
        break;
    case ORACLE:
        expectedSql = "select\n"
             + "    sales_fact_1997.unit_sales as Unit Sales\n"
             + "from\n"
             + "    time_by_day time_by_day,\n"
             + "    sales_fact_1997 sales_fact_1997,\n"
             + "    customer customer\n"
             + "where\n"
             + "    sales_fact_1997.time_id = time_by_day.time_id\n"
             + "and\n"
             + "    time_by_day.the_year = 1997\n"
             + "and\n"
             + "    sales_fact_1997.customer_id = customer.customer_id\n"
             + "and\n"
             + "    customer.gender = 'F'";
        break;
    default:
        return;
    }

    String actual = rCell.getDrillThroughSQL(attributes, true);
    int expectedRowsNumber = 42831;

    context.assertSqlEquals(expectedSql, actual, expectedRowsNumber);
  }
}

// End DrillThroughFieldListTest.java
