/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014-2014 Pentaho and others
// All Rights Reserved.
 */
package mondrian.rolap.aggmatcher;

import mondrian.rolap.BatchTestCase;
import mondrian.spi.Dialect.DatabaseProduct;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;

public class DefaultRecognizerTest extends BatchTestCase {
  /**
   * This is a test for <a href="http://jira.pentaho.com/browse/MONDRIAN-2116">MONDRIAN-2116</a>
   * When an alias is used for the fact table, the default agg table recognizer
   * doesn't use the agg tables anymore.
   */
  public void testDefaultRecognizerWithFactAlias() {
    final String cube =
        "<Cube name=\"Sales\" defaultMeasure=\"Unit Sales\"> "
        // For this test, we use an alias on the fact table.
        + "  <Table name=\"sales_fact_1997\" alias=\"foobar\"> "
        + "      <AggExclude name=\"agg_c_special_sales_fact_1997\"/>"
        + "      <AggExclude name=\"agg_c_14_sales_fact_1997\"/>"
        + "  </Table>"
        + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/> "
        + "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
        + "      formatString=\"Standard\"/>\n"
        + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n"
        + "      formatString=\"#,###.00\"/>\n"
        + "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n"
        + "      formatString=\"#,###.00\"/>"
        + "  <Measure name=\"Customer Count\" column=\"customer_id\" aggregator=\"distinct-count\" formatString=\"#,###\" />"
        + "</Cube>";
    final String dimension =
        "<Dimension name=\"Time\" type=\"TimeDimension\"> "
        + "  <Hierarchy hasAll=\"false\" primaryKey=\"time_id\"> "
        + "    <Table name=\"time_by_day\"/> "
        + "    <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\" levelType=\"TimeYears\"/> "
        + "    <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\" levelType=\"TimeQuarters\"/> "
        + "    <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\" levelType=\"TimeMonths\"/> "
        + "  </Hierarchy> "
        + "</Dimension>";
    final String simpleSchema =
        "<Schema name=\"FoodMart\">" + dimension + cube + "</Schema>";

    final String query =
        "select "
        + "  NON EMPTY {[Measures].[Customer Count]} ON COLUMNS, "
        + "  NON EMPTY {[Time].[1997].[Q1].Children} ON ROWS "
        + "from [Sales]";

    // Turn on agg tables for this test
    propSaver.set(propSaver.properties.UseAggregates, true);
    propSaver.set(propSaver.properties.ReadAggregates, true);
    propSaver.set(propSaver.properties.GenerateFormattedSql, true);

    // We expect the agg tables to be used.
    final String expectedSql =
        "select\n"
        + "    `agg_c_10_sales_fact_1997`.`the_year` as `c0`,\n"
        + "    `agg_c_10_sales_fact_1997`.`quarter` as `c1`,\n"
        + "    `agg_c_10_sales_fact_1997`.`month_of_year` as `c2`,\n"
        + "    `agg_c_10_sales_fact_1997`.`customer_count` as `m0`\n"
        + "from\n"
        + "    `agg_c_10_sales_fact_1997` as `agg_c_10_sales_fact_1997`\n"
        + "where\n"
        + "    `agg_c_10_sales_fact_1997`.`the_year` = 1997\n"
        + "and\n"
        + "    `agg_c_10_sales_fact_1997`.`quarter` = 'Q1'\n"
        + "and\n"
        + "    `agg_c_10_sales_fact_1997`.`month_of_year` in (1, 2, 3)";

    assertQuerySqlOrNot(
        TestContext.instance().withSchema(simpleSchema),
        query,
        new SqlPattern[]{
            new SqlPattern(
                DatabaseProduct.MYSQL,
                expectedSql,
                expectedSql.length())},
        false,
        true,
        true);
}
 }
// End DefaultRecognizerTest.java