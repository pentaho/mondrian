/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014-2017 Hitachi Vantara and others
// All Rights Reserved.
 */
package mondrian.rolap.aggmatcher;

import mondrian.rolap.BatchTestCase;
import mondrian.test.TestContext;

public class DefaultRecognizerTest extends BatchTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        propSaver.set(propSaver.properties.EnableNativeCrossJoin, true);
        propSaver.set(propSaver.properties.EnableNativeNonEmpty, true);
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        propSaver.set(propSaver.properties.UseAggregates, true);
        propSaver.set(propSaver.properties.ReadAggregates, true);
        TestContext.instance().flushSchemaCache();
    }


    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-2116">MONDRIAN-2116</a>
     * When an alias is used for the fact table, the default agg table
     * recognizer doesn't use the agg tables anymore.
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
            mysqlPattern(expectedSql),
            false, true, true);
    }

    public void testTupleReaderWithDistinctCountMeasureInContext() {
        // Validates that if a distinct count measure is in context
        // SqlTupleReader is able to find an appropriate agg table, if
        // available. MONDRIAN-2376

        String query =
            "select non empty crossjoin(Gender.F, crossjoin([Marital Status].M, "
            + "crossjoin([Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], "
            + "Time.Month.members))) on 0 "
            + "from Sales where measures.[customer count]";

        String sql =
            "select\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`gender` as `c0`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`marital_status` as `c1`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`product_family` as `c2`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`product_department` as `c3`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`product_category` as `c4`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`the_year` as `c5`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`quarter` as `c6`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`month_of_year` as `c7`\n"
            + "from\n"
            + "    `agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997`\n"
            + "where\n"
            + "    (`agg_g_ms_pcat_sales_fact_1997`.`gender` = 'F')\n"
            + "and\n"
            + "    (`agg_g_ms_pcat_sales_fact_1997`.`marital_status` = 'M')\n"
            + "and\n"
            + "    (`agg_g_ms_pcat_sales_fact_1997`.`product_category` = 'Beer and Wine' and `agg_g_ms_pcat_sales_fact_1997`.`product_department` = 'Alcoholic Beverages' and `agg_g_ms_pcat_sales_fact_1997`.`product_family` = 'Drink')\n"
            + "group by\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`gender`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`marital_status`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`product_family`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`product_department`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`product_category`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`the_year`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`quarter`,\n"
            + "    `agg_g_ms_pcat_sales_fact_1997`.`month_of_year`\n"
            + "order by\n"
            + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                + "    ISNULL(`c1`) ASC, `c1` ASC,\n"
                + "    ISNULL(`c2`) ASC, `c2` ASC,\n"
                + "    ISNULL(`c3`) ASC, `c3` ASC,\n"
                + "    ISNULL(`c4`) ASC, `c4` ASC,\n"
                + "    ISNULL(`c5`) ASC, `c5` ASC,\n"
                + "    ISNULL(`c6`) ASC, `c6` ASC,\n"
                + "    ISNULL(`c7`) ASC, `c7` ASC"
                : "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`gender`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`gender` ASC,\n"
                + "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`marital_status`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`marital_status` ASC,\n"
                + "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`product_family`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`product_family` ASC,\n"
                + "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`product_department`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`product_department` ASC,\n"
                + "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`product_category`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`product_category` ASC,\n"
                + "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`the_year`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`the_year` ASC,\n"
                + "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`quarter`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`quarter` ASC,\n"
                + "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`month_of_year`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`month_of_year` ASC");
        assertQuerySqlOrNot(
            TestContext.instance(),
            query,
            mysqlPattern(sql),
            false, true, true);
    }

}
// End DefaultRecognizerTest.java
