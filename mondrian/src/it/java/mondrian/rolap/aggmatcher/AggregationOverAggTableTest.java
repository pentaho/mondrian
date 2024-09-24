/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
package mondrian.rolap.aggmatcher;

import mondrian.test.TestContext;

/**
 * @author Andrey Khayrutdinov
 */
public class AggregationOverAggTableTest extends AggTableTestCase {

    @Override
    protected String getFileName() {
        return "aggregation-over-agg-table.csv";
    }

    @Override
    protected TestContext createTestContext() {
        return getTestContext();
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        propSaver.set(propSaver.properties.EnableNativeCrossJoin, true);
        propSaver.set(propSaver.properties.EnableNativeNonEmpty, true);
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        TestContext.instance().flushSchemaCache();
    }

    public void testAvgMeasureLowestGranularity() throws Exception {
        TestContext testContext = ExplicitRecognizerTest.setupMultiColDimCube(
            "",
            "column=\"the_year\"",
            "column=\"quarter\"",
            "column=\"month_of_year\" ",
            "");

        String query =
            "select {[Measures].[Avg Unit Sales]} on columns, "
            + "non empty CrossJoin({[TimeExtra].[1997].[Q1].Children},{[Gender].[M]}) on rows "
            + "from [ExtraCol]";

        testContext.assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Avg Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[TimeExtra].[1997].[Q1].[1], [Gender].[M]}\n"
            + "{[TimeExtra].[1997].[Q1].[2], [Gender].[M]}\n"
            + "{[TimeExtra].[1997].[Q1].[3], [Gender].[M]}\n"
            + "Row #0: 3\n"
            + "Row #1: 3\n"
            + "Row #2: 3\n");

        assertQuerySqlOrNot(
            testContext,
            query,
            mysqlPattern(
                "select\n"
                + "    `agg_c_avg_sales_fact_1997`.`the_year` as `c0`,\n"
                + "    `agg_c_avg_sales_fact_1997`.`quarter` as `c1`,\n"
                + "    `agg_c_avg_sales_fact_1997`.`month_of_year` as `c2`,\n"
                + "    `agg_c_avg_sales_fact_1997`.`gender` as `c3`,\n"
                + "    (`agg_c_avg_sales_fact_1997`.`unit_sales`) / (`agg_c_avg_sales_fact_1997`.`fact_count`) as `m0`\n"
                + "from\n"
                + "    `agg_c_avg_sales_fact_1997` as `agg_c_avg_sales_fact_1997`\n"
                + "where\n"
                + "    `agg_c_avg_sales_fact_1997`.`the_year` = 1997\n"
                + "and\n"
                + "    `agg_c_avg_sales_fact_1997`.`quarter` = 'Q1'\n"
                + "and\n"
                + "    `agg_c_avg_sales_fact_1997`.`month_of_year` in (1, 2, 3)\n"
                + "and\n"
                + "    `agg_c_avg_sales_fact_1997`.`gender` = 'M'"),
            false, false, true);
    }
}

// End AggregationOverAggTableTest.java
