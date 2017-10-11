/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2017 Hitachi Vantara
// All Rights Reserved.
*/
package mondrian.rolap.aggmatcher;

import mondrian.test.*;

import java.sql.*;

public class ExplicitRecognizerTest extends AggTableTestCase {

    protected void setUp() throws Exception {
        super.setUp(); // parent setUp enabled agg
        propSaver.set(propSaver.properties.EnableNativeCrossJoin, true);
        propSaver.set(propSaver.properties.EnableNativeNonEmpty, true);
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        TestContext.instance().flushSchemaCache();
    }

    @Override
    protected String getFileName() {
        return "explicit_aggs.csv";
    }

    @Override
    protected TestContext createTestContext() {
        return getTestContext();
    }

    public void testExplicitAggExtraColsRequiringJoin() throws SQLException {
        TestContext testContext = setupMultiColDimCube(
            "    <AggName name=\"agg_g_ms_pcat_sales_fact_1997\">\n"
            + "        <AggFactCount column=\"FACT_COUNT\"/>\n"
            + "        <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"UNIT_SALES\" />\n"
            + "        <AggLevel name=\"[Gender].[Gender]\" column=\"gender\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Year]\" column=\"the_year\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Quarter]\" column=\"quarter\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Month]\" column=\"month_of_year\" />\n"
            + "    </AggName>\n",
            "column=\"the_year\"",
            "column=\"quarter\"",
            "column=\"month_of_year\" captionColumn=\"the_month\" ordinalColumn=\"month_of_year\"",
            "");

        String query =
            "select {[Measures].[Unit Sales]} on columns, "
            + "non empty CrossJoin({[TimeExtra].[Month].members},{[Gender].[M]}) on rows "
            + "from [ExtraCol] ";
        assertQuerySql(
            testContext,
            query,
            mysqlPattern(
                "select\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`the_year` as `c0`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`quarter` as `c1`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`month_of_year` as `c2`,\n"
                + "    `time_by_day`.`the_month` as `c3`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender` as `c4`\n"
                + "from\n"
                + "    `agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `time_by_day`.`month_of_year` = `agg_g_ms_pcat_sales_fact_1997`.`month_of_year`\n"
                + "and\n"
                + "    (`agg_g_ms_pcat_sales_fact_1997`.`gender` = 'M')\n"
                + "group by\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`the_year`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`quarter`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`month_of_year`,\n"
                + "    `time_by_day`.`the_month`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender`\n"
                + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                    ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                    + "    ISNULL(`c1`) ASC, `c1` ASC,\n"
                    + "    ISNULL(`c2`) ASC, `c2` ASC,\n"
                    + "    ISNULL(`c4`) ASC, `c4` ASC"
                    : "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`the_year`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`the_year` ASC,\n"
                    + "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`quarter`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`quarter` ASC,\n"
                    + "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`month_of_year`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`month_of_year` ASC,\n"
                    + "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`gender`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`gender` ASC")));
        assertQuerySql(
            testContext,
            query,
            mysqlPattern(
                "select\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`the_year` as `c0`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`quarter` as `c1`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`month_of_year` as `c2`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender` as `c3`,\n"
                + "    sum(`agg_g_ms_pcat_sales_fact_1997`.`unit_sales`) as `m0`\n"
                + "from\n"
                + "    `agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997`\n"
                + "where\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`the_year` = 1997\n"
                + "and\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender` = 'M'\n"
                + "group by\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`the_year`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`quarter`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`month_of_year`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender`"));
    }

    public void testExplicitForeignKey() {
        TestContext testContext = setupMultiColDimCube(
            "    <AggName name=\"agg_c_14_sales_fact_1997\">\n"
            + "        <AggFactCount column=\"FACT_COUNT\"/>\n"
            + "        <AggForeignKey factColumn=\"store_id\" aggColumn=\"store_id\" />"
            + "        <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\" />\n"
            + "        <AggMeasure name=\"[Measures].[Store Cost]\" column=\"store_cost\" />\n"
            + "        <AggLevel name=\"[Gender].[Gender]\" column=\"gender\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Year]\" column=\"the_year\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Quarter]\" column=\"quarter\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Month]\" column=\"month_of_year\" />\n"
            + "    </AggName>\n",
            "column=\"the_year\"",
            "column=\"quarter\"",
            "column=\"month_of_year\" captionColumn=\"the_month\" ordinalColumn=\"month_of_year\"",
            "");


        String query =
            "select {[Measures].[Unit Sales]} on columns, "
            + "non empty CrossJoin({[TimeExtra].[Month].members},{[Store].[Store Name].members}) on rows "
            + "from [ExtraCol] ";
        // Run the query twice, verifying both the SqlTupleReader and
        // Segment load queries.
        assertQuerySql(
            testContext,
            query,
            mysqlPattern(
                "select\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` as `c0`,\n"
                + "    `agg_c_14_sales_fact_1997`.`quarter` as `c1`,\n"
                + "    `agg_c_14_sales_fact_1997`.`month_of_year` as `c2`,\n"
                + "    `time_by_day`.`the_month` as `c3`,\n"
                + "    `store`.`store_country` as `c4`,\n"
                + "    `store`.`store_state` as `c5`,\n"
                + "    `store`.`store_city` as `c6`,\n"
                + "    `store`.`store_name` as `c7`,\n"
                + "    `store`.`store_street_address` as `c8`\n"
                + "from\n"
                + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                + "    `time_by_day` as `time_by_day`,\n"
                + "    `store` as `store`\n"
                + "where\n"
                + "    `time_by_day`.`month_of_year` = `agg_c_14_sales_fact_1997`.`month_of_year`\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "group by\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year`,\n"
                + "    `agg_c_14_sales_fact_1997`.`quarter`,\n"
                + "    `agg_c_14_sales_fact_1997`.`month_of_year`,\n"
                + "    `time_by_day`.`the_month`,\n"
                + "    `store`.`store_country`,\n"
                + "    `store`.`store_state`,\n"
                + "    `store`.`store_city`,\n"
                + "    `store`.`store_name`,\n"
                + "    `store`.`store_street_address`\n"
                + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                    ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                    + "    ISNULL(`c1`) ASC, `c1` ASC,\n"
                    + "    ISNULL(`c2`) ASC, `c2` ASC,\n"
                    + "    ISNULL(`c4`) ASC, `c4` ASC,\n"
                    + "    ISNULL(`c5`) ASC, `c5` ASC,\n"
                    + "    ISNULL(`c6`) ASC, `c6` ASC,\n"
                    + "    ISNULL(`c7`) ASC, `c7` ASC"
                    : "    ISNULL(`agg_c_14_sales_fact_1997`.`the_year`) ASC, `agg_c_14_sales_fact_1997`.`the_year` ASC,\n"
                    + "    ISNULL(`agg_c_14_sales_fact_1997`.`quarter`) ASC, `agg_c_14_sales_fact_1997`.`quarter` ASC,\n"
                    + "    ISNULL(`agg_c_14_sales_fact_1997`.`month_of_year`) ASC, `agg_c_14_sales_fact_1997`.`month_of_year` ASC,\n"
                    + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
                    + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
                    + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC,\n"
                    + "    ISNULL(`store`.`store_name`) ASC, `store`.`store_name` ASC")));

        assertQuerySql(
            testContext,
            query,
            mysqlPattern(
                "select\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` as `c0`,\n"
                + "    `agg_c_14_sales_fact_1997`.`quarter` as `c1`,\n"
                + "    `agg_c_14_sales_fact_1997`.`month_of_year` as `c2`,\n"
                + "    `store`.`store_name` as `c3`,\n"
                + "    sum(`agg_c_14_sales_fact_1997`.`unit_sales`) as `m0`\n"
                + "from\n"
                + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                + "    `store` as `store`\n"
                + "where\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "group by\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year`,\n"
                + "    `agg_c_14_sales_fact_1997`.`quarter`,\n"
                + "    `agg_c_14_sales_fact_1997`.`month_of_year`,\n"
                + "    `store`.`store_name`"));
    }


    public void testExplicitAggOrdinalOnAggTable() throws SQLException {
        TestContext testContext = setupMultiColDimCube(
            "    <AggName name=\"exp_agg_test\">\n"
            + "        <AggFactCount column=\"FACT_COUNT\"/>\n"
            + "        <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"test_unit_sales\" />\n"
            + "        <AggLevel name=\"[Gender].[Gender]\" column=\"gender\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Year]\" column=\"testyear\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Quarter]\" column=\"testqtr\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Month]\" column=\"testmonthname\"  ordinalColumn=\"testmonthord\"/>\n"
            + "    </AggName>\n",
            "column=\"the_year\"",
            "column=\"quarter\"",
            "column=\"the_month\"  ordinalColumn=\"month_of_year\"",
            "");

        String query =
            "select {[Measures].[Unit Sales]} on columns, "
            + "non empty CrossJoin({[TimeExtra].[Month].members},{[Gender].[M]}) on rows "
            + "from [ExtraCol] ";

        assertQuerySql(
            testContext,
            query,
            mysqlPattern(
                "select\n"
                + "    `exp_agg_test`.`testyear` as `c0`,\n"
                + "    `exp_agg_test`.`testqtr` as `c1`,\n"
                + "    `exp_agg_test`.`testmonthname` as `c2`,\n"
                + "    `exp_agg_test`.`testmonthord` as `c3`,\n"
                + "    `exp_agg_test`.`gender` as `c4`\n"
                + "from\n"
                + "    `exp_agg_test` as `exp_agg_test`\n"
                + "where\n"
                + "    (`exp_agg_test`.`gender` = 'M')\n"
                + "group by\n"
                + "    `exp_agg_test`.`testyear`,\n"
                + "    `exp_agg_test`.`testqtr`,\n"
                + "    `exp_agg_test`.`testmonthname`,\n"
                + "    `exp_agg_test`.`testmonthord`,\n"
                + "    `exp_agg_test`.`gender`\n"
                + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                    ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                    + "    ISNULL(`c1`) ASC, `c1` ASC,\n"
                    + "    ISNULL(`c3`) ASC, `c3` ASC,\n"
                    + "    ISNULL(`c4`) ASC, `c4` ASC"
                    : "    ISNULL(`exp_agg_test`.`testyear`) ASC, `exp_agg_test`.`testyear` ASC,\n"
                    + "    ISNULL(`exp_agg_test`.`testqtr`) ASC, `exp_agg_test`.`testqtr` ASC,\n"
                    + "    ISNULL(`exp_agg_test`.`testmonthord`) ASC, `exp_agg_test`.`testmonthord` ASC,\n"
                    + "    ISNULL(`exp_agg_test`.`gender`) ASC, `exp_agg_test`.`gender` ASC")));
    }

    public void testExplicitAggCaptionOnAggTable() throws SQLException {
        TestContext testContext = setupMultiColDimCube(
            "    <AggName name=\"exp_agg_test\">\n"
            + "        <AggFactCount column=\"FACT_COUNT\"/>\n"
            + "        <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"test_unit_sales\" />\n"
            + "        <AggLevel name=\"[Gender].[Gender]\" column=\"gender\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Year]\" column=\"testyear\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Quarter]\" column=\"testqtr\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Month]\" column=\"testmonthname\"  captionColumn=\"testmonthcap\"/>\n"
            + "    </AggName>\n",
            "column=\"the_year\"",
            "column=\"quarter\"",
            "column=\"the_month\"  captionColumn=\"month_of_year\"",
            "");

        String query =
            "select {[Measures].[Unit Sales]} on columns, "
            + "non empty CrossJoin({[TimeExtra].[Month].members},{[Gender].[M]}) on rows "
            + "from [ExtraCol] ";

        assertQuerySql(
            testContext,
            query,
            mysqlPattern(
                "select\n"
                + "    `exp_agg_test`.`testyear` as `c0`,\n"
                + "    `exp_agg_test`.`testqtr` as `c1`,\n"
                + "    `exp_agg_test`.`testmonthname` as `c2`,\n"
                + "    `exp_agg_test`.`testmonthcap` as `c3`,\n"
                + "    `exp_agg_test`.`gender` as `c4`\n"
                + "from\n"
                + "    `exp_agg_test` as `exp_agg_test`\n"
                + "where\n"
                + "    (`exp_agg_test`.`gender` = 'M')\n"
                + "group by\n"
                + "    `exp_agg_test`.`testyear`,\n"
                + "    `exp_agg_test`.`testqtr`,\n"
                + "    `exp_agg_test`.`testmonthname`,\n"
                + "    `exp_agg_test`.`testmonthcap`,\n"
                + "    `exp_agg_test`.`gender`\n"
                + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                    ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                    + "    ISNULL(`c1`) ASC, `c1` ASC,\n"
                    + "    ISNULL(`c2`) ASC, `c2` ASC,\n"
                    + "    ISNULL(`c4`) ASC, `c4` ASC"
                    : "    ISNULL(`exp_agg_test`.`testyear`) ASC, `exp_agg_test`.`testyear` ASC,\n"
                    + "    ISNULL(`exp_agg_test`.`testqtr`) ASC, `exp_agg_test`.`testqtr` ASC,\n"
                    + "    ISNULL(`exp_agg_test`.`testmonthname`) ASC, `exp_agg_test`.`testmonthname` ASC,\n"
                    + "    ISNULL(`exp_agg_test`.`gender`) ASC, `exp_agg_test`.`gender` ASC")));
    }

    public void testExplicitAggNameColumnOnAggTable() throws SQLException {
        TestContext testContext = setupMultiColDimCube(
            "    <AggName name=\"exp_agg_test\">\n"
            + "        <AggFactCount column=\"FACT_COUNT\"/>\n"
            + "        <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"test_unit_sales\" />\n"
            + "        <AggLevel name=\"[Gender].[Gender]\" column=\"gender\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Year]\" column=\"testyear\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Quarter]\" column=\"testqtr\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Month]\" column=\"testmonthname\"  nameColumn=\"testmonthcap\">\n"
            + "             <AggLevelProperty name='aProperty' column='testmonprop1'  />"
            + "          </AggLevel>"
            + "    </AggName>\n",
            "column=\"the_year\"",
            "column=\"quarter\"",
            "column=\"the_month\"  nameColumn=\"month_of_year\"",
            "<Property name='aProperty' column='fiscal_period' /> ");

        String query =
            "select {[Measures].[Unit Sales]} on columns, "
            + "non empty CrossJoin({[TimeExtra].[Month].members},{[Gender].[M]}) on rows "
            + "from [ExtraCol] ";

        assertQuerySql(
            testContext,
            query,
            mysqlPattern(
                "select\n"
                + "    `exp_agg_test`.`testyear` as `c0`,\n"
                + "    `exp_agg_test`.`testqtr` as `c1`,\n"
                + "    `exp_agg_test`.`testmonthname` as `c2`,\n"
                + "    `exp_agg_test`.`testmonthcap` as `c3`,\n"
                + "    `exp_agg_test`.`testmonprop1` as `c4`,\n"
                + "    `exp_agg_test`.`gender` as `c5`\n"
                + "from\n"
                + "    `exp_agg_test` as `exp_agg_test`\n"
                + "where\n"
                + "    (`exp_agg_test`.`gender` = 'M')\n"
                + "group by\n"
                + "    `exp_agg_test`.`testyear`,\n"
                + "    `exp_agg_test`.`testqtr`,\n"
                + "    `exp_agg_test`.`testmonthname`,\n"
                + "    `exp_agg_test`.`testmonthcap`,\n"
                + "    `exp_agg_test`.`testmonprop1`,\n"
                + "    `exp_agg_test`.`gender`\n"
                + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                    ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                    + "    ISNULL(`c1`) ASC, `c1` ASC,\n"
                    + "    ISNULL(`c2`) ASC, `c2` ASC,\n"
                    + "    ISNULL(`c5`) ASC, `c5` ASC"
                    : "    ISNULL(`exp_agg_test`.`testyear`) ASC, `exp_agg_test`.`testyear` ASC,\n"
                    + "    ISNULL(`exp_agg_test`.`testqtr`) ASC, `exp_agg_test`.`testqtr` ASC,\n"
                    + "    ISNULL(`exp_agg_test`.`testmonthname`) ASC, `exp_agg_test`.`testmonthname` ASC,\n"
                    + "    ISNULL(`exp_agg_test`.`gender`) ASC, `exp_agg_test`.`gender` ASC")));
    }


    public void testExplicitAggPropertiesOnAggTable() throws SQLException {
        TestContext testContext = setupMultiColDimCube(
            "    <AggName name=\"exp_agg_test_distinct_count\">\n"
            + "        <AggFactCount column=\"FACT_COUNT\"/>\n"
            + "        <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_s\" />\n"
            + "        <AggMeasure name=\"[Measures].[Customer Count]\" column=\"cust_cnt\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Year]\" column=\"testyear\" />\n"
            + "        <AggLevel name=\"[Gender].[Gender]\" column=\"gender\" />\n"
            + "        <AggLevel name=\"[Store].[Store Country]\" column=\"store_country\" />\n"
            + "        <AggLevel name=\"[Store].[Store State]\" column=\"store_st\" />\n"
            + "        <AggLevel name=\"[Store].[Store City]\" column=\"store_cty\" />\n"
            + "        <AggLevel name=\"[Store].[Store Name]\" column=\"store_name\" >\n"
            + "           <AggLevelProperty name='Street address' column='store_add' />"
            + "        </AggLevel>\n"
            + "    </AggName>\n",
            "column=\"the_year\"",
            "column=\"quarter\"",
            "column=\"month_of_year\" captionColumn=\"the_month\" ordinalColumn=\"month_of_year\"",
            "");

        String query =
            "with member measures.propVal as 'Store.CurrentMember.Properties(\"Street Address\")'"
            + "select { measures.[propVal], measures.[Customer Count], [Measures].[Unit Sales]} on columns, "
            + "non empty CrossJoin({[Gender].Gender.members},{[Store].[USA].[WA].[Spokane].[Store 16]}) on rows "
            + "from [ExtraCol]";
        assertQuerySql(
            testContext,
            query,
            mysqlPattern(
                "select\n"
                + "    `exp_agg_test_distinct_count`.`gender` as `c0`,\n"
                + "    `exp_agg_test_distinct_count`.`store_country` as `c1`,\n"
                + "    `exp_agg_test_distinct_count`.`store_st` as `c2`,\n"
                + "    `exp_agg_test_distinct_count`.`store_cty` as `c3`,\n"
                + "    `exp_agg_test_distinct_count`.`store_name` as `c4`,\n"
                + "    `exp_agg_test_distinct_count`.`store_add` as `c5`\n"
                + "from\n"
                + "    `exp_agg_test_distinct_count` as `exp_agg_test_distinct_count`\n"
                + "where\n"
                + "    (`exp_agg_test_distinct_count`.`store_name` = 'Store 16')\n"
                + "group by\n"
                + "    `exp_agg_test_distinct_count`.`gender`,\n"
                + "    `exp_agg_test_distinct_count`.`store_country`,\n"
                + "    `exp_agg_test_distinct_count`.`store_st`,\n"
                + "    `exp_agg_test_distinct_count`.`store_cty`,\n"
                + "    `exp_agg_test_distinct_count`.`store_name`,\n"
                + "    `exp_agg_test_distinct_count`.`store_add`\n"
                + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                    ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                    + "    ISNULL(`c1`) ASC, `c1` ASC,\n"
                    + "    ISNULL(`c2`) ASC, `c2` ASC,\n"
                    + "    ISNULL(`c3`) ASC, `c3` ASC,\n"
                    + "    ISNULL(`c4`) ASC, `c4` ASC"
                    : "    ISNULL(`exp_agg_test_distinct_count`.`gender`) ASC, `exp_agg_test_distinct_count`.`gender` ASC,\n"
                    + "    ISNULL(`exp_agg_test_distinct_count`.`store_country`) ASC, `exp_agg_test_distinct_count`.`store_country` ASC,\n"
                    + "    ISNULL(`exp_agg_test_distinct_count`.`store_st`) ASC, `exp_agg_test_distinct_count`.`store_st` ASC,\n"
                    + "    ISNULL(`exp_agg_test_distinct_count`.`store_cty`) ASC, `exp_agg_test_distinct_count`.`store_cty` ASC,\n"
                    + "    ISNULL(`exp_agg_test_distinct_count`.`store_name`) ASC, `exp_agg_test_distinct_count`.`store_name` ASC")));

        testContext.assertQueryReturns(
            "Store Address Property should be '5922 La Salle Ct'",
            query,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[propVal]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F], [Store].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Gender].[M], [Store].[USA].[WA].[Spokane].[Store 16]}\n"
            + "Row #0: 5922 La Salle Ct\n"
            + "Row #0: 45\n"
            + "Row #0: 12,068\n"
            + "Row #1: 5922 La Salle Ct\n"
            + "Row #1: 39\n"
            + "Row #1: 11,523\n");
        // Should use agg table for distinct count measure
        assertQuerySql(
            testContext,
            query,
            mysqlPattern(
                "select\n"
                + "    `exp_agg_test_distinct_count`.`testyear` as `c0`,\n"
                + "    `exp_agg_test_distinct_count`.`gender` as `c1`,\n"
                + "    `exp_agg_test_distinct_count`.`store_name` as `c2`,\n"
                + "    `exp_agg_test_distinct_count`.`unit_s` as `m0`,\n"
                + "    `exp_agg_test_distinct_count`.`cust_cnt` as `m1`\n"
                + "from\n"
                + "    `exp_agg_test_distinct_count` as `exp_agg_test_distinct_count`\n"
                + "where\n"
                + "    `exp_agg_test_distinct_count`.`testyear` = 1997\n"
                + "and\n"
                + "    `exp_agg_test_distinct_count`.`store_name` = 'Store 16'"));
    }


    public void testCountDistinctAllowableRollup() throws SQLException {
        TestContext testContext = setupMultiColDimCube(
            "    <AggName name=\"exp_agg_test_distinct_count\">\n"
            + "        <AggFactCount column=\"FACT_COUNT\"/>\n"
            + "        <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_s\" />\n"
            + "        <AggMeasure name=\"[Measures].[Customer Count]\" column=\"cust_cnt\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Year]\" column=\"testyear\" />\n"
            + "        <AggLevel name=\"[Gender].[Gender]\" column=\"gender\" />\n"
            + "        <AggLevel name=\"[Store].[Store Country]\" column=\"store_country\" />\n"
            + "        <AggLevel name=\"[Store].[Store State]\" column=\"store_st\" />\n"
            + "        <AggLevel name=\"[Store].[Store City]\" column=\"store_cty\" />\n"
            + "        <AggLevel name=\"[Store].[Store Name]\" column=\"store_name\" >\n"
            + "           <AggLevelProperty name='Street address' column='store_add' />"
            + "        </AggLevel>\n"
            + "    </AggName>\n",
            "column=\"the_year\"",
            "column=\"quarter\"",
            "column=\"month_of_year\" captionColumn=\"the_month\" ordinalColumn=\"month_of_year\"",
            "", "Customer Count");

        // Query brings in Year and Store Name, omitting Gender.
        // It's okay to roll up the agg table in this case
        // since Customer Count is dependent on Gender.
        String query =
            "select { measures.[Customer Count], [Measures].[Unit Sales]} on columns, "
            + "non empty CrossJoin({[TimeExtra].Year.members},{[Store].[USA].[WA].[Spokane].[Store 16]}) on rows "
            + "from [ExtraCol]";

        assertQuerySql(
            testContext,
            query,
            mysqlPattern(
                "select\n"
                + "    `exp_agg_test_distinct_count`.`testyear` as `c0`,\n"
                + "    `exp_agg_test_distinct_count`.`store_country` as `c1`,\n"
                + "    `exp_agg_test_distinct_count`.`store_st` as `c2`,\n"
                + "    `exp_agg_test_distinct_count`.`store_cty` as `c3`,\n"
                + "    `exp_agg_test_distinct_count`.`store_name` as `c4`,\n"
                + "    `exp_agg_test_distinct_count`.`store_add` as `c5`\n"
                + "from\n"
                + "    `exp_agg_test_distinct_count` as `exp_agg_test_distinct_count`\n"
                + "where\n"
                + "    (`exp_agg_test_distinct_count`.`store_name` = 'Store 16')\n"
                + "group by\n"
                + "    `exp_agg_test_distinct_count`.`testyear`,\n"
                + "    `exp_agg_test_distinct_count`.`store_country`,\n"
                + "    `exp_agg_test_distinct_count`.`store_st`,\n"
                + "    `exp_agg_test_distinct_count`.`store_cty`,\n"
                + "    `exp_agg_test_distinct_count`.`store_name`,\n"
                + "    `exp_agg_test_distinct_count`.`store_add`\n"
                + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                    ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                    + "    ISNULL(`c1`) ASC, `c1` ASC,\n"
                    + "    ISNULL(`c2`) ASC, `c2` ASC,\n"
                    + "    ISNULL(`c3`) ASC, `c3` ASC,\n"
                    + "    ISNULL(`c4`) ASC, `c4` ASC"
                    : "    ISNULL(`exp_agg_test_distinct_count`.`testyear`) ASC, `exp_agg_test_distinct_count`.`testyear` ASC,\n"
                    + "    ISNULL(`exp_agg_test_distinct_count`.`store_country`) ASC, `exp_agg_test_distinct_count`.`store_country` ASC,\n"
                    + "    ISNULL(`exp_agg_test_distinct_count`.`store_st`) ASC, `exp_agg_test_distinct_count`.`store_st` ASC,\n"
                    + "    ISNULL(`exp_agg_test_distinct_count`.`store_cty`) ASC, `exp_agg_test_distinct_count`.`store_cty` ASC,\n"
                    + "    ISNULL(`exp_agg_test_distinct_count`.`store_name`) ASC, `exp_agg_test_distinct_count`.`store_name` ASC")));

        assertQuerySql(
            testContext,
            query,
            mysqlPattern(
                "select\n"
                + "    `exp_agg_test_distinct_count`.`testyear` as `c0`,\n"
                + "    `exp_agg_test_distinct_count`.`store_name` as `c1`,\n"
                + "    sum(`exp_agg_test_distinct_count`.`unit_s`) as `m0`,\n"
                + "    sum(`exp_agg_test_distinct_count`.`cust_cnt`) as `m1`\n"
                + "from\n"
                + "    `exp_agg_test_distinct_count` as `exp_agg_test_distinct_count`\n"
                + "where\n"
                + "    `exp_agg_test_distinct_count`.`testyear` = 1997\n"
                + "and\n"
                + "    `exp_agg_test_distinct_count`.`store_name` = 'Store 16'\n"
                + "group by\n"
                + "    `exp_agg_test_distinct_count`.`testyear`,\n"
                + "    `exp_agg_test_distinct_count`.`store_name`"));
    }

    public void testCountDisallowedRollup() throws SQLException {
        TestContext testContext = setupMultiColDimCube(
            "    <AggName name=\"exp_agg_test_distinct_count\">\n"
            + "        <AggFactCount column=\"FACT_COUNT\"/>\n"
            + "        <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_s\" />\n"
            + "        <AggMeasure name=\"[Measures].[Customer Count]\" column=\"cust_cnt\" />\n"
            + "        <AggLevel name=\"[TimeExtra].[Year]\" column=\"testyear\" />\n"
            + "        <AggLevel name=\"[Gender].[Gender]\" column=\"gender\" />\n"
            + "        <AggLevel name=\"[Store].[Store Country]\" column=\"store_country\" />\n"
            + "        <AggLevel name=\"[Store].[Store State]\" column=\"store_st\" />\n"
            + "        <AggLevel name=\"[Store].[Store City]\" column=\"store_cty\" />\n"
            + "        <AggLevel name=\"[Store].[Store Name]\" column=\"store_name\" >\n"
            + "           <AggLevelProperty name='Street address' column='store_add' />"
            + "        </AggLevel>\n"
            + "    </AggName>\n",
            "column=\"the_year\"",
            "column=\"quarter\"",
            "column=\"month_of_year\" captionColumn=\"the_month\" ordinalColumn=\"month_of_year\"",
            "", "Customer Count");

        String query =
            "select { measures.[Customer Count]} on columns, "
            + "non empty CrossJoin({[TimeExtra].Year.members},{[Gender].[F]}) on rows "
            + "from [ExtraCol]";


        // Seg load query should not use agg table, since the independent
        // attributes for store are on the aggStar bitkey and not part of the
        // request and rollup is not safe
        assertQuerySql(
            testContext,
            query,
            mysqlPattern(
                "select\n"
                + "    `time_by_day`.`the_year` as `c0`,\n"
                + "    `customer`.`gender` as `c1`,\n"
                + "    count(distinct `sales_fact_1997`.`customer_id`) as `m0`\n"
                + "from\n"
                + "    `time_by_day` as `time_by_day`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `customer` as `customer`\n"
                + "where\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
                + "and\n"
                + "    `customer`.`gender` = 'F'\n"
                + "group by\n"
                + "    `time_by_day`.`the_year`,\n"
                + "    `customer`.`gender`"));
    }

    static TestContext setupMultiColDimCube(
        String aggName, String yearCols, String qtrCols, String monthCols,
        String monthProp)
    {
        return setupMultiColDimCube(
            aggName, yearCols, qtrCols, monthCols, monthProp, "Unit Sales");
    }

    static TestContext setupMultiColDimCube(
        String aggName, String yearCols, String qtrCols, String monthCols,
        String monthProp, String defaultMeasure)
    {
        String cube =
            "<?xml version=\"1.0\"?>\n"
            + "<Schema name=\"FoodMart\">\n"
            + "  <Dimension name=\"Store\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\">\n"
            + "        <Property name=\"Street address\" column=\"store_street_address\" type=\"String\"/>\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Product\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
            + "      <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
            + "        <Table name=\"product\"/>\n"
            + "        <Table name=\"product_class\"/>\n"
            + "      </Join>\n"
            + "      <Level name=\"Product Family\" table=\"product_class\" column=\"product_family\"\n"
            + "          uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Product Department\" table=\"product_class\" column=\"product_department\"\n"
            + "          uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Product Category\" table=\"product_class\" column=\"product_category\"\n"
            + "          uniqueMembers=\"false\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "<Cube name=\"ExtraCol\" defaultMeasure='#DEFMEASURE#'>\n"
            + "  <Table name=\"sales_fact_1997\">\n"
            + "           #AGGNAME# "
            + "  </Table>"
            + "  <Dimension name=\"TimeExtra\" foreignKey=\"time_id\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year\" #YEARCOLS#  type=\"Numeric\" uniqueMembers=\"true\""
            + "          levelType=\"TimeYears\">\n"
            + "      </Level>\n"
            + "      <Level name=\"Quarter\" #QTRCOLS#  uniqueMembers=\"false\""
            + "          levelType=\"TimeQuarters\">\n"
            + "      </Level>\n"
            + "      <Level name=\"Month\" #MONTHCOLS# uniqueMembers=\"false\" type=\"Numeric\""
            + "          levelType=\"TimeMonths\">\n"
            + "           #MONTHPROP# "
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Gender\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
            + "    <Table name=\"customer\"/>\n"
            + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>  "
            + "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>"
            + "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\" visible=\"false\"/>\n"
            + "<Measure name=\"Avg Unit Sales\" column=\"unit_sales\" aggregator=\"avg\"\n"
            + "      formatString=\"Standard\" visible=\"false\"/>\n"
            + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###.00\"/>\n"
            + "<Measure name=\"Customer Count\" column=\"customer_id\" aggregator=\"distinct-count\" formatString=\"#,###\"/>"
            + "</Cube>\n"
            + "</Schema>";
        cube = cube
            .replace("#AGGNAME#", aggName)
            .replace("#YEARCOLS#", yearCols)
            .replace("#QTRCOLS#", qtrCols)
            .replace("#MONTHCOLS#", monthCols)
            .replace("#MONTHPROP#", monthProp)
            .replace("#DEFMEASURE#", defaultMeasure);
        return TestContext.instance().withSchema(cube);
    }

}

// End ExplicitRecognizerTest.java
