/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2014 Pentaho Corporation.  All rights reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.spi.Dialect;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;

/**
 * Test case for pushing MDX filter conditions down to SQL.
 */
public class NativeFilterMatchingTest extends BatchTestCase {
    public void testPositiveMatching() throws Exception {
        if (!MondrianProperties.instance().EnableNativeFilter.get()) {
            // No point testing these if the native filters
            // are turned off.
            return;
        }
        final String sqlOracle =
            "select \"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", \"fname\" || ' ' || \"lname\" as \"c4\", \"fname\" || ' ' || \"lname\" as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" \"customer\" group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", \"fname\" || ' ' || \"lname\", \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having REGEXP_LIKE(\"fname\" || ' ' || \"lname\", '.*jeanne.*', 'i') order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, \"fname\" || ' ' || \"lname\" ASC NULLS LAST";
        final String sqlPgsql =
            "select \"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", fullname as \"c4\", fullname as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" as \"customer\" group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", fullname, \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having cast(fullname as text) ~ '(?i).*jeanne.*' order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, fullname ASC NULLS LAST";
        final String sqlMysql =
            "select `customer`.`country` as `c0`, `customer`.`state_province` as `c1`, `customer`.`city` as `c2`, `customer`.`customer_id` as `c3`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`, `customer`.`gender` as `c6`, `customer`.`marital_status` as `c7`, `customer`.`education` as `c8`, `customer`.`yearly_income` as `c9` from `customer` as `customer` group by `customer`.`country`, `customer`.`state_province`, `customer`.`city`, `customer`.`customer_id`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`), `customer`.`gender`, `customer`.`marital_status`, `customer`.`education`, `customer`.`yearly_income` having UPPER(c5) REGEXP '.*JEANNE.*' order by ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC, ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC, ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC, ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE,
                sqlOracle,
                sqlOracle.length()),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysql,
                sqlMysql.length()),
            new SqlPattern(
                Dialect.DatabaseProduct.POSTGRESQL,
                sqlPgsql,
                sqlPgsql.length())
        };
        final String queryResults =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customers].[USA].[WA].[Issaquah].[Jeanne Derry], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Customers].[USA].[CA].[Los Angeles].[Jeannette Eldridge], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Customers].[USA].[CA].[Burbank].[Jeanne Bohrnstedt], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Customers].[USA].[OR].[Portland].[Jeanne Zysko], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Customers].[USA].[WA].[Everett].[Jeanne McDill], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Customers].[USA].[CA].[West Covina].[Jeanne Whitaker], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Customers].[USA].[WA].[Everett].[Jeanne Turner], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Customers].[USA].[WA].[Puyallup].[Jeanne Wentz], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Customers].[USA].[OR].[Albany].[Jeannette Bura], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Customers].[USA].[WA].[Lynnwood].[Jeanne Ibarra], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Row #0: 50\n"
            + "Row #0: 21\n"
            + "Row #0: 31\n"
            + "Row #0: 42\n"
            + "Row #0: 110\n"
            + "Row #0: 59\n"
            + "Row #0: 42\n"
            + "Row #0: 157\n"
            + "Row #0: 146\n"
            + "Row #0: 78\n";
        final String query =
            "With\n"
            + "Set [*NATIVE_CJ_SET] as 'Filter([*BASE_MEMBERS_Customers], Not IsEmpty ([Measures].[Unit Sales]))'\n"
            + "Set [*SORTED_COL_AXIS] as 'Order([*CJ_COL_AXIS],[Customers].CurrentMember.OrderKey,BASC,Ancestor([Customers].CurrentMember,[Customers].[City]).OrderKey,BASC)'\n"
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[Name].Members,[Customers].CurrentMember.Caption Matches (\"(?i).*\\Qjeanne\\E.*\"))'\n"
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "Set [*CJ_COL_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember)})'\n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=400\n"
            + "Select\n"
            + "CrossJoin([*SORTED_COL_AXIS],[*BASE_MEMBERS_Measures]) on columns\n"
            + "From [Sales]";
        assertQuerySqlOrNot(
            getTestContext(),
            query,
            patterns,
            false,
            true,
            true);
        assertQueryReturns(
            query,
            queryResults);
        verifySameNativeAndNot(query, null, getTestContext());
    }

    public void testNegativeMatching() throws Exception {
        if (!MondrianProperties.instance().EnableNativeFilter.get()) {
             // No point testing these if the native filters
             // are turned off.
            return;
        }
        final String sqlOracle =
            "select \"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", \"fname\" || ' ' || \"lname\" as \"c4\", \"fname\" || ' ' || \"lname\" as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" \"customer\" group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", \"fname\" || ' ' || \"lname\", \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having NOT(REGEXP_LIKE(\"fname\" || ' ' || \"lname\", '.*jeanne.*', 'i'))  order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, \"fname\" || ' ' || \"lname\" ASC NULLS LAST";
        final String sqlPgsql =
            "select \"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", fullname as \"c4\", fullname as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" as \"customer\" group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", fullname, \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having NOT(cast(fullname as text) ~ '(?i).*jeanne.*')  order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, fullname ASC NULLS LAST";
        final String sqlMysql =
            "select `customer`.`country` as `c0`, `customer`.`state_province` as `c1`, `customer`.`city` as `c2`, `customer`.`customer_id` as `c3`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`, `customer`.`gender` as `c6`, `customer`.`marital_status` as `c7`, `customer`.`education` as `c8`, `customer`.`yearly_income` as `c9` from `customer` as `customer` group by `customer`.`country`, `customer`.`state_province`, `customer`.`city`, `customer`.`customer_id`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`), `customer`.`gender`, `customer`.`marital_status`, `customer`.`education`, `customer`.`yearly_income` having NOT(UPPER(c5) REGEXP '.*JEANNE.*')  order by ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC, ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC, ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC, ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE,
                sqlOracle,
                sqlOracle.length()),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysql,
                sqlMysql.length()),
            new SqlPattern(
                Dialect.DatabaseProduct.POSTGRESQL,
                sqlPgsql,
                sqlPgsql.length())
        };

        final String query =
            "With\n"
            + "Set [*NATIVE_CJ_SET] as 'Filter([*BASE_MEMBERS_Customers], Not IsEmpty ([Measures].[Unit Sales]))'\n"
            + "Set [*SORTED_COL_AXIS] as 'Order([*CJ_COL_AXIS],[Customers].CurrentMember.OrderKey,BASC,Ancestor([Customers].CurrentMember,[Customers].[City]).OrderKey,BASC)'\n"
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[Name].Members,[Customers].CurrentMember.Caption Not Matches (\"(?i).*\\Qjeanne\\E.*\"))'\n"
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "Set [*CJ_COL_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember)})'\n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=400\n"
            + "Select\n"
            + "CrossJoin([*SORTED_COL_AXIS],[*BASE_MEMBERS_Measures]) on columns\n"
            + "From [Sales]";

        assertQuerySqlOrNot(
            getTestContext(),
            query,
            patterns,
            false,
            true,
            true);

        final Result result = executeQuery(query);
        final String resultString = TestContext.toString(result);
        assertFalse(resultString.contains("Jeanne"));
        verifySameNativeAndNot(query, null, getTestContext());
    }

    /**
     * <p>System test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-983">MONDRIAN-983,
     * "Regression: Unable to execute MDX statement with native MATCHES"</a>.
     *
     * @see mondrian.test.DialectTest#testRegularExpressionSqlInjection()
     */
    public void testMatchBugMondrian983() {
        assertQueryReturns(
            "With\n"
            + "Set [*NATIVE_CJ_SET] as 'Filter([*BASE_MEMBERS_Product], Not IsEmpty ([Measures].[Unit Sales]))' \n"
            + "Set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS],[Product].CurrentMember.OrderKey,BASC,Ancestor([Product].CurrentMember,[Product].[Product Department]).OrderKey,BASC)' \n"
            + "Set [*NATIVE_MEMBERS_Product] as 'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})' \n"
            + "Set [*BASE_MEMBERS_Product] as 'Filter([Product].[Product Category].Members,[Product].CurrentMember.Caption Matches (\"(?i).*\\Qa\"\"\\); window.alert(\"\"woot'');\\E.*\"))' \n"
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}' \n"
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Product].currentMember)})' \n"
            + "Set [*CJ_COL_AXIS] as '[*NATIVE_CJ_SET]' \n"
            + "Member [Product].[*TOTAL_MEMBER_SEL~SUM] as 'Sum([*NATIVE_MEMBERS_Product])', SOLVE_ORDER=-100 \n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=400 \n"
            + "Select\n"
            + "[*BASE_MEMBERS_Measures] on columns,\n"
            + "Union({[Product].[*TOTAL_MEMBER_SEL~SUM]},[*SORTED_ROW_AXIS]) on rows\n"
            + "From [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Product].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "Row #0: \n");
    }

    public void testNativeFilterAgainstAggTableWithNotAllMeasures() {
        // http://jira.pentaho.com/browse/MONDRIAN-1703
        // If a filter condition contains one or more measures that are
        // not present in the aggregate table, the SQL should omit the
        // having clause altogether.

        if (!MondrianProperties.instance().UseAggregates.get()
            || !MondrianProperties.instance().EnableNativeFilter.get())
        {
            // test is not applicable
            return;
        }
        propSaver.set(
            propSaver.properties.GenerateFormattedSql,
            true);

        String sqlMysqlNoHaving =
            "select\n"
            + "    `agg_c_10_sales_fact_1997`.`the_year` as `c0`,\n"
            + "    `agg_c_10_sales_fact_1997`.`quarter` as `c1`\n"
            + "from\n"
            + "    `agg_c_10_sales_fact_1997` as `agg_c_10_sales_fact_1997`\n"
            + "where\n"
            + "    (`agg_c_10_sales_fact_1997`.`the_year` = 1997)\n"
            + "group by\n"
            + "    `agg_c_10_sales_fact_1997`.`the_year`,\n"
            + "    `agg_c_10_sales_fact_1997`.`quarter`\n"
            + "order by\n"
            + "    ISNULL(`agg_c_10_sales_fact_1997`.`the_year`) ASC, `agg_c_10_sales_fact_1997`.`the_year` ASC,\n"
            + "    ISNULL(`agg_c_10_sales_fact_1997`.`quarter`) ASC, `agg_c_10_sales_fact_1997`.`quarter` ASC";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlNoHaving,
                sqlMysqlNoHaving.length())
        };

        // This query should hit the agg_c_10_sales_fact_1997 agg table,
        // which has [unit sales] but not [store count], so should
        // not include the filter condition in the having.
        assertQuerySqlOrNot(
            getTestContext(),
            "select filter(Time.[1997].children,  "
            + "measures.[Sales Count] +  measures.[unit sales] > 0) on 0 "
            + "from [sales]",
            patterns,
            false,
            true,
            true);

        String mySqlWithHaving =
            "select\n"
            + "    `agg_c_10_sales_fact_1997`.`the_year` as `c0`,\n"
            + "    `agg_c_10_sales_fact_1997`.`quarter` as `c1`\n"
            + "from\n"
            + "    `agg_c_10_sales_fact_1997` as `agg_c_10_sales_fact_1997`\n"
            + "where\n"
            + "    (`agg_c_10_sales_fact_1997`.`the_year` = 1997)\n"
            + "group by\n"
            + "    `agg_c_10_sales_fact_1997`.`the_year`,\n"
            + "    `agg_c_10_sales_fact_1997`.`quarter`\n"
            + "having\n"
            + "    ((sum(`agg_c_10_sales_fact_1997`.`store_sales`) + sum(`agg_c_10_sales_fact_1997`.`unit_sales`)) > 0)\n"
            + "order by\n"
            + "    ISNULL(`agg_c_10_sales_fact_1997`.`the_year`) ASC, `agg_c_10_sales_fact_1997`.`the_year` ASC,\n"
            + "    ISNULL(`agg_c_10_sales_fact_1997`.`quarter`) ASC, `agg_c_10_sales_fact_1997`.`quarter` ASC";

        patterns[0] = new SqlPattern(
            Dialect.DatabaseProduct.MYSQL,
            mySqlWithHaving,
            mySqlWithHaving.length());

        // both measures are present on the agg table, so this one *should*
        // include having.
        assertQuerySqlOrNot(
            getTestContext(),
            "select filter(Time.[1997].children,  "
            + "measures.[Store Sales] +  measures.[unit sales] > 0) on 0 "
            + "from [sales]",
            patterns,
            false,
            true,
            true);
    }


    public void testNativeFilterSameAsNonNative() {
        // http://jira.pentaho.com/browse/MONDRIAN-1694
        // In some cases native filter would includes an unnecessary fact table
        // join which incorrectly eliminated some tuples from the set
        verifySameNativeAndNot(
            "select Filter([Store].[Store Name].Members, Store.CurrentMember.Name matches \"Store.*\") "
            + " on 0 from sales",
            "Filter w/ regex.", getTestContext());

        verifySameNativeAndNot(
            "select Filter([Store].[Store Name].Members, Measures.[Unit Sales] > 100 and Store.CurrentMember.Name matches \"Store.*\") "
            + " on 0 from sales",
            "Filter w/ regex and measure constraint.", getTestContext());

        verifySameNativeAndNot(
            "select Filter([Store].[Store Name].Members, measures.[Unit Sales] > 100) "
            + " on 0 from sales",
            "Filter w/ measure constraint.", getTestContext());

        verifySameNativeAndNot(
            "select non empty Filter([Store].[Store Name].Members, Store.CurrentMember.Name matches \"Store.*\") "
            + " on 0 from sales",
            "Filter w/ regex in non-empty context.", getTestContext());

        verifySameNativeAndNot(
            "with set [filterSet] as 'Filter([Store].[Store Name].Members, Store.CurrentMember.Name matches \"Store.*\")'"
            + " select [filterSet] on 0 from sales",
            "Filter w/ regex defined in named set.",
            getTestContext());
    }

    public void testCachedNativeFilter() {
        // http://jira.pentaho.com/browse/MONDRIAN-1694

        // verify that the RolapNativeSet cached values from NON EMPTY context
        // are not reused when not NON EMPTY.
        verifySameNativeAndNot(
            "select NON EMPTY Filter([Store].[Store Name].Members, Store.CurrentMember.Name matches \"Store.*\") "
            + " on 0 from sales",
            "Filter w/ regex.", getTestContext());
        verifySameNativeAndNot(
            "select Filter([Store].[Store Name].Members, Store.CurrentMember.Name matches \"Store.*\") "
            + " on 0 from sales",
            "Filter w/ regex.", getTestContext());
    }

    public void testMatchesWithAccessControl() {
        String dimension =
            "<Dimension name=\"Store2\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"store_id\"  >\n"
            + "    <Table name=\"store\"/>\n"
            + "    <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n";

        String cube =
            "<Cube name=\"TinySales\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Store2\" source=\"Store2\" foreignKey=\"store_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"/>\n"
            + "</Cube>";


        final String roleDefs =
            "<Role name=\"test\">\n"
            + "        <SchemaGrant access=\"none\">\n"
            + "            <CubeGrant cube=\"TinySales\" access=\"all\">\n"
            + "                <HierarchyGrant hierarchy=\"[Store2]\" access=\"custom\"\n"
            + "                                 rollupPolicy=\"PARTIAL\">\n"
            + "                    <MemberGrant member=\"[Store2].[USA].[CA]\" access=\"all\"/>\n"
            + "                    <MemberGrant member=\"[Store2].[USA].[OR]\" access=\"all\"/>\n"
            + "                    <MemberGrant member=\"[Store2].[Canada]\" access=\"all\"/>\n"
            + "                </HierarchyGrant>\n"
            + "            </CubeGrant>\n"
            + "        </SchemaGrant>\n"
            + "    </Role> ";

        final TestContext context = getTestContext().create(
            dimension,
            cube, null, null, null,
            roleDefs).withRole("test");
        verifySameNativeAndNot(
            "select Filter([Product].[Product Category].Members, [Product].CurrentMember.Name matches \"(?i).*Food.*\")"
            + " on 0 from tinysales",
            "Filter on dim with full access.", context);
        verifySameNativeAndNot(
            "select Filter([Store2].[USA].Children, [Store2].CurrentMember.Name matches \"WA.*\")"
            + " on 0 from tinysales",
            "Filter on restricted dimension.  Should be empty set.", context);
        verifySameNativeAndNot(
            "select Filter(CrossJoin({[Store2].[USA].Children}, [Product].[Product Category].Members), [Store2].CurrentMember.Name matches \".*A.*\")"
            + " on 0 from tinysales",
            "Filter on partially accessible set of tuples.", context);
    }

    /**
     * http://jira.pentaho.com/browse/MONDRIAN-2049
     * Native filter in Mondrian does not include compound slicers
     * in the constraint
     */
    public void testNativeFilterWithCompoundSlicer() {
    assertQueryReturns(
        "with member measures.avgQtrs as 'avg( filter( time.quarter.members, measures.[unit sales] > 80))' "
        + "select measures.avgQtrs * gender.members on 0 from sales where head( product.[product name].members, 3)",
        "Axis #0:\n"
        + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n"
        + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
        + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl].[Pearl Imported Beer]}\n"
        + "Axis #1:\n"
        + "{[Measures].[avgQtrs], [Gender].[All Gender]}\n"
        + "{[Measures].[avgQtrs], [Gender].[F]}\n"
        + "{[Measures].[avgQtrs], [Gender].[M]}\n"
        + "Row #0: 111\n"
        + "Row #0: \n"
        + "Row #0: \n");
    }

    public void testNativeFilterWithCompoundSlicerUsingAggregates() {
        propSaver.set(propSaver.properties.UseAggregates, true);
        propSaver.set(propSaver.properties.ReadAggregates, true);

        testNativeFilterWithCompoundSlicer();
    }

    public void testNativeFilterWithCompoundSlicerDifferentProducts() {
        assertQueryReturns(
            "with member measures.avgQtrs as 'count(filter(Customers.[Name].members, [Unit Sales] > 0))' "
            + "select measures.avgQtrs on 0 from sales where ( {[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer], [Product].[Food].[Baked Goods].[Bread].[Muffins]} )",
            "Axis #0:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Muffins]}\n"
            + "Axis #1:\n"
            + "{[Measures].[avgQtrs]}\n"
            + "Row #0: 1,281\n");
    }
}

// End NativeFilterMatchingTest.java
