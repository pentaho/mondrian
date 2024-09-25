/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2017 Hitachi Vantara.  All rights reserved.
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
            "select \"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", \"fname\" || ' ' || \"lname\" as \"c4\", \"fname\" || ' ' || \"lname\" as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" \"customer\" group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", \"fname\" || ' ' || \"lname\", \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having \"fname\" || ' ' || \"lname\" IS NOT NULL AND REGEXP_LIKE(\"fname\" || ' ' || \"lname\", '.*jeanne.*', 'i') order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, \"fname\" || ' ' || \"lname\" ASC NULLS LAST";
        final String sqlPgsql =
            "select \"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", fullname as \"c4\", fullname as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" as \"customer\" group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", fullname, \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having cast(fullname as text) is not null and cast(fullname as text) ~ '(?i).*jeanne.*'  order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, fullname ASC NULLS LAST";
        final String sqlMysql =
            "select `customer`.`country` as `c0`, `customer`.`state_province` as `c1`, `customer`.`city` as `c2`, `customer`.`customer_id` as `c3`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`, `customer`.`gender` as `c6`, `customer`.`marital_status` as `c7`, `customer`.`education` as `c8`, `customer`.`yearly_income` as `c9` from `customer` as `customer` group by `customer`.`country`, `customer`.`state_province`, `customer`.`city`, `customer`.`customer_id`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`), `customer`.`gender`, `customer`.`marital_status`, `customer`.`education`, `customer`.`yearly_income` having c5 IS NOT NULL AND UPPER(c5) REGEXP '.*JEANNE.*' order by "
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                    ? "ISNULL(`c0`) ASC, `c0` ASC, "
                    + "ISNULL(`c1`) ASC, `c1` ASC, "
                    + "ISNULL(`c2`) ASC, `c2` ASC, "
                    + "ISNULL(`c4`) ASC, `c4` ASC"
                    : "ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC, ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC, ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC, ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC");

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
            "select \"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", \"fname\" || ' ' || \"lname\" as \"c4\", \"fname\" || ' ' || \"lname\" as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" \"customer\" group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", \"fname\" || ' ' || \"lname\", \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having NOT(\"fname\" || ' ' || \"lname\" IS NOT NULL AND REGEXP_LIKE(\"fname\" || ' ' || \"lname\", '.*jeanne.*', 'i'))  order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, \"fname\" || ' ' || \"lname\" ASC NULLS LAST";
        final String sqlPgsql =
            "select \"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", fullname as \"c4\", fullname as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" as \"customer\" group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", fullname, \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having NOT(cast(fullname as text) is not null and cast(fullname as text) ~ '(?i).*jeanne.*')  order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, fullname ASC NULLS LAST";
        final String sqlMysql =
            "select `customer`.`country` as `c0`, `customer`.`state_province` as `c1`, `customer`.`city` as `c2`, `customer`.`customer_id` as `c3`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`, `customer`.`gender` as `c6`, `customer`.`marital_status` as `c7`, `customer`.`education` as `c8`, `customer`.`yearly_income` as `c9` from `customer` as `customer` group by `customer`.`country`, `customer`.`state_province`, `customer`.`city`, `customer`.`customer_id`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`), `customer`.`gender`, `customer`.`marital_status`, `customer`.`education`, `customer`.`yearly_income` having NOT(c5 IS NOT NULL AND UPPER(c5) REGEXP '.*JEANNE.*')  order by "
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                    ? "ISNULL(`c0`) ASC, `c0` ASC, "
                    + "ISNULL(`c1`) ASC, `c1` ASC, "
                    + "ISNULL(`c2`) ASC, `c2` ASC, "
                    + "ISNULL(`c4`) ASC, `c4` ASC"
                    : "ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC, ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC, ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC, ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC");
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

    public void testNativeFilterWithCompoundSlicer() {
        propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
        final String mdx =
            "with member measures.avgQtrs as 'avg( filter( time.quarter.members, measures.[unit sales] > 80))' "
            + "select measures.avgQtrs * gender.members on 0 from sales where head( product.[product name].members, 3)";

        if (MondrianProperties.instance().EnableNativeFilter.get()
            && MondrianProperties.instance().EnableNativeNonEmpty.get())
        {
            boolean requiresOrderByAlias =
                    TestContext.instance().getDialect().requiresOrderByAlias();
            final String sqlMysql =
                propSaver.properties.UseAggregates.get() == false
                    ? "select\n"
                    + "    `time_by_day`.`the_year` as `c0`,\n"
                    + "    `time_by_day`.`quarter` as `c1`\n"
                    + "from\n"
                    + "    `time_by_day` as `time_by_day`,\n"
                    + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                    + "    `product` as `product`,\n"
                    + "    `customer` as `customer`\n"
                    + "where\n"
                    + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                    + "and\n"
                    + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                    + "and\n"
                    + "    `product`.`product_name` in ('Good Imported Beer', 'Good Light Beer', 'Pearl Imported Beer')\n"
                    + "and\n"
                    + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
                    + "and\n"
                    + "    `customer`.`gender` = 'M'\n"
                    + "group by\n"
                    + "    `time_by_day`.`the_year`,\n"
                    + "    `time_by_day`.`quarter`\n"
                    + "having\n"
                    + "    (sum(`sales_fact_1997`.`unit_sales`) > 80)\n"
                    + "order by\n"
                    + (requiresOrderByAlias
                        ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                        + "    ISNULL(`c1`) ASC, `c1` ASC"
                        : "    ISNULL(`time_by_day`.`the_year`) ASC, `time_by_day`.`the_year` ASC,\n"
                        + "    ISNULL(`time_by_day`.`quarter`) ASC, `time_by_day`.`quarter` ASC")

                    : "select\n"
                    + "    `agg_c_14_sales_fact_1997`.`the_year` as `c0`,\n"
                    + "    `agg_c_14_sales_fact_1997`.`quarter` as `c1`\n"
                    + "from\n"
                    + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                    + "    `product` as `product`,\n"
                    + "    `customer` as `customer`\n"
                    + "where\n"
                    + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                    + "and\n"
                    + "    `product`.`product_name` in ('Good Imported Beer', 'Good Light Beer', 'Pearl Imported Beer')\n"
                    + "and\n"
                    + "    `agg_c_14_sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
                    + "and\n"
                    + "    `customer`.`gender` = 'M'\n"
                    + "group by\n"
                    + "    `agg_c_14_sales_fact_1997`.`the_year`,\n"
                    + "    `agg_c_14_sales_fact_1997`.`quarter`\n"
                    + "having\n"
                    + "    (sum(`agg_c_14_sales_fact_1997`.`unit_sales`) > 80)\n"
                    + "order by\n"
                    + (requiresOrderByAlias
                        ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                        + "    ISNULL(`c1`) ASC, `c1` ASC"
                        : "    ISNULL(`agg_c_14_sales_fact_1997`.`the_year`) ASC, `agg_c_14_sales_fact_1997`.`the_year` ASC,\n"
                        + "    ISNULL(`agg_c_14_sales_fact_1997`.`quarter`) ASC, `agg_c_14_sales_fact_1997`.`quarter` ASC");
            final SqlPattern[] patterns = mysqlPattern(sqlMysql);

            // Make sure the tuples list is using the HAVING clause.
            assertQuerySqlOrNot(
                getTestContext(),
                mdx,
                patterns,
                false,
                true,
                true);
        }
        // Make sure the numbers are right
        assertQueryReturns(
            mdx,
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

    public void testNativeFilterWithCompoundSlicerWithAggs() {
        propSaver.set(MondrianProperties.instance().UseAggregates, true);
        propSaver.set(MondrianProperties.instance().ReadAggregates, true);
        propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
        final String mdx =
            "with member measures.avgQtrs as 'avg( filter( time.quarter.members, measures.[unit sales] > 80))' "
            + "select measures.avgQtrs * gender.members on 0 from sales where head( product.[product name].members, 3)";
        if (MondrianProperties.instance().EnableNativeFilter.get()
            && MondrianProperties.instance().EnableNativeNonEmpty.get())
        {
            final String sqlMysql =
                "select\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` as `c0`,\n"
                + "    `agg_c_14_sales_fact_1997`.`quarter` as `c1`\n"
                + "from\n"
                + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                + "    `product` as `product`,\n"
                + "    `customer` as `customer`\n"
                + "where\n"
                + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_name` in ('Good Imported Beer', 'Good Light Beer', 'Pearl Imported Beer')\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
                + "and\n"
                + "    `customer`.`gender` = 'M'\n"
                + "group by\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year`,\n"
                + "    `agg_c_14_sales_fact_1997`.`quarter`\n"
                + "having\n"
                + "    (sum(`agg_c_14_sales_fact_1997`.`unit_sales`) > 80)\n"
                + "order by\n"
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                    ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                    + "    ISNULL(`c1`) ASC, `c1` ASC"
                    : "    ISNULL(`agg_c_14_sales_fact_1997`.`the_year`) ASC, `agg_c_14_sales_fact_1997`.`the_year` ASC,\n"
                    + "    ISNULL(`agg_c_14_sales_fact_1997`.`quarter`) ASC, `agg_c_14_sales_fact_1997`.`quarter` ASC");
            final SqlPattern[] patterns = mysqlPattern(sqlMysql);

            // Make sure the tuples list is using the HAVING clause.
            assertQuerySqlOrNot(
                getTestContext(),
                mdx,
                patterns,
                false,
                true,
                true);
        }
        // Make sure the numbers are right
        assertQueryReturns(
            mdx,
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

    public void testNativeFilterWithCompoundSlicer_1() {
        propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
        final String mdx =
            "with member [measures].[avgQtrs] as 'count(filter([Customers].[Name].Members, [Measures].[Unit Sales] > 0))' "
            + "select [measures].[avgQtrs] on 0 from sales where ( {[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer], [Product].[Food].[Baked Goods].[Bread].[Muffins]} )";
        if (MondrianProperties.instance().EnableNativeFilter.get()
            && MondrianProperties.instance().EnableNativeNonEmpty.get())
        {
            boolean requiresOrderByAlias =
                    TestContext.instance().getDialect().requiresOrderByAlias();
            final String sqlMysql =
                propSaver.properties.UseAggregates.get() == false
                    ? "select\n"
                    + "    `customer`.`country` as `c0`,\n"
                    + "    `customer`.`state_province` as `c1`,\n"
                    + "    `customer`.`city` as `c2`,\n"
                    + "    `customer`.`customer_id` as `c3`,\n"
                    + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`,\n"
                    + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`,\n"
                    + "    `customer`.`gender` as `c6`,\n"
                    + "    `customer`.`marital_status` as `c7`,\n"
                    + "    `customer`.`education` as `c8`,\n"
                    + "    `customer`.`yearly_income` as `c9`\n"
                    + "from\n"
                    + "    `customer` as `customer`,\n"
                    + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                    + "    `time_by_day` as `time_by_day`,\n"
                    + "    `product_class` as `product_class`,\n"
                    + "    `product` as `product`\n"
                    + "where\n"
                    + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
                    + "and\n"
                    + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                    + "and\n"
                    + "    `time_by_day`.`the_year` = 1997\n"
                    + "and\n"
                    + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                    //
                    + "and\n"
                    + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                    + "and\n"
                    + "    `product_class`.`product_family` in ('Drink', 'Food')\n"
                    + "and\n"
                    + "    `product_class`.`product_department` in ('Alcoholic Beverages', 'Baked Goods')\n"
                    + "and\n"
                    + "    `product_class`.`product_category` in ('Beer and Wine', 'Bread')\n"
                    + "and\n"
                    + "    `product_class`.`product_subcategory` in ('Beer', 'Muffins')\n"
                    + "group by\n"
                    + "    `customer`.`country`,\n"
                    + "    `customer`.`state_province`,\n"
                    + "    `customer`.`city`,\n"
                    + "    `customer`.`customer_id`,\n"
                    + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`),\n"
                    + "    `customer`.`gender`,\n"
                    + "    `customer`.`marital_status`,\n"
                    + "    `customer`.`education`,\n"
                    + "    `customer`.`yearly_income`\n"
                    + "having\n"
                    + "    (sum(`sales_fact_1997`.`unit_sales`) > 0)\n"
                    // ^^^^ This is what we are interested in. ^^^^
                    + "order by\n"
                    + (requiresOrderByAlias
                        ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                        + "    ISNULL(`c1`) ASC, `c1` ASC,\n"
                        + "    ISNULL(`c2`) ASC, `c2` ASC,\n"
                        + "    ISNULL(`c4`) ASC, `c4` ASC"
                        : "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
                        + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
                        + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
                        + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC")

                    : "select\n"
                    + "    `customer`.`country` as `c0`,\n"
                    + "    `customer`.`state_province` as `c1`,\n"
                    + "    `customer`.`city` as `c2`,\n"
                    + "    `customer`.`customer_id` as `c3`,\n"
                    + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`,\n"
                    + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`,\n"
                    + "    `customer`.`gender` as `c6`,\n"
                    + "    `customer`.`marital_status` as `c7`,\n"
                    + "    `customer`.`education` as `c8`,\n"
                    + "    `customer`.`yearly_income` as `c9`\n"
                    + "from\n"
                    + "    `customer` as `customer`,\n"
                    + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                    + "    `product_class` as `product_class`,\n"
                    + "    `product` as `product`\n"
                    + "where\n"
                    + "    `agg_c_14_sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
                    + "and\n"
                    + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n"
                    + "and\n"
                    + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                    + "and\n"
                    + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                    + "and\n"
                    + "    `product_class`.`product_family` in ('Drink', 'Food')\n"
                    + "and\n"
                    + "    `product_class`.`product_department` in ('Alcoholic Beverages', 'Baked Goods')\n"
                    + "and\n"
                    + "    `product_class`.`product_category` in ('Beer and Wine', 'Bread')\n"
                    + "and\n"
                    + "    `product_class`.`product_subcategory` in ('Beer', 'Muffins')\n"
                    + "group by\n"
                    + "    `customer`.`country`,\n"
                    + "    `customer`.`state_province`,\n"
                    + "    `customer`.`city`,\n"
                    + "    `customer`.`customer_id`,\n"
                    + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`),\n"
                    + "    `customer`.`gender`,\n"
                    + "    `customer`.`marital_status`,\n"
                    + "    `customer`.`education`,\n"
                    + "    `customer`.`yearly_income`\n"
                    + "having\n"
                    + "    (sum(`agg_c_14_sales_fact_1997`.`unit_sales`) > 0)\n"
                    // ^^^^ This is what we are interested in. ^^^^
                    + "order by\n"
                    + "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
                    + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
                    + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
                    + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
            final SqlPattern[] patterns = mysqlPattern(sqlMysql);

            // Make sure the tuples list is using the HAVING clause.
            assertQuerySqlOrNot(
                getTestContext(),
                mdx,
                patterns,
                false,
                true,
                true);
        }
        // Make sure the numbers are right
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Muffins]}\n"
            + "Axis #1:\n"
            + "{[Measures].[avgQtrs]}\n"
            + "Row #0: 1,281\n");
    }

    public void testNativeFilterWithCompoundSlicer_2() {
        verifySameNativeAndNot(
            "WITH MEMBER [Measures].[TotalVal] AS 'Aggregate(Filter({[Store].[Store City].members}, ([Measures].[Unit Sales] > 1000 OR ( [Measures].[Unit Sales] > 40 AND [Store].[Store City].CurrentMember.Name = \"San Francisco\" ) ) ) )'\n"
            + "SELECT [Measures].[TotalVal] ON 0, [Product].[All Products].Children on 1 FROM [Sales] WHERE {[Time].[1997].[Q1],[Time].[1997].[Q2]}",
            "Failed.",
            getTestContext());
    }

    public void testNativeFilterWithCompoundSlicer_3() {
        verifySameNativeAndNot(
            "WITH MEMBER [Measures].[TotalVal] AS 'Aggregate(Filter({[Store].[Store City].members}, [Measures].[Unit Sales] > 1000 ) )'\n"
            + "SELECT [Measures].[TotalVal] ON 0, [Product].[All Products].Children on 1 FROM [Sales] WHERE {[Time].[1997].[Q1],[Time].[1997].[Q2]}",
            "Failed.",
            getTestContext());
    }

    public void testNativeFilterWithCompoundSlicer_4() {
        verifySameNativeAndNot(
            "WITH MEMBER [Measures].[TotalVal] AS 'Aggregate(Filter({[Store].[Store City].members}, ([Measures].[Unit Sales] > 1000 OR ( [Measures].[Unit Sales] > 500 AND [Store].[Store City].CurrentMember.Name = \"San Francisco\" ) ) ) )'\n"
            + "SELECT [Measures].[TotalVal] ON 0, [Product].[All Products].Children on 1 FROM [Sales] WHERE {[Time].[1997].[Q1],[Time].[1997].[Q2]}",
            "Failed.",
            getTestContext());
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
