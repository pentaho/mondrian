/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2012 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.spi.Dialect;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;

/**
 * Test case for pushing MDX filter conditions down to SQL.
 *
 * @version $Id$
 */
public class NativeFilterMatchingTest extends BatchTestCase {
    public void testPositiveMatching() throws Exception {
        if (!MondrianProperties.instance().EnableNativeFilter.get()) {
            /*
             * No point testing these if the native filters
             * are turned off.
             */
            return;
        }
        final String sqlOracle =
            MondrianProperties.instance().UseAggregates.get()
                ? "select "
                    + "\"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", \"fname\" || ' ' || \"lname\" as \"c4\", \"fname\" || ' ' || \"lname\" as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" \"customer\", \"agg_l_03_sales_fact_1997\" \"agg_l_03_sales_fact_1997\", \"time_by_day\" \"time_by_day\" where \"agg_l_03_sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"agg_l_03_sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", \"fname\" || ' ' || \"lname\", \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having REGEXP_LIKE(\"fname\" || ' ' || \"lname\", '.*jeanne.*', 'i') order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, \"fname\" || ' ' || \"lname\" ASC NULLS LAST"
                : "select "
                    + "\"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", \"fname\" || ' ' || \"lname\" as \"c4\", \"fname\" || ' ' || \"lname\" as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\", \"time_by_day\" \"time_by_day\" where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", \"fname\" || ' ' || \"lname\", \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having REGEXP_LIKE(\"fname\" || ' ' || \"lname\", '.*jeanne.*', 'i') order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, \"fname\" || ' ' || \"lname\" ASC NULLS LAST";
        final String sqlPgsql =
            MondrianProperties.instance().UseAggregates.get()
                ? "select "
                    + "\"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", fullname as \"c4\", fullname as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" as \"customer\", \"agg_l_03_sales_fact_1997\" as \"agg_l_03_sales_fact_1997\", \"time_by_day\" as \"time_by_day\" where \"agg_l_03_sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"agg_l_03_sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", fullname, \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having fullname ~ '(?i).*jeanne.*' order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, fullname ASC NULLS LAST"
                : "select "
                    + "\"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", fullname as \"c4\", fullname as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" as \"customer\", \"sales_fact_1997\" as \"sales_fact_1997\", \"time_by_day\" as \"time_by_day\" where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", fullname, \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having fullname ~ '(?i).*jeanne.*' order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, fullname ASC NULLS LAST";
        final String sqlMysql =
            MondrianProperties.instance().UseAggregates.get()
                ? "select "
                    + "`customer`.`country` as `c0`, `customer`.`state_province` as `c1`, `customer`.`city` as `c2`, `customer`.`customer_id` as `c3`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`, `customer`.`gender` as `c6`, `customer`.`marital_status` as `c7`, `customer`.`education` as `c8`, `customer`.`yearly_income` as `c9` from `customer` as `customer`, `agg_l_03_sales_fact_1997` as `agg_l_03_sales_fact_1997`, `time_by_day` as `time_by_day` where `agg_l_03_sales_fact_1997`.`customer_id` = `customer`.`customer_id` and `agg_l_03_sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and `time_by_day`.`the_year` = 1997 group by `customer`.`country`, `customer`.`state_province`, `customer`.`city`, `customer`.`customer_id`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`), `customer`.`gender`, `customer`.`marital_status`, `customer`.`education`, `customer`.`yearly_income` having UPPER(c5) REGEXP '.*jeanne.*' order by ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC, ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC, ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC, ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC"
                : "select "
                    + "`customer`.`country` as `c0`, `customer`.`state_province` as `c1`, `customer`.`city` as `c2`, `customer`.`customer_id` as `c3`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`, `customer`.`gender` as `c6`, `customer`.`marital_status` as `c7`, `customer`.`education` as `c8`, `customer`.`yearly_income` as `c9` from `customer` as `customer`, `sales_fact_1997` as `sales_fact_1997`, `time_by_day` as `time_by_day` where `sales_fact_1997`.`customer_id` = `customer`.`customer_id` and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and `time_by_day`.`the_year` = 1997 group by `customer`.`country`, `customer`.`state_province`, `customer`.`city`, `customer`.`customer_id`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`), `customer`.`gender`, `customer`.`marital_status`, `customer`.`education`, `customer`.`yearly_income` having UPPER(c5) REGEXP '.*jeanne.*' order by ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC, ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC, ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC, ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
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
    }

    public void testNegativeMatching() throws Exception {
        if (!MondrianProperties.instance().EnableNativeFilter.get()) {
            /*
             * No point testing these if the native filters
             * are turned off.
             */
            return;
        }
        final String sqlOracle =
            MondrianProperties.instance().UseAggregates.get()
                ? "select "
                    + "\"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", \"fname\" || ' ' || \"lname\" as \"c4\", \"fname\" || ' ' || \"lname\" as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" \"customer\", \"agg_l_03_sales_fact_1997\" \"agg_l_03_sales_fact_1997\", \"time_by_day\" \"time_by_day\" where \"agg_l_03_sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"agg_l_03_sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", \"fname\" || ' ' || \"lname\", \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having NOT(REGEXP_LIKE(\"fname\" || ' ' || \"lname\", '.*jeanne.*', 'i'))  order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, \"fname\" || ' ' || \"lname\" ASC NULLS LAST"
                : "select "
                    + "\"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", \"fname\" || ' ' || \"lname\" as \"c4\", \"fname\" || ' ' || \"lname\" as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\", \"time_by_day\" \"time_by_day\" where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", \"fname\" || ' ' || \"lname\", \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having NOT(REGEXP_LIKE(\"fname\" || ' ' || \"lname\", '.*jeanne.*', 'i'))  order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, \"fname\" || ' ' || \"lname\" ASC NULLS LAST";
        final String sqlPgsql =
            MondrianProperties.instance().UseAggregates.get()
                ? "select "
                    + "\"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", fullname as \"c4\", fullname as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" as \"customer\", \"agg_l_03_sales_fact_1997\" as \"agg_l_03_sales_fact_1997\", \"time_by_day\" as \"time_by_day\" where \"agg_l_03_sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"agg_l_03_sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", fullname, \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having NOT(fullname ~ '(?i).*jeanne.*')  order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, fullname ASC NULLS LAST"
                : "select "
                    + "\"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", fullname as \"c4\", fullname as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" as \"customer\", \"sales_fact_1997\" as \"sales_fact_1997\", \"time_by_day\" as \"time_by_day\" where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", fullname, \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" having NOT(fullname ~ '(?i).*jeanne.*')  order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, fullname ASC NULLS LAST";
        final String sqlMysql =
            MondrianProperties.instance().UseAggregates.get()
                ? "select "
                    + "`customer`.`country` as `c0`, `customer`.`state_province` as `c1`, `customer`.`city` as `c2`, `customer`.`customer_id` as `c3`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`, `customer`.`gender` as `c6`, `customer`.`marital_status` as `c7`, `customer`.`education` as `c8`, `customer`.`yearly_income` as `c9` from `customer` as `customer`, `agg_l_03_sales_fact_1997` as `agg_l_03_sales_fact_1997`, `time_by_day` as `time_by_day` where `agg_l_03_sales_fact_1997`.`customer_id` = `customer`.`customer_id` and `agg_l_03_sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and `time_by_day`.`the_year` = 1997 group by `customer`.`country`, `customer`.`state_province`, `customer`.`city`, `customer`.`customer_id`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`), `customer`.`gender`, `customer`.`marital_status`, `customer`.`education`, `customer`.`yearly_income` having NOT(UPPER(c5) REGEXP '.*jeanne.*')  order by ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC, ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC, ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC, ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC"
                : "select "
                    + "`customer`.`country` as `c0`, `customer`.`state_province` as `c1`, `customer`.`city` as `c2`, `customer`.`customer_id` as `c3`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`, `customer`.`gender` as `c6`, `customer`.`marital_status` as `c7`, `customer`.`education` as `c8`, `customer`.`yearly_income` as `c9` from `customer` as `customer`, `sales_fact_1997` as `sales_fact_1997`, `time_by_day` as `time_by_day` where `sales_fact_1997`.`customer_id` = `customer`.`customer_id` and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and `time_by_day`.`the_year` = 1997 group by `customer`.`country`, `customer`.`state_province`, `customer`.`city`, `customer`.`customer_id`, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`), `customer`.`gender`, `customer`.`marital_status`, `customer`.`education`, `customer`.`yearly_income` having NOT(UPPER(c5) REGEXP '.*jeanne.*')  order by ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC, ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC, ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC, ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";

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
}

// End NativeFilterMatchingTest.java
