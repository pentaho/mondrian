/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.MondrianProperties;
import mondrian.spi.Dialect;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;

public class NativeRangeTest extends BatchTestCase {

    private static final String CUBE_1 =
        "<Cube name=\"date_cube\">\n"
        + "    <Table name=\"employee\">\n"
        + "    </Table>\n"
        + "    <Dimension name=\"TimeDate\">\n"
        + "        <Hierarchy name=\"TimeDate\" hasAll=\"true\" allMemberName=\"All the time\" type=\"TimeDimension\">\n"
        + "            <Level name=\"DateLevel\" column=\"birth_date\" type=\"Date\" levelType=\"TimeDays\" uniqueMembers=\"true\"/>\n"
        + "        </Hierarchy>\n"
        + "    </Dimension>"
        + "    <Dimension name=\"TimeStamp\">\n"
        + "        <Hierarchy name=\"TimeStamp\" hasAll=\"true\" allMemberName=\"All the time\" type=\"TimeDimension\">\n"
        + "            <Level name=\"DateLevel\" column=\"hire_date\" type=\"Timestamp\" levelType=\"TimeUndefined\" uniqueMembers=\"true\"/>\n"
        + "        </Hierarchy>\n"
        + "    </Dimension>"
        + "    <Measure name=\"Salary\" column=\"salary\" aggregator=\"sum\"/>\n"
        + "</Cube>\n";

    private static final String CUBE_2 =
        "<Cube name=\"Bacon\" defaultMeasure=\"Unit Sales\">\n"
        + "  <Table name=\"sales_fact_1997\">\n"
        + "    <AggExclude name=\"agg_c_special_sales_fact_1997\" />\n"
        + "    <AggExclude name=\"agg_lc_100_sales_fact_1997\" />\n"
        + "    <AggExclude name=\"agg_lc_10_sales_fact_1997\" />\n"
        + "    <AggExclude name=\"agg_pc_10_sales_fact_1997\" />\n"
        + "  </Table>\n"
        + "  <Dimension name=\"SQFT\" foreignKey=\"store_id\">\n"
        + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
        + "      <Table name=\"store\"/>\n"
        + "      <Level name=\"SQFT\" column=\"store_sqft\" ordinalColumn=\"store_sqft\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>\n"
        + "  <Dimension name=\"Promotion\" foreignKey=\"promotion_id\">\n"
        + "    <Hierarchy hasAll=\"true\" primaryKey=\"promotion_id\">\n"
        + "      <Table name=\"promotion\"/>\n"
        + "      <Level name=\"Promotion\" column=\"promotion_id\" ordinalColumn=\"promotion_id\" uniqueMembers=\"true\"/>\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>\n"
        + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
        + "</Cube>\n";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
    }

    public void testDateRangeOnAxis() throws Exception {
        // Test using keys for member names
        runDateRangeOnAxis(
            "select {[TimeDate].[DateLevel].&[1942-10-08] : [TimeDate].[DateLevel].&[1967-06-20]} on 0\n"
            + "from [date_cube]");
        // Test using the member name
        runDateRangeOnAxis(
            "select {[TimeDate].[DateLevel].[1942-10-08] : [TimeDate].[DateLevel].[1967-06-20]} on 0\n"
            + "from [date_cube]");
    }

    public void runDateRangeOnAxis(String mdx) throws Exception {
        final TestContext tc = getTestContext().create(
            null,
            CUBE_1,
            null, null, null, null);

        // Test that the request to get the default members is constrained.
        final String sqlMysqlDefaultMemberWithConstraint =
            "select\n"
            + "    `employee`.`birth_date` as `c0`\n"
            + "from\n"
            + "    `employee` as `employee`\n"
            + "where\n"
            + "    `employee`.`birth_date` = DATE '1942-10-08'\n"
            + "group by\n"
            + "    `employee`.`birth_date`\n"
            + "order by\n"
            + "    ISNULL(`employee`.`birth_date`) ASC, `employee`.`birth_date` ASC";
        final SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlDefaultMemberWithConstraint,
                sqlMysqlDefaultMemberWithConstraint.length())
        };
        assertQuerySqlOrNot(
            tc,
            mdx,
            patterns,
            false,
            true,
            true);

        // Test that the request to get the children is constrained
        final String sqlMysqlChildrenWithConstraint =
            "select\n"
            + "    `employee`.`birth_date` as `c0`\n"
            + "from\n"
            + "    `employee` as `employee`\n"
            + "where\n"
            + "    `employee`.`birth_date` BETWEEN DATE '1942-10-08' AND DATE '1967-06-20'\n"
            + "group by\n"
            + "    `employee`.`birth_date`\n"
            + "order by\n"
            + "    ISNULL(`employee`.`birth_date`) ASC, `employee`.`birth_date` ASC";
        final SqlPattern[] patternsForChildren = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlChildrenWithConstraint,
                sqlMysqlChildrenWithConstraint.length())
        };
        assertQuerySqlOrNot(
            tc,
            mdx,
            patternsForChildren,
            false,
            true,
            true);

        // Make sure the cell data is good.
        tc.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[TimeDate].[1942-10-08]}\n"
            + "{[TimeDate].[1946-04-18]}\n"
            + "{[TimeDate].[1947-10-07]}\n"
            + "{[TimeDate].[1949-03-27]}\n"
            + "{[TimeDate].[1949-08-26]}\n"
            + "{[TimeDate].[1951-05-10]}\n"
            + "{[TimeDate].[1952-09-26]}\n"
            + "{[TimeDate].[1953-07-20]}\n"
            + "{[TimeDate].[1954-08-25]}\n"
            + "{[TimeDate].[1958-04-09]}\n"
            + "{[TimeDate].[1959-01-23]}\n"
            + "{[TimeDate].[1960-12-10]}\n"
            + "{[TimeDate].[1961-04-06]}\n"
            + "{[TimeDate].[1961-08-26]}\n"
            + "{[TimeDate].[1961-09-24]}\n"
            + "{[TimeDate].[1965-03-27]}\n"
            + "{[TimeDate].[1967-06-20]}\n"
            + "Row #0: 25,000\n"
            + "Row #0: 94,800\n"
            + "Row #0: 35,000\n"
            + "Row #0: 15,000\n"
            + "Row #0: 50,000\n"
            + "Row #0: 35,000\n"
            + "Row #0: 12,000\n"
            + "Row #0: 10,000\n"
            + "Row #0: 15,000\n"
            + "Row #0: 8,000\n"
            + "Row #0: 10,000\n"
            + "Row #0: 272,000\n"
            + "Row #0: 6,700\n"
            + "Row #0: 80,000\n"
            + "Row #0: 421,000\n"
            + "Row #0: 5,000\n"
            + "Row #0: 15,000\n");
    }

    public void testTimeRangeOnAxis() throws Exception {
        // Try with keys
        runTimeRangeOnAxis(
            "select {[TimeStamp].[DateLevel].&[1995-01-01 00:00:00.0] : [TimeStamp].[DateLevel].&[1997-01-01 00:00:00.0]} on 0\n"
            + "from [date_cube]");
        // Try with names
        runTimeRangeOnAxis(
            "select {[TimeStamp].[DateLevel].[1995-01-01 00:00:00.0] : [TimeStamp].[DateLevel].[1997-01-01 00:00:00.0]} on 0\n"
            + "from [date_cube]");
    }

    public void runTimeRangeOnAxis(String mdx) throws Exception {
        final TestContext tc = getTestContext().create(
            null,
            CUBE_1,
            null, null, null, null);

        // Test that the request to get the default members is constrained.
        final String sqlMysqlDefaultMemberWithConstraint =
            "select\n"
            + "    `employee`.`hire_date` as `c0`\n"
            + "from\n"
            + "    `employee` as `employee`\n"
            + "where\n"
            + "    `employee`.`hire_date` = TIMESTAMP '1997-01-01 00:00:00.0'\n"
            + "group by\n"
            + "    `employee`.`hire_date`\n"
            + "order by\n"
            + "    ISNULL(`employee`.`hire_date`) ASC, `employee`.`hire_date` ASC";
        final SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlDefaultMemberWithConstraint,
                sqlMysqlDefaultMemberWithConstraint.length())
        };
        assertQuerySqlOrNot(
            tc,
            mdx,
            patterns,
            false,
            true,
            true);

        // Test that the request to get the children is constrained
        final String sqlMysqlChildrenWithConstraint =
            "select\n"
            + "    `employee`.`hire_date` as `c0`\n"
            + "from\n"
            + "    `employee` as `employee`\n"
            + "where\n"
            + "    `employee`.`hire_date` BETWEEN TIMESTAMP '1995-01-01 00:00:00.0' AND TIMESTAMP '1997-01-01 00:00:00.0'\n"
            + "group by\n"
            + "    `employee`.`hire_date`\n"
            + "order by\n"
            + "    ISNULL(`employee`.`hire_date`) ASC, `employee`.`hire_date` ASC";
        final SqlPattern[] patternsForChildren = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlChildrenWithConstraint,
                sqlMysqlChildrenWithConstraint.length())
        };
        assertQuerySqlOrNot(
            tc,
            mdx,
            patternsForChildren,
            false,
            true,
            true);

        // Make sure the cell data is good.
        tc.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[TimeStamp].[1995-01-01 00:00:00.0]}\n"
            + "{[TimeStamp].[1995-01-25 00:00:00.0]}\n"
            + "{[TimeStamp].[1996-01-01 00:00:00.0]}\n"
            + "{[TimeStamp].[1997-01-01 00:00:00.0]}\n"
            + "Row #0: 16,500\n"
            + "Row #0: 47,400\n"
            + "Row #0: 1,386,660\n"
            + "Row #0: 575,900\n");
    }

    public void testMemberRangeOnAxis() throws Exception {
        // Try with keys
        runMemberRangeOnAxis(
            "select {[SQFT].[SQFT].&[23593] : [SQFT].[SQFT].&[24597]} on 0\n"
            + "from [Bacon]");
        // Try with names
        runMemberRangeOnAxis(
            "select {[SQFT].[SQFT].[23593] : [SQFT].[SQFT].[24597]} on 0\n"
            + "from [Bacon]");
    }

    public void runMemberRangeOnAxis(String mdx) throws Exception {
        final TestContext tc = getTestContext().create(
            null,
            CUBE_2,
            null, null, null, null);

        // Test that the request to get the default members is constrained.
        final String sqlMysqlDefaultMemberWithConstraint1 =
            "select\n"
            + "    `store`.`store_sqft` as `c0`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `store`.`store_sqft` = 23593\n"
            + "group by\n"
            + "    `store`.`store_sqft`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_sqft`) ASC, `store`.`store_sqft` ASC";
        final String sqlMysqlDefaultMemberWithConstraint2 =
            "select\n"
            + "    `store`.`store_sqft` as `c0`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `store`.`store_sqft` = 24597\n"
            + "group by\n"
            + "    `store`.`store_sqft`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_sqft`) ASC, `store`.`store_sqft` ASC";
        final SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlDefaultMemberWithConstraint1,
                sqlMysqlDefaultMemberWithConstraint1.length()),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlDefaultMemberWithConstraint2,
                sqlMysqlDefaultMemberWithConstraint2.length())
        };
        assertQuerySqlOrNot(
            tc,
            mdx,
            patterns,
            false,
            true,
            true);

        // Test that the request to get the children is constrained
        final String sqlMysqlChildrenWithConstraint =
            "select\n"
            + "    `store`.`store_sqft` as `c0`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `store`.`store_sqft` BETWEEN 23593 AND 24597\n"
            + "group by\n"
            + "    `store`.`store_sqft`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_sqft`) ASC, `store`.`store_sqft` ASC";
        final SqlPattern[] patternsForChildren = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlChildrenWithConstraint,
                sqlMysqlChildrenWithConstraint.length())
        };
        assertQuerySqlOrNot(
            tc,
            mdx,
            patternsForChildren,
            false,
            true,
            true);

        // Test that no full table scan is done.
        final String sqlMysqlForbiddenQuery =
            "select\n"
            + "    `store`.`store_sqft` as `c0`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "group by\n"
            + "    `store`.`store_sqft`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_sqft`) ASC, `store`.`store_sqft` ASC";
        final SqlPattern[] patternsForForbiddenQuery = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlForbiddenQuery,
                sqlMysqlForbiddenQuery.length())
        };
        assertQuerySqlOrNot(
            tc,
            mdx,
            patternsForForbiddenQuery,
            true,
            true,
            true);

        // Make sure the cell data is good.
        tc.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[SQFT].[23593]}\n"
            + "{[SQFT].[23598]}\n"
            + "{[SQFT].[23688]}\n"
            + "{[SQFT].[23759]}\n"
            + "{[SQFT].[24597]}\n"
            + "Row #0: \n"
            + "Row #0: 25,663\n"
            + "Row #0: 21,333\n"
            + "Row #0: \n"
            + "Row #0: \n");
    }

    public void testMemberRangeOnSlicer() throws Exception {
        // Try with keys
        runMemberRangeOnSlicer(
            "select {[Measures].[Unit Sales]} on 0\n"
            + "from [Bacon]\n"
            + "where {[SQFT].[SQFT].&[23593] : [SQFT].[SQFT].&[24597]}");
        // Try with names
        runMemberRangeOnSlicer(
            "select {[Measures].[Unit Sales]} on 0\n"
            + "from [Bacon]\n"
            + "where {[SQFT].[SQFT].[23593] : [SQFT].[SQFT].[24597]}");
    }

    public void runMemberRangeOnSlicer(String mdx) throws Exception {
        final TestContext tc = getTestContext().create(
            null,
            CUBE_2,
            null, null, null, null);

        // Test that the request to get the default members is constrained.
        final String sqlMysqlDefaultMemberWithConstraint1 =
            "select\n"
            + "    `store`.`store_sqft` as `c0`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `store`.`store_sqft` = 23593\n"
            + "group by\n"
            + "    `store`.`store_sqft`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_sqft`) ASC, `store`.`store_sqft` ASC";
        final String sqlMysqlDefaultMemberWithConstraint2 =
            "select\n"
            + "    `store`.`store_sqft` as `c0`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `store`.`store_sqft` = 24597\n"
            + "group by\n"
            + "    `store`.`store_sqft`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_sqft`) ASC, `store`.`store_sqft` ASC";
        final SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlDefaultMemberWithConstraint1,
                sqlMysqlDefaultMemberWithConstraint1.length()),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlDefaultMemberWithConstraint2,
                sqlMysqlDefaultMemberWithConstraint2.length())
        };
        assertQuerySqlOrNot(
            tc,
            mdx,
            patterns,
            false,
            true,
            true);

        // Test that the request to get the children is constrained
        final String sqlMysqlChildrenWithConstraint =
            "select\n"
            + "    `store`.`store_sqft` as `c0`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `store`.`store_sqft` BETWEEN 23593 AND 24597\n"
            + "group by\n"
            + "    `store`.`store_sqft`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_sqft`) ASC, `store`.`store_sqft` ASC";
        final SqlPattern[] patternsForChildren = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlChildrenWithConstraint,
                sqlMysqlChildrenWithConstraint.length())
        };
        assertQuerySqlOrNot(
            tc,
            mdx,
            patternsForChildren,
            false,
            true,
            true);

        // Test that no full table scan is done.
        final String sqlMysqlForbiddenQuery =
            "select\n"
            + "    `store`.`store_sqft` as `c0`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "group by\n"
            + "    `store`.`store_sqft`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_sqft`) ASC, `store`.`store_sqft` ASC";
        final SqlPattern[] patternsForForbiddenQuery = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlForbiddenQuery,
                sqlMysqlForbiddenQuery.length())
        };
        assertQuerySqlOrNot(
            tc,
            mdx,
            patternsForForbiddenQuery,
            true,
            true,
            true);

        // Make sure the cell data is good.
        tc.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[SQFT].[23593]}\n"
            + "{[SQFT].[23598]}\n"
            + "{[SQFT].[23688]}\n"
            + "{[SQFT].[23759]}\n"
            + "{[SQFT].[24597]}\n"
            + "Row #0: \n"
            + "Row #0: 25,663\n"
            + "Row #0: 21,333\n"
            + "Row #0: \n"
            + "Row #0: \n");
    }

    public void testMemberRangeInCrossjoin() throws Exception {
        // Try with keys
        runMemberRangeInCrossjoin(
            "select { Crossjoin("
            + "  {[SQFT].[SQFT].&[23598] : [SQFT].[SQFT].&[23688]},"
            + "  {[Promotion].[Promotion].&[501] : [Promotion].[Promotion].&[526]})} on 0\n"
            + "from [Bacon]");
        // Try with names
        runMemberRangeInCrossjoin(
            "select { Crossjoin("
            + "  {[SQFT].[SQFT].[23598] : [SQFT].[SQFT].[23688]},"
            + "  {[Promotion].[Promotion].[501] : [Promotion].[Promotion].[526]})} on 0\n"
            + "from [Bacon]");
    }

    public void runMemberRangeInCrossjoin(String mdx) throws Exception {
        final TestContext tc = getTestContext().create(
            null,
            CUBE_2,
            null, null, null, null);

        // Test that the request to get the default members is constrained.
        final String sqlMysqlDefaultMemberWithConstraint1 =
            "select\n"
            + "    `store`.`store_sqft` as `c0`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `store`.`store_sqft` = 23598\n"
            + "group by\n"
            + "    `store`.`store_sqft`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_sqft`) ASC, `store`.`store_sqft` ASC";
        final String sqlMysqlDefaultMemberWithConstraint2 =
            "select\n"
            + "    `store`.`store_sqft` as `c0`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `store`.`store_sqft` = 23688\n"
            + "group by\n"
            + "    `store`.`store_sqft`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_sqft`) ASC, `store`.`store_sqft` ASC";
        final SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlDefaultMemberWithConstraint1,
                sqlMysqlDefaultMemberWithConstraint1.length()),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlDefaultMemberWithConstraint2,
                sqlMysqlDefaultMemberWithConstraint2.length())
        };
        assertQuerySqlOrNot(
            tc,
            mdx,
            patterns,
            false,
            true,
            true);

        // Test that the request to get the children is constrained
        // for both the levels.
        final String sqlMysqlChildrenWithConstraint1 =
            "select\n"
            + "    `store`.`store_sqft` as `c0`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `store`.`store_sqft` BETWEEN 23598 AND 23688\n"
            + "group by\n"
            + "    `store`.`store_sqft`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_sqft`) ASC, `store`.`store_sqft` ASC";
        final String sqlMysqlChildrenWithConstraint2 =
            "select\n"
            + "    `promotion`.`promotion_id` as `c0`\n"
            + "from\n"
            + "    `promotion` as `promotion`\n"
            + "where\n"
            + "    `promotion`.`promotion_id` BETWEEN '501' AND '526'\n"
            + "group by\n"
            + "    `promotion`.`promotion_id`\n"
            + "order by\n"
            + "    ISNULL(`promotion`.`promotion_id`) ASC, `promotion`.`promotion_id` ASC";
        final SqlPattern[] patternsForChildren = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlChildrenWithConstraint1,
                sqlMysqlChildrenWithConstraint1.length()),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlChildrenWithConstraint2,
                sqlMysqlChildrenWithConstraint2.length()),
        };
        assertQuerySqlOrNot(
            tc,
            mdx,
            patternsForChildren,
            false,
            true,
            true);

        // Test that no full table scan is done.
        final String sqlMysqlForbiddenQuery1 =
            "select\n"
            + "    `store`.`store_sqft` as `c0`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "group by\n"
            + "    `store`.`store_sqft`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_sqft`) ASC, `store`.`store_sqft` ASC";
        final String sqlMysqlForbiddenQuery2 =
            "select\n"
            + "    `promotion`.`promotion_id` as `c0`\n"
            + "from\n"
            + "    `promotion` as `promotion`\n"
            + "group by\n"
            + "    `promotion`.`promotion_id`\n"
            + "order by\n"
            + "    ISNULL(`promotion`.`promotion_id`) ASC, `promotion`.`promotion_id` ASC";
        final SqlPattern[] patternsForForbiddenQuery = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlForbiddenQuery1,
                sqlMysqlForbiddenQuery1.length()),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                sqlMysqlForbiddenQuery2,
                sqlMysqlForbiddenQuery2.length())
        };
        assertQuerySqlOrNot(
            tc,
            mdx,
            patternsForForbiddenQuery,
            true,
            true,
            true);

        // Make sure the cell data is good.
        tc.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[SQFT].[23598], [Promotion].[501]}\n"
            + "{[SQFT].[23598], [Promotion].[502]}\n"
            + "{[SQFT].[23598], [Promotion].[503]}\n"
            + "{[SQFT].[23598], [Promotion].[504]}\n"
            + "{[SQFT].[23598], [Promotion].[505]}\n"
            + "{[SQFT].[23598], [Promotion].[506]}\n"
            + "{[SQFT].[23598], [Promotion].[507]}\n"
            + "{[SQFT].[23598], [Promotion].[508]}\n"
            + "{[SQFT].[23598], [Promotion].[509]}\n"
            + "{[SQFT].[23598], [Promotion].[510]}\n"
            + "{[SQFT].[23598], [Promotion].[511]}\n"
            + "{[SQFT].[23598], [Promotion].[512]}\n"
            + "{[SQFT].[23598], [Promotion].[513]}\n"
            + "{[SQFT].[23598], [Promotion].[514]}\n"
            + "{[SQFT].[23598], [Promotion].[515]}\n"
            + "{[SQFT].[23598], [Promotion].[516]}\n"
            + "{[SQFT].[23598], [Promotion].[517]}\n"
            + "{[SQFT].[23598], [Promotion].[518]}\n"
            + "{[SQFT].[23598], [Promotion].[519]}\n"
            + "{[SQFT].[23598], [Promotion].[520]}\n"
            + "{[SQFT].[23598], [Promotion].[521]}\n"
            + "{[SQFT].[23598], [Promotion].[522]}\n"
            + "{[SQFT].[23598], [Promotion].[523]}\n"
            + "{[SQFT].[23598], [Promotion].[524]}\n"
            + "{[SQFT].[23598], [Promotion].[525]}\n"
            + "{[SQFT].[23598], [Promotion].[526]}\n"
            + "{[SQFT].[23688], [Promotion].[501]}\n"
            + "{[SQFT].[23688], [Promotion].[502]}\n"
            + "{[SQFT].[23688], [Promotion].[503]}\n"
            + "{[SQFT].[23688], [Promotion].[504]}\n"
            + "{[SQFT].[23688], [Promotion].[505]}\n"
            + "{[SQFT].[23688], [Promotion].[506]}\n"
            + "{[SQFT].[23688], [Promotion].[507]}\n"
            + "{[SQFT].[23688], [Promotion].[508]}\n"
            + "{[SQFT].[23688], [Promotion].[509]}\n"
            + "{[SQFT].[23688], [Promotion].[510]}\n"
            + "{[SQFT].[23688], [Promotion].[511]}\n"
            + "{[SQFT].[23688], [Promotion].[512]}\n"
            + "{[SQFT].[23688], [Promotion].[513]}\n"
            + "{[SQFT].[23688], [Promotion].[514]}\n"
            + "{[SQFT].[23688], [Promotion].[515]}\n"
            + "{[SQFT].[23688], [Promotion].[516]}\n"
            + "{[SQFT].[23688], [Promotion].[517]}\n"
            + "{[SQFT].[23688], [Promotion].[518]}\n"
            + "{[SQFT].[23688], [Promotion].[519]}\n"
            + "{[SQFT].[23688], [Promotion].[520]}\n"
            + "{[SQFT].[23688], [Promotion].[521]}\n"
            + "{[SQFT].[23688], [Promotion].[522]}\n"
            + "{[SQFT].[23688], [Promotion].[523]}\n"
            + "{[SQFT].[23688], [Promotion].[524]}\n"
            + "{[SQFT].[23688], [Promotion].[525]}\n"
            + "{[SQFT].[23688], [Promotion].[526]}\n"
            + "Row #0: 394\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 2,012\n"
            + "Row #0: 602\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 400\n"
            + "Row #0: \n"
            + "Row #0: 423\n"
            + "Row #0: \n"
            + "Row #0: 617\n"
            + "Row #0: 491\n"
            + "Row #0: 422\n"
            + "Row #0: 350\n"
            + "Row #0: \n"
            + "Row #0: 470\n"
            + "Row #0: \n"
            + "Row #0: 1,138\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 515\n"
            + "Row #0: \n"
            + "Row #0: 511\n"
            + "Row #0: \n"
            + "Row #0: 642\n"
            + "Row #0: \n"
            + "Row #0: 258\n"
            + "Row #0: \n"
            + "Row #0: 1,013\n"
            + "Row #0: \n"
            + "Row #0: 323\n"
            + "Row #0: 335\n"
            + "Row #0: \n"
            + "Row #0: 398\n"
            + "Row #0: \n"
            + "Row #0: 248\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 358\n"
            + "Row #0: 389\n"
            + "Row #0: \n"
            + "Row #0: 541\n"
            + "Row #0: 443\n"
            + "Row #0: 779\n"
            + "Row #0: 530\n"
            + "Row #0: \n"
            + "Row #0: 514\n");
    }
}
// End NativeRangeTest.java