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

public class NativeDateKeyRangeTest extends BatchTestCase {

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

    public void testDateRangeOnAxis() throws Exception {
        propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
        final String mdx =
            "select {[TimeDate].[DateLevel].&[1942-10-08] : [TimeDate].[DateLevel].&[1967-06-20]} on 0\n"
            + "from [date_cube]";

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
        SqlPattern[] patterns = {
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
        SqlPattern[] patternsForChildren = {
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
        propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
        final String mdx =
            "select {[TimeStamp].[DateLevel].&[1995-01-01 00:00:00] : [TimeStamp].[DateLevel].&[1997-01-01 00:00:00]} on 0\n"
            + "from [date_cube]";

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
            + "    `employee`.`hire_date` = TIMESTAMP '1997-01-01 00:00:00'\n"
            + "group by\n"
            + "    `employee`.`hire_date`\n"
            + "order by\n"
            + "    ISNULL(`employee`.`hire_date`) ASC, `employee`.`hire_date` ASC";
        SqlPattern[] patterns = {
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
        SqlPattern[] patternsForChildren = {
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
}
// End NativeDateKeyRangeTest.java