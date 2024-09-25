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
// remberson, Jan 31, 2006
*/
package mondrian.olap;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

import org.olap4j.*;

import java.sql.SQLException;
import java.util.List;

public class HierarchyBugTest extends FoodMartTestCase {
    public HierarchyBugTest(String name) {
        super(name);
    }
    public HierarchyBugTest() {
        super();
    }

    /**
     * This is code that demonstrates a bug that appears when using
     * JPivot with the current version of Mondrian. With the previous
     * version of Mondrian (and JPivot), pre compilation Mondrian,
     * this was not a bug (or at least Mondrian did not have a null
     * hierarchy).
     * Here the Time dimension is not returned in axis == 0, rather
     * null is returned. This causes a NullPointer exception in JPivot
     * when it tries to access the (null) hierarchy's name.
     * If the Time hierarchy is miss named in the query string, then
     * the parse ought to pick it up.
     **/
    public void testNoHierarchy() {
        String queryString =
            "select NON EMPTY "
            + "Crossjoin(Hierarchize(Union({[Time].[Time].LastSibling}, "
            + "[Time].[Time].LastSibling.Children)), "
            + "{[Measures].[Unit Sales],      "
            + "[Measures].[Store Cost]}) ON columns, "
            + "NON EMPTY Hierarchize(Union({[Store].[All Stores]}, "
            + "[Store].[All Stores].Children)) ON rows "
            + "from [Sales]";

        Connection conn = getConnection();
        Query query = conn.parseQuery(queryString);

        String failStr = null;
        int len = query.getAxes().length;
        for (int i = 0; i < len; i++) {
            Hierarchy[] hs =
                query.getMdxHierarchiesOnAxis(
                    AxisOrdinal.StandardAxisOrdinal.forLogicalOrdinal(i));
            if (hs == null) {
            } else {
                for (Hierarchy h : hs) {
                    // This should NEVER be null, but it is.
                    if (h == null) {
                        failStr =
                            "Got a null Hierarchy, "
                            + "Should be Time Hierarchy";
                    }
                }
            }
        }
        if (failStr != null) {
            fail(failStr);
        }
    }

    /**
     * Test cases for <a href="http://jira.pentaho.com/browse/MONDRIAN-1126">
     * MONDRIAN-1126:
     * member getHierarchy vs. level.getHierarchy differences in Time Dimension
     * </a>
     */
    public void testNamesIdentitySsasCompatibleTimeHierarchy() {
        propSaver.set(
            MondrianProperties.instance().SsasCompatibleNaming, true);
        String mdxTime = "SELECT\n"
            + "   [Measures].[Unit Sales] ON COLUMNS,\n"
            + "   [Time].[Time].[Year].Members ON ROWS\n"
            + "FROM [Sales]";
        Result resultTime = executeQuery(mdxTime);
        verifyMemberLevelNamesIdentityMeasureAxis(
            resultTime.getAxes()[0], "[Measures]");
        verifyMemberLevelNamesIdentityDimAxis(
            resultTime.getAxes()[1], "[Time]");
        flushContextSchemaCache();
    }

    public void testNamesIdentitySsasCompatibleWeeklyHierarchy() {
        propSaver.set(
            MondrianProperties.instance().SsasCompatibleNaming, true);
        String mdxWeekly = "SELECT\n"
            + "   [Measures].[Unit Sales] ON COLUMNS,\n"
            + "   [Time].[Weekly].[Year].Members ON ROWS\n"
            + "FROM [Sales]";
        Result resultWeekly =
            getTestContext().withFreshConnection().executeQuery(mdxWeekly);
        verifyMemberLevelNamesIdentityMeasureAxis(
            resultWeekly.getAxes()[0], "[Measures]");
        verifyMemberLevelNamesIdentityDimAxis(
            resultWeekly.getAxes()[1], "[Time].[Weekly]");
        flushContextSchemaCache();
    }

    public void testNamesIdentitySsasInCompatibleTimeHierarchy() {
        // SsasCompatibleNaming defaults to false
        String mdxTime = "SELECT\n"
            + "   [Measures].[Unit Sales] ON COLUMNS,\n"
            + "   [Time].[Year].Members ON ROWS\n"
            + "FROM [Sales]";
        Result resultTime = executeQuery(mdxTime);
        verifyMemberLevelNamesIdentityMeasureAxis(
            resultTime.getAxes()[0], "[Measures]");
        verifyMemberLevelNamesIdentityDimAxis(
            resultTime.getAxes()[1], "[Time]");
        flushContextSchemaCache();
    }

    public void testNamesIdentitySsasInCompatibleWeeklyHierarchy() {
        String mdxWeekly = "SELECT\n"
            + "   [Measures].[Unit Sales] ON COLUMNS,\n"
            + "   [Time.Weekly].[Year].Members ON ROWS\n"
            + "FROM [Sales]";
        Result resultWeekly =
            getTestContext().withFreshConnection().executeQuery(mdxWeekly);
        verifyMemberLevelNamesIdentityMeasureAxis(
            resultWeekly.getAxes()[0], "[Measures]");
        verifyMemberLevelNamesIdentityDimAxis(
            resultWeekly.getAxes()[1], "[Time.Weekly]");
        flushContextSchemaCache();
    }

    private String verifyMemberLevelNamesIdentityMeasureAxis(
        Axis axis, String expected)
    {
        OlapElement unitSales =
            axis.getPositions().get(0).get(0);
        String unitSalesHierarchyName =
            unitSales.getHierarchy().getUniqueName();
        assertEquals(expected, unitSalesHierarchyName);
        return unitSalesHierarchyName;
    }

    private void verifyMemberLevelNamesIdentityDimAxis(
        Axis axis, String expected)
    {
        Member year1997 = axis.getPositions().get(0).get(0);
        String year1997HierarchyName = year1997.getHierarchy().getUniqueName();
        assertEquals(expected, year1997HierarchyName);
        Level year = year1997.getLevel();
        String yearHierarchyName = year.getHierarchy().getUniqueName();
        assertEquals(year1997HierarchyName, yearHierarchyName);
    }

    public void testNamesIdentitySsasCompatibleOlap4j() throws SQLException {
        propSaver.set(
            MondrianProperties.instance().SsasCompatibleNaming, true);
        verifyLevelMemberNamesIdentityOlap4jTimeHierarchy();
    }

    public void testNamesIdentitySsasInCompatibleOlap4j() throws SQLException {
        // SsasCompatibleNaming defaults to false
        verifyLevelMemberNamesIdentityOlap4jTimeHierarchy();
    }

    private void verifyLevelMemberNamesIdentityOlap4jTimeHierarchy()
        throws SQLException
    {
        // essential here, in time hierarchy, is hasAll="false"
        // so that we expect "[Time]"
        String mdx = "SELECT\n"
            + "   [Measures].[Unit Sales] ON COLUMNS,\n"
            + "   [Time].[Time].[Year].Members ON ROWS\n"
            + "FROM [Sales]";
        verifyLevelMemberNamesIdentityOlap4j(mdx, getTestContext(), "[Time]");
    }

    public void testNamesIdentitySsasCompatibleOlap4jWeekly()
        throws SQLException
    {
        propSaver.set(
            MondrianProperties.instance().SsasCompatibleNaming, true);
        String mdx = "SELECT\n"
            + "   [Measures].[Unit Sales] ON COLUMNS,\n"
            + "   [Time].[Weekly].[Year].Members ON ROWS\n"
            + "FROM [Sales]";
        verifyLevelMemberNamesIdentityOlap4j(
            mdx, getTestContext(), "[Time].[Weekly]");
    }

    public void testNamesIdentitySsasInCompatibleOlap4jWeekly()
        throws SQLException
    {
        // SsasCompatibleNaming defaults to false
        String mdx = "SELECT\n"
            + "   [Measures].[Unit Sales] ON COLUMNS,\n"
            + "   [Time.Weekly].[Year].Members ON ROWS\n"
            + "FROM [Sales]";
        verifyLevelMemberNamesIdentityOlap4j(
            mdx, getTestContext(), "[Time.Weekly]");
    }

    public void testNamesIdentitySsasCompatibleOlap4jDateDim()
        throws SQLException
    {
        propSaver.set(
            MondrianProperties.instance().SsasCompatibleNaming, true);
        verifyMemberLevelNamesIdentityOlap4jDateDim();
    }

    public void testNamesSsasInCompatibleOlap4jDateDim()
        throws SQLException
    {
        // SsasCompatibleNaming defaults to false
        verifyMemberLevelNamesIdentityOlap4jDateDim();
    }

    private void verifyMemberLevelNamesIdentityOlap4jDateDim()
        throws SQLException
    {
        String mdx =
            "SELECT\n"
            + "   [Measures].[Unit Sales] ON COLUMNS,\n"
            + "   [Date].[Date].[Year].Members ON ROWS\n"
            + "FROM [Sales]";
        TestContext context = getTestContext();
        String dateDim  =
            "<Dimension name=\"Date\" type=\"TimeDimension\" foreignKey=\"time_id\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
            + "          levelType=\"TimeYears\"/>\n"
            + "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n"
            + "          levelType=\"TimeQuarters\"/>\n"
            + "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n"
            + "          levelType=\"TimeMonths\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>";
        context = context.createSubstitutingCube("Sales", dateDim);
        verifyLevelMemberNamesIdentityOlap4j(mdx, context, "[Date]");
    }

    public void testNamesIdentitySsasCompatibleOlap4jDateWeekly()
        throws SQLException
    {
        propSaver.set(
            MondrianProperties.instance().SsasCompatibleNaming, true);
        String mdx = "SELECT\n"
            + "   [Measures].[Unit Sales] ON COLUMNS,\n"
            + "   [Date].[Weekly].[Year].Members ON ROWS\n"
            + "FROM [Sales]";
        verifyMemberLevelNamesIdentityOlap4jWeekly(mdx, "[Date].[Weekly]");
    }

    public void testNamesIdentitySsasInCompatibleOlap4jDateDim()
        throws SQLException
    {
        // SsasCompatibleNaming defaults to false
        String mdx = "SELECT\n"
            + "   [Measures].[Unit Sales] ON COLUMNS,\n"
            + "   [Date.Weekly].[Year].Members ON ROWS\n"
            + "FROM [Sales]";
        verifyMemberLevelNamesIdentityOlap4jWeekly(mdx, "[Date.Weekly]");
    }

    private void verifyMemberLevelNamesIdentityOlap4jWeekly(
        String mdx, String expected) throws SQLException
    {
        TestContext context = getTestContext();
        String dateDim =
            "<Dimension name=\"Date\" type=\"TimeDimension\" foreignKey=\"time_id\">\n"
            + "    <Hierarchy hasAll=\"true\" name=\"Weekly\" primaryKey=\"time_id\">\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
            + "          levelType=\"TimeYears\"/>\n"
            + "      <Level name=\"Week\" column=\"week_of_year\" type=\"Numeric\" uniqueMembers=\"false\"\n"
            + "          levelType=\"TimeWeeks\"/>\n"
            + "      <Level name=\"Day\" column=\"day_of_month\" uniqueMembers=\"false\" type=\"Numeric\"\n"
            + "          levelType=\"TimeDays\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>";
        context = context.createSubstitutingCube("Sales", dateDim);
        verifyLevelMemberNamesIdentityOlap4j(mdx, context, expected);
    }

    private void verifyLevelMemberNamesIdentityOlap4j(
        String mdx, TestContext context, String expected) throws SQLException
    {
        CellSet result = context.executeOlap4jQuery(mdx);

        List<org.olap4j.Position> positions =
            result.getAxes().get(1).getPositions();
        org.olap4j.metadata.Member year1997 =
            positions.get(0).getMembers().get(0);
        String year1997HierarchyName = year1997.getHierarchy().getUniqueName();
        assertEquals(expected, year1997HierarchyName);

        org.olap4j.metadata.Level year = year1997.getLevel();
        String yearHierarchyName = year.getHierarchy().getUniqueName();
        assertEquals(year1997HierarchyName, yearHierarchyName);
        flushContextSchemaCache();
    }

    private void flushContextSchemaCache() {
        getTestContext().flushSchemaCache();
    }
}

// End HierarchyBugTest.java
