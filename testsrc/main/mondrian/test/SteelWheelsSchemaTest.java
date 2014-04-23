/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.spi.impl.FilterDynamicSchemaProcessor;
import mondrian.util.Bug;

import java.io.InputStream;
import java.util.Arrays;

public class SteelWheelsSchemaTest extends SteelWheelsTestCase {
    /**
     * Sanity check, that enumerates the Measures dimension.
     */
    public void testMeasures() {
        TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }
        testContext.assertAxisReturns(
            "Measures.Members",
            "[Measures].[Quantity]\n"
            + "[Measures].[Sales]");
    }

    /**
     * Test case for Infobright issue where [Markets].[All Markets].[Japan]
     * was not found but [Markets].[All Markets].[JAPAN] was OK.
     * (We've since dropped 'All Xxx' from member unique names.)
     */
    public void testMarkets() {
        TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }
        testContext.assertQueryReturns(
            "select [Markets].[Markets].[All Markets].[Japan] on 0 from [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Markets].[Markets].[Japan]}\n"
            + "Row #0: 4,923\n");

        testContext.assertQueryReturns(
            "select [Markets].[Markets].Children on 0 from [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Markets].[Markets].[#null]}\n"
            + "{[Markets].[Markets].[APAC]}\n"
            + "{[Markets].[Markets].[EMEA]}\n"
            + "{[Markets].[Markets].[Japan]}\n"
            + "{[Markets].[Markets].[NA]}\n"
            + "Row #0: \n"
            + "Row #0: 12,878\n"
            + "Row #0: 49,578\n"
            + "Row #0: 4,923\n"
            + "Row #0: 37,952\n");
    }

    public void testMarkets2() {
        TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }
        testContext.assertQueryReturns(
            "select Subset([Markets].[Markets].Members, 130, 8) on 0 from [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Markets].[Markets].[EMEA].[UK].[Isle of Wight].[Cowes]}\n"
            + "{[Markets].[Markets].[Japan]}\n"
            + "{[Markets].[Markets].[Japan].[Hong Kong]}\n"
            + "{[Markets].[Markets].[Japan].[Hong Kong].[#null]}\n"
            + "{[Markets].[Markets].[Japan].[Hong Kong].[#null].[Central Hong Kong]}\n"
            + "{[Markets].[Markets].[Japan].[Japan]}\n"
            + "{[Markets].[Markets].[Japan].[Japan].[Osaka]}\n"
            + "{[Markets].[Markets].[Japan].[Japan].[Osaka].[Osaka]}\n"
            + "Row #0: 895\n"
            + "Row #0: 4,923\n"
            + "Row #0: 596\n"
            + "Row #0: 596\n"
            + "Row #0: 596\n"
            + "Row #0: 1,842\n"
            + "Row #0: 692\n"
            + "Row #0: 692\n");

        testContext.assertQueryReturns(
            "select [Markets].[Markets].[Territory].Members on 0 from "
            + "[SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Markets].[Markets].[#null]}\n"
            + "{[Markets].[Markets].[APAC]}\n"
            + "{[Markets].[Markets].[EMEA]}\n"
            + "{[Markets].[Markets].[Japan]}\n"
            + "{[Markets].[Markets].[NA]}\n"
            + "Row #0: \n"
            + "Row #0: 12,878\n"
            + "Row #0: 49,578\n"
            + "Row #0: 4,923\n"
            + "Row #0: 37,952\n");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-755">
     * MONDRIAN-755, "Getting drillthrough count results in exception"</a>.
     */
    public void testBugMondrian755() {
        TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }
        // One-dimensional query, using set and trivial calc member.
        checkCellZero(
            testContext,
            "With \n"
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Sales]', FORMAT_STRING = '#,###', SOLVE_ORDER=400\n"
            + "Select\n"
            + "[*BASE_MEMBERS_Measures] on columns\n"
            + "From [SteelWheelsSales]");

        // One-dimensional query, using trivial calc member.
        checkCellZero(
            testContext,
            "With \n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Sales]', FORMAT_STRING = '#,###', SOLVE_ORDER=400\n"
            + "Select\n"
            + " {[Measures].[*FORMATTED_MEASURE_0]} on columns\n"
            + "From [SteelWheelsSales]");

        // One-dimensional query, using simple calc member.
        checkCellZero(
            testContext,
            "With \n"
            + "Member [Measures].[Avg Price] as '[Measures].[Sales] / [Measures].[Quantity]', FORMAT_STRING = '#.##'\n"
            + "Select\n"
            + " {[Measures].[Avg Price]} on columns\n"
            + "From [SteelWheelsSales]");

        // Zero dim query
        checkCellZero(
            testContext,
            "Select\n"
            + "From [SteelWheelsSales]");

        // Zero dim query on calc member
        checkCellZero(
            testContext,
            "With \n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Sales]', FORMAT_STRING = '#,###', SOLVE_ORDER=400\n"
            + "Select\n"
            + "From [SteelWheelsSales]\n"
            + "Where [Measures].[*FORMATTED_MEASURE_0]");

        // Two-dimensional query, using trivial calc member.
        checkCellZero(
            testContext,
            "With \n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures]"
            + ".[Sales]', FORMAT_STRING = '#,###', SOLVE_ORDER=400\n"
            + "Select\n"
            + " {[Measures].[*FORMATTED_MEASURE_0]} on columns,"
            + " [Product].[All Products] * [Customers].[All Customers] on "
            + "rows\n"
            + "From [SteelWheelsSales]");
    }

    private void checkCellZero(TestContext testContext, String mdx) {
        final Result result = testContext.executeQuery(mdx);
        final Cell cell = result.getCell(new int[result.getAxes().length]);
        assertTrue(cell.canDrillThrough());
        assertEquals(2996, cell.getDrillThroughCount());
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-756">
     * MONDRIAN-756, "Error in RolapResult.replaceNonAllMembers leads to
     * NPE"</a>.
     *
     * @see #testBugMondrian805() duplicate bug MONDRIAN-805
     */
    public void testBugMondrian756() {
        TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }
        final Util.PropertyList propertyList =
            testContext.getConnectionProperties().clone();
        propertyList.put(
            RolapConnectionProperties.DynamicSchemaProcessor.name(),
            Mondrian756SchemaProcessor.class.getName());
        checkBugMondrian756(testContext.withProperties(propertyList));
    }

    private void checkBugMondrian756(TestContext testContext) {
        testContext.assertQueryReturns(
            "select NON EMPTY {[Measures].[Quantity]} ON COLUMNS,\n"
            + "NON EMPTY {[Markets].[APAC]} ON ROWS\n"
            + "from [SteelWheelsSales]\n"
            + "where [Time].[2004]",
            "Axis #0:\n"
            + "{[Time].[Time].[2004]}\n"
            + "Axis #1:\n"
            + "Axis #2:\n");
    }

    public static class Mondrian756SchemaProcessor
        extends FilterDynamicSchemaProcessor
    {
        @Override
        public String filter(
            String schemaUrl,
            Util.PropertyList connectInfo,
            InputStream stream) throws Exception
        {
            String schema = super.filter(schemaUrl, connectInfo, stream);
            return Util.replace(
                schema, " hasAll=\"true\"", " hasAll=\"false\"");
        }
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-756">
     * MONDRIAN-756, "Error in RolapResult.replaceNonAllMembers leads to
     * NPE"</a>.
     *
     * @see #testBugMondrian805() duplicate bug MONDRIAN-805
     */
    public void testBugMondrian756b() {
        final TestContext testContext0 = getTestContext();
        if (!testContext0.databaseIsValid()) {
            return;
        }
        String schema = testContext0.getRawSchema()
            .replaceAll(
                " hasAll=\"true\"",
                " hasAll=\"false\"");
        final TestContext testContext = testContext0.withSchema(schema);
        testContext.assertQueryReturns(
            "select NON EMPTY {[Measures].[Quantity]} ON COLUMNS,\n"
            + "NON EMPTY {[Markets].[Markets].[APAC]} ON ROWS\n"
            + "from [SteelWheelsSales]\n"
            + "where [Time].[Time].[2004]",
            "Axis #0:\n"
            + "{[Time].[Time].[2004]}\n"
            + "Axis #1:\n"
            + "Axis #2:\n");
    }

    /**
     * Test case for
     * bug <a href="http://jira.pentaho.com/browse/MONDRIAN-805">MONDRIAN-805,
     * "Two dimensions with hasAll=false fail"</a>.
     */
    public void testBugMondrian805() {
        final TestContext testContext0 = getTestContext();
        if (!testContext0.databaseIsValid()) {
            return;
        }
        String schema = testContext0.getRawSchema()
            .replaceAll(
                "<Hierarchy hasAll=\"true\" allMemberName=\"All Markets\" ",
                "<Hierarchy hasAll=\"false\" allMemberName=\"All Markets\" ")
            .replaceAll(
                "<Hierarchy hasAll=\"true\" allMemberName=\"All Status Types\" ",
                "<Hierarchy hasAll=\"false\" allMemberName=\"All Status Types\" ");
        final TestContext testContext = testContext0.withSchema(schema);
        testContext.assertQueryReturns(
            "select NON EMPTY {[Measures].[Quantity]} ON COLUMNS, \n"
            + "  NON EMPTY {([Markets].[APAC], [Customers].[All Customers], "
            + "[Product].[All Products], [Time].[All Years])} ON ROWS \n"
            + "from [SteelWheelsSales] \n"
            + "WHERE [Order Status].[Cancelled]",
            "Axis #0:\n"
            + "{[Order Status].[Order Status].[Cancelled]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Quantity]}\n"
            + "Axis #2:\n"
            + "{[Markets].[Markets].[APAC], [Customers].[Customers].[All Customers], [Product].[Product].[All Products], [Time].[Time].[All Years]}\n"
            + "Row #0: 596\n");

        // same query, pivoted
        testContext.assertQueryReturns(
            "select NON EMPTY {[Measures].[Quantity]} ON COLUMNS, \n"
            + "  NON EMPTY {([Customers].[All Customers], "
            + "[Product].[All Products], "
            + "[Time].[All Years], [Order Status].[Cancelled])} ON ROWS \n"
            + "from [SteelWheelsSales] \n"
            + "where [Markets].[APAC]",
            "Axis #0:\n"
            + "{[Markets].[Markets].[APAC]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Quantity]}\n"
            + "Axis #2:\n"
            + "{[Customers].[Customers].[All Customers], [Product].[Product].[All Products], [Time].[Time].[All Years], [Order Status].[Order Status].[Cancelled]}\n"
            + "Row #0: 596\n");
    }

    public void testMondrianBug476_770_957() throws Exception {
        final TestContext context =
            TestContext.instance().with(
                TestContext.DataSet.STEELWHEELS,
                "<Schema name=\"test_namecolumn\">"
                + "<Dimension type=\"StandardDimension\" highCardinality=\"false\" name=\"Markets\">"
                + "<Hierarchy hasAll=\"true\" allMemberName=\"All Markets\" primaryKey=\"CUSTOMERNUMBER\">"
                + "<Table name=\"CUSTOMER_W_TER\">\n"
                + "</Table>"
                + "<Level name=\"Territory\" column=\"TERRITORY\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level>"
                + "<Level name=\"Country\" column=\"COUNTRY\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level>"
                + "<Level name=\"State Province\" column=\"STATE\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level>"
                + "<Level name=\"City\" column=\"CITY\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level>"
                + "</Hierarchy>"
                + "</Dimension>"
                + "<Dimension type=\"StandardDimension\" highCardinality=\"false\" name=\"Customers\">"
                + "<Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKey=\"CUSTOMERNUMBER\">"
                + "<Table name=\"CUSTOMER_W_TER\">\n"
                + "</Table>"
                + "<Level name=\"Customer\" column=\"CUSTOMERNAME\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">"
                + "<Property name=\"Customer Number\" column=\"CUSTOMERNUMBER\" type=\"Numeric\">\n"
                + "</Property>"
                + "<Property name=\"Contact First Name\" column=\"CONTACTFIRSTNAME\" type=\"String\">\n"
                + "</Property>"
                + "<Property name=\"Contact Last Name\" column=\"CONTACTLASTNAME\" type=\"String\">\n"
                + "</Property>"
                + "<Property name=\"Phone\" column=\"PHONE\" type=\"String\">\n"
                + "</Property>"
                + "<Property name=\"Address\" column=\"ADDRESSLINE1\" type=\"String\">\n"
                + "</Property>"
                + "<Property name=\"Credit Limit\" column=\"CREDITLIMIT\" type=\"Numeric\">\n"
                + "</Property>"
                + "</Level>"
                + "</Hierarchy>"
                + "</Dimension>"
                + "<Dimension type=\"StandardDimension\" highCardinality=\"false\" name=\"Product\">"
                + "<Hierarchy hasAll=\"true\" allMemberName=\"All Products\" primaryKey=\"PRODUCTCODE\">"
                + "<Table name=\"PRODUCTS\">\n"
                + "</Table>"
                + "<Level name=\"Line\" table=\"PRODUCTS\" column=\"PRODUCTLINE\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level>"
                + "<Level name=\"Vendor\" table=\"PRODUCTS\" column=\"PRODUCTVENDOR\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level>"
                + "<Level name=\"Product\" table=\"PRODUCTS\" column=\"PRODUCTNAME\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">"
                + "<Property name=\"Code\" column=\"PRODUCTCODE\" type=\"String\">\n"
                + "</Property>"
                + "<Property name=\"Vendor\" column=\"PRODUCTVENDOR\" type=\"String\">\n"
                + "</Property>"
                + "<Property name=\"Description\" column=\"PRODUCTDESCRIPTION\" type=\"String\">\n"
                + "</Property>"
                + "</Level>"
                + "</Hierarchy>"
                + "</Dimension>"
                + "<Dimension type=\"TimeDimension\" highCardinality=\"false\" name=\"Time\">"
                + "<Hierarchy hasAll=\"true\" allMemberName=\"All Years\" primaryKey=\"TIME_ID\">"
                + "<Table name=\"time\">\n"
                + "</Table>"
                + "<Level name=\"Years\" column=\"YEAR_ID\" type=\"String\" uniqueMembers=\"true\" levelType=\"TimeYears\" hideMemberIf=\"Never\">\n"
                + "</Level>"
                + "<Level name=\"Quarters\" column=\"QTR_ID\" nameColumn=\"QTR_NAME\" ordinalColumn=\"QTR_ID\" type=\"String\" uniqueMembers=\"false\" levelType=\"TimeQuarters\" hideMemberIf=\"Never\">\n"
                + "</Level>"
                + "<Level name=\"Months\" column=\"MONTH_ID\" nameColumn=\"MONTH_NAME\" ordinalColumn=\"MONTH_ID\" type=\"String\" uniqueMembers=\"false\" levelType=\"TimeMonths\" hideMemberIf=\"Never\">\n"
                + "</Level>"
                + "</Hierarchy>"
                + "</Dimension>"
                + "<Dimension type=\"StandardDimension\" highCardinality=\"false\" name=\"Order Status\">"
                + "<Hierarchy hasAll=\"true\" allMemberName=\"All Status Types\" primaryKey=\"STATUS\">"
                + "<Level name=\"Type\" column=\"STATUS\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level>"
                + "</Hierarchy>"
                + "</Dimension>"
                + "<Cube name=\"SteelWheelsSales1\" cache=\"true\" enabled=\"true\">"
                + "<Table name=\"orderfact\">\n"
                + "</Table>"
                + "<DimensionUsage source=\"Markets\" name=\"Markets\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n</DimensionUsage>"
                + "<DimensionUsage source=\"Customers\" name=\"Customers\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n</DimensionUsage>"
                + "<DimensionUsage source=\"Product\" name=\"Product\" foreignKey=\"PRODUCTCODE\" highCardinality=\"false\">\n</DimensionUsage>"
                + "<DimensionUsage source=\"Time\" usagePrefix=\"TR_\" name=\"Time\" foreignKey=\"ORDERDATE\" highCardinality=\"false\">\n</DimensionUsage>"
                + "<Measure name=\"Price Each\" column=\"PRICEEACH\" formatString=\"#,###.0\" aggregator=\"sum\">\n</Measure>"
                + "<Measure name=\"Total Price\" column=\"TOTALPRICE\" formatString=\"#,###.00\" aggregator=\"sum\">\n</Measure>"
                + "</Cube>"
                + "<Cube name=\"SteelWheelsSales2\" cache=\"true\" enabled=\"true\">"
                + "<Table name=\"orderfact\">\n"
                + "</Table>"
                + "<DimensionUsage source=\"Markets\" name=\"Markets\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n"
                + "</DimensionUsage>"
                + "<DimensionUsage source=\"Customers\" name=\"Customers\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n"
                + "</DimensionUsage>"
                + "<DimensionUsage source=\"Product\" name=\"Product\" foreignKey=\"PRODUCTCODE\" highCardinality=\"false\">\n"
                + "</DimensionUsage>"
                + "<DimensionUsage source=\"Time\" usagePrefix=\"TC_\" name=\"Time\" foreignKey=\"REQUIREDDATE\" highCardinality=\"false\">\n"
                + "</DimensionUsage>"
                + "<Measure name=\"Price Each\" column=\"PRICEEACH\" formatString=\"#,###.0\" aggregator=\"sum\">\n"
                + "</Measure>"
                + "<Measure name=\"Total Price\" column=\"TOTALPRICE\" formatString=\"#,###.00\" aggregator=\"sum\">\n"
                + "</Measure>"
                + "</Cube>"
                + "<Cube name=\"SteelWheelsSales3\" cache=\"true\" enabled=\"true\">"
                + "<Table name=\"orderfact\">\n"
                + "</Table>"
                + "<DimensionUsage source=\"Markets\" name=\"Markets\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n"
                + "</DimensionUsage>"
                + "<DimensionUsage source=\"Customers\" name=\"Customers\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n"
                + "</DimensionUsage>"
                + "<DimensionUsage source=\"Product\" name=\"Product\" foreignKey=\"PRODUCTCODE\" highCardinality=\"false\">\n"
                + "</DimensionUsage>"
                + "<DimensionUsage source=\"Time\" usagePrefix=\"TW_\" name=\"Time\" foreignKey=\"SHIPPEDDATE\" highCardinality=\"false\">\n"
                + "</DimensionUsage>"
                + "<Measure name=\"Price Each\" column=\"PRICEEACH\" formatString=\"#,###.0\" aggregator=\"sum\">\n"
                + "</Measure>"
                + "<Measure name=\"Total Price\" column=\"TOTALPRICE\" formatString=\"#,###.00\" aggregator=\"sum\">\n"
                + "</Measure>"
                + "</Cube>"
                + "</Schema>\n")
            .withCube("SteelWheelsSales1");
        final String mdxQuery =
            "with set [*NATIVE_CJ_SET] as 'Filter([*BASE_MEMBERS_Time], (NOT IsEmpty([Measures].[Price Each])))'\n"
            + "  set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS], Ancestor([Time].CurrentMember, [Time].[Years]).OrderKey, BASC, [Time].CurrentMember.OrderKey, BASC)'\n"
            + "  set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "  set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {[Time].CurrentMember})'\n"
            + "  set [*BASE_MEMBERS_Time] as '[Time].[Quarters].Members'\n"
            + "  set [*CJ_COL_AXIS] as '[*NATIVE_CJ_SET]'\n"
            + "  member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Price Each]', FORMAT_STRING = \"#,###.0\", SOLVE_ORDER = 400.0\n"
            + "select [*BASE_MEMBERS_Measures] ON COLUMNS,\n"
            + "  [*SORTED_ROW_AXIS] ON ROWS\n"
            + "from [SteelWheelsSales2]\n";
        if (!context.databaseIsValid()) {
            return;
        }
        context.assertQueryReturns(
            mdxQuery,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[2003].[QTR1]}\n"
            + "{[Time].[Time].[2003].[QTR2]}\n"
            + "{[Time].[Time].[2003].[QTR3]}\n"
            + "{[Time].[Time].[2003].[QTR4]}\n"
            + "{[Time].[Time].[2004].[QTR1]}\n"
            + "{[Time].[Time].[2004].[QTR2]}\n"
            + "{[Time].[Time].[2004].[QTR3]}\n"
            + "{[Time].[Time].[2004].[QTR4]}\n"
            + "{[Time].[Time].[2005].[QTR1]}\n"
            + "{[Time].[Time].[2005].[QTR2]}\n"
            + "Row #0: 3,373.8\n"
            + "Row #1: 2,384.9\n"
            + "Row #2: 4,480.1\n"
            + "Row #3: 19,829.8\n"
            + "Row #4: 6,167.2\n"
            + "Row #5: 5,493.5\n"
            + "Row #6: 6,433.7\n"
            + "Row #7: 25,362.9\n"
            + "Row #8: 12,406.3\n"
            + "Row #9: 6,107.0\n");
    }

    public void testBugMondrian935() {
        final TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }
        testContext.assertQueryReturns(
            "with set [*NATIVE_CJ_SET] as '[*BASE_MEMBERS_Product]' \n"
            + "  set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS], "
            + "[Product].[Product].CurrentMember.OrderKey, BASC)' \n"
            + "  set [*BASE_MEMBERS_Product] as '[Product].[Line].Members' \n"
            + "  set [*BASE_MEMBERS_Measures] as '{[Measures].[*ZERO]}' \n"
            + "  set [*CJ_COL_AXIS] as '[*NATIVE_CJ_SET]' \n"
            + "  set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], "
            + "{[Product].CurrentMember})' \n"
            + "  member [Measures].[*ZERO] as '0.0', SOLVE_ORDER = 0.0 \n"
            + "select [*BASE_MEMBERS_Measures] ON COLUMNS, \n"
            + "  [*SORTED_ROW_AXIS] ON ROWS \n"
            + "from [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*ZERO]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[Classic Cars]}\n"
            + "{[Product].[Product].[Motorcycles]}\n"
            + "{[Product].[Product].[Planes]}\n"
            + "{[Product].[Product].[Ships]}\n"
            + "{[Product].[Product].[Trains]}\n"
            + "{[Product].[Product].[Trucks and Buses]}\n"
            + "{[Product].[Product].[Vintage Cars]}\n"
            + "Row #0: 0\n"
            + "Row #1: 0\n"
            + "Row #2: 0\n"
            + "Row #3: 0\n"
            + "Row #4: 0\n"
            + "Row #5: 0\n"
            + "Row #6: 0\n");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-626">
     * MONDRIAN-626, "DATE type Levels can cause errors with certain JDBC
     * drivers (e.g. Oracle 5/6)"</a>.
     *
     * <p>A Parameter type of date or timestamp
     * was causing an exception because those types were not implemented
     * correctly.</p>
     *
     * @throws Exception on error
     */
    public void testPropertyWithParameterOfTimestampType() throws Exception {
        final TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }
        TestContext context =
            getTestContext().with(
                TestContext.DataSet.STEELWHEELS,
                "<Schema name=\"FooBar\">\n"
                + "    <Cube name=\"Foo\">\n"
                + "        <Table name=\"orderfact\"></Table>\n"
                + "        <Dimension foreignKey=\"ORDERNUMBER\" name=\"Orders\">\n"
                + "            <Hierarchy hasAll=\"true\" allMemberName=\"All Orders\" primaryKey=\"ORDERNUMBER\">\n"
                + "                <Table name=\"orders\">\n"
                + "                </Table>\n"
                + "                <Level name=\"Order\" column=\"ORDERNUMBER\" type=\"Integer\" uniqueMembers=\"true\">\n"
                + "                    <Property name=\"OrderDate\" column=\"ORDERDATE\" type=\"Timestamp\"/>\n"
                + "                </Level>\n"
                + "            </Hierarchy>\n"
                + "        </Dimension>\n"
                + "        <Dimension foreignKey=\"CUSTOMERNUMBER\" name=\"Customers\">\n"
                + "            <Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKey=\"CUSTOMERNUMBER\">\n"
                + "                <Table name=\"customer_w_ter\">\n"
                + "                </Table>\n"
                + "                <Level name=\"Customer\" column=\"CUSTOMERNAME\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "                </Level>\n"
                + "            </Hierarchy>\n"
                + "        </Dimension>\n"
                + "        <Measure name=\"Quantity\" column=\"QUANTITYORDERED\" formatString=\"#,###\" aggregator=\"sum\">\n"
                + "        </Measure>\n"
                + "        <Measure name=\"Sales\" column=\"TOTALPRICE\" formatString=\"#,###\" aggregator=\"sum\">\n"
                + "        </Measure>\n"
                + "    </Cube>\n"
                + "</Schema>\n");
        context.assertQueryReturns(
            "with member [Measures].[Date] as 'Format([Orders].CurrentMember.Properties(\"OrderDate\"), \"yyyy-mm-dd\")'\n"
            + "select {[Orders].[Order].[10421]} on rows,\n"
            + "{[Measures].[Date]} on columns from [Foo]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Date]}\n"
            + "Axis #2:\n"
            + "{[Orders].[Orders].[10421]}\n"
            + "Row #0: 2005-05-29\n");
    }

    /**
     * Tests a query that is generated by Analyzer to query members. It should
     * only execute one SQL query, basically, "select year_id from time group by
     * year_id order by year_id". It should definitely not join to fact table.
     */
    public void testEsr1587() {
        final TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }
        testContext.assertQueryReturns(
            "with set [*NATIVE_CJ_SET] as '[*BASE_MEMBERS_Time]'\n"
            + "  set [*SORTED_COL_AXIS] as 'Order([*CJ_COL_AXIS], [Time].CurrentMember.OrderKey, BASC)'\n"
            + "  set [*BASE_MEMBERS_Measures] as '{[Measures].[*ZERO]}'\n"
            + "  set [*BASE_MEMBERS_Time] as 'TopCount([Time].[Years].Members, 200)'\n"
            + "  set [*CJ_COL_AXIS] as 'Generate([*NATIVE_CJ_SET], {[Time].CurrentMember})'\n"
            + "  member [Measures].[*ZERO] as '0', SOLVE_ORDER = 0\n"
            + "select Crossjoin([*SORTED_COL_AXIS], [*BASE_MEMBERS_Measures]) ON COLUMNS\n"
            + "from [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[2003], [Measures].[*ZERO]}\n"
            + "{[Time].[Time].[2004], [Measures].[*ZERO]}\n"
            + "{[Time].[Time].[2005], [Measures].[*ZERO]}\n"
            + "Row #0: 0\n"
            + "Row #0: 0\n"
            + "Row #0: 0\n");
    }

    public void testMondrian1133() {
        getTestContext().executeQuery(
            "With\n"
            + "Set [*NATIVE_CJ_SET] as 'Filter([*BASE_MEMBERS_Markets], Not IsEmpty ([Measures].[Sales]))'\n"
            + "Set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS],[Markets].CurrentMember.OrderKey,BASC,Ancestor([Markets].CurrentMember,[Markets].[Territory]).OrderKey,BASC)'\n"
            + "Set [*BASE_MEMBERS_Markets] as '[Markets].[Country].Members'\n"
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Markets].currentMember)})'\n"
            + "Set [*CJ_COL_AXIS] as '[*NATIVE_CJ_SET]'\n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Sales]', FORMAT_STRING = '#,###', SOLVE_ORDER=400\n"
            + "Select\n"
            + "[*BASE_MEMBERS_Measures] on columns,\n"
            + "[*SORTED_ROW_AXIS] on rows\n"
            + "From [SteelWheelsSales]\n");
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1285">MONDRIAN-1285</a>
     *
     * On MS SQL server only the MDX query would throw
     * java.lang.ClassCastException:
     * java.lang.Integer cannot be cast to java.lang.Double
     * keys.
     */
    public void testBug1285() {
        if (!getTestContext().databaseIsValid()) {
            return;
        }
        getTestContext().assertQueryReturns(
            "with set [*NATIVE_CJ_SET] as 'Filter(NonEmptyCrossJoin([*BASE_MEMBERS_Time], [*BASE_MEMBERS_Product]), (NOT IsEmpty([Measures].[Quantity])))'\n"
            + "  set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS], [Product].CurrentMember.OrderKey, BASC)'\n"
            + "  set [*SORTED_COL_AXIS] as 'Order([*CJ_COL_AXIS], [Time].CurrentMember.OrderKey, BASC)'\n"
            + "  set [*NATIVE_MEMBERS_Product] as 'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})'\n"
            + "  set [*BASE_MEMBERS_Product] as '[Product].[Line].Members'\n"
            + "  set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_1], [Measures].[*SUMMARY_MEASURE_0]}'\n"
            + "  set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})'\n"
            + "  set [*BASE_MEMBERS_Time] as '[Time].[Years].Members'\n"
            + "  set [*CJ_COL_AXIS] as 'Generate([*NATIVE_CJ_SET], {[Time].CurrentMember})'\n"
            + "  member [Measures].[*SUMMARY_MEASURE_0] as 'Rank([Product].CurrentMember, [*CJ_ROW_AXIS], [Measures].[Quantity])', FORMAT_STRING = \"#,##0\", SOLVE_ORDER = 100\n"
            + "  member [Product].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate({[Product].[All Products]})', SOLVE_ORDER = (- 100)\n"
            + "  member [Measures].[*FORMATTED_MEASURE_1] as '[Measures].[Quantity]', FORMAT_STRING = \"#,###\", SOLVE_ORDER = 400\n"
            + "select Crossjoin([*SORTED_COL_AXIS], [*BASE_MEMBERS_Measures]) ON COLUMNS,\n"
            + "  Union({[Product].[*TOTAL_MEMBER_SEL~AGG]}, [*SORTED_ROW_AXIS]) ON ROWS\n"
            + "from [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[2003], [Measures].[*FORMATTED_MEASURE_1]}\n"
            + "{[Time].[Time].[2003], [Measures].[*SUMMARY_MEASURE_0]}\n"
            + "{[Time].[Time].[2004], [Measures].[*FORMATTED_MEASURE_1]}\n"
            + "{[Time].[Time].[2004], [Measures].[*SUMMARY_MEASURE_0]}\n"
            + "{[Time].[Time].[2005], [Measures].[*FORMATTED_MEASURE_1]}\n"
            + "{[Time].[Time].[2005], [Measures].[*SUMMARY_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[*TOTAL_MEMBER_SEL~AGG]}\n"
            + "{[Product].[Product].[Classic Cars]}\n"
            + "{[Product].[Product].[Motorcycles]}\n"
            + "{[Product].[Product].[Planes]}\n"
            + "{[Product].[Product].[Ships]}\n"
            + "{[Product].[Product].[Trains]}\n"
            + "{[Product].[Product].[Trucks and Buses]}\n"
            + "{[Product].[Product].[Vintage Cars]}\n"
            + "Row #0: 36,439\n"
            + "Row #0: 1\n"
            + "Row #0: 49,417\n"
            + "Row #0: 1\n"
            + "Row #0: 19,475\n"
            + "Row #0: 1\n"
            + "Row #1: 12,762\n"
            + "Row #1: 1\n"
            + "Row #1: 16,085\n"
            + "Row #1: 1\n"
            + "Row #1: 6,705\n"
            + "Row #1: 1\n"
            + "Row #2: 4,031\n"
            + "Row #2: 4\n"
            + "Row #2: 5,906\n"
            + "Row #2: 3\n"
            + "Row #2: 2,771\n"
            + "Row #2: 3\n"
            + "Row #3: 3,833\n"
            + "Row #3: 5\n"
            + "Row #3: 5,820\n"
            + "Row #3: 4\n"
            + "Row #3: 2,207\n"
            + "Row #3: 4\n"
            + "Row #4: 2,844\n"
            + "Row #4: 6\n"
            + "Row #4: 4,309\n"
            + "Row #4: 6\n"
            + "Row #4: 1,346\n"
            + "Row #4: 6\n"
            + "Row #5: 1,000\n"
            + "Row #5: 7\n"
            + "Row #5: 1,409\n"
            + "Row #5: 7\n"
            + "Row #5: 409\n"
            + "Row #5: 7\n"
            + "Row #6: 4,056\n"
            + "Row #6: 3\n"
            + "Row #6: 5,024\n"
            + "Row #6: 5\n"
            + "Row #6: 1,921\n"
            + "Row #6: 5\n"
            + "Row #7: 7,913\n"
            + "Row #7: 2\n"
            + "Row #7: 10,864\n"
            + "Row #7: 2\n"
            + "Row #7: 4,116\n"
            + "Row #7: 2\n");
    }

    public void testDoubleValueCanBeRankedAmongIntegers() {
        if (!getTestContext().databaseIsValid()) {
            return;
        }
        getTestContext().assertQueryReturns(
            "with \n"
            + "  member [Product].[agg] as \n"
            + "    'Aggregate({[Product].[Line].[Motorcycles]})'\n"
            + "  member [Measures].[rank] as \n"
            + "    'Rank([Product].[agg], [Product].[Line].members, [Measures].[Quantity])'\n"
            + "select [Measures].[rank] on columns from [SteelWheelsSales]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[rank]}\n"
            + "Row #0: 3\n");
        getTestContext().assertQueryReturns(
            "with \n"
            + "  member [Product].[agg] as \n"
            + "    'Aggregate({[Product].[Line].[Motorcycles]})'\n"
            + "  member [Measures].[rank] as \n"
            + "    'Rank(([Time].[2003],[Product].[agg]),"
            + "          Crossjoin({[Time].[2003]},[Product].[Line].members),"
            + "          [Measures].[Quantity])'\n"
            + "select [Measures].[rank] on columns from [SteelWheelsSales]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[rank]}\n"
            + "Row #0: 4\n");
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1360">MONDRIAN-1360</a>
     *
     * Member Unique Name is incorrect for a calculated member on a shared
     * dimension whose name is different from the dimension it is based on.
     */
    public void testMondrian1360() {
        if (!Bug.BugMondrian1416Fixed) {
            return;
        }
        TestContext testContext = getTestContext().legacy().withSchema(
            "<Schema name='SteelWheels'>\n"
            + "    <Dimension type='StandardDimension' highCardinality='false' name='Product'>\n"
            + "        <Hierarchy hasAll='true' allMemberName='All Products' primaryKey='PRODUCTCODE'>\n"
            + "            <Table name='products'>\n"
            + "            </Table>\n"
            + "            <Level name='Line' table='products' column='PRODUCTLINE' type='String' uniqueMembers='false' levelType='Regular' hideMemberIf='Never'>\n"
            + "            </Level>\n"
            + "            <Level name='Vendor' table='products' column='PRODUCTVENDOR' type='String' uniqueMembers='false' levelType='Regular' hideMemberIf='Never'>\n"
            + "            </Level>\n"
            + "            <Level name='Product' table='products' column='PRODUCTNAME' type='String' uniqueMembers='true' levelType='Regular' hideMemberIf='Never'>\n"
            + "                <Property name='Code' column='PRODUCTCODE' type='String'>\n"
            + "                </Property>\n"
            + "                <Property name='Vendor' column='PRODUCTVENDOR' type='String'>\n"
            + "                </Property>\n"
            + "                <Property name='Description' column='PRODUCTDESCRIPTION' type='String'>\n"
            + "                </Property>\n"
            + "            </Level>\n"
            + "        </Hierarchy>\n"
            + "    </Dimension>\n"
            + "    <Cube name='SteelWheelsSales' cache='true' enabled='true'>\n"
            + "  <Table name='orderfact'>\n"
            + "  </Table>\n"
            + "  <Dimension foreignKey='CUSTOMERNUMBER' name='Markets'>\n"
            + "   <Hierarchy hasAll='true' allMemberName='All Markets' primaryKey='CUSTOMERNUMBER' primaryKeyTable=''>\n"
            + "    <Table name='customer_w_ter'>\n"
            + "    </Table>\n"
            + "    <Level name='Territory' column='TERRITORY' type='String' uniqueMembers='true' levelType='Regular' hideMemberIf='Never'>\n"
            + "    </Level>\n"
            + "    <Level name='Country' column='COUNTRY' type='String' uniqueMembers='true' levelType='Regular' hideMemberIf='Never'>\n"
            + "    </Level>\n"
            + "    <Level name='State Province' column='STATE' type='String' uniqueMembers='true' levelType='Regular' hideMemberIf='Never'>\n"
            + "    </Level>\n"
            + "    <Level name='City' column='CITY' type='String' uniqueMembers='true' levelType='Regular' hideMemberIf='Never'>\n"
            + "    </Level>\n"
            + "   </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension foreignKey='CUSTOMERNUMBER' name='Customers'>\n"
            + "   <Hierarchy hasAll='true' allMemberName='All Customers' primaryKey='CUSTOMERNUMBER'>\n"
            + "    <Table name='customer_w_ter'>\n"
            + "    </Table>\n"
            + "    <Level name='Customer' column='CUSTOMERNAME' type='String' uniqueMembers='true' levelType='Regular' hideMemberIf='Never'>\n"
            + "    </Level>\n"
            + "   </Hierarchy>\n"
            + "  </Dimension>\n"
            + "        <DimensionUsage source='Product' name='MyProduct' foreignKey='PRODUCTCODE'>\n"
            + "        </DimensionUsage>\n"
            + "        <Dimension type='TimeDimension' foreignKey='TIME_ID' name='Time'>\n"
            + "   <Hierarchy hasAll='true' allMemberName='All Years' primaryKey='TIME_ID'>\n"
            + "    <Table name='time'>\n"
            + "    </Table>\n"
            + "    <Level name='Years' column='YEAR_ID' type='String' uniqueMembers='true' levelType='TimeYears' hideMemberIf='Never'>\n"
            + "    </Level>\n"
            + "    <Level name='Quarters' column='QTR_NAME' ordinalColumn='QTR_ID' type='String' uniqueMembers='false' levelType='TimeQuarters' hideMemberIf='Never'>\n"
            + "    </Level>\n"
            + "    <Level name='Months' column='MONTH_NAME' ordinalColumn='MONTH_ID' type='String' uniqueMembers='false' levelType='TimeMonths' hideMemberIf='Never'>\n"
            + "    </Level>\n"
            + "   </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension foreignKey='STATUS' name='Order Status'>\n"
            + "   <Hierarchy hasAll='true' allMemberName='All Status Types' primaryKey='STATUS'>\n"
            + "    <Level name='Type' column='STATUS' type='String' uniqueMembers='true' levelType='Regular' hideMemberIf='Never'>\n"
            + "    </Level>\n"
            + "   </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name='Quantity' column='QUANTITYORDERED' formatString='#,###' aggregator='sum'>\n"
            + "  </Measure>\n"
            + "  <Measure name='Sales' column='TOTALPRICE' formatString='#,###' aggregator='sum'>\n"
            + "  </Measure>\n"
            + " </Cube>\n"
            + "</Schema>\n");

        testContext.assertQueryReturns(
            "WITH \n"
            + "SET [*NATIVE_CJ_SET] AS 'FILTER([*BASE_MEMBERS_MyProduct], NOT ISEMPTY ([Measures].[Sales]))' \n"
            + "SET [*SORTED_ROW_AXIS] AS "
            + "'ORDER([*CJ_ROW_AXIS],ANCESTOR([MyProduct].CURRENTMEMBER, [MyProduct].[Line]).ORDERKEY,BASC,[MyProduct].CURRENTMEMBER.ORDERKEY,BASC)' \n"
            + "SET [*BASE_MEMBERS_MyProduct] AS "
            + "'FILTER([MyProduct].[Vendor].MEMBERS,(ANCESTOR([MyProduct].CURRENTMEMBER, [MyProduct].[Line]) IN {[MyProduct].[Classic Cars]}) "
            + "AND ([MyProduct].CURRENTMEMBER IN {[MyProduct].[Classic Cars].[Autoart Studio Design]}))' \n"
            + "SET [*BASE_MEMBERS_Measures] AS '{[Measures].[*FORMATTED_MEASURE_0]}' \n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([MyProduct].CURRENTMEMBER)})' \n"
            + "SET [*NATIVE_MEMBERS_MyProduct] AS 'GENERATE([*NATIVE_CJ_SET], {[MyProduct].CURRENTMEMBER})' \n"
            + "SET [*CJ_COL_AXIS] AS '[*NATIVE_CJ_SET]' \n"
            + "MEMBER [MyProduct].[Classic Cars].[*TOTAL_MEMBER_SEL~AGG] AS "
            + "'AGGREGATE(FILTER([*NATIVE_MEMBERS_MyProduct],ANCESTOR([MyProduct].CURRENTMEMBER, [MyProduct].[Line]) "
            + "IS [MyProduct].[Classic Cars]))', SOLVE_ORDER=-100 \n"
            + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Sales]', FORMAT_STRING = '#,###', SOLVE_ORDER=400 \n"
            + "SELECT \n"
            + "[*BASE_MEMBERS_Measures] ON COLUMNS \n"
            + ",UNION({[MyProduct].[Classic Cars].[*TOTAL_MEMBER_SEL~AGG]},[*SORTED_ROW_AXIS]) ON ROWS \n"
            + "FROM [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[MyProduct].[Classic Cars].[*TOTAL_MEMBER_SEL~AGG]}\n"
            + "{[MyProduct].[Classic Cars].[Autoart Studio Design]}\n"
            + "Row #0: 153,268\n"
            + "Row #1: 153,268\n");
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1464">MONDRIAN-1464</a>
     *
     * Invalid filter SQL generated on numeric column
     */
    public void testMondrian1464() {
        if (!getTestContext().databaseIsValid()) {
            return;
        }
        final PropertySaver propSaver = new PropertySaver();
        propSaver.set(MondrianProperties.instance().IgnoreInvalidMembers, true);
        propSaver.set(
            MondrianProperties.instance().IgnoreInvalidMembersDuringQuery,
            true);
        getTestContext().assertQueryReturns(
            "WITH \n"
            + "SET [*NATIVE_CJ_SET] AS '[*BASE_MEMBERS_Time]' \n"
            + "SET [*BASE_MEMBERS_Measures] AS '{[Measures].[*ZERO]}' \n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Time].CURRENTMEMBER)})' \n"
            + "SET [*BASE_MEMBERS_Time] AS 'FILTER([Time].[Quarters].MEMBERS,[Time].CURRENTMEMBER IN([Time].[PARAM].[qtr1] : [Time].[2003].[QTR2]))' \n"
            + "SET [*CJ_COL_AXIS] AS '[*NATIVE_CJ_SET]' \n"
            + "MEMBER [Measures].[*ZERO] AS '0', SOLVE_ORDER=0 \n"
            + "SELECT \n"
            + "[*BASE_MEMBERS_Measures] ON COLUMNS \n"
            + ",ORDER([*CJ_ROW_AXIS],[Time].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Time].CURRENTMEMBER,[Time].[Years]).ORDERKEY,BASC) ON ROWS \n"
            + "FROM [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*ZERO]}\n"
            + "Axis #2:\n");
        propSaver.reset();
    }

    /**
     * Members stored in cache after role filtering would get picked up by a
     * different role, resulting in fewer results or none at all.
     *
     * On .Members in NonEmpty, with mondrian.native.nonempty.enabled=true
     */
    public void testMondrian1252() throws Exception {
        if (!getTestContext().databaseIsValid()) {
            return;
        }
        final String roleDefs =
            "  <Role name=\"CUBE_SCHEMA_ALL\">\n "
            + "          <SchemaGrant access=\"all\" />\n "
            + "  </Role>\n "
            + "\n "
            + "  <Role name=\"CUBE_SALES_MINIMAL\">\n "
            + "          <SchemaGrant access=\"none\">\n "
            + "                  <CubeGrant cube=\"SteelWheelsSales\" access=\"all\">\n "
            + "                          <HierarchyGrant hierarchy=\"[Markets]\" access=\"none\"  />\n "
            + "                  </CubeGrant>\n "
            + "          </SchemaGrant>\n "
            + "  </Role>\n "
            + "  <Role name='DIM_MARKETAREA_MARKET_800'>\n "
            + "    <SchemaGrant access='none'>\n "
            + "        <CubeGrant cube='SteelWheelsSales' access='none'>\n "
            + "            <HierarchyGrant hierarchy='[Markets]'\n "
            + "                            access='custom' rollupPolicy=\"partial\"\n "
            + "                            topLevel='[Markets].[Markets].[Territory]'>\n "
            + "                <MemberGrant member='[Markets].[Markets].[Territory].[APAC]' access='all' />\n "
            + "            </HierarchyGrant>\n "
            + "        </CubeGrant>\n "
            + "    </SchemaGrant>\n "
            + "  </Role>\n "
            + "  <Role name='DIM_MARKETAREA_MARKET_850'>\n "
            + "    <SchemaGrant access='none'>\n "
            + "        <CubeGrant cube='SteelWheelsSales' access='none'>\n "
            + "            <HierarchyGrant hierarchy='[Markets]'\n "
            + "                            access='custom' rollupPolicy=\"partial\"\n "
            + "                            topLevel='[Markets].[Markets].[Territory]'>\n "
            + "                <MemberGrant member='[Markets].[Markets].[Territory].[EMEA]' access='all' />\n "
            + "            </HierarchyGrant>\n "
            + "        </CubeGrant>\n "
            + "    </SchemaGrant>\n "
            + "  </Role>\n ";

        TestContext ctx = getTestContext();
        // insert role definitions
        String swSchema = ctx.getRawSchema();
        int i = swSchema.indexOf("</Schema>");
        swSchema = swSchema.substring(0, i)
            + roleDefs
            + swSchema.substring(i);
        ctx = ctx.withSchema(swSchema);
        Schema schema = ctx.getConnection().getSchema();
        // get roles
        // TODO: another way to use composite roles
        Role minimal = schema.lookupRole("CUBE_SALES_MINIMAL");
        Role market_800 = RoleImpl.union(
            Arrays.asList(
                new Role[]{
                    minimal,
                    schema.lookupRole("DIM_MARKETAREA_MARKET_800")}));
        Role market_800_850 = RoleImpl.union(
            Arrays.asList(
                new Role[]{
                    minimal,
                    schema.lookupRole("DIM_MARKETAREA_MARKET_850"),
                    schema.lookupRole("DIM_MARKETAREA_MARKET_800")}));

        final String nonEmptyMembersQuery =
            "select \n "
            + " [Time].[Years].[2005]\n "
            + " * \n "
            + " {\n "
            + "   [Measures].[Quantity],\n "
            + "   [Measures].[Sales]\n "
            + " }\n "
            + " ON COLUMNS,\n "
            + " NON EMPTY \n "
            + "   [Markets].[Territory].Members\n "
            + " ON ROWS\n "
            + "from [SteelWheelsSales]";

        // [Markets].[Territory].Members got cached after role filter..
        ctx.getConnection().setRole(market_800);
        ctx.assertQueryReturns(
            nonEmptyMembersQuery,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[2005], [Measures].[Quantity]}\n"
            + "{[Time].[Time].[2005], [Measures].[Sales]}\n"
            + "Axis #2:\n"
            + "{[Markets].[Markets].[APAC]}\n"
            + "Row #0: 3,411\n"
            + "Row #0: 337,018\n");
        // ..and prevent EMEA from appearing in the results
        ctx.getConnection().setRole(market_800_850);
        ctx.assertQueryReturns(
            nonEmptyMembersQuery,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[2005], [Measures].[Quantity]}\n"
            + "{[Time].[Time].[2005], [Measures].[Sales]}\n"
            + "Axis #2:\n"
            + "{[Markets].[Markets].[APAC]}\n"
            + "{[Markets].[Markets].[EMEA]}\n"
            + "Row #0: 3,411\n"
            + "Row #0: 337,018\n"
            + "Row #1: 9,237\n"
            + "Row #1: 929,829\n");
    }
}

// End SteelWheelsSchemaTest.java
