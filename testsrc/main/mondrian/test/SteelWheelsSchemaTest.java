/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.spi.impl.FilterDynamicSchemaProcessor;

import java.io.InputStream;

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
            + "[Measures].[Sales]\n"
            + "[Measures].[Fact Count]");
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
            "select [Markets].[All Markets].[Japan] on 0 from [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Markets].[Japan]}\n"
            + "Row #0: 4,923\n");

        testContext.assertQueryReturns(
            "select [Markets].Children on 0 from [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Markets].[#null]}\n"
            + "{[Markets].[APAC]}\n"
            + "{[Markets].[EMEA]}\n"
            + "{[Markets].[Japan]}\n"
            + "{[Markets].[NA]}\n"
            + "Row #0: \n"
            + "Row #0: 12,878\n"
            + "Row #0: 49,578\n"
            + "Row #0: 4,923\n"
            + "Row #0: 37,952\n");

        testContext.assertQueryReturns(
            "select Subset([Markets].Members, 130, 8) on 0 from [SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Markets].[EMEA].[UK].[Isle of Wight].[Cowes]}\n"
            + "{[Markets].[Japan]}\n"
            + "{[Markets].[Japan].[Hong Kong]}\n"
            + "{[Markets].[Japan].[Hong Kong].[#null]}\n"
            + "{[Markets].[Japan].[Hong Kong].[#null].[Central Hong Kong]}\n"
            + "{[Markets].[Japan].[Japan]}\n"
            + "{[Markets].[Japan].[Japan].[Osaka]}\n"
            + "{[Markets].[Japan].[Japan].[Osaka].[Osaka]}\n"
            + "Row #0: 895\n"
            + "Row #0: 4,923\n"
            + "Row #0: 596\n"
            + "Row #0: 58,396\n"
            + "Row #0: 596\n"
            + "Row #0: 1,842\n"
            + "Row #0: 692\n"
            + "Row #0: 692\n");

        testContext.assertQueryReturns(
            "select [Markets].[Territory].Members on 0 from "
            + "[SteelWheelsSales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Markets].[#null]}\n"
            + "{[Markets].[APAC]}\n"
            + "{[Markets].[EMEA]}\n"
            + "{[Markets].[Japan]}\n"
            + "{[Markets].[NA]}\n"
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
        TestContext testContext0 = getTestContext();
        if (!testContext0.databaseIsValid()) {
            return;
        }
        final Util.PropertyList propertyList =
            testContext0.getConnectionProperties().clone();
        propertyList.put(
            RolapConnectionProperties.DynamicSchemaProcessor.name(),
            Mondrian756SchemaProcessor.class.getName());
        TestContext testContext = testContext0.withProperties(propertyList);
        testContext.assertQueryReturns(
            "select NON EMPTY {[Measures].[Quantity]} ON COLUMNS,\n"
            + "NON EMPTY {[Markets].[APAC]} ON ROWS\n"
            + "from [SteelWheelsSales]\n"
            + "where [Time].[2004]",
            "Axis #0:\n"
            + "{[Time].[2004]}\n"
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
            + "NON EMPTY {[Markets].[APAC]} ON ROWS\n"
            + "from [SteelWheelsSales]\n"
            + "where [Time].[2004]",
            "Axis #0:\n"
            + "{[Time].[2004]}\n"
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
            + "{[Order Status].[Cancelled]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Quantity]}\n"
            + "Axis #2:\n"
            + "{[Markets].[APAC], [Customers].[All Customers], [Product].[All Products], [Time].[All Years]}\n"
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
            + "{[Markets].[APAC]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Quantity]}\n"
            + "Axis #2:\n"
            + "{[Customers].[All Customers], [Product].[All Products], "
            + "[Time].[All Years], [Order Status].[Cancelled]}\n"
            + "Row #0: 596\n");
    }

    public void testMondrianBug476_770_957() throws Exception {
        final TestContext context =
            SteelWheelsTestCase.createContext(
                TestContext.instance(),
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
            + "{[Time].[2003].[QTR1]}\n"
            + "{[Time].[2003].[QTR2]}\n"
            + "{[Time].[2003].[QTR3]}\n"
            + "{[Time].[2003].[QTR4]}\n"
            + "{[Time].[2004].[QTR1]}\n"
            + "{[Time].[2004].[QTR2]}\n"
            + "{[Time].[2004].[QTR3]}\n"
            + "{[Time].[2004].[QTR4]}\n"
            + "{[Time].[2005].[QTR1]}\n"
            + "{[Time].[2005].[QTR2]}\n"
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
            + "[Product].CurrentMember.OrderKey, BASC)' \n"
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
            + "{[Product].[Classic Cars]}\n"
            + "{[Product].[Motorcycles]}\n"
            + "{[Product].[Planes]}\n"
            + "{[Product].[Ships]}\n"
            + "{[Product].[Trains]}\n"
            + "{[Product].[Trucks and Buses]}\n"
            + "{[Product].[Vintage Cars]}\n"
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
            createContext(
                getTestContext(),
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
            + "{[Orders].[10421]}\n"
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
            "Axis #0:\n" + "{}\n" + "Axis #1:\n"
            + "{[Time].[2003], [Measures].[*ZERO]}\n"
            + "{[Time].[2004], [Measures].[*ZERO]}\n"
            + "{[Time].[2005], [Measures].[*ZERO]}\n" + "Row #0: 0\n"
            + "Row #0: 0\n" + "Row #0: 0\n");
    }

    /**
     * Fix for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1197">MONDRIAN-1197</a>
     *
     * Not all members were created the same. Some were using null keys,
     * others RolapUtil.sqlNullValue. I've standardized the code with
     * RolapUtil.sqlNullValue and added assertions to verify the change.
     * I've also created a bunch of tests with null keys and I sort them
     * every which way to verify the correct ordering.
     */
    public void testMondrian1197() {
        final TestContext testContext = getTestContext();

        if (!testContext.databaseIsValid()) {
            return;
        }

        final String[] sortOrder =
            new String[] {
                "ASC",
                "DESC",
                "BASC",
                "BDESC"
            };

        final String[] results =
            new String[] {
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[*ZERO]}\n"
                + "Axis #2:\n"
                + "{[Markets].[#null].[Germany].[#null]}\n"
                + "{[Markets].[#null].[Ireland].[Co. Cork]}\n"
                + "{[Markets].[#null].[Israel].[#null]}\n"
                + "{[Markets].[#null].[Netherlands].[#null]}\n"
                + "{[Markets].[#null].[Poland].[#null]}\n"
                + "{[Markets].[#null].[Portugal].[#null]}\n"
                + "{[Markets].[#null].[Russia].[#null]}\n"
                + "{[Markets].[#null].[Singapore].[#null]}\n"
                + "{[Markets].[#null].[South Africa].[#null]}\n"
                + "{[Markets].[#null].[Spain].[#null]}\n"
                + "{[Markets].[#null].[Switzerland].[#null]}\n"
                + "{[Markets].[APAC].[Australia].[NSW]}\n"
                + "{[Markets].[APAC].[Australia].[Queensland]}\n"
                + "{[Markets].[APAC].[Australia].[Victoria]}\n"
                + "{[Markets].[APAC].[New Zealand].[#null]}\n"
                + "{[Markets].[APAC].[New Zealand].[]}\n"
                + "{[Markets].[APAC].[Singapore].[#null]}\n"
                + "{[Markets].[EMEA].[Austria].[#null]}\n"
                + "{[Markets].[EMEA].[Belgium].[#null]}\n"
                + "{[Markets].[EMEA].[Denmark].[#null]}\n"
                + "{[Markets].[EMEA].[Finland].[#null]}\n"
                + "{[Markets].[EMEA].[France].[#null]}\n"
                + "{[Markets].[EMEA].[Germany].[#null]}\n"
                + "{[Markets].[EMEA].[Ireland].[#null]}\n"
                + "{[Markets].[EMEA].[Italy].[#null]}\n"
                + "{[Markets].[EMEA].[Norway].[#null]}\n"
                + "{[Markets].[EMEA].[Spain].[#null]}\n"
                + "{[Markets].[EMEA].[Sweden].[#null]}\n"
                + "{[Markets].[EMEA].[Switzerland].[#null]}\n"
                + "{[Markets].[EMEA].[UK].[#null]}\n"
                + "{[Markets].[EMEA].[UK].[Isle of Wight]}\n"
                + "{[Markets].[Japan].[Hong Kong].[#null]}\n"
                + "{[Markets].[Japan].[Japan].[Osaka]}\n"
                + "{[Markets].[Japan].[Japan].[Tokyo]}\n"
                + "{[Markets].[Japan].[Philippines].[#null]}\n"
                + "{[Markets].[Japan].[Singapore].[#null]}\n"
                + "{[Markets].[NA].[Canada].[BC]}\n"
                + "{[Markets].[NA].[Canada].[Quu00e9bec]}\n"
                + "{[Markets].[NA].[USA].[CA]}\n"
                + "{[Markets].[NA].[USA].[CT]}\n"
                + "{[Markets].[NA].[USA].[MA]}\n"
                + "{[Markets].[NA].[USA].[NH]}\n"
                + "{[Markets].[NA].[USA].[NJ]}\n"
                + "{[Markets].[NA].[USA].[NV]}\n"
                + "{[Markets].[NA].[USA].[NY]}\n"
                + "{[Markets].[NA].[USA].[PA]}\n"
                + "Row #0: 0\n"
                + "Row #1: 0\n"
                + "Row #2: 0\n"
                + "Row #3: 0\n"
                + "Row #4: 0\n"
                + "Row #5: 0\n"
                + "Row #6: 0\n"
                + "Row #7: 0\n"
                + "Row #8: 0\n"
                + "Row #9: 0\n"
                + "Row #10: 0\n"
                + "Row #11: 0\n"
                + "Row #12: 0\n"
                + "Row #13: 0\n"
                + "Row #14: 0\n"
                + "Row #15: 0\n"
                + "Row #16: 0\n"
                + "Row #17: 0\n"
                + "Row #18: 0\n"
                + "Row #19: 0\n"
                + "Row #20: 0\n"
                + "Row #21: 0\n"
                + "Row #22: 0\n"
                + "Row #23: 0\n"
                + "Row #24: 0\n"
                + "Row #25: 0\n"
                + "Row #26: 0\n"
                + "Row #27: 0\n"
                + "Row #28: 0\n"
                + "Row #29: 0\n"
                + "Row #30: 0\n"
                + "Row #31: 0\n"
                + "Row #32: 0\n"
                + "Row #33: 0\n"
                + "Row #34: 0\n"
                + "Row #35: 0\n"
                + "Row #36: 0\n"
                + "Row #37: 0\n"
                + "Row #38: 0\n"
                + "Row #39: 0\n"
                + "Row #40: 0\n"
                + "Row #41: 0\n"
                + "Row #42: 0\n"
                + "Row #43: 0\n"
                + "Row #44: 0\n"
                + "Row #45: 0\n",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[*ZERO]}\n"
                + "Axis #2:\n"
                + "{[Markets].[NA].[USA].[PA]}\n"
                + "{[Markets].[NA].[USA].[NY]}\n"
                + "{[Markets].[NA].[USA].[NV]}\n"
                + "{[Markets].[NA].[USA].[NJ]}\n"
                + "{[Markets].[NA].[USA].[NH]}\n"
                + "{[Markets].[NA].[USA].[MA]}\n"
                + "{[Markets].[NA].[USA].[CT]}\n"
                + "{[Markets].[NA].[USA].[CA]}\n"
                + "{[Markets].[NA].[Canada].[Quu00e9bec]}\n"
                + "{[Markets].[NA].[Canada].[BC]}\n"
                + "{[Markets].[Japan].[Singapore].[#null]}\n"
                + "{[Markets].[Japan].[Philippines].[#null]}\n"
                + "{[Markets].[Japan].[Japan].[Tokyo]}\n"
                + "{[Markets].[Japan].[Japan].[Osaka]}\n"
                + "{[Markets].[Japan].[Hong Kong].[#null]}\n"
                + "{[Markets].[EMEA].[UK].[Isle of Wight]}\n"
                + "{[Markets].[EMEA].[UK].[#null]}\n"
                + "{[Markets].[EMEA].[Switzerland].[#null]}\n"
                + "{[Markets].[EMEA].[Sweden].[#null]}\n"
                + "{[Markets].[EMEA].[Spain].[#null]}\n"
                + "{[Markets].[EMEA].[Norway].[#null]}\n"
                + "{[Markets].[EMEA].[Italy].[#null]}\n"
                + "{[Markets].[EMEA].[Ireland].[#null]}\n"
                + "{[Markets].[EMEA].[Germany].[#null]}\n"
                + "{[Markets].[EMEA].[France].[#null]}\n"
                + "{[Markets].[EMEA].[Finland].[#null]}\n"
                + "{[Markets].[EMEA].[Denmark].[#null]}\n"
                + "{[Markets].[EMEA].[Belgium].[#null]}\n"
                + "{[Markets].[EMEA].[Austria].[#null]}\n"
                + "{[Markets].[APAC].[Singapore].[#null]}\n"
                + "{[Markets].[APAC].[New Zealand].[]}\n"
                + "{[Markets].[APAC].[New Zealand].[#null]}\n"
                + "{[Markets].[APAC].[Australia].[Victoria]}\n"
                + "{[Markets].[APAC].[Australia].[Queensland]}\n"
                + "{[Markets].[APAC].[Australia].[NSW]}\n"
                + "{[Markets].[#null].[Switzerland].[#null]}\n"
                + "{[Markets].[#null].[Spain].[#null]}\n"
                + "{[Markets].[#null].[South Africa].[#null]}\n"
                + "{[Markets].[#null].[Singapore].[#null]}\n"
                + "{[Markets].[#null].[Russia].[#null]}\n"
                + "{[Markets].[#null].[Portugal].[#null]}\n"
                + "{[Markets].[#null].[Poland].[#null]}\n"
                + "{[Markets].[#null].[Netherlands].[#null]}\n"
                + "{[Markets].[#null].[Israel].[#null]}\n"
                + "{[Markets].[#null].[Ireland].[Co. Cork]}\n"
                + "{[Markets].[#null].[Germany].[#null]}\n"
                + "Row #0: 0\n"
                + "Row #1: 0\n"
                + "Row #2: 0\n"
                + "Row #3: 0\n"
                + "Row #4: 0\n"
                + "Row #5: 0\n"
                + "Row #6: 0\n"
                + "Row #7: 0\n"
                + "Row #8: 0\n"
                + "Row #9: 0\n"
                + "Row #10: 0\n"
                + "Row #11: 0\n"
                + "Row #12: 0\n"
                + "Row #13: 0\n"
                + "Row #14: 0\n"
                + "Row #15: 0\n"
                + "Row #16: 0\n"
                + "Row #17: 0\n"
                + "Row #18: 0\n"
                + "Row #19: 0\n"
                + "Row #20: 0\n"
                + "Row #21: 0\n"
                + "Row #22: 0\n"
                + "Row #23: 0\n"
                + "Row #24: 0\n"
                + "Row #25: 0\n"
                + "Row #26: 0\n"
                + "Row #27: 0\n"
                + "Row #28: 0\n"
                + "Row #29: 0\n"
                + "Row #30: 0\n"
                + "Row #31: 0\n"
                + "Row #32: 0\n"
                + "Row #33: 0\n"
                + "Row #34: 0\n"
                + "Row #35: 0\n"
                + "Row #36: 0\n"
                + "Row #37: 0\n"
                + "Row #38: 0\n"
                + "Row #39: 0\n"
                + "Row #40: 0\n"
                + "Row #41: 0\n"
                + "Row #42: 0\n"
                + "Row #43: 0\n"
                + "Row #44: 0\n"
                + "Row #45: 0\n",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[*ZERO]}\n"
                + "Axis #2:\n"
                + "{[Markets].[EMEA].[Austria].[#null]}\n"
                + "{[Markets].[EMEA].[Belgium].[#null]}\n"
                + "{[Markets].[EMEA].[Denmark].[#null]}\n"
                + "{[Markets].[EMEA].[Finland].[#null]}\n"
                + "{[Markets].[EMEA].[France].[#null]}\n"
                + "{[Markets].[#null].[Germany].[#null]}\n"
                + "{[Markets].[EMEA].[Germany].[#null]}\n"
                + "{[Markets].[Japan].[Hong Kong].[#null]}\n"
                + "{[Markets].[EMEA].[Ireland].[#null]}\n"
                + "{[Markets].[#null].[Israel].[#null]}\n"
                + "{[Markets].[EMEA].[Italy].[#null]}\n"
                + "{[Markets].[#null].[Netherlands].[#null]}\n"
                + "{[Markets].[APAC].[New Zealand].[#null]}\n"
                + "{[Markets].[EMEA].[Norway].[#null]}\n"
                + "{[Markets].[Japan].[Philippines].[#null]}\n"
                + "{[Markets].[#null].[Poland].[#null]}\n"
                + "{[Markets].[#null].[Portugal].[#null]}\n"
                + "{[Markets].[#null].[Russia].[#null]}\n"
                + "{[Markets].[#null].[Singapore].[#null]}\n"
                + "{[Markets].[APAC].[Singapore].[#null]}\n"
                + "{[Markets].[Japan].[Singapore].[#null]}\n"
                + "{[Markets].[#null].[South Africa].[#null]}\n"
                + "{[Markets].[#null].[Spain].[#null]}\n"
                + "{[Markets].[EMEA].[Spain].[#null]}\n"
                + "{[Markets].[EMEA].[Sweden].[#null]}\n"
                + "{[Markets].[#null].[Switzerland].[#null]}\n"
                + "{[Markets].[EMEA].[Switzerland].[#null]}\n"
                + "{[Markets].[EMEA].[UK].[#null]}\n"
                + "{[Markets].[APAC].[New Zealand].[]}\n"
                + "{[Markets].[NA].[Canada].[BC]}\n"
                + "{[Markets].[NA].[USA].[CA]}\n"
                + "{[Markets].[#null].[Ireland].[Co. Cork]}\n"
                + "{[Markets].[NA].[USA].[CT]}\n"
                + "{[Markets].[EMEA].[UK].[Isle of Wight]}\n"
                + "{[Markets].[NA].[USA].[MA]}\n"
                + "{[Markets].[NA].[USA].[NH]}\n"
                + "{[Markets].[NA].[USA].[NJ]}\n"
                + "{[Markets].[APAC].[Australia].[NSW]}\n"
                + "{[Markets].[NA].[USA].[NV]}\n"
                + "{[Markets].[NA].[USA].[NY]}\n"
                + "{[Markets].[Japan].[Japan].[Osaka]}\n"
                + "{[Markets].[NA].[USA].[PA]}\n"
                + "{[Markets].[APAC].[Australia].[Queensland]}\n"
                + "{[Markets].[NA].[Canada].[Quu00e9bec]}\n"
                + "{[Markets].[Japan].[Japan].[Tokyo]}\n"
                + "{[Markets].[APAC].[Australia].[Victoria]}\n"
                + "Row #0: 0\n"
                + "Row #1: 0\n"
                + "Row #2: 0\n"
                + "Row #3: 0\n"
                + "Row #4: 0\n"
                + "Row #5: 0\n"
                + "Row #6: 0\n"
                + "Row #7: 0\n"
                + "Row #8: 0\n"
                + "Row #9: 0\n"
                + "Row #10: 0\n"
                + "Row #11: 0\n"
                + "Row #12: 0\n"
                + "Row #13: 0\n"
                + "Row #14: 0\n"
                + "Row #15: 0\n"
                + "Row #16: 0\n"
                + "Row #17: 0\n"
                + "Row #18: 0\n"
                + "Row #19: 0\n"
                + "Row #20: 0\n"
                + "Row #21: 0\n"
                + "Row #22: 0\n"
                + "Row #23: 0\n"
                + "Row #24: 0\n"
                + "Row #25: 0\n"
                + "Row #26: 0\n"
                + "Row #27: 0\n"
                + "Row #28: 0\n"
                + "Row #29: 0\n"
                + "Row #30: 0\n"
                + "Row #31: 0\n"
                + "Row #32: 0\n"
                + "Row #33: 0\n"
                + "Row #34: 0\n"
                + "Row #35: 0\n"
                + "Row #36: 0\n"
                + "Row #37: 0\n"
                + "Row #38: 0\n"
                + "Row #39: 0\n"
                + "Row #40: 0\n"
                + "Row #41: 0\n"
                + "Row #42: 0\n"
                + "Row #43: 0\n"
                + "Row #44: 0\n"
                + "Row #45: 0\n",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[*ZERO]}\n"
                + "Axis #2:\n"
                + "{[Markets].[APAC].[Australia].[Victoria]}\n"
                + "{[Markets].[Japan].[Japan].[Tokyo]}\n"
                + "{[Markets].[NA].[Canada].[Quu00e9bec]}\n"
                + "{[Markets].[APAC].[Australia].[Queensland]}\n"
                + "{[Markets].[NA].[USA].[PA]}\n"
                + "{[Markets].[Japan].[Japan].[Osaka]}\n"
                + "{[Markets].[NA].[USA].[NY]}\n"
                + "{[Markets].[NA].[USA].[NV]}\n"
                + "{[Markets].[APAC].[Australia].[NSW]}\n"
                + "{[Markets].[NA].[USA].[NJ]}\n"
                + "{[Markets].[NA].[USA].[NH]}\n"
                + "{[Markets].[NA].[USA].[MA]}\n"
                + "{[Markets].[EMEA].[UK].[Isle of Wight]}\n"
                + "{[Markets].[NA].[USA].[CT]}\n"
                + "{[Markets].[#null].[Ireland].[Co. Cork]}\n"
                + "{[Markets].[NA].[USA].[CA]}\n"
                + "{[Markets].[NA].[Canada].[BC]}\n"
                + "{[Markets].[APAC].[New Zealand].[]}\n"
                + "{[Markets].[EMEA].[UK].[#null]}\n"
                + "{[Markets].[#null].[Switzerland].[#null]}\n"
                + "{[Markets].[EMEA].[Switzerland].[#null]}\n"
                + "{[Markets].[EMEA].[Sweden].[#null]}\n"
                + "{[Markets].[#null].[Spain].[#null]}\n"
                + "{[Markets].[EMEA].[Spain].[#null]}\n"
                + "{[Markets].[#null].[South Africa].[#null]}\n"
                + "{[Markets].[#null].[Singapore].[#null]}\n"
                + "{[Markets].[APAC].[Singapore].[#null]}\n"
                + "{[Markets].[Japan].[Singapore].[#null]}\n"
                + "{[Markets].[#null].[Russia].[#null]}\n"
                + "{[Markets].[#null].[Portugal].[#null]}\n"
                + "{[Markets].[#null].[Poland].[#null]}\n"
                + "{[Markets].[Japan].[Philippines].[#null]}\n"
                + "{[Markets].[EMEA].[Norway].[#null]}\n"
                + "{[Markets].[APAC].[New Zealand].[#null]}\n"
                + "{[Markets].[#null].[Netherlands].[#null]}\n"
                + "{[Markets].[EMEA].[Italy].[#null]}\n"
                + "{[Markets].[#null].[Israel].[#null]}\n"
                + "{[Markets].[EMEA].[Ireland].[#null]}\n"
                + "{[Markets].[Japan].[Hong Kong].[#null]}\n"
                + "{[Markets].[#null].[Germany].[#null]}\n"
                + "{[Markets].[EMEA].[Germany].[#null]}\n"
                + "{[Markets].[EMEA].[France].[#null]}\n"
                + "{[Markets].[EMEA].[Finland].[#null]}\n"
                + "{[Markets].[EMEA].[Denmark].[#null]}\n"
                + "{[Markets].[EMEA].[Belgium].[#null]}\n"
                + "{[Markets].[EMEA].[Austria].[#null]}\n"
                + "Row #0: 0\n"
                + "Row #1: 0\n"
                + "Row #2: 0\n"
                + "Row #3: 0\n"
                + "Row #4: 0\n"
                + "Row #5: 0\n"
                + "Row #6: 0\n"
                + "Row #7: 0\n"
                + "Row #8: 0\n"
                + "Row #9: 0\n"
                + "Row #10: 0\n"
                + "Row #11: 0\n"
                + "Row #12: 0\n"
                + "Row #13: 0\n"
                + "Row #14: 0\n"
                + "Row #15: 0\n"
                + "Row #16: 0\n"
                + "Row #17: 0\n"
                + "Row #18: 0\n"
                + "Row #19: 0\n"
                + "Row #20: 0\n"
                + "Row #21: 0\n"
                + "Row #22: 0\n"
                + "Row #23: 0\n"
                + "Row #24: 0\n"
                + "Row #25: 0\n"
                + "Row #26: 0\n"
                + "Row #27: 0\n"
                + "Row #28: 0\n"
                + "Row #29: 0\n"
                + "Row #30: 0\n"
                + "Row #31: 0\n"
                + "Row #32: 0\n"
                + "Row #33: 0\n"
                + "Row #34: 0\n"
                + "Row #35: 0\n"
                + "Row #36: 0\n"
                + "Row #37: 0\n"
                + "Row #38: 0\n"
                + "Row #39: 0\n"
                + "Row #40: 0\n"
                + "Row #41: 0\n"
                + "Row #42: 0\n"
                + "Row #43: 0\n"
                + "Row #44: 0\n"
                + "Row #45: 0\n"
        };

        for (int i = 0; i < results.length; i++) {
            // Run this test 4 times, sorting differently each time.
            final String mdx =
                "With\n"
                + "Set [*NATIVE_CJ_SET] as '[*BASE_MEMBERS_Markets]'\n"
                + "Set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS],[Markets].CurrentMember.OrderKey,"
                + sortOrder[i]
                + ",Ancestor([Markets].CurrentMember,[Markets].[Country]).OrderKey,"
                + sortOrder[i]
                + ")'\n"
                + "Set [*BASE_MEMBERS_Markets] as '[Markets].[State Province].Members'\n"
                + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*ZERO]}'\n"
                + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Markets].currentMember)})'\n"
                + "Set [*CJ_COL_AXIS] as '[*NATIVE_CJ_SET]'\n"
                + "Member [Measures].[*ZERO] as '0', SOLVE_ORDER=0\n"
                + "Select\n"
                + "[*BASE_MEMBERS_Measures] on columns,\n"
                + "[*SORTED_ROW_AXIS] on rows\n"
                + "From [SteelWheelsSales]\n";
            testContext.assertQueryReturns(
                mdx,
                results[i]);
        }
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/ANALYZER-1259">ANALYZER-1259</a>
     *
     * Using the range operator on a level with null keys returned weird
     * results. This was because of a confusion of the ordering of null
     * keys.
     */
    public void testRangeSortWithNullKeys() {
        final String mdx =
            "With\n"
            + "Member [Measures].[*ZERO] as '0', SOLVE_ORDER=0\n"
            + "Select\n"
            + "{[Measures].[*ZERO]} on columns,\n"
            + "{[Markets].[#null].[Germany].[#null] : [Markets].[EMEA].[France].[#null]} on rows\n"
            + "From [SteelWheelsSales]\n";
        getTestContext().assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*ZERO]}\n"
            + "Axis #2:\n"
            + "{[Markets].[#null].[Germany].[#null]}\n"
            + "{[Markets].[#null].[Ireland].[#null]}\n"
            + "{[Markets].[#null].[Ireland].[Co. Cork]}\n"
            + "{[Markets].[#null].[Israel].[#null]}\n"
            + "{[Markets].[#null].[Netherlands].[#null]}\n"
            + "{[Markets].[#null].[Poland].[#null]}\n"
            + "{[Markets].[#null].[Portugal].[#null]}\n"
            + "{[Markets].[#null].[Russia].[#null]}\n"
            + "{[Markets].[#null].[Singapore].[#null]}\n"
            + "{[Markets].[#null].[South Africa].[#null]}\n"
            + "{[Markets].[#null].[Spain].[#null]}\n"
            + "{[Markets].[#null].[Switzerland].[#null]}\n"
            + "{[Markets].[APAC].[Australia].[NSW]}\n"
            + "{[Markets].[APAC].[Australia].[Queensland]}\n"
            + "{[Markets].[APAC].[Australia].[Victoria]}\n"
            + "{[Markets].[APAC].[New Zealand].[#null]}\n"
            + "{[Markets].[APAC].[New Zealand].[]}\n"
            + "{[Markets].[APAC].[Singapore].[#null]}\n"
            + "{[Markets].[EMEA].[Austria].[#null]}\n"
            + "{[Markets].[EMEA].[Belgium].[#null]}\n"
            + "{[Markets].[EMEA].[Denmark].[#null]}\n"
            + "{[Markets].[EMEA].[Finland].[#null]}\n"
            + "{[Markets].[EMEA].[France].[#null]}\n"
            + "Row #0: 0\n"
            + "Row #1: 0\n"
            + "Row #2: 0\n"
            + "Row #3: 0\n"
            + "Row #4: 0\n"
            + "Row #5: 0\n"
            + "Row #6: 0\n"
            + "Row #7: 0\n"
            + "Row #8: 0\n"
            + "Row #9: 0\n"
            + "Row #10: 0\n"
            + "Row #11: 0\n"
            + "Row #12: 0\n"
            + "Row #13: 0\n"
            + "Row #14: 0\n"
            + "Row #15: 0\n"
            + "Row #16: 0\n"
            + "Row #17: 0\n"
            + "Row #18: 0\n"
            + "Row #19: 0\n"
            + "Row #20: 0\n"
            + "Row #21: 0\n"
            + "Row #22: 0\n");
    }
}

// End SteelWheelsSchemaTest.java
