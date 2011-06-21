/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import java.io.InputStream;

import mondrian.olap.Cell;
import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.spi.impl.FilterDynamicSchemaProcessor;

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
            "select [Markets].[Territory].Members on 0 from [SteelWheelsSales]",
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
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Sales]', FORMAT_STRING = '#,###', SOLVE_ORDER=400\n"
            + "Select\n"
            + " {[Measures].[*FORMATTED_MEASURE_0]} on columns,"
            + " [Product].[All Products] * [Customers].[All Customers] on rows\n"
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
     */
    public void testBugMondrian756() {
        TestContext testContext =
            new SteelWheelsTestContext(TestContext.instance()) {
                @Override
                public Util.PropertyList getFoodMartConnectionProperties() {
                    Util.PropertyList propertyList =
                        super.getFoodMartConnectionProperties();
                    propertyList.put(
                        RolapConnectionProperties.DynamicSchemaProcessor.name(),
                        Mondrian756SchemaProcessor.class.getName());
                    return propertyList;
                }
            };
        if (!testContext.databaseIsValid()) {
            return;
        }
        testContext.assertQueryReturns(
            "select NON EMPTY {[Measures].[Quantity]} ON COLUMNS,\n"
            + "NON EMPTY {[Markets].[APAC]} ON ROWS\n"
            + "from [SteelWheelsSales]\n"
            + "where [Time].[2004]",
            "Axis #0:\n"
            + "{[Time].[2004]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Quantity]}\n"
            + "Axis #2:\n"
            + "{[Markets].[APAC]}\n"
            + "Row #0: 5,938\n");
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

    public void testMondrianBug476_770_957() throws Exception {
        final TestContext context =
            SteelWheelsTestContext.create(
                "<Schema name=\"test_namecolumn\"><Dimension type=\"StandardDimension\" highCardinality=\"false\" name=\"Markets\"><Hierarchy hasAll=\"true\" allMemberName=\"All Markets\" primaryKey=\"CUSTOMERNUMBER\"><Table name=\"CUSTOMER_W_TER\">\n"
                + "</Table><Level name=\"Territory\" column=\"TERRITORY\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level><Level name=\"Country\" column=\"COUNTRY\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level><Level name=\"State Province\" column=\"STATE\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level><Level name=\"City\" column=\"CITY\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level></Hierarchy></Dimension><Dimension type=\"StandardDimension\" highCardinality=\"false\" name=\"Customers\"><Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKey=\"CUSTOMERNUMBER\"><Table name=\"CUSTOMER_W_TER\">\n"
                + "</Table><Level name=\"Customer\" column=\"CUSTOMERNAME\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\"><Property name=\"Customer Number\" column=\"CUSTOMERNUMBER\" type=\"Numeric\">\n"
                + "</Property><Property name=\"Contact First Name\" column=\"CONTACTFIRSTNAME\" type=\"String\">\n"
                + "</Property><Property name=\"Contact Last Name\" column=\"CONTACTLASTNAME\" type=\"String\">\n"
                + "</Property><Property name=\"Phone\" column=\"PHONE\" type=\"String\">\n"
                + "</Property><Property name=\"Address\" column=\"ADDRESSLINE1\" type=\"String\">\n"
                + "</Property><Property name=\"Credit Limit\" column=\"CREDITLIMIT\" type=\"Numeric\">\n"
                + "</Property></Level></Hierarchy></Dimension><Dimension type=\"StandardDimension\" highCardinality=\"false\" name=\"Product\"><Hierarchy hasAll=\"true\" allMemberName=\"All Products\" primaryKey=\"PRODUCTCODE\"><Table name=\"PRODUCTS\">\n"
                + "</Table><Level name=\"Line\" table=\"PRODUCTS\" column=\"PRODUCTLINE\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level><Level name=\"Vendor\" table=\"PRODUCTS\" column=\"PRODUCTVENDOR\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level><Level name=\"Product\" table=\"PRODUCTS\" column=\"PRODUCTNAME\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\"><Property name=\"Code\" column=\"PRODUCTCODE\" type=\"String\">\n"
                + "</Property><Property name=\"Vendor\" column=\"PRODUCTVENDOR\" type=\"String\">\n"
                + "</Property><Property name=\"Description\" column=\"PRODUCTDESCRIPTION\" type=\"String\">\n"
                + "</Property></Level></Hierarchy></Dimension><Dimension type=\"TimeDimension\" highCardinality=\"false\" name=\"Time\"><Hierarchy hasAll=\"true\" allMemberName=\"All Years\" primaryKey=\"TIME_ID\"><Table name=\"TIME\">\n"
                + "</Table><Level name=\"Years\" column=\"YEAR_ID\" type=\"String\" uniqueMembers=\"true\" levelType=\"TimeYears\" hideMemberIf=\"Never\">\n"
                + "</Level><Level name=\"Quarters\" column=\"QTR_ID\" nameColumn=\"QTR_NAME\" ordinalColumn=\"QTR_ID\" type=\"String\" uniqueMembers=\"false\" levelType=\"TimeQuarters\" hideMemberIf=\"Never\">\n"
                + "</Level><Level name=\"Months\" column=\"MONTH_ID\" nameColumn=\"MONTH_NAME\" ordinalColumn=\"MONTH_ID\" type=\"String\" uniqueMembers=\"false\" levelType=\"TimeMonths\" hideMemberIf=\"Never\">\n"
                + "</Level></Hierarchy></Dimension><Dimension type=\"StandardDimension\" highCardinality=\"false\" name=\"Order Status\"><Hierarchy hasAll=\"true\" allMemberName=\"All Status Types\" primaryKey=\"STATUS\"><Level name=\"Type\" column=\"STATUS\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "</Level></Hierarchy></Dimension><Cube name=\"SteelWheelsSales1\" cache=\"true\" enabled=\"true\"><Table name=\"ORDERFACT\">\n"
                + "</Table><DimensionUsage source=\"Markets\" name=\"Markets\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n"
                + "</DimensionUsage><DimensionUsage source=\"Customers\" name=\"Customers\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n"
                + "</DimensionUsage><DimensionUsage source=\"Product\" name=\"Product\" foreignKey=\"PRODUCTCODE\" highCardinality=\"false\">\n"
                + "</DimensionUsage><DimensionUsage source=\"Time\" usagePrefix=\"TR_\" name=\"Time\" foreignKey=\"ORDERDATE\" highCardinality=\"false\">\n"
                + "</DimensionUsage><Measure name=\"Price Each\" column=\"PRICEEACH\" formatString=\"#,###.0\" aggregator=\"sum\">\n"
                + "</Measure><Measure name=\"Total Price\" column=\"TOTALPRICE\" formatString=\"#,###.00\" aggregator=\"sum\">\n"
                + "</Measure></Cube><Cube name=\"SteelWheelsSales2\" cache=\"true\" enabled=\"true\"><Table name=\"ORDERFACT\">\n"
                + "</Table><DimensionUsage source=\"Markets\" name=\"Markets\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n"
                + "</DimensionUsage><DimensionUsage source=\"Customers\" name=\"Customers\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n"
                + "</DimensionUsage><DimensionUsage source=\"Product\" name=\"Product\" foreignKey=\"PRODUCTCODE\" highCardinality=\"false\">\n"
                + "</DimensionUsage><DimensionUsage source=\"Time\" usagePrefix=\"TC_\" name=\"Time\" foreignKey=\"REQUIREDDATE\" highCardinality=\"false\">\n"
                + "</DimensionUsage><Measure name=\"Price Each\" column=\"PRICEEACH\" formatString=\"#,###.0\" aggregator=\"sum\">\n"
                + "</Measure><Measure name=\"Total Price\" column=\"TOTALPRICE\" formatString=\"#,###.00\" aggregator=\"sum\">\n"
                + "</Measure></Cube><Cube name=\"SteelWheelsSales3\" cache=\"true\" enabled=\"true\"><Table name=\"ORDERFACT\">\n"
                + "</Table><DimensionUsage source=\"Markets\" name=\"Markets\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n"
                + "</DimensionUsage><DimensionUsage source=\"Customers\" name=\"Customers\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n"
                + "</DimensionUsage><DimensionUsage source=\"Product\" name=\"Product\" foreignKey=\"PRODUCTCODE\" highCardinality=\"false\">\n"
                + "</DimensionUsage><DimensionUsage source=\"Time\" usagePrefix=\"TW_\" name=\"Time\" foreignKey=\"SHIPPEDDATE\" highCardinality=\"false\">\n"
                + "</DimensionUsage><Measure name=\"Price Each\" column=\"PRICEEACH\" formatString=\"#,###.0\" aggregator=\"sum\">\n"
                + "</Measure><Measure name=\"Total Price\" column=\"TOTALPRICE\" formatString=\"#,###.00\" aggregator=\"sum\">\n"
                + "</Measure></Cube></Schema>\n");
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
        context.executeQuery(mdxQuery);
    }
}
// End SteelWheelsSchemaTest.java