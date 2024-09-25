/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2019 Hitachi Vantara..  All rights reserved.
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.rolap.*;
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
            + "[Measures].[Sales]\n"
            + "[Measures].[Fact Count]");
    }

    public void testMondrian1273() {
        final String schema =
            "<Schema name=\"SteelWheels\">\n"
            + "  <Cube name=\"SteelWheelsSales\" cache=\"true\" enabled=\"true\">\n"
            + "    <Table name=\"orderfact\">\n"
            + "    </Table>\n"
            + "    <Dimension foreignKey=\"CUSTOMERNUMBER\" name=\"Markets\">\n"
            + "      <Hierarchy hasAll=\"true\" allMemberName=\"All Markets\" primaryKey=\"CUSTOMERNUMBER\" primaryKeyTable=\"\">\n"
            + "        <Table name=\"customer_w_ter\">\n"
            + "        </Table>\n"
            + "        <Level name=\"Territory\" column=\"TERRITORY\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\"\n"
            + "               hideMemberIf=\"Never\">\n"
            + "        </Level>\n"
            + "        <Level name=\"Country\" column=\"COUNTRY\" levelType=\"Regular\"\n"
            + "               hideMemberIf=\"Never\">\n"
            + "          <Annotations>\n"
            + "            <Annotation name=\"Data.Role\">Geography</Annotation>\n"
            + "            <Annotation name=\"Geo.Role\">country</Annotation>\n"
            + "          </Annotations>\n"
            + "        </Level>\n"
            + "        <Level name=\"State Province\" column=\"STATE\" type=\"String\" levelType=\"Regular\"\n"
            + "               hideMemberIf=\"Never\">\n"
            + "          <Annotations>\n"
            + "            <Annotation name=\"Data.Role\">Geography</Annotation>\n"
            + "            <Annotation name=\"Geo.Role\">state</Annotation>\n"
            + "            <Annotation name=\"Geo.RequiredParents\">country</Annotation>\n"
            + "          </Annotations>\n"
            + "        </Level>\n"
            + "        <Level name=\"City\" column=\"CITY\" type=\"String\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "          <Annotations>\n"
            + "            <Annotation name=\"Data.Role\">Geography</Annotation>\n"
            + "            <Annotation name=\"Geo.Role\">city</Annotation>\n"
            + "            <Annotation name=\"Geo.RequiredParents\">country,state</Annotation>\n"
            + "          </Annotations>\n"
            + "        </Level>\n"
            + "      </Hierarchy>\n"
            + "    </Dimension>\n"
            + "    <Dimension foreignKey=\"CUSTOMERNUMBER\" name=\"Customers\">\n"
            + "      <Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKey=\"CUSTOMERNUMBER\">\n"
            + "        <Table name=\"CUSTOMER_W_TER\">\n"
            + "        </Table>\n"
            + "        <Level name=\"Customer\" column=\"CUSTOMERNAME\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\"\n"
            + "               hideMemberIf=\"Never\">\n"
            + "          <Property name=\"Customer Number\" column=\"CUSTOMERNUMBER\" type=\"Numeric\"/>\n"
            + "          <Property name=\"Contact First Name\" column=\"CONTACTFIRSTNAME\" type=\"String\"/>\n"
            + "          <Property name=\"Contact Last Name\" column=\"CONTACTLASTNAME\" type=\"String\"/>\n"
            + "          <Property name=\"Phone\" column=\"PHONE\" type=\"String\"/>\n"
            + "          <Property name=\"Address\" column=\"ADDRESSLINE1\" type=\"String\"/>\n"
            + "          <Property name=\"Credit Limit\" column=\"CREDITLIMIT\" type=\"Numeric\"/>\n"
            + "        </Level>\n"
            + "      </Hierarchy>\n"
            + "    </Dimension>\n"
            + "    <Dimension foreignKey=\"PRODUCTCODE\" name=\"Product\">\n"
            + "      <Hierarchy name=\"\" hasAll=\"true\" allMemberName=\"All Products\" primaryKey=\"PRODUCTCODE\" primaryKeyTable=\"PRODUCTS\"\n"
            + "                 caption=\"\">\n"
            + "        <Table name=\"PRODUCTS\">\n"
            + "        </Table>\n"
            + "        <Level name=\"Line\" table=\"PRODUCTS\" column=\"PRODUCTLINE\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\"\n"
            + "               hideMemberIf=\"Never\">\n"
            + "        </Level>\n"
            + "        <Level name=\"Vendor\" table=\"PRODUCTS\" column=\"PRODUCTVENDOR\" type=\"String\" uniqueMembers=\"false\"\n"
            + "               levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "        </Level>\n"
            + "        <Level name=\"Product\" table=\"PRODUCTS\" column=\"PRODUCTNAME\" type=\"String\" uniqueMembers=\"true\"\n"
            + "               levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "          <Property name=\"Code\" column=\"PRODUCTCODE\" type=\"String\"/>\n"
            + "          <Property name=\"Vendor\" column=\"PRODUCTVENDOR\" type=\"String\"/>\n"
            + "          <Property name=\"Description\" column=\"PRODUCTDESCRIPTION\" type=\"String\"/>\n"
            + "        </Level>\n"
            + "      </Hierarchy>\n"
            + "    </Dimension>\n"
            + "    <Dimension type=\"TimeDimension\" foreignKey=\"TIME_ID\" name=\"Time\">\n"
            + "      <Hierarchy hasAll=\"true\" allMemberName=\"All Years\" primaryKey=\"TIME_ID\">\n"
            + "        <Table name=\"DIM_TIME\">\n"
            + "        </Table>\n"
            + "        <Level name=\"Years\" column=\"YEAR_ID\" type=\"String\" uniqueMembers=\"true\" levelType=\"TimeYears\"\n"
            + "               hideMemberIf=\"Never\">\n"
            + "          <Annotations>\n"
            + "            <Annotation name=\"AnalyzerDateFormat\">[yyyy]</Annotation>\n"
            + "          </Annotations>\n"
            + "        </Level>\n"
            + "        <Level name=\"Quarters\" column=\"QTR_NAME\" ordinalColumn=\"QTR_ID\" type=\"String\" uniqueMembers=\"false\"\n"
            + "               levelType=\"TimeQuarters\" hideMemberIf=\"Never\">\n"
            + "          <Annotations>\n"
            + "            <Annotation name=\"AnalyzerDateFormat\">[yyyy].['QTR'q]</Annotation>\n"
            + "          </Annotations>\n"
            + "        </Level>\n"
            + "        <Level name=\"Months\" column=\"MONTH_NAME\" ordinalColumn=\"MONTH_ID\" type=\"String\" uniqueMembers=\"false\"\n"
            + "               levelType=\"TimeMonths\" hideMemberIf=\"Never\">\n"
            + "          <Annotations>\n"
            + "            <Annotation name=\"AnalyzerDateFormat\">[yyyy].['QTR'q].[MMM]</Annotation>\n"
            + "          </Annotations>\n"
            + "        </Level>\n"
            + "      </Hierarchy>\n"
            + "    </Dimension>\n"
            + "    <Dimension foreignKey=\"STATUS\" name=\"Order Status\">\n"
            + "      <Hierarchy hasAll=\"true\" allMemberName=\"All Status Types\" primaryKey=\"STATUS\">\n"
            + "        <Level name=\"Type\" column=\"STATUS\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "        </Level>\n"
            + "      </Hierarchy>\n"
            + "    </Dimension>\n"
            + "    <Measure name=\"Quantity\" column=\"QUANTITYORDERED\" formatString=\"#,###\" aggregator=\"sum\">\n"
            + "      <Annotations>\n"
            + "        <Annotation name=\"AnalyzerBusinessGroup\">Measures</Annotation>\n"
            + "      </Annotations>\n"
            + "    </Measure>\n"
            + "    <Measure name=\"Sales\" column=\"TOTALPRICE\" formatString=\"#,###\" aggregator=\"sum\">\n"
            + "      <Annotations>\n"
            + "        <Annotation name=\"AnalyzerBusinessGroup\">Measures</Annotation>\n"
            + "      </Annotations>\n"
            + "    </Measure>\n"
            + "  </Cube>\n"
            + "  <Role name=\"dev\">\n"
            + "    <SchemaGrant access=\"all\">\n"
            + "      <CubeGrant cube=\"SteelWheelsSales\" access=\"all\">\n"
            + "        <HierarchyGrant hierarchy=\"[Markets]\" topLevel=\"[Markets].[Territory]\" bottomLevel=\"[Markets].[Country]\" rollupPolicy=\"Partial\" access=\"custom\">\n"
            + "          <MemberGrant member=\"[Markets].[APAC]\" access=\"all\"> </MemberGrant>\n"
            + "          <MemberGrant member=\"[Markets].[APAC].[Australia]\" access=\"none\"> </MemberGrant>\n"
            + "        </HierarchyGrant> \n"
            + "      </CubeGrant>\n"
            + "    </SchemaGrant>\n"
            + "    <SchemaGrant access=\"all\">\n"
            + "      <CubeGrant cube=\"SteelWheelsSales\" access=\"all\">\n"
            + "        <HierarchyGrant hierarchy=\"Measures\" access=\"custom\">\n"
            + "          <MemberGrant member=\"[Measures].[Quantity]\" access=\"none\"> </MemberGrant>\n"
            + "          <MemberGrant member=\"[Measures].[Sales]\" access=\"all\"> </MemberGrant>\n"
            + "        </HierarchyGrant>\n"
            + "      </CubeGrant>\n"
            + "    </SchemaGrant>\n"
            + "  </Role> \n"
            + "  <Role name=\"cto\">\n"
            + "    <SchemaGrant access=\"all\">\n"
            + "      <CubeGrant cube=\"SteelWheelsSales\" access=\"all\">\n"
            + "        <HierarchyGrant hierarchy=\"Measures\" access=\"custom\">\n"
            + "          <MemberGrant member=\"[Measures].[Quantity]\" access=\"none\"> </MemberGrant>\n"
            + "          <MemberGrant member=\"[Measures].[Sales]\" access=\"all\"> </MemberGrant>\n"
            + "        </HierarchyGrant>\n"
            + "      </CubeGrant>\n"
            + "    </SchemaGrant>\n"
            + "  </Role> \n"
            + "  <Role name=\"Admin\">\n"
            + "    <SchemaGrant access=\"all\">\n"
            + "      <CubeGrant cube=\"SteelWheelsSales\" access=\"all\"/>\n"
            + "    </SchemaGrant>\n"
            + "  </Role>\n"
            + "</Schema>\n";
        TestContext testContext =
            createContext(TestContext.instance(), schema)
                .withCube("SteelWheelsSales").withRole("dev");
        if (!testContext.databaseIsValid()) {
            return;
        }
        testContext.assertQueryReturns(
            "with set [*NATIVE_CJ_SET] as 'Filter([*BASE_MEMBERS_Markets], (NOT IsEmpty([Measures].[Sales])))'\n"
            + "  set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS], [Markets].CurrentMember.OrderKey, BASC)'\n"
            + "  set [*BASE_MEMBERS_Markets] as '[Markets].[Territory].Members'\n"
            + "  set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "  set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {[Markets].CurrentMember})'\n"
            + "  set [*CJ_COL_AXIS] as '[*NATIVE_CJ_SET]'\n"
            + "  member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Sales]', FORMAT_STRING = \"#,###\", SOLVE_ORDER = 400\n"
            + "select [*BASE_MEMBERS_Measures] ON COLUMNS,\n"
            + "  [*SORTED_ROW_AXIS] ON ROWS\n"
            + "from [SteelWheelsSales]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Markets].[APAC]}\n"
            + "Row #0: 651,083\n");
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

    public void testMondrian1133() {
        if (!getTestContext().databaseIsValid()) {
            return;
        }
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
        if (!getTestContext().databaseIsValid()) {
            return;
        }
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
            + "Row #21: 0\n");
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
            + "{[Time].[2003], [Measures].[*FORMATTED_MEASURE_1]}\n"
            + "{[Time].[2003], [Measures].[*SUMMARY_MEASURE_0]}\n"
            + "{[Time].[2004], [Measures].[*FORMATTED_MEASURE_1]}\n"
            + "{[Time].[2004], [Measures].[*SUMMARY_MEASURE_0]}\n"
            + "{[Time].[2005], [Measures].[*FORMATTED_MEASURE_1]}\n"
            + "{[Time].[2005], [Measures].[*SUMMARY_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Product].[*TOTAL_MEMBER_SEL~AGG]}\n"
            + "{[Product].[Classic Cars]}\n"
            + "{[Product].[Motorcycles]}\n"
            + "{[Product].[Planes]}\n"
            + "{[Product].[Ships]}\n"
            + "{[Product].[Trains]}\n"
            + "{[Product].[Trucks and Buses]}\n"
            + "{[Product].[Vintage Cars]}\n"
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
        TestContext testContext = getTestContext().withSchema(
            "<Schema name=\"SteelWheels\">\n"
            + "    <Dimension type=\"StandardDimension\" highCardinality=\"false\" name=\"Product\">\n"
            + "        <Hierarchy hasAll=\"true\" allMemberName=\"All Products\" primaryKey=\"PRODUCTCODE\">\n"
            + "            <Table name=\"products\">\n"
            + "            </Table>\n"
            + "            <Level name=\"Line\" table=\"products\" column=\"PRODUCTLINE\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "            </Level>\n"
            + "            <Level name=\"Vendor\" table=\"products\" column=\"PRODUCTVENDOR\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "            </Level>\n"
            + "            <Level name=\"Product\" table=\"products\" column=\"PRODUCTNAME\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "                <Property name=\"Code\" column=\"PRODUCTCODE\" type=\"String\">\n"
            + "                </Property>\n"
            + "                <Property name=\"Vendor\" column=\"PRODUCTVENDOR\" type=\"String\">\n"
            + "                </Property>\n"
            + "                <Property name=\"Description\" column=\"PRODUCTDESCRIPTION\" type=\"String\">\n"
            + "                </Property>\n"
            + "            </Level>\n"
            + "        </Hierarchy>\n"
            + "    </Dimension>\n"
            + "    <Cube name=\"SteelWheelsSales\" cache=\"true\" enabled=\"true\">\n"
            + "  <Table name=\"orderfact\">\n"
            + "  </Table>\n"
            + "  <Dimension foreignKey=\"CUSTOMERNUMBER\" name=\"Markets\">\n"
            + "   <Hierarchy hasAll=\"true\" allMemberName=\"All Markets\" primaryKey=\"CUSTOMERNUMBER\" primaryKeyTable=\"\">\n"
            + "    <Table name=\"customer_w_ter\">\n"
            + "    </Table>\n"
            + "    <Level name=\"Territory\" column=\"TERRITORY\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "    </Level>\n"
            + "    <Level name=\"Country\" column=\"COUNTRY\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "    </Level>\n"
            + "    <Level name=\"State Province\" column=\"STATE\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "    </Level>\n"
            + "    <Level name=\"City\" column=\"CITY\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "    </Level>\n"
            + "   </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension foreignKey=\"CUSTOMERNUMBER\" name=\"Customers\">\n"
            + "   <Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKey=\"CUSTOMERNUMBER\">\n"
            + "    <Table name=\"customer_w_ter\">\n"
            + "    </Table>\n"
            + "    <Level name=\"Customer\" column=\"CUSTOMERNAME\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "    </Level>\n"
            + "   </Hierarchy>\n"
            + "  </Dimension>\n"
            + "        <DimensionUsage source=\"Product\" name=\"MyProduct\" foreignKey=\"PRODUCTCODE\">\n"
            + "        </DimensionUsage>\n"
            + "        <Dimension type=\"TimeDimension\" foreignKey=\"TIME_ID\" name=\"Time\">\n"
            + "   <Hierarchy hasAll=\"true\" allMemberName=\"All Years\" primaryKey=\"TIME_ID\">\n"
            + "    <Table name=\"time\">\n"
            + "    </Table>\n"
            + "    <Level name=\"Years\" column=\"YEAR_ID\" type=\"String\" uniqueMembers=\"true\" levelType=\"TimeYears\" hideMemberIf=\"Never\">\n"
            + "    </Level>\n"
            + "    <Level name=\"Quarters\" column=\"QTR_NAME\" ordinalColumn=\"QTR_ID\" type=\"String\" uniqueMembers=\"false\" levelType=\"TimeQuarters\" hideMemberIf=\"Never\">\n"
            + "    </Level>\n"
            + "    <Level name=\"Months\" column=\"MONTH_NAME\" ordinalColumn=\"MONTH_ID\" type=\"String\" uniqueMembers=\"false\" levelType=\"TimeMonths\" hideMemberIf=\"Never\">\n"
            + "    </Level>\n"
            + "   </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension foreignKey=\"STATUS\" name=\"Order Status\">\n"
            + "   <Hierarchy hasAll=\"true\" allMemberName=\"All Status Types\" primaryKey=\"STATUS\">\n"
            + "    <Level name=\"Type\" column=\"STATUS\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "    </Level>\n"
            + "   </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name=\"Quantity\" column=\"QUANTITYORDERED\" formatString=\"#,###\" aggregator=\"sum\">\n"
            + "  </Measure>\n"
            + "  <Measure name=\"Sales\" column=\"TOTALPRICE\" formatString=\"#,###\" aggregator=\"sum\">\n"
            + "  </Measure>\n"
            + " </Cube>\n"
            + "</Schema>\n");
        if (!testContext.databaseIsValid()) {
            return;
        }
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
            + "                            topLevel='[Markets].[Territory]'>\n "
            + "                <MemberGrant member='[Markets].[Territory].[APAC]' access='all' />\n "
            + "            </HierarchyGrant>\n "
            + "        </CubeGrant>\n "
            + "    </SchemaGrant>\n "
            + "  </Role>\n "
            + "  <Role name='DIM_MARKETAREA_MARKET_850'>\n "
            + "    <SchemaGrant access='none'>\n "
            + "        <CubeGrant cube='SteelWheelsSales' access='none'>\n "
            + "            <HierarchyGrant hierarchy='[Markets]'\n "
            + "                            access='custom' rollupPolicy=\"partial\"\n "
            + "                            topLevel='[Markets].[Territory]'>\n "
            + "                <MemberGrant member='[Markets].[Territory].[EMEA]' access='all' />\n "
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

        // [Markets].[Territory].Members would get cached after role filter..
        ctx.getConnection().setRole(market_800);
        ctx.assertQueryReturns(
            nonEmptyMembersQuery,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[2005], [Measures].[Quantity]}\n"
            + "{[Time].[2005], [Measures].[Sales]}\n"
            + "Axis #2:\n"
            + "{[Markets].[APAC]}\n"
            + "Row #0: 3,411\n"
            + "Row #0: 337,018\n");
        // ..and prevent EMEA from appearing in the results
        ctx.getConnection().setRole(market_800_850);
        ctx.assertQueryReturns(
            nonEmptyMembersQuery,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[2005], [Measures].[Quantity]}\n"
            + "{[Time].[2005], [Measures].[Sales]}\n"
            + "Axis #2:\n"
            + "{[Markets].[APAC]}\n"
            + "{[Markets].[EMEA]}\n"
            + "Row #0: 3,411\n"
            + "Row #0: 337,018\n"
            + "Row #1: 9,237\n"
            + "Row #1: 929,829\n");
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1750">MONDRIAN-1750</a>
     *
     * Compound slicer getting applied to CurrentDateMember
     */
    public void testMondrian1750() throws Exception {
        TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }
        testContext.assertQueryReturns(
            "with member [Measures].[CYQ] as\n"
            + "'Aggregate(CurrentDateMember([Time],\"[Ti\\me]\\.[Year\\s]\\.[yyyy]\", BEFORE), [Quantity])'\n"
            + "select\n"
            + "{[Measures].[Quantity], [Measures].[CYQ]} on columns,\n"
            + "{[Markets].[Territory].Members} on rows\n"
            + "from [SteelWheelsSales]\n"
            + "where {[Time].[Years].[2004], [Time].[Years].[2005]}\n",
            "Axis #0:\n"
            + "{[Time].[2004]}\n"
            + "{[Time].[2005]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Quantity]}\n"
            + "{[Measures].[CYQ]}\n"
            + "Axis #2:\n"
            + "{[Markets].[#null]}\n"
            + "{[Markets].[APAC]}\n"
            + "{[Markets].[EMEA]}\n"
            + "{[Markets].[Japan]}\n"
            + "{[Markets].[NA]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #1: 9,349\n"
            + "Row #1: 3,411\n"
            + "Row #2: 32,867\n"
            + "Row #2: 9,237\n"
            + "Row #3: 2,072\n"
            + "Row #3: 380\n"
            + "Row #4: 24,604\n"
            + "Row #4: 6,447\n");
    }

    public void testMondrian2411_1() throws Exception {
        if (!getTestContext().databaseIsValid()) {
            return;
        }
        // Tests a user query followed by an admin query
        final String schema =
            "<Schema name=\"SteelWheels\" description=\"1 admin role, 1 user role. For testing MemberGrant with caching in 5.1.2\"> \n"
            + "  <Dimension type=\"StandardDimension\" visible=\"true\" highCardinality=\"false\" name=\"Customers Dimension\">\n"
            + "    <Hierarchy name=\"Customers Hierarchy\" visible=\"true\" hasAll=\"true\" primaryKey=\"CUSTOMERNUMBER\" caption=\"Customer Hierarchy\">\n"
            + "      <Table name=\"customer_w_ter\">\n"
            + "      </Table>\n"
            + "      <Level name=\"Address\" visible=\"true\" column=\"ADDRESSLINE1\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\" caption=\"Address Line 1\">\n"
            + "      </Level>\n"
            + "      <Level name=\"Name\" visible=\"true\" column=\"CONTACTLASTNAME\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\" caption=\"Contact Last Name\">\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + " <Cube name=\"Customers Cube\" visible=\"true\" cache=\"true\" enabled=\"true\"> \n"
            + "     <Table name=\"orderfact\"> \n"
            + "     </Table> \n"
            + "     <DimensionUsage source=\"Customers Dimension\" name=\"Customer_DimUsage\" visible=\"true\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\"> \n"
            + "     </DimensionUsage> \n"
            + "     <Measure name=\"Price Each\" column=\"PRICEEACH\" aggregator=\"sum\" visible=\"true\"> \n"
            + "     </Measure> \n"
            + "     <Measure name=\"Total Price\" column=\"TOTALPRICE\" aggregator=\"sum\" visible=\"true\"> \n"
            + "     </Measure> \n"
            + " </Cube> \n"
            + " <Role name=\"Administrator\"> \n"
            + "     <SchemaGrant access=\"all\"> \n"
            + "         <CubeGrant cube=\"Customers Cube\" access=\"all\"> \n"
            + "         </CubeGrant> \n"
            + "     </SchemaGrant> \n"
            + " </Role> \n"
            + " <Role name=\"Power User\"> \n"
            + "     <SchemaGrant access=\"none\"> \n"
            + "         <CubeGrant cube=\"Customers Cube\" access=\"all\"> \n"
            + "             <DimensionGrant dimension=\"Measures\" access=\"all\"> \n"
            + "             </DimensionGrant>\n"
            + "             <HierarchyGrant hierarchy=\"[Customer_DimUsage.Customers Hierarchy]\" topLevel=\"[Customer_DimUsage.Customers Hierarchy].[Name]\" rollupPolicy=\"partial\" access=\"custom\"> \n"
            + "                 <MemberGrant member=\"[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]\" access=\"all\"> \n"
            + "                 </MemberGrant> \n"
            + "             </HierarchyGrant> \n"
            + "         </CubeGrant> \n"
            + "     </SchemaGrant> \n"
            + " </Role>     \n"
            + "</Schema>\n";
        TestContext context = getTestContext().withSchema(schema);

        context.withRole("Power User").assertQueryReturns(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'FILTER([*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_], NOT ISEMPTY ([Measures].[Price Each]))'\n"
            + "SET [*NATIVE_CJ_SET] AS '[*NATIVE_CJ_SET_WITH_SLICER]'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER,[Customer_DimUsage.Customers Hierarchy].[Address]).ORDERKEY,BASC)'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[Price Each]}'\n"
            + "SET [*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_] AS '[Customer_DimUsage.Customers Hierarchy].[Name].MEMBERS'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER)})'\n"
            + "SELECT\n"
            + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            + ",[*SORTED_ROW_AXIS] ON ROWS\n"
            + "FROM [Customers Cube]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Price Each]}\n"
            + "Axis #2:\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]}\n"
            + "Row #0: 1,701.95\n");

        context.withRole("Administrator").assertQueryReturns(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'FILTER([*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_], NOT ISEMPTY ([Measures].[Price Each]))'\n"
            + "SET [*NATIVE_CJ_SET] AS '[*NATIVE_CJ_SET_WITH_SLICER]'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER,[Customer_DimUsage.Customers Hierarchy].[Address]).ORDERKEY,BASC)'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[Price Each]}'\n"
            + "SET [*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_] AS '[Customer_DimUsage.Customers Hierarchy].[Name].MEMBERS'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER)})'\n"
            + "SELECT\n"
            + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            + ",[*SORTED_ROW_AXIS] ON ROWS\n"
            + "FROM [Customers Cube]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Price Each]}\n"
            + "Axis #2:\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Via Monte Bianco 34].[Accorti]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Fauntleroy Circus].[Ashworth]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7635 Spinnaker Dr.].[Barajas]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[1785 First Street].[Benitez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Garden House Crowther Way].[Bennett]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Berguvsvu00e4gen  8].[Berglund]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Erling Skakkes gate 78].[Bergulfsen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[265, boulevard Charonne].[Bertrand]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[35 King George].[Brown]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7476 Moss Rd.].[Brown]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7734 Strong St.].[Brown]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[31 Duncan St. West End].[Calaghan]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Boulevard Tirou, 255].[Cartrain]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[25 Maiden Lane].[Cassidy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[782 First Street].[Cervantes]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[6047 Douglas Av.].[Chandler]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[24, place Klu00e9ber].[Citeaux]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7 Allen Street].[Connery]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[15 McCallum Street - NatWest Center #13-03].[Cruz]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[27 rue du Colonel Pierre Avia].[Da Cunha]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Berkeley Gardens 12  Brewery].[Devon]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Rue Joseph-Bens 532].[Dewey]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Hansastr. 15].[Donnermeyer]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[636 St Kilda Road].[Ferguson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Merchants House, 27-30 Merchant's Quay].[Fernandez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[6251 Ingle Ln.].[Franco]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[43 rue St. Laurent].[Fresniu00e8re]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[C/ Moralzarzal, 86].[Freyre]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2678 Kingston Rd.].[Frick]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[3086 Ingle Ln.].[Frick]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[3758 North Pendale Street].[Frick]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[162-164 Grafton Road].[Graham]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[120 Hanover Sq.].[Hardy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[59 rue de l'Abbaye].[Henriot]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[11328 Douglas Av.].[Hernandez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[39323 Spinnaker Dr.].[Hernandez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[5905 Pompton St.].[Hernandez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[9408 Furth Circle].[Hirano]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Grenzacherweg 237].[Holz]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Monitor Money Building, 815 Pacific Hwy].[Huxley]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Smagsloget 45].[Ibsen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Keskuskatu 45].[Karttunen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Lyonerstr. 34].[Keitel]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Dojima Avanza 4F, 1-6-20 Dojima, Kita-ku].[Kentary]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[25593 South Bay Ln.].[King]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[8489 Strong St.].[King]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Drammensveien 126 A, PB 744 Sentrum].[Klaeboe]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Torikatu 38].[Koskitalo]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[5290 North Pendale Street].[Kuo]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[67, rue des Cinquante Otages].[Labrune]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[u00c5kergatan 24].[Larsson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[12, rue des Bouchers].[Lebihan]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2440 Pompton St.].[Lewis]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[23 Tsawassen Blvd.].[Lincoln]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[199 Great North Road].[MacKinlay]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[101 Lambton Quay].[McRoy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Kirchgasse 6].[Mendel]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Strada Provinciale 124].[Moroni]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[5557 North Pendale Street].[Murphy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[567 North Pendale Street].[Murphy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Bronz Sok., Bronz Apt. 3/6 Tesvikiye].[Natividad]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[5677 Strong St.].[Nelson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7825 Douglas Av.].[Nelson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[201 Miller Street].[O'Hara]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Drammen 121, PR 744 Sentrum].[Oeztan]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[25, rue Lauriston].[Perrier]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Vinbu00e6ltet 34].[Petersen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Mehrheimerstr. 369].[Pfalzheim]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Geislweg 14].[Pipps]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[184, chaussu00e9e de Tournai].[Rancu00e9]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[20093 Cologno Monzese, via Alessandro Volta 16].[Ricotti]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[C/ Romero, 33].[Roel]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Via Ludovico il Moro 22].[Rovelli]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Rambla de Cataluu00f1a, 23].[Saavedra]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2, rue du Commerce].[Saveley]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[54, rue Royale].[Schmitt]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2-2-8 Roppongi].[Shimamura]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Arenales 1938 3'A'].[Snowden]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[C/ Araquil, 67].[Sommer]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Bank of China Tower, 1 Garden Road].[Sunwoo]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Software Engineering Center, SEC Oy].[Suominen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[4575 Hillside Dr.].[Tam]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[1900 Oak St.].[Tannamuri]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[16780 Pompton St.].[Taylor]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2793 Furth Circle].[Taylor]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[361 Furth Circle].[Thompson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[3675 Furth Circle].[Thompson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[67, avenue de l'Europe].[Tonini]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[4658 Baden Av.].[Tseng]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Village Close - 106 Linden Road Sandown].[Victorino]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[8616 Spinnaker Dr.].[Yoshido]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2304 Long Airport Avenue].[Young]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[4092 Furth Circle].[Young]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[4097 Douglas Av.].[Young]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[78934 Hillside Dr.].[Young]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7586 Pompton St.].[Yu]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[897 Long Airport Avenue].[Yu]}\n"
            + "Row #0: 2,218.41\n"
            + "Row #1: 3,975.33\n"
            + "Row #2: 1,223.4\n"
            + "Row #3: 2,253.35\n"
            + "Row #4: 2,131.78\n"
            + "Row #5: 1,720.14\n"
            + "Row #6: 2,812.1\n"
            + "Row #7: 2,016.97\n"
            + "Row #8: 2,186.14\n"
            + "Row #9: 1,784.96\n"
            + "Row #10: 3,355.71\n"
            + "Row #11: 1,271.05\n"
            + "Row #12: 727.82\n"
            + "Row #13: 1,377.98\n"
            + "Row #14: 1,815.08\n"
            + "Row #15: 1,208.48\n"
            + "Row #16: 1,565.18\n"
            + "Row #17: 1,809.71\n"
            + "Row #18: 2,144.99\n"
            + "Row #19: 1,718.95\n"
            + "Row #20: 2,546.32\n"
            + "Row #21: 2,159.49\n"
            + "Row #22: 1,116.06\n"
            + "Row #23: 4,714.48\n"
            + "Row #24: 1,130.53\n"
            + "Row #25: 1,467.92\n"
            + "Row #26: 1,896.25\n"
            + "Row #27: 21,195.4\n"
            + "Row #28: 2,108.11\n"
            + "Row #29: 3,271.79\n"
            + "Row #30: 2,084.23\n"
            + "Row #31: 3,942.92\n"
            + "Row #32: 1,043.13\n"
            + "Row #33: 3,320.84\n"
            + "Row #34: 1,956.98\n"
            + "Row #35: 2,301.53\n"
            + "Row #36: 1,779.62\n"
            + "Row #37: 2,954.51\n"
            + "Row #38: 2,713.09\n"
            + "Row #39: 3,810.23\n"
            + "Row #40: 2,378.21\n"
            + "Row #41: 2,527.21\n"
            + "Row #42: 1,850.19\n"
            + "Row #43: 1,642.42\n"
            + "Row #44: 2,149\n"
            + "Row #45: 2,332.81\n"
            + "Row #46: 1,949.42\n"
            + "Row #47: 2,628.41\n"
            + "Row #48: 752.9\n"
            + "Row #49: 4,420.77\n"
            + "Row #50: 3,223.37\n"
            + "Row #51: 1,919.25\n"
            + "Row #52: 1,508.45\n"
            + "Row #53: 2,165\n"
            + "Row #54: 2,164.9\n"
            + "Row #55: 2,229.65\n"
            + "Row #56: 1,299.89\n"
            + "Row #57: 3,358.3\n"
            + "Row #58: 1,820.17\n"
            + "Row #59: 1,585.52\n"
            + "Row #60: 3,718.97\n"
            + "Row #61: 14,996.69\n"
            + "Row #62: 1,989.67\n"
            + "Row #63: 3,843.67\n"
            + "Row #64: 2,556.66\n"
            + "Row #65: 2,188.82\n"
            + "Row #66: 3,125.68\n"
            + "Row #67: 2,218.05\n"
            + "Row #68: 3,459.27\n"
            + "Row #69: 1,606.93\n"
            + "Row #70: 739.77\n"
            + "Row #71: 1,155.06\n"
            + "Row #72: 1,701.95\n"
            + "Row #73: 3,752.69\n"
            + "Row #74: 1,919.63\n"
            + "Row #75: 3,417.92\n"
            + "Row #76: 558.43\n"
            + "Row #77: 2,647.84\n"
            + "Row #78: 4,118.68\n"
            + "Row #79: 2,641.92\n"
            + "Row #80: 1,289.09\n"
            + "Row #81: 2,566.53\n"
            + "Row #82: 2,915.38\n"
            + "Row #83: 1,895.8\n"
            + "Row #84: 720.72\n"
            + "Row #85: 1,122.35\n"
            + "Row #86: 2,045.6\n"
            + "Row #87: 1,030.99\n"
            + "Row #88: 1,484.86\n"
            + "Row #89: 954.79\n"
            + "Row #90: 2,862.93\n"
            + "Row #91: 2,170.89\n"
            + "Row #92: 2,834.34\n"
            + "Row #93: 4,104.23\n"
            + "Row #94: 255.5\n"
            + "Row #95: 2,406.21\n"
            + "Row #96: 2,662.14\n"
            + "Row #97: 4,235.63\n");
    }

    public void testMondrian2411_2() throws Exception {
        if (!getTestContext().databaseIsValid()) {
            return;
        }
        // Tests an admin query followed by a user query
        final String schema =
            "<Schema name=\"SteelWheels\" description=\"1 admin role, 1 user role. For testing MemberGrant with caching in 5.1.2\"> \n"
            + "  <Dimension type=\"StandardDimension\" visible=\"true\" highCardinality=\"false\" name=\"Customers Dimension\">\n"
            + "    <Hierarchy name=\"Customers Hierarchy\" visible=\"true\" hasAll=\"true\" primaryKey=\"CUSTOMERNUMBER\" caption=\"Customer Hierarchy\">\n"
            + "      <Table name=\"customer_w_ter\">\n"
            + "      </Table>\n"
            + "      <Level name=\"Address\" visible=\"true\" column=\"ADDRESSLINE1\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\" caption=\"Address Line 1\">\n"
            + "      </Level>\n"
            + "      <Level name=\"Name\" visible=\"true\" column=\"CONTACTLASTNAME\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\" caption=\"Contact Last Name\">\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + " <Cube name=\"Customers Cube\" visible=\"true\" cache=\"true\" enabled=\"true\"> \n"
            + "     <Table name=\"orderfact\"> \n"
            + "     </Table> \n"
            + "     <DimensionUsage source=\"Customers Dimension\" name=\"Customer_DimUsage\" visible=\"true\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\"> \n"
            + "     </DimensionUsage> \n"
            + "     <Measure name=\"Price Each\" column=\"PRICEEACH\" aggregator=\"sum\" visible=\"true\"> \n"
            + "     </Measure> \n"
            + "     <Measure name=\"Total Price\" column=\"TOTALPRICE\" aggregator=\"sum\" visible=\"true\"> \n"
            + "     </Measure> \n"
            + " </Cube> \n"
            + " <Role name=\"Administrator\"> \n"
            + "     <SchemaGrant access=\"all\"> \n"
            + "         <CubeGrant cube=\"Customers Cube\" access=\"all\"> \n"
            + "         </CubeGrant> \n"
            + "     </SchemaGrant> \n"
            + " </Role> \n"
            + " <Role name=\"Power User\"> \n"
            + "     <SchemaGrant access=\"none\"> \n"
            + "         <CubeGrant cube=\"Customers Cube\" access=\"all\"> \n"
            + "             <DimensionGrant dimension=\"Measures\" access=\"all\"> \n"
            + "             </DimensionGrant>\n"
            + "             <HierarchyGrant hierarchy=\"[Customer_DimUsage.Customers Hierarchy]\" topLevel=\"[Customer_DimUsage.Customers Hierarchy].[Name]\" rollupPolicy=\"partial\" access=\"custom\"> \n"
            + "                 <MemberGrant member=\"[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]\" access=\"all\"> \n"
            + "                 </MemberGrant> \n"
            + "             </HierarchyGrant> \n"
            + "         </CubeGrant> \n"
            + "     </SchemaGrant> \n"
            + " </Role>     \n"
            + "</Schema>\n";
        TestContext context = getTestContext().withSchema(schema);

        context.withRole("Administrator").assertQueryReturns(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'FILTER([*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_], NOT ISEMPTY ([Measures].[Price Each]))'\n"
            + "SET [*NATIVE_CJ_SET] AS '[*NATIVE_CJ_SET_WITH_SLICER]'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER,[Customer_DimUsage.Customers Hierarchy].[Address]).ORDERKEY,BASC)'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[Price Each]}'\n"
            + "SET [*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_] AS '[Customer_DimUsage.Customers Hierarchy].[Name].MEMBERS'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER)})'\n"
            + "SELECT\n"
            + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            + ",[*SORTED_ROW_AXIS] ON ROWS\n"
            + "FROM [Customers Cube]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Price Each]}\n"
            + "Axis #2:\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Via Monte Bianco 34].[Accorti]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Fauntleroy Circus].[Ashworth]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7635 Spinnaker Dr.].[Barajas]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[1785 First Street].[Benitez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Garden House Crowther Way].[Bennett]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Berguvsvu00e4gen  8].[Berglund]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Erling Skakkes gate 78].[Bergulfsen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[265, boulevard Charonne].[Bertrand]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[35 King George].[Brown]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7476 Moss Rd.].[Brown]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7734 Strong St.].[Brown]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[31 Duncan St. West End].[Calaghan]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Boulevard Tirou, 255].[Cartrain]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[25 Maiden Lane].[Cassidy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[782 First Street].[Cervantes]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[6047 Douglas Av.].[Chandler]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[24, place Klu00e9ber].[Citeaux]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7 Allen Street].[Connery]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[15 McCallum Street - NatWest Center #13-03].[Cruz]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[27 rue du Colonel Pierre Avia].[Da Cunha]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Berkeley Gardens 12  Brewery].[Devon]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Rue Joseph-Bens 532].[Dewey]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Hansastr. 15].[Donnermeyer]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[636 St Kilda Road].[Ferguson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Merchants House, 27-30 Merchant's Quay].[Fernandez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[6251 Ingle Ln.].[Franco]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[43 rue St. Laurent].[Fresniu00e8re]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[C/ Moralzarzal, 86].[Freyre]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2678 Kingston Rd.].[Frick]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[3086 Ingle Ln.].[Frick]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[3758 North Pendale Street].[Frick]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[162-164 Grafton Road].[Graham]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[120 Hanover Sq.].[Hardy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[59 rue de l'Abbaye].[Henriot]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[11328 Douglas Av.].[Hernandez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[39323 Spinnaker Dr.].[Hernandez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[5905 Pompton St.].[Hernandez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[9408 Furth Circle].[Hirano]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Grenzacherweg 237].[Holz]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Monitor Money Building, 815 Pacific Hwy].[Huxley]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Smagsloget 45].[Ibsen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Keskuskatu 45].[Karttunen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Lyonerstr. 34].[Keitel]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Dojima Avanza 4F, 1-6-20 Dojima, Kita-ku].[Kentary]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[25593 South Bay Ln.].[King]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[8489 Strong St.].[King]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Drammensveien 126 A, PB 744 Sentrum].[Klaeboe]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Torikatu 38].[Koskitalo]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[5290 North Pendale Street].[Kuo]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[67, rue des Cinquante Otages].[Labrune]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[u00c5kergatan 24].[Larsson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[12, rue des Bouchers].[Lebihan]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2440 Pompton St.].[Lewis]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[23 Tsawassen Blvd.].[Lincoln]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[199 Great North Road].[MacKinlay]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[101 Lambton Quay].[McRoy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Kirchgasse 6].[Mendel]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Strada Provinciale 124].[Moroni]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[5557 North Pendale Street].[Murphy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[567 North Pendale Street].[Murphy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Bronz Sok., Bronz Apt. 3/6 Tesvikiye].[Natividad]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[5677 Strong St.].[Nelson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7825 Douglas Av.].[Nelson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[201 Miller Street].[O'Hara]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Drammen 121, PR 744 Sentrum].[Oeztan]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[25, rue Lauriston].[Perrier]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Vinbu00e6ltet 34].[Petersen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Mehrheimerstr. 369].[Pfalzheim]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Geislweg 14].[Pipps]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[184, chaussu00e9e de Tournai].[Rancu00e9]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[20093 Cologno Monzese, via Alessandro Volta 16].[Ricotti]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[C/ Romero, 33].[Roel]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Via Ludovico il Moro 22].[Rovelli]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Rambla de Cataluu00f1a, 23].[Saavedra]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2, rue du Commerce].[Saveley]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[54, rue Royale].[Schmitt]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2-2-8 Roppongi].[Shimamura]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Arenales 1938 3'A'].[Snowden]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[C/ Araquil, 67].[Sommer]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Bank of China Tower, 1 Garden Road].[Sunwoo]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Software Engineering Center, SEC Oy].[Suominen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[4575 Hillside Dr.].[Tam]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[1900 Oak St.].[Tannamuri]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[16780 Pompton St.].[Taylor]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2793 Furth Circle].[Taylor]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[361 Furth Circle].[Thompson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[3675 Furth Circle].[Thompson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[67, avenue de l'Europe].[Tonini]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[4658 Baden Av.].[Tseng]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Village Close - 106 Linden Road Sandown].[Victorino]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[8616 Spinnaker Dr.].[Yoshido]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2304 Long Airport Avenue].[Young]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[4092 Furth Circle].[Young]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[4097 Douglas Av.].[Young]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[78934 Hillside Dr.].[Young]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7586 Pompton St.].[Yu]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[897 Long Airport Avenue].[Yu]}\n"
            + "Row #0: 2,218.41\n"
            + "Row #1: 3,975.33\n"
            + "Row #2: 1,223.4\n"
            + "Row #3: 2,253.35\n"
            + "Row #4: 2,131.78\n"
            + "Row #5: 1,720.14\n"
            + "Row #6: 2,812.1\n"
            + "Row #7: 2,016.97\n"
            + "Row #8: 2,186.14\n"
            + "Row #9: 1,784.96\n"
            + "Row #10: 3,355.71\n"
            + "Row #11: 1,271.05\n"
            + "Row #12: 727.82\n"
            + "Row #13: 1,377.98\n"
            + "Row #14: 1,815.08\n"
            + "Row #15: 1,208.48\n"
            + "Row #16: 1,565.18\n"
            + "Row #17: 1,809.71\n"
            + "Row #18: 2,144.99\n"
            + "Row #19: 1,718.95\n"
            + "Row #20: 2,546.32\n"
            + "Row #21: 2,159.49\n"
            + "Row #22: 1,116.06\n"
            + "Row #23: 4,714.48\n"
            + "Row #24: 1,130.53\n"
            + "Row #25: 1,467.92\n"
            + "Row #26: 1,896.25\n"
            + "Row #27: 21,195.4\n"
            + "Row #28: 2,108.11\n"
            + "Row #29: 3,271.79\n"
            + "Row #30: 2,084.23\n"
            + "Row #31: 3,942.92\n"
            + "Row #32: 1,043.13\n"
            + "Row #33: 3,320.84\n"
            + "Row #34: 1,956.98\n"
            + "Row #35: 2,301.53\n"
            + "Row #36: 1,779.62\n"
            + "Row #37: 2,954.51\n"
            + "Row #38: 2,713.09\n"
            + "Row #39: 3,810.23\n"
            + "Row #40: 2,378.21\n"
            + "Row #41: 2,527.21\n"
            + "Row #42: 1,850.19\n"
            + "Row #43: 1,642.42\n"
            + "Row #44: 2,149\n"
            + "Row #45: 2,332.81\n"
            + "Row #46: 1,949.42\n"
            + "Row #47: 2,628.41\n"
            + "Row #48: 752.9\n"
            + "Row #49: 4,420.77\n"
            + "Row #50: 3,223.37\n"
            + "Row #51: 1,919.25\n"
            + "Row #52: 1,508.45\n"
            + "Row #53: 2,165\n"
            + "Row #54: 2,164.9\n"
            + "Row #55: 2,229.65\n"
            + "Row #56: 1,299.89\n"
            + "Row #57: 3,358.3\n"
            + "Row #58: 1,820.17\n"
            + "Row #59: 1,585.52\n"
            + "Row #60: 3,718.97\n"
            + "Row #61: 14,996.69\n"
            + "Row #62: 1,989.67\n"
            + "Row #63: 3,843.67\n"
            + "Row #64: 2,556.66\n"
            + "Row #65: 2,188.82\n"
            + "Row #66: 3,125.68\n"
            + "Row #67: 2,218.05\n"
            + "Row #68: 3,459.27\n"
            + "Row #69: 1,606.93\n"
            + "Row #70: 739.77\n"
            + "Row #71: 1,155.06\n"
            + "Row #72: 1,701.95\n"
            + "Row #73: 3,752.69\n"
            + "Row #74: 1,919.63\n"
            + "Row #75: 3,417.92\n"
            + "Row #76: 558.43\n"
            + "Row #77: 2,647.84\n"
            + "Row #78: 4,118.68\n"
            + "Row #79: 2,641.92\n"
            + "Row #80: 1,289.09\n"
            + "Row #81: 2,566.53\n"
            + "Row #82: 2,915.38\n"
            + "Row #83: 1,895.8\n"
            + "Row #84: 720.72\n"
            + "Row #85: 1,122.35\n"
            + "Row #86: 2,045.6\n"
            + "Row #87: 1,030.99\n"
            + "Row #88: 1,484.86\n"
            + "Row #89: 954.79\n"
            + "Row #90: 2,862.93\n"
            + "Row #91: 2,170.89\n"
            + "Row #92: 2,834.34\n"
            + "Row #93: 4,104.23\n"
            + "Row #94: 255.5\n"
            + "Row #95: 2,406.21\n"
            + "Row #96: 2,662.14\n"
            + "Row #97: 4,235.63\n");

        context.withRole("Power User").assertQueryReturns(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'FILTER([*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_], NOT ISEMPTY ([Measures].[Price Each]))'\n"
            + "SET [*NATIVE_CJ_SET] AS '[*NATIVE_CJ_SET_WITH_SLICER]'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER,[Customer_DimUsage.Customers Hierarchy].[Address]).ORDERKEY,BASC)'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[Price Each]}'\n"
            + "SET [*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_] AS '[Customer_DimUsage.Customers Hierarchy].[Name].MEMBERS'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER)})'\n"
            + "SELECT\n"
            + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            + ",[*SORTED_ROW_AXIS] ON ROWS\n"
            + "FROM [Customers Cube]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Price Each]}\n"
            + "Axis #2:\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]}\n"
            + "Row #0: 1,701.95\n");
    }

    public void testMondrian2411_3() throws Exception {
        // Tests an admin query followed by a user query, but both are wrapped
        // with a no-op role in a union.
        if (!getTestContext().databaseIsValid()
                || (MondrianProperties.instance().UseAggregates.get()
                    && !Bug.BugMondrian2440Fixed))
        {
            return;
        }
        final String schema =
            "<Schema name=\"SteelWheels\" description=\"1 admin role, 1 user role. For testing MemberGrant with caching in 5.1.2\"> \n"
            + "  <Dimension type=\"StandardDimension\" visible=\"true\" highCardinality=\"false\" name=\"Customers Dimension\">\n"
            + "    <Hierarchy name=\"Customers Hierarchy\" visible=\"true\" hasAll=\"true\" primaryKey=\"CUSTOMERNUMBER\" caption=\"Customer Hierarchy\">\n"
            + "      <Table name=\"customer_w_ter\">\n"
            + "      </Table>\n"
            + "      <Level name=\"Address\" visible=\"true\" column=\"ADDRESSLINE1\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\" caption=\"Address Line 1\">\n"
            + "      </Level>\n"
            + "      <Level name=\"Name\" visible=\"true\" column=\"CONTACTLASTNAME\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\" caption=\"Contact Last Name\">\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + " <Cube name=\"Customers Cube\" visible=\"true\" cache=\"true\" enabled=\"true\"> \n"
            + "     <Table name=\"orderfact\"> \n"
            + "     </Table> \n"
            + "     <DimensionUsage source=\"Customers Dimension\" name=\"Customer_DimUsage\" visible=\"true\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\"> \n"
            + "     </DimensionUsage> \n"
            + "     <Measure name=\"Price Each\" column=\"PRICEEACH\" aggregator=\"sum\" visible=\"true\"> \n"
            + "     </Measure> \n"
            + "     <Measure name=\"Total Price\" column=\"TOTALPRICE\" aggregator=\"sum\" visible=\"true\"> \n"
            + "     </Measure> \n"
            + " </Cube> \n"
            + " <Role name=\"Administrator\"> \n"
            + "     <SchemaGrant access=\"all\"> \n"
            + "         <CubeGrant cube=\"Customers Cube\" access=\"all\"> \n"
            + "         </CubeGrant> \n"
            + "     </SchemaGrant> \n"
            + " </Role> \n"
            + " <Role name=\"Foo\"> \n"
            + "     <SchemaGrant access=\"none\"> \n"
            + "     </SchemaGrant> \n"
            + " </Role>\n"
            + " <Role name=\"Power User\"> \n"
            + "     <SchemaGrant access=\"none\"> \n"
            + "         <CubeGrant cube=\"Customers Cube\" access=\"all\"> \n"
            + "             <DimensionGrant dimension=\"Measures\" access=\"all\"> \n"
            + "             </DimensionGrant>\n"
            + "             <HierarchyGrant hierarchy=\"[Customer_DimUsage.Customers Hierarchy]\" topLevel=\"[Customer_DimUsage.Customers Hierarchy].[Name]\" rollupPolicy=\"partial\" access=\"custom\"> \n"
            + "                 <MemberGrant member=\"[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]\" access=\"all\"> \n"
            + "                 </MemberGrant> \n"
            + "             </HierarchyGrant> \n"
            + "         </CubeGrant> \n"
            + "     </SchemaGrant> \n"
            + " </Role>\n"
            + " <Role name=\"Administrator Union\"> \n"
            + "     <Union> \n"
            + "         <RoleUsage roleName=\"Administrator\"/> \n"
            + "         <RoleUsage roleName=\"Foo\"/> \n"
            + "     </Union> \n"
            + " </Role>\n"
            + " <Role name=\"Power User Union\"> \n"
            + "     <Union> \n"
            + "         <RoleUsage roleName=\"Power User\"/> \n"
            + "         <RoleUsage roleName=\"Foo\"/> \n"
            + "     </Union> \n"
            + " </Role>\n"
            + "</Schema>\n";
        TestContext context = getTestContext().withSchema(schema);

        context.withRole("Administrator Union").assertQueryReturns(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'FILTER([*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_], NOT ISEMPTY ([Measures].[Price Each]))'\n"
            + "SET [*NATIVE_CJ_SET] AS '[*NATIVE_CJ_SET_WITH_SLICER]'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER,[Customer_DimUsage.Customers Hierarchy].[Address]).ORDERKEY,BASC)'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[Price Each]}'\n"
            + "SET [*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_] AS '[Customer_DimUsage.Customers Hierarchy].[Name].MEMBERS'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER)})'\n"
            + "SELECT\n"
            + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            + ",[*SORTED_ROW_AXIS] ON ROWS\n"
            + "FROM [Customers Cube]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Price Each]}\n"
            + "Axis #2:\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Via Monte Bianco 34].[Accorti]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Fauntleroy Circus].[Ashworth]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7635 Spinnaker Dr.].[Barajas]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[1785 First Street].[Benitez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Garden House Crowther Way].[Bennett]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Berguvsvu00e4gen  8].[Berglund]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Erling Skakkes gate 78].[Bergulfsen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[265, boulevard Charonne].[Bertrand]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[35 King George].[Brown]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7476 Moss Rd.].[Brown]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7734 Strong St.].[Brown]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[31 Duncan St. West End].[Calaghan]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Boulevard Tirou, 255].[Cartrain]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[25 Maiden Lane].[Cassidy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[782 First Street].[Cervantes]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[6047 Douglas Av.].[Chandler]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[24, place Klu00e9ber].[Citeaux]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7 Allen Street].[Connery]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[15 McCallum Street - NatWest Center #13-03].[Cruz]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[27 rue du Colonel Pierre Avia].[Da Cunha]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Berkeley Gardens 12  Brewery].[Devon]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Rue Joseph-Bens 532].[Dewey]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Hansastr. 15].[Donnermeyer]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[636 St Kilda Road].[Ferguson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Merchants House, 27-30 Merchant's Quay].[Fernandez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[6251 Ingle Ln.].[Franco]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[43 rue St. Laurent].[Fresniu00e8re]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[C/ Moralzarzal, 86].[Freyre]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2678 Kingston Rd.].[Frick]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[3086 Ingle Ln.].[Frick]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[3758 North Pendale Street].[Frick]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[162-164 Grafton Road].[Graham]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[120 Hanover Sq.].[Hardy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[59 rue de l'Abbaye].[Henriot]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[11328 Douglas Av.].[Hernandez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[39323 Spinnaker Dr.].[Hernandez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[5905 Pompton St.].[Hernandez]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[9408 Furth Circle].[Hirano]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Grenzacherweg 237].[Holz]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Monitor Money Building, 815 Pacific Hwy].[Huxley]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Smagsloget 45].[Ibsen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Keskuskatu 45].[Karttunen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Lyonerstr. 34].[Keitel]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Dojima Avanza 4F, 1-6-20 Dojima, Kita-ku].[Kentary]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[25593 South Bay Ln.].[King]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[8489 Strong St.].[King]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Drammensveien 126 A, PB 744 Sentrum].[Klaeboe]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Torikatu 38].[Koskitalo]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[5290 North Pendale Street].[Kuo]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[67, rue des Cinquante Otages].[Labrune]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[u00c5kergatan 24].[Larsson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[12, rue des Bouchers].[Lebihan]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2440 Pompton St.].[Lewis]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[23 Tsawassen Blvd.].[Lincoln]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[199 Great North Road].[MacKinlay]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[101 Lambton Quay].[McRoy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Kirchgasse 6].[Mendel]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Strada Provinciale 124].[Moroni]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[5557 North Pendale Street].[Murphy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[567 North Pendale Street].[Murphy]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Bronz Sok., Bronz Apt. 3/6 Tesvikiye].[Natividad]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[5677 Strong St.].[Nelson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7825 Douglas Av.].[Nelson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[201 Miller Street].[O'Hara]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Drammen 121, PR 744 Sentrum].[Oeztan]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[25, rue Lauriston].[Perrier]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Vinbu00e6ltet 34].[Petersen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Mehrheimerstr. 369].[Pfalzheim]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Geislweg 14].[Pipps]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[184, chaussu00e9e de Tournai].[Rancu00e9]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[20093 Cologno Monzese, via Alessandro Volta 16].[Ricotti]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[C/ Romero, 33].[Roel]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Via Ludovico il Moro 22].[Rovelli]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Rambla de Cataluu00f1a, 23].[Saavedra]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2, rue du Commerce].[Saveley]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[54, rue Royale].[Schmitt]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2-2-8 Roppongi].[Shimamura]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Arenales 1938 3'A'].[Snowden]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[C/ Araquil, 67].[Sommer]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Bank of China Tower, 1 Garden Road].[Sunwoo]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Software Engineering Center, SEC Oy].[Suominen]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[4575 Hillside Dr.].[Tam]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[1900 Oak St.].[Tannamuri]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[16780 Pompton St.].[Taylor]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2793 Furth Circle].[Taylor]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[361 Furth Circle].[Thompson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[3675 Furth Circle].[Thompson]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[67, avenue de l'Europe].[Tonini]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[4658 Baden Av.].[Tseng]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[Village Close - 106 Linden Road Sandown].[Victorino]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[8616 Spinnaker Dr.].[Yoshido]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[2304 Long Airport Avenue].[Young]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[4092 Furth Circle].[Young]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[4097 Douglas Av.].[Young]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[78934 Hillside Dr.].[Young]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[7586 Pompton St.].[Yu]}\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[897 Long Airport Avenue].[Yu]}\n"
            + "Row #0: 2,218.41\n"
            + "Row #1: 3,975.33\n"
            + "Row #2: 1,223.4\n"
            + "Row #3: 2,253.35\n"
            + "Row #4: 2,131.78\n"
            + "Row #5: 1,720.14\n"
            + "Row #6: 2,812.1\n"
            + "Row #7: 2,016.97\n"
            + "Row #8: 2,186.14\n"
            + "Row #9: 1,784.96\n"
            + "Row #10: 3,355.71\n"
            + "Row #11: 1,271.05\n"
            + "Row #12: 727.82\n"
            + "Row #13: 1,377.98\n"
            + "Row #14: 1,815.08\n"
            + "Row #15: 1,208.48\n"
            + "Row #16: 1,565.18\n"
            + "Row #17: 1,809.71\n"
            + "Row #18: 2,144.99\n"
            + "Row #19: 1,718.95\n"
            + "Row #20: 2,546.32\n"
            + "Row #21: 2,159.49\n"
            + "Row #22: 1,116.06\n"
            + "Row #23: 4,714.48\n"
            + "Row #24: 1,130.53\n"
            + "Row #25: 1,467.92\n"
            + "Row #26: 1,896.25\n"
            + "Row #27: 21,195.4\n"
            + "Row #28: 2,108.11\n"
            + "Row #29: 3,271.79\n"
            + "Row #30: 2,084.23\n"
            + "Row #31: 3,942.92\n"
            + "Row #32: 1,043.13\n"
            + "Row #33: 3,320.84\n"
            + "Row #34: 1,956.98\n"
            + "Row #35: 2,301.53\n"
            + "Row #36: 1,779.62\n"
            + "Row #37: 2,954.51\n"
            + "Row #38: 2,713.09\n"
            + "Row #39: 3,810.23\n"
            + "Row #40: 2,378.21\n"
            + "Row #41: 2,527.21\n"
            + "Row #42: 1,850.19\n"
            + "Row #43: 1,642.42\n"
            + "Row #44: 2,149\n"
            + "Row #45: 2,332.81\n"
            + "Row #46: 1,949.42\n"
            + "Row #47: 2,628.41\n"
            + "Row #48: 752.9\n"
            + "Row #49: 4,420.77\n"
            + "Row #50: 3,223.37\n"
            + "Row #51: 1,919.25\n"
            + "Row #52: 1,508.45\n"
            + "Row #53: 2,165\n"
            + "Row #54: 2,164.9\n"
            + "Row #55: 2,229.65\n"
            + "Row #56: 1,299.89\n"
            + "Row #57: 3,358.3\n"
            + "Row #58: 1,820.17\n"
            + "Row #59: 1,585.52\n"
            + "Row #60: 3,718.97\n"
            + "Row #61: 14,996.69\n"
            + "Row #62: 1,989.67\n"
            + "Row #63: 3,843.67\n"
            + "Row #64: 2,556.66\n"
            + "Row #65: 2,188.82\n"
            + "Row #66: 3,125.68\n"
            + "Row #67: 2,218.05\n"
            + "Row #68: 3,459.27\n"
            + "Row #69: 1,606.93\n"
            + "Row #70: 739.77\n"
            + "Row #71: 1,155.06\n"
            + "Row #72: 1,701.95\n"
            + "Row #73: 3,752.69\n"
            + "Row #74: 1,919.63\n"
            + "Row #75: 3,417.92\n"
            + "Row #76: 558.43\n"
            + "Row #77: 2,647.84\n"
            + "Row #78: 4,118.68\n"
            + "Row #79: 2,641.92\n"
            + "Row #80: 1,289.09\n"
            + "Row #81: 2,566.53\n"
            + "Row #82: 2,915.38\n"
            + "Row #83: 1,895.8\n"
            + "Row #84: 720.72\n"
            + "Row #85: 1,122.35\n"
            + "Row #86: 2,045.6\n"
            + "Row #87: 1,030.99\n"
            + "Row #88: 1,484.86\n"
            + "Row #89: 954.79\n"
            + "Row #90: 2,862.93\n"
            + "Row #91: 2,170.89\n"
            + "Row #92: 2,834.34\n"
            + "Row #93: 4,104.23\n"
            + "Row #94: 255.5\n"
            + "Row #95: 2,406.21\n"
            + "Row #96: 2,662.14\n"
            + "Row #97: 4,235.63\n");

        context.withRole("Power User Union").assertQueryReturns(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'FILTER([*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_], NOT ISEMPTY ([Measures].[Price Each]))'\n"
            + "SET [*NATIVE_CJ_SET] AS '[*NATIVE_CJ_SET_WITH_SLICER]'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER,[Customer_DimUsage.Customers Hierarchy].[Address]).ORDERKEY,BASC)'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[Price Each]}'\n"
            + "SET [*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_] AS '[Customer_DimUsage.Customers Hierarchy].[Name].MEMBERS'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER)})'\n"
            + "SELECT\n"
            + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            + ",[*SORTED_ROW_AXIS] ON ROWS\n"
            + "FROM [Customers Cube]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Price Each]}\n"
            + "Axis #2:\n"
            + "{[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]}\n"
            + "Row #0: 1,701.95\n");
    }

    /**
     * this tests the fix for
     * http://jira.pentaho.com/browse/Mondrian-2652
     */
    public void testMondrian2652() {
        // Check if there is a valid SteelWheels database.
        TestContext testContext = getTestContext();
        if (!testContext.databaseIsValid()) {
            return;
        }

        final String schema =
            "<Schema name=\"rolesTest\">\n"
            + "  <Dimension visible=\"true\" highCardinality=\"false\" name=\"Dimension1\">\n"
            + "    <Hierarchy visible=\"true\" hasAll=\"true\" allMemberName=\"All Products\" primaryKey=\"PRODUCTCODE\">\n"
            + "      <Table name=\"products\">\n"
            + "      </Table>\n"
            + "      <Level name=\"Level1\" visible=\"true\" column=\"PRODUCTLINE\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension visible=\"true\" highCardinality=\"false\" name=\"Dimension2\">\n"
            + "    <Hierarchy visible=\"true\" hasAll=\"true\" allMemberName=\"All Customers\" primaryKey=\"CUSTOMERNUMBER\">\n"
            + "      <Table name=\"customer_w_ter\">\n"
            + "      </Table>\n"
            + "      <Level name=\"Level2\" visible=\"true\" column=\"CUSTOMERNAME\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Cube name=\"rolesTest1\" visible=\"true\" cache=\"true\" enabled=\"true\">\n"
            + "    <Table name=\"orderfact\" alias=\"rolesTest1\">\n"
            + "    </Table>\n"
            + "    <DimensionUsage source=\"Dimension1\" name=\"Dimension1\" visible=\"true\" foreignKey=\"PRODUCTCODE\" highCardinality=\"false\">\n"
            + "    </DimensionUsage>\n"
            + "    <DimensionUsage source=\"Dimension2\" name=\"Dimension2\" visible=\"true\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n"
            + "    </DimensionUsage>\n"
            + "    <Measure name=\"Measure1\" column=\"QUANTITYORDERED\" aggregator=\"sum\">\n"
            + "    </Measure>\n"
            + "  </Cube>\n"
            + "  <Cube name=\"rolesTest2\" visible=\"true\" cache=\"true\" enabled=\"true\">\n"
            + "    <Table name=\"orderfact\" alias=\"rolesTest2\">\n"
            + "    </Table>\n"
            + "    <DimensionUsage source=\"Dimension2\" name=\"Dimension2\" visible=\"true\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\">\n"
            + "    </DimensionUsage>\n"
            + "    <Measure name=\"Measure2:Internal\" column=\"TOTALPRICE\" aggregator=\"sum\">\n"
            + "    </Measure>\n"
            + "  </Cube>\n"
            + "  <VirtualCube enabled=\"true\" name=\"rolesTest\" defaultMeasure=\"Quantity\" caption=\"Test\" visible=\"true\">\n"
            + "    <CubeUsages>\n"
            + "      <CubeUsage cubeName=\"rolesTest1\" ignoreUnrelatedDimensions=\"true\">\n"
            + "      </CubeUsage>\n"
            + "      <CubeUsage cubeName=\"rolesTest2\" ignoreUnrelatedDimensions=\"true\">\n"
            + "      </CubeUsage>\n"
            + "    </CubeUsages>\n"
            + "    <VirtualCubeDimension cubeName=\"rolesTest1\" visible=\"true\" highCardinality=\"false\" name=\"Dimension1\">\n"
            + "    </VirtualCubeDimension>\n"
            + "    <VirtualCubeDimension cubeName=\"rolesTest1\" visible=\"true\" highCardinality=\"false\" name=\"Dimension2\">\n"
            + "    </VirtualCubeDimension>\n"
            + "    <VirtualCubeMeasure cubeName=\"rolesTest1\" name=\"[Measures].[Measure1]\" visible=\"true\">\n"
            + "    </VirtualCubeMeasure>\n"
            + "    <VirtualCubeMeasure cubeName=\"rolesTest2\" name=\"[Measures].[Measure2:Internal]\" visible=\"true\">\n"
            + "    </VirtualCubeMeasure>\n"
            + "    <CalculatedMember name=\"Measure2\" formula=\"ValidMeasure([Measures].[Measure2:Internal])\" dimension=\"Measures\">\n"
            + "    </CalculatedMember>\n"
            + "  </VirtualCube>\n"
            + "  <Role name=\"Administrator\">\n"
            + "    <SchemaGrant access=\"all\">\n"
            + "    </SchemaGrant>\n"
            + "  </Role>\n"
            + "  <Role name=\"Report Author\">\n"
            + "    <SchemaGrant access=\"custom\">\n"
            + "      <CubeGrant cube=\"rolesTest\" access=\"all\">\n"
            + "        <HierarchyGrant hierarchy=\"[Dimension2]\" topLevel=\"[Dimension2].[Level2]\" access=\"custom\">\n"
            + "          <MemberGrant member=\"[Dimension2].[BG&#38;E Collectables]\" access=\"all\">\n"
            + "          </MemberGrant>\n"
            + "          <MemberGrant member=\"[Dimension2].[Baane Mini Imports]\" access=\"all\">\n"
            + "          </MemberGrant>\n"
            + "          <MemberGrant member=\"[Dimension2].[Blauer See Auto, Co.]\" access=\"all\">\n"
            + "          </MemberGrant>\n"
            + "          <MemberGrant member=\"[Dimension2].[Boards &#38; Toys Co.]\" access=\"all\">\n"
            + "          </MemberGrant>\n"
            + "        </HierarchyGrant>\n"
            + "      </CubeGrant>\n"
            + "    </SchemaGrant>\n"
            + "  </Role>\n"
            + "</Schema>\n";

        final String mdxQuery =
            "  WITH\n"
            + " SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Dimension1_],[*BASE_MEMBERS__Dimension2_])'\n"
            +  " SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Dimension1].CURRENTMEMBER.ORDERKEY,BASC,[Dimension2].CURRENTMEMBER.ORDERKEY,BASC)'\n"
            +  " SET [*BASE_MEMBERS__Dimension1_] AS '[Dimension1].[Level1].MEMBERS'\n"
            +  " SET [*BASE_MEMBERS__Dimension2_] AS '{[Dimension2].[All Customers].[Alpha Cognac]}'\n"
            +  " SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[Measure1],[Measures].[Measure2],[Measures].[Measure2:Internal]}'\n"
            +  " SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Dimension1].CURRENTMEMBER,[Dimension2].CURRENTMEMBER)})'\n"
            +  " SELECT\n"
            +  " [*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            +  " , NON EMPTY\n"
            +  " [*SORTED_ROW_AXIS] ON ROWS\n"
            + " FROM [rolesTest]";

        TestContext testContextReportAuthor =
                createContext(TestContext.instance(), schema)
                        .withCube("SteelWheelsSales").withRole("Report Author");

        // Report Author gets an exception since
        // he has no access to [Dimension2].[All Customers].[Alpha Cognac].
        testContextReportAuthor.assertQueryThrows(
            mdxQuery,
    "MDX object '[Dimension2].[All Customers].[Alpha Cognac]' not found in cube 'rolesTest'");

        TestContext testContextAdministrator =
            createContext(TestContext.instance(), schema)
                .withCube("SteelWheelsSales").withRole("Administrator");

        // Administrator has full access to the data,
        // So he gets the expected result.
        testContextAdministrator.assertQueryReturns(
            mdxQuery,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Measure1]}\n"
            + "{[Measures].[Measure2]}\n"
            + "{[Measures].[Measure2:Internal]}\n"
            + "Axis #2:\n"
            + "{[Dimension1].[Classic Cars], [Dimension2].[Alpha Cognac]}\n"
            + "{[Dimension1].[Planes], [Dimension2].[Alpha Cognac]}\n"
            + "{[Dimension1].[Ships], [Dimension2].[Alpha Cognac]}\n"
            + "{[Dimension1].[Vintage Cars], [Dimension2].[Alpha Cognac]}\n"
            + "Row #0: 126\n"
            + "Row #0: 70,488.44\n"
            + "Row #0: \n"
            + "Row #1: 218\n"
            + "Row #1: 70,488.44\n"
            + "Row #1: \n"
            + "Row #2: 247\n"
            + "Row #2: 70,488.44\n"
            + "Row #2: \n"
            + "Row #3: 96\n"
            + "Row #3: 70,488.44\n"
            + "Row #3: \n");
    }
}

// End SteelWheelsSchemaTest.java
