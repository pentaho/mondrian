/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.MondrianProperties;

/**
 * Test case to
 * push unrelatedDimensions to top level when ignoreUnrelatedDimensions property
 * is set to true on a base cube usage.
 *
 * @author ajoglekar
 * @since Dec 03, 2007
 */
public class IgnoreUnrelatedDimensionsTest extends FoodMartTestCase {

    private static final String cubeSales3 =
        "<Cube name=\"Sales 3\">\n"
        + "   <Table name=\"sales_fact_1997\"/>\n"
        + "   <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
        + "   <Dimension name=\"Education Level\" foreignKey=\"customer_id\">\n"
        + "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
        + "      <Table name=\"customer\"/>\n"
        + "      <Level name=\"Education Level\" column=\"education\" uniqueMembers=\"true\"/>\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>\n"
        + "   <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
        + "   <Dimension name=\"Gender\" foreignKey=\"customer_id\">\n"
        + "     <Hierarchy hasAll=\"true\" defaultMember=\"[Gender].[F]\" "
        + "allMemberName=\"All Gender\" primaryKey=\"customer_id\">\n"
        + "       <Table name=\"customer\"/>\n"
        + "       <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>\n"
        + "     </Hierarchy>\n"
        + "   </Dimension>\n"
        + "   <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"    formatString=\"Standard\">\n"
        + "     <CalculatedMemberProperty name=\"MEMBER_ORDINAL\" value=\"2\"/>\n"
        + "   </Measure>\n"
        + "</Cube>";

    private static final String cubeWarehouseAndSales3 =
        "<VirtualCube name=\"Warehouse and Sales 3\" defaultMeasure=\"Store Invoice\">\n"
        + "  <CubeUsages>\n"
        + "   <CubeUsage cubeName=\"Sales 3\" ignoreUnrelatedDimensions=\"false\"/>\n"
        + "   <CubeUsage cubeName=\"Warehouse\" ignoreUnrelatedDimensions=\"true\"/></CubeUsages>\n"
        + "  <VirtualCubeDimension cubeName=\"Sales 3\" name=\"Gender\"/>\n"
        + "  <VirtualCubeDimension cubeName=\"Sales 3\" name=\"Education Level\"/>\n"
        + "  <VirtualCubeDimension name=\"Product\"/>\n"
        + "  <VirtualCubeDimension name=\"Time\"/>\n"
        + "  <VirtualCubeDimension cubeName=\"Warehouse\" name=\"Warehouse\"/>\n"
        + "  <VirtualCubeMeasure cubeName=\"Sales 3\" name=\"[Measures].[Unit Sales]\"/>\n"
        + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Store Invoice]\"/>\n"
        + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n"
        + "</VirtualCube>";

    protected void setUp() throws Exception {
        super.setUp();
        propSaver.set(propSaver.props.EnableNonEmptyOnAllAxis, true);
    }

    public TestContext getTestContext() {
        return TestContext.instance().legacy().create(
            null,
            null,
            "<VirtualCube name=\"Warehouse and Sales2\" defaultMeasure=\"Store Sales\">\n"
            + "  <CubeUsages>"
            + "   <CubeUsage cubeName=\"Sales\" ignoreUnrelatedDimensions=\"true\"/>\n"
            + "   <CubeUsage cubeName=\"Warehouse\" ignoreUnrelatedDimensions=\"true\"/>\n"
            + "  </CubeUsages>"
            + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Customers\"/>\n"
            + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Education Level\"/>\n"
            + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Gender\"/>\n"
            + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Marital Status\"/>\n"
            + "  <VirtualCubeDimension name=\"Product\"/>\n"
            + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Promotion Media\"/>\n"
            + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Promotions\"/>\n"
            + "  <VirtualCubeDimension name=\"Store\"/>\n"
            + "  <VirtualCubeDimension name=\"Time\"/>\n"
            + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Yearly Income\"/>\n"
            + "  <VirtualCubeDimension cubeName=\"Warehouse\" name=\"Warehouse\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Sales Count]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Cost]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Profit]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Profit Growth]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Profit last Period]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Store Invoice]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Supply Time]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Units Ordered]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Units Shipped]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Cost]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Profit]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Average Warehouse Sale]\"/>\n"
            + "  <CalculatedMember name=\"Profit Per Unit Shipped\" dimension=\"Measures\">\n"
            + "    <Formula>[Measures].[Profit] / [Measures].[Units Shipped]</Formula>\n"
            + "  </CalculatedMember>\n"
            + "</VirtualCube>",
            null,
            null,
            null);
    }

    public void testTotalingOnCrossJoinOfJoiningAndNonJoiningDimensions() {
        assertQueryReturns(
            "WITH MEMBER [Measures].[Unit Sales VM] AS "
            + "'ValidMeasure([Measures].[Unit Sales])', SOLVE_ORDER = 3000 "
            + "MEMBER Gender.G AS 'AGGREGATE(CROSSJOIN({[Gender].[Gender].[Gender].MEMBERS},"
            + "[WAREHOUSE].[STATE PROVINCE].MEMBERS))'"
            + "SELECT "
            + "{[MEASURES].[Unit Sales VM]} ON 0,"
            + "{Gender.G} ON 1 "
            + "FROM [WAREHOUSE AND SALES2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales VM]}\n"
            + "Axis #2:\n"
            + "{[Gender].[Gender].[G]}\n"
            + "Row #0: 266,773\n");
    }

    public void testVMShouldNotPushUpAggMemberDefinedOnNonJoiningDimension() {
        assertQueryReturns(
            "WITH MEMBER [Measures].[Total Sales] AS "
            + "'ValidMeasure(Measures.[Warehouse Sales]) + [Measures].[Unit Sales]',"
            + "SOLVE_ORDER = 3000 "
            + "MEMBER Gender.G AS "
            + "'AGGREGATE(CROSSJOIN({GENDER.[M]},{[Product].[All Products].[Drink]}))',"
            + "SOLVE_ORDER = 4 "
            + "SELECT "
            + "{[MEASURES].[Total Sales]} ON 0,"
            + "{Gender.G} ON 1 FROM [WAREHOUSE AND SALES2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Total Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[Gender].[G]}\n"
            + "Row #0: 30,405.602\n");
    }

    public void testAggMemberDefinedOnNonJoiningDimensionWithNonAllDefltMember()
    {
        // Gender dim to have Gender.F as default member
        final TestContext context = TestContext.instance().legacy().create(
            null, cubeSales3, cubeWarehouseAndSales3, null, null, null);
        context.assertQueryReturns(
            "WITH MEMBER [Measures].[Total Sales] AS "
            + "'ValidMeasure(Measures.[Warehouse Sales]) + [Measures].[Unit Sales]',"
            + "SOLVE_ORDER = 3000 "
            + "MEMBER Gender.G AS "
            + "'AGGREGATE(CROSSJOIN({GENDER.[M]},{[Product].[All Products].[Drink]}))',"
            + "SOLVE_ORDER = 4 "
            + "SELECT "
            + "{[MEASURES].[Total Sales]} ON 0,"
            + "{Gender.G} ON 1 FROM [WAREHOUSE AND SALES 3]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Total Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[Gender].[G]}\n"
            + "Row #0: 30,405.602\n");
    }

    /**
     * Without a fix for MONDRIAN-1837, this result of the following query
     * would be empty.
     */
    public void testIgnoreUnrelatedDimsOnSlicer() {
        propSaver.set(
            MondrianProperties.instance().IgnoreMeasureForNonJoiningDimension,
            true);
        final TestContext context = TestContext.instance().legacy().create(
            null, cubeSales3, cubeWarehouseAndSales3, null, null, null);
        context.assertQueryReturns(
            "SELECT "
            + "{[Measures].[Warehouse Sales]} ON 0"
            + " FROM [WAREHOUSE AND SALES 3] where ([Gender].[M])",
            "Axis #0:\n"
            + "{[Gender].[Gender].[M]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Warehouse Sales]}\n"
            + "Row #0: 196,770.888\n");
    }


    public void testIgnoreUnrelatedDimsOnCompoundSlicer() {
        // MONDRIAN-2072
        propSaver.set(
            MondrianProperties.instance().IgnoreMeasureForNonJoiningDimension,
            true);
        final TestContext context = TestContext.instance().legacy().create(
            null, cubeSales3, cubeWarehouseAndSales3, null, null, null);
        context.assertQueryReturns(
            "SELECT "
            + "{[Measures].[Warehouse Sales]} ON 0"
            + " FROM [WAREHOUSE AND SALES 3] where "
            + "{[Education Level].[Graduate Degree],"
            + "[Education Level].[High School Degree]}",
            "Axis #0:\n"
            + "{[Education Level].[Education Level].[Graduate Degree]}\n"
            + "{[Education Level].[Education Level].[High School Degree]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Warehouse Sales]}\n"
            + "Row #0: 196,770.888\n");
    }

    public void testRelatedAndUnrelatedDimsOnCompoundSlicer() {
        // MONDRIAN-2072
        propSaver.set(
            MondrianProperties.instance().IgnoreMeasureForNonJoiningDimension,
            true);
        final TestContext context = TestContext.instance().legacy().create(
            null, cubeSales3, cubeWarehouseAndSales3, null, null, null);
        context.assertQueryReturns(
            "SELECT "
            + "{[Measures].[Warehouse Sales]} ON 0"
            + " FROM [WAREHOUSE AND SALES 3] where "
            + "Crossjoin( {[Education Level].[Graduate Degree],"
            + "[Education Level].[High School Degree]},"
            + "  {[Warehouse].[USA].[WA], [Warehouse].[USA].[CA]} )",
            "Axis #0:\n"
            + "{[Education Level].[Education Level].[Graduate Degree], [Warehouse].[Warehouse].[USA].[WA]}\n"
            + "{[Education Level].[Education Level].[Graduate Degree], [Warehouse].[Warehouse].[USA].[CA]}\n"
            + "{[Education Level].[Education Level].[High School Degree], [Warehouse].[Warehouse].[USA].[WA]}\n"
            + "{[Education Level].[Education Level].[High School Degree], [Warehouse].[Warehouse].[USA].[CA]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Warehouse Sales]}\n"
            + "Row #0: 157,935.834\n");
    }

    public void testPartiallyRelatedMeasureWithCompoundSlicer() {
        // MONDRIAN-2072
        propSaver.set(
            MondrianProperties.instance().IgnoreMeasureForNonJoiningDimension,
            true);
        final TestContext context = TestContext.instance().legacy().create(
            null, cubeSales3, cubeWarehouseAndSales3, null, null, null);
        // Should equal the [Unit Sales] of [Graduate Degree] and
        // [High School Degree] (with default Gender.F),
        //  plus the total [warehouse sales].
        context.assertQueryReturns(
            "with member measures.bothCubes as "
            + "'measures.[unit sales] + measures.[warehouse sales]'"
            + " SELECT "
            + "{[Measures].[bothCubes]} ON 0"
            + " FROM [WAREHOUSE AND SALES 3] where "
            + " {[Education Level].[Graduate Degree],"
            + "[Education Level].[High School Degree]} ",
            "Axis #0:\n"
            + "{[Education Level].[Education Level].[Graduate Degree]}\n"
            + "{[Education Level].[Education Level].[High School Degree]}\n"
            + "Axis #1:\n"
            + "{[Measures].[bothCubes]}\n"
            + "Row #0: 243,135\n");
        // [Sales] does not ignoreUnrelatedDimensions, so the [Unit Sales]
        // part of the formula below should result in NULL given the
        // [Warehouse] dim in the slicer.
        context.assertQueryReturns(
            "with member measures.bothCubes as "
            + "'measures.[unit sales] + measures.[warehouse sales]'"
            + " SELECT "
            + "{[Measures].[bothCubes]} ON 0"
            + " FROM [WAREHOUSE AND SALES 3] where "
            + "Crossjoin( {[Education Level].[Graduate Degree],"
            + "[Education Level].[High School Degree]},"
            + "  {[Warehouse].[USA].[WA], [Warehouse].[USA].[CA]} )",
            "Axis #0:\n"
            + "{[Education Level].[Education Level].[Graduate Degree], [Warehouse].[Warehouse].[USA].[WA]}\n"
            + "{[Education Level].[Education Level].[Graduate Degree], [Warehouse].[Warehouse].[USA].[CA]}\n"
            + "{[Education Level].[Education Level].[High School Degree], [Warehouse].[Warehouse].[USA].[WA]}\n"
            + "{[Education Level].[Education Level].[High School Degree], [Warehouse].[Warehouse].[USA].[CA]}\n"
            + "Axis #1:\n"
            + "{[Measures].[bothCubes]}\n"
            + "Row #0: 157,936\n");
    }

    public void testNonJoiningDimWithMeasureInCompoundSlicer() {
        // MONDRIAN-2072
        propSaver.set(
            MondrianProperties.instance().IgnoreMeasureForNonJoiningDimension,
            true);
        getTestContext().assertQueryReturns(
            " SELECT "
            + " FROM [WAREHOUSE AND SALES2] where "
            + "crossjoin( measures.[warehouse sales], "
            + " {[Education Level].[Graduate Degree],"
            + "[Education Level].[High School Degree]}) ",
            "Axis #0:\n"
            + "{[Measures].[Warehouse Sales], [Education Level].[Education Level].[Graduate Degree]}\n"
            + "{[Measures].[Warehouse Sales], [Education Level].[Education Level].[High School Degree]}\n"
            + "196,770.888");
    }

    public void testTotalingForValidAndNonValidMeasuresWithJoiningDimensions() {
        assertQueryReturns(
            "WITH MEMBER [Measures].[Unit Sales VM] AS "
            + "'ValidMeasure([Measures].[Unit Sales])',"
            + "SOLVE_ORDER = 3000 "
            + "MEMBER PRODUCT.G AS 'AGGREGATE(CROSSJOIN({PRODUCT.[PRODUCT NAME].MEMBERS},"
            + "[STORE].[STORE NAME].MEMBERS))'"
            + "SELECT "
            + "{[MEASURES].[Unit Sales VM], [MEASURES].[STORE COST]} ON 0,"
            + "{PRODUCT.G} ON 1 FROM [WAREHOUSE AND SALES2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales VM]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[G]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 225,627.23\n");
    }

    public void testTotalingWhenIgnoreUnrelatedDimensionsPropertyIsTrue() {
        assertQueryReturns(
            "WITH MEMBER [Measures].[Unit Sales VM] AS "
            + "'ValidMeasure([Measures].[Unit Sales])', SOLVE_ORDER = 3000 "
            + "MEMBER [Gender].[COG_OQP_USR_Aggregate(Gender SET)] AS "
            + "'IIF([Measures].CURRENTMEMBER IS [Measures].[Unit Sales VM], "
            + "([Gender].[COG_OQP_INT_m2], [Measures].[Unit Sales VM]), "
            + "AGGREGATE([COG_OQP_INT_s1]))', SOLVE_ORDER = 4 "
            + "MEMBER [Gender].[COG_OQP_INT_m2] AS "
            + "'AGGREGATE([COG_OQP_INT_s1])', SOLVE_ORDER = 4 "
            + "MEMBER [WAREHOUSE].[COG_OQP_USR_Aggregate(WAREHOUSE SET)] AS "
            + "'IIF([Measures].CURRENTMEMBER IS [Measures].[Unit Sales VM], "
            + "([WAREHOUSE].[COG_OQP_INT_m1], [Measures].[Unit Sales VM]), "
            + "AGGREGATE({[Warehouse].[State Province].&[DF].[Mexico City].[Freeman And Co], "
            + "[Warehouse].[State Province].&[BC].[Vancouver].[Bellmont Distributing]}))', "
            + "SOLVE_ORDER = 8 "
            + "MEMBER [WAREHOUSE].[COG_OQP_INT_m1] AS "
            + "'AGGREGATE({[Warehouse].[State Province].&[DF].[Mexico City].[Freeman And Co], "
            + "[Warehouse].[State Province].&[BC].[Vancouver].[Bellmont Distributing]})', "
            + "SOLVE_ORDER = 8 "
            + "SET [COG_OQP_INT_s2] AS "
            + "'CROSSJOIN({[Gender].[All Gender].[M], [Gender].[All Gender].[F]}, "
            + "{{[Warehouse].[State Province].&[DF].[Mexico City].[Freeman And Co], "
            + "[Warehouse].[State Province].&[BC].[Vancouver].[Bellmont Distributing]}, "
            + "{([Warehouse].[COG_OQP_USR_Aggregate(Warehouse SET)])}})' "
            + "SET [COG_OQP_INT_s1] AS "
            + "'CROSSJOIN({[Gender].[All Gender].[M], [Gender].[All Gender].[F]}, "
            + "{[Warehouse].[State Province].&[DF].[Mexico City].[Freeman And Co], "
            + "[Warehouse].[State Province].&[BC].[Vancouver].[Bellmont Distributing]})' "
            + "SELECT "
            + "{[Measures].[Unit Sales VM]} ON AXIS(0), "
            + "{[COG_OQP_INT_s2], HEAD({([Gender].[COG_OQP_USR_Aggregate(Gender SET)], "
            + "[WAREHOUSE].DEFAULTMEMBER)}, "
            + "IIF(COUNT([COG_OQP_INT_s1], INCLUDEEMPTY) > 0, 1, 0))} ON AXIS(1) "
            + "FROM [WAREHOUSE AND SALES2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales VM]}\n"
            + "Axis #2:\n"
            + "{[Gender].[Gender].[M], [Warehouse].[Warehouse].[Mexico].[DF].[Mexico City].[Freeman And Co]}\n"
            + "{[Gender].[Gender].[M], [Warehouse].[Warehouse].[Canada].[BC].[Vancouver].[Bellmont Distributing]}\n"
            + "{[Gender].[Gender].[M], [Warehouse].[Warehouse].[COG_OQP_USR_Aggregate(WAREHOUSE SET)]}\n"
            + "{[Gender].[Gender].[F], [Warehouse].[Warehouse].[Mexico].[DF].[Mexico City].[Freeman And Co]}\n"
            + "{[Gender].[Gender].[F], [Warehouse].[Warehouse].[Canada].[BC].[Vancouver].[Bellmont Distributing]}\n"
            + "{[Gender].[Gender].[F], [Warehouse].[Warehouse].[COG_OQP_USR_Aggregate(WAREHOUSE SET)]}\n"
            + "{[Gender].[Gender].[COG_OQP_USR_Aggregate(Gender SET)], [Warehouse].[Warehouse].[All Warehouses]}\n"
            + "Row #0: 135,215\n"
            + "Row #1: 135,215\n"
            + "Row #2: 135,215\n"
            + "Row #3: 131,558\n"
            + "Row #4: 131,558\n"
            + "Row #5: 131,558\n"
            + "Row #6: 266,773\n");
    }

    public void testTotalingOnNonJoiningDimension() {
        assertQueryReturns(
            "WITH MEMBER [Measures].[Unit Sales VM] AS "
            + "'ValidMeasure([Measures].[Unit Sales])', SOLVE_ORDER =3000"
            + "MEMBER MEASURES.[VirtualMeasure] AS "
            + "'[Measures].[Store Invoice]/[Measures].[Unit Sales VM]', SOLVE_ORDER=3000 "
            + "MEMBER [Warehouse].[COG_OQP_USR_Aggregate(Warehouse set)] AS "
            + "'IIF([Measures].CURRENTMEMBER IS "
            + "[Measures].[VirtualMeasure], ([Warehouse].[COG_OQP_INT_m1], "
            + "[Measures].[VirtualMeasure]), "
            + "AGGREGATE({[Warehouse].[All Warehouses].[USA].[OR],"
            + "[Warehouse].[All Warehouses].[USA].[WA]}))', SOLVE_ORDER = 8 "
            + "MEMBER [Warehouse].[COG_OQP_INT_m1] AS "
            + "'AGGREGATE({[Warehouse].[All Warehouses].[USA].[OR],"
            + "[Warehouse].[All Warehouses].[USA].[WA]})', SOLVE_ORDER = 8 "
            + "MEMBER [Product].[COG_OQP_USR_Aggregate(Product Set)1] "
            + "AS 'IIF([Measures].CURRENTMEMBER IS [Measures].[VirtualMeasure], "
            + "([Product].[COG_OQP_INT_m2], [Measures].[VirtualMeasure]), "
            + "AGGREGATE([COG_OQP_INT_s3]))', SOLVE_ORDER = 4 "
            + "MEMBER [Product].[COG_OQP_INT_m2] "
            + "AS 'AGGREGATE([COG_OQP_INT_s3])', SOLVE_ORDER = 4 "
            + "SET [COG_OQP_INT_s4] AS "
            + "'CROSSJOIN({[Product].[All Products].[Drink],[Product].[All Products].[Food]}, "
            + "{{[Warehouse].[All Warehouses].[USA].[OR],[Warehouse].[All Warehouses].[USA].[WA]}, "
            + "{([Warehouse].[COG_OQP_USR_Aggregate(Warehouse set)])}})' "
            + "SET [COG_OQP_INT_s3] AS "
            + "'CROSSJOIN({[Product].[All Products].[Drink],[Product].[All Products].[Food]},"
            + "{{[Warehouse].[All Warehouses].[USA].[OR],[Warehouse].[All Warehouses].[USA].[WA]}})' "
            + "SET [COG_OQP_INT_s2] AS "
            + "'{[Measures].[Store Invoice],[Measures].[Unit Sales VM],[Measures].[VirtualMeasure]}' "
            + "SELECT "
            + "[COG_OQP_INT_s2] DIMENSION PROPERTIES PARENT_LEVEL, "
            + "PARENT_UNIQUE_NAME ON AXIS(0), "
            + "{[COG_OQP_INT_s4], HEAD({([Product].[COG_OQP_USR_Aggregate(Product Set)1], "
            + "[Warehouse].DEFAULTMEMBER)}, "
            + "IIF(COUNT([COG_OQP_INT_s3], INCLUDEEMPTY) > 0, 1, 0))} "
            + "DIMENSION PROPERTIES PARENT_LEVEL, PARENT_UNIQUE_NAME ON AXIS(1) "
            + "FROM [WAREHOUSE AND SALES2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Invoice]}\n"
            + "{[Measures].[Unit Sales VM]}\n"
            + "{[Measures].[VirtualMeasure]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[Drink], [Warehouse].[Warehouse].[USA].[OR]}\n"
            + "{[Product].[Product].[Drink], [Warehouse].[Warehouse].[USA].[WA]}\n"
            + "{[Product].[Product].[Drink], [Warehouse].[Warehouse].[COG_OQP_USR_Aggregate(Warehouse set)]}\n"
            + "{[Product].[Product].[Food], [Warehouse].[Warehouse].[USA].[OR]}\n"
            + "{[Product].[Product].[Food], [Warehouse].[Warehouse].[USA].[WA]}\n"
            + "{[Product].[Product].[Food], [Warehouse].[Warehouse].[COG_OQP_USR_Aggregate(Warehouse set)]}\n"
            + "{[Product].[Product].[COG_OQP_USR_Aggregate(Product Set)1], [Warehouse].[Warehouse].[All Warehouses]}\n"
            + "Row #0: 2,057.232\n"
            + "Row #0: 24,597\n"
            + "Row #0: 0.084\n"
            + "Row #1: 4,868.471\n"
            + "Row #1: 24,597\n"
            + "Row #1: 0.198\n"
            + "Row #2: 6,925.702\n"
            + "Row #2: 24,597\n"
            + "Row #2: 0.282\n"
            + "Row #3: 13,726.825\n"
            + "Row #3: 191,940\n"
            + "Row #3: 0.072\n"
            + "Row #4: 37,712.692\n"
            + "Row #4: 191,940\n"
            + "Row #4: 0.196\n"
            + "Row #5: 51,439.517\n"
            + "Row #5: 191,940\n"
            + "Row #5: 0.268\n"
            + "Row #6: 58,365.22\n"
            + "Row #6: 216,537\n"
            + "Row #6: 0.27\n");
    }

    public void testUnrelatedDimPropOverridesIgnoreMeasure() {
        propSaver.set(
            propSaver.props.IgnoreMeasureForNonJoiningDimension, true);
        assertQueryReturns(
            "WITH\n"
            + "MEMBER [Measures].[Total Sales] AS '[Measures].[Store Sales] + "
            + "[Measures].[Warehouse Sales]'\n"
            + "MEMBER [Product].[AggSP1_1] AS\n"
            + "'IIF([Measures].CURRENTMEMBER IS [Measures].[Total Sales],\n"
            + "[Warehouse].[All Warehouses],\n"
            + "[Warehouse].[All Warehouses])'\n"
            + "MEMBER [Product].[AggSP1_2] AS\n"
            + "'IIF([Measures].CURRENTMEMBER IS [Measures].[Total Sales],\n"
            + "([Warehouse].[All Warehouses]),\n"
            + "([Warehouse].[All Warehouses]))'\n"
            + "\n"
            + "SELECT\n"
            + "{[Measures].[Total Sales]} ON AXIS(0),\n"
            + "{[Product].[AggSP1_1], [Product].[AggSP1_2]} ON AXIS(1)\n"
            + "FROM\n"
            + "[Warehouse and Sales2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Total Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[AggSP1_1]}\n"
            + "{[Product].[Product].[AggSP1_2]}\n"
            + "Row #0: 762,009.02\n"
            + "Row #1: 762,009.02\n");
    }
}

// End IgnoreUnrelatedDimensionsTest.java
