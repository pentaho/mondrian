/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

/**
 * <code>SharedDimensionTest</code> tests shared dimensions.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class SharedDimensionTest extends FoodMartTestCase {

    public static final String sharedDimension =
        "<Dimension name=\"Employee\">\n"
        + "  <Hierarchy hasAll=\"true\" primaryKey=\"employee_id\" primaryKeyTable=\"employee\">\n"
        + "    <Join leftKey=\"supervisor_id\" rightKey=\"employee_id\">\n"
        + "      <Table name=\"employee\" alias=\"employee\" />\n"
        + "      <Table name=\"employee\" alias=\"employee_manager\" />\n"
        + "    </Join>\n"
        + "    <Level name=\"Role\" table=\"employee_manager\" column=\"management_role\" uniqueMembers=\"true\"/>\n"
        + "    <Level name=\"Title\" table=\"employee_manager\" column=\"position_title\" uniqueMembers=\"false\"/>\n"
        + "  </Hierarchy>\n"
        + "</Dimension>";

    // Base Cube A: use product_id as foreign key for Employee diemnsion
    // because there exist rows satidfying the join condition
    // "employee.employee_id = inventory_fact_1997.product_id"
    public static final String cubeA =
        "<Cube name=\"Employee Store Analysis A\">\n"
        + "  <Table name=\"inventory_fact_1997\" alias=\"inventory\" />\n"
        + "  <DimensionUsage name=\"Employee\" source=\"Employee\" foreignKey=\"product_id\" />\n"
        + "  <DimensionUsage name=\"Store Type\" source=\"Store Type\" foreignKey=\"warehouse_id\" />\n"
        + "  <Measure name=\"Employee Store Sales\" aggregator=\"sum\" formatString=\"$#,##0\" column=\"warehouse_sales\" />\n"
        + "  <Measure name=\"Employee Store Cost\" aggregator=\"sum\" formatString=\"$#,##0\" column=\"warehouse_cost\" />\n"
        + "</Cube>";

    // Base Cube B: use time_id as foreign key for Employee diemnsion
    // because there exist rows satidfying the join condition
    // "employee.employee_id = inventory_fact_1997.time_id"
    public static final String cubeB =
        "<Cube name=\"Employee Store Analysis B\">\n"
        + "  <Table name=\"inventory_fact_1997\" alias=\"inventory\" />\n"
        + "  <DimensionUsage name=\"Employee\" source=\"Employee\" foreignKey=\"time_id\" />\n"
        + "  <DimensionUsage name=\"Store Type\" source=\"Store Type\" foreignKey=\"store_id\" />\n"
        + "  <Measure name=\"Employee Store Sales\" aggregator=\"sum\" formatString=\"$#,##0\" column=\"warehouse_sales\" />\n"
        + "  <Measure name=\"Employee Store Cost\" aggregator=\"sum\" formatString=\"$#,##0\" column=\"warehouse_cost\" />\n"
        + "</Cube>";

    public static final String virtualCube =
        "<VirtualCube name=\"Employee Store Analysis\">\n"
        + "  <VirtualCubeDimension name=\"Employee\"/>\n"
        + "  <VirtualCubeDimension name=\"Store Type\"/>\n"
        + "  <VirtualCubeMeasure cubeName=\"Employee Store Analysis A\" name=\"[Measures].[Employee Store Sales]\"/>\n"
        + "  <VirtualCubeMeasure cubeName=\"Employee Store Analysis B\" name=\"[Measures].[Employee Store Cost]\"/>\n"
        + "</VirtualCube>";

    public static final String queryCubeA =
        "with\n"
        + "  set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Employee], [*BASE_MEMBERS_Store Type])'\n"
        + "  set [*BASE_MEMBERS_Measures] as '{[Measures].[Employee Store Sales], [Measures].[Employee Store Cost]}'\n"
        + "  set [*BASE_MEMBERS_Employee] as '[Employee].[Role].Members'\n"
        + "  set [*NATIVE_MEMBERS_Employee] as 'Generate([*NATIVE_CJ_SET], {[Employee].CurrentMember})'\n"
        + "  set [*BASE_MEMBERS_Store Type] as '[Store Type].[Store Type].Members'\n"
        + "  set [*NATIVE_MEMBERS_Store Type] as 'Generate([*NATIVE_CJ_SET], {[Store Type].CurrentMember})'\n"
        + "select\n"
        + "  [*BASE_MEMBERS_Measures] ON COLUMNS,\n"
        + "  NON EMPTY Generate([*NATIVE_CJ_SET], {([Employee].CurrentMember, [Store Type].CurrentMember)}) ON ROWS\n"
        + "from\n"
        + "  [Employee Store Analysis A]";

    public static final String queryCubeB =
        "with\n"
        + "  set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Employee], [*BASE_MEMBERS_Store Type])'\n"
        + "  set [*BASE_MEMBERS_Measures] as '{[Measures].[Employee Store Sales], [Measures].[Employee Store Cost]}'\n"
        + "  set [*BASE_MEMBERS_Employee] as '[Employee].[Role].Members'\n"
        + "  set [*NATIVE_MEMBERS_Employee] as 'Generate([*NATIVE_CJ_SET], {[Employee].CurrentMember})'\n"
        + "  set [*BASE_MEMBERS_Store Type] as '[Store Type].[Store Type].Members'\n"
        + "  set [*NATIVE_MEMBERS_Store Type] as 'Generate([*NATIVE_CJ_SET], {[Store Type].CurrentMember})'\n"
        + "select\n"
        + "  [*BASE_MEMBERS_Measures] ON COLUMNS,\n"
        + "  NON EMPTY Generate([*NATIVE_CJ_SET], {([Employee].CurrentMember, [Store Type].CurrentMember)}) ON ROWS\n"
        + "from\n"
        + "  [Employee Store Analysis B]";

    public static final String queryVirtualCube =
        "with\n"
        + "  set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Employee], [*BASE_MEMBERS_Store Type])'\n"
        + "  set [*BASE_MEMBERS_Measures] as '{[Measures].[Employee Store Sales], [Measures].[Employee Store Cost]}'\n"
        + "  set [*BASE_MEMBERS_Employee] as '[Employee].[Role].Members'\n"
        + "  set [*NATIVE_MEMBERS_Employee] as 'Generate([*NATIVE_CJ_SET], {[Employee].CurrentMember})'\n"
        + "  set [*BASE_MEMBERS_Store Type] as '[Store Type].[Store Type].Members'\n"
        + "  set [*NATIVE_MEMBERS_Store Type] as 'Generate([*NATIVE_CJ_SET], {[Store Type].CurrentMember})'\n"
        + "select\n"
        + "  [*BASE_MEMBERS_Measures] ON COLUMNS,\n"
        + "  NON EMPTY Generate([*NATIVE_CJ_SET], {([Employee].CurrentMember, [Store Type].CurrentMember)}) ON ROWS\n"
        + "from\n"
        + "  [Employee Store Analysis]";

    public static final String queryStoreCube =
        "with set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Store Type], [*BASE_MEMBERS_Store])'\n"
        + "set [*BASE_MEMBERS_Measures] as '{[Measures].[Store Sqft]}'\n"
        + "set [*BASE_MEMBERS_Store Type] as '[Store Type].[Store Type].Members'\n"
        + "set [*BASE_MEMBERS_Store] as '[Store].[Store State].Members'\n"
        + "select [*BASE_MEMBERS_Measures] ON COLUMNS,\n"
        + "Non Empty Generate([*NATIVE_CJ_SET], {[Store Type].CurrentMember}) on rows from [Store]";

    public static final String queryNECJMemberList =
        "select {[Measures].[Employee Store Sales]} on columns,\n"
        + "NonEmptyCrossJoin([Store Type].[Store Type].Members,\n"
        + "{[Employee].[All Employees].[Middle Management],\n"
        + " [Employee].[All Employees].[Store Management]})\n"
        + "on rows from [Employee Store Analysis B]";

    public static final String queryNECJMultiLevelMemberList =
        "select {[Employee Store Sales]} on columns, "
        + "NonEmptyCrossJoin([Store Type].[Store Type].Members, "
        + "{[Employee].[Store Management].[Store Manager], "
        + " [Employee].[Senior Management].[President]}) "
        + "on rows from [Employee Store Analysis B]";

    public static final String querySF1711865 =
        "select NON EMPTY {[Product].[Product Family].Members} ON COLUMNS from [Sales 2]";

    public static final String resultCubeA =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Employee Store Sales]}\n"
        + "{[Measures].[Employee Store Cost]}\n"
        + "Axis #2:\n"
        + "{[Employee].[All Employees].[Middle Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n"
        + "{[Employee].[All Employees].[Middle Management], [Store Type].[All Store Types].[Supermarket]}\n"
        + "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n"
        + "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Gourmet Supermarket]}\n"
        + "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Mid-Size Grocery]}\n"
        + "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Small Grocery]}\n"
        + "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Supermarket]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Gourmet Supermarket]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Mid-Size Grocery]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Small Grocery]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Supermarket]}\n"
        + "Row #0: $200\n"
        + "Row #0: $87\n"
        + "Row #1: $161\n"
        + "Row #1: $68\n"
        + "Row #2: $1,721\n"
        + "Row #2: $739\n"
        + "Row #3: $261\n"
        + "Row #3: $114\n"
        + "Row #4: $257\n"
        + "Row #4: $111\n"
        + "Row #5: $196\n"
        + "Row #5: $101\n"
        + "Row #6: $3,993\n"
        + "Row #6: $1,858\n"
        + "Row #7: $45,014\n"
        + "Row #7: $20,604\n"
        + "Row #8: $7,231\n"
        + "Row #8: $3,211\n"
        + "Row #9: $8,171\n"
        + "Row #9: $3,635\n"
        + "Row #10: $4,471\n"
        + "Row #10: $2,045\n"
        + "Row #11: $77,236\n"
        + "Row #11: $34,842\n";

    public static final String resultCubeB =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Employee Store Sales]}\n"
        + "{[Measures].[Employee Store Cost]}\n"
        + "Axis #2:\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Gourmet Supermarket]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Mid-Size Grocery]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Small Grocery]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Supermarket]}\n"
        + "Row #0: $61,860\n"
        + "Row #0: $28,093\n"
        + "Row #1: $10,156\n"
        + "Row #1: $4,482\n"
        + "Row #2: $10,212\n"
        + "Row #2: $4,576\n"
        + "Row #3: $5,932\n"
        + "Row #3: $2,714\n"
        + "Row #4: $108,610\n"
        + "Row #4: $49,178\n";

    public static final String resultVirtualCube =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Employee Store Sales]}\n"
        + "{[Measures].[Employee Store Cost]}\n"
        + "Axis #2:\n"
        + "{[Employee].[All Employees].[Middle Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n"
        + "{[Employee].[All Employees].[Middle Management], [Store Type].[All Store Types].[Supermarket]}\n"
        + "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n"
        + "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Gourmet Supermarket]}\n"
        + "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Mid-Size Grocery]}\n"
        + "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Small Grocery]}\n"
        + "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Supermarket]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Gourmet Supermarket]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Mid-Size Grocery]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Small Grocery]}\n"
        + "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Supermarket]}\n"
        + "Row #0: $200\n"
        + "Row #0: \n"
        + "Row #1: $161\n"
        + "Row #1: \n"
        + "Row #2: $1,721\n"
        + "Row #2: \n"
        + "Row #3: $261\n"
        + "Row #3: \n"
        + "Row #4: $257\n"
        + "Row #4: \n"
        + "Row #5: $196\n"
        + "Row #5: \n"
        + "Row #6: $3,993\n"
        + "Row #6: \n"
        + "Row #7: $45,014\n"
        + "Row #7: $28,093\n"
        + "Row #8: $7,231\n"
        + "Row #8: $4,482\n"
        + "Row #9: $8,171\n"
        + "Row #9: $4,576\n"
        + "Row #10: $4,471\n"
        + "Row #10: $2,714\n"
        + "Row #11: $77,236\n"
        + "Row #11: $49,178\n";

    // This result is actually incorrect for native evaluation.
    // Keep the test case here to test the SQL generation.
    public static final String resultStoreCube =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Store Sqft]}\n"
        + "Axis #2:\n"
        + "{[Store Type].[All Store Types].[Deluxe Supermarket]}\n"
        + "{[Store Type].[All Store Types].[Gourmet Supermarket]}\n"
        + "{[Store Type].[All Store Types].[Mid-Size Grocery]}\n"
        + "{[Store Type].[All Store Types].[Small Grocery]}\n"
        + "{[Store Type].[All Store Types].[Supermarket]}\n"
        + "Row #0: 146,045\n"
        + "Row #1: 47,447\n"
        + "Row #2: 109,343\n"
        + "Row #3: 75,281\n"
        + "Row #4: 193,480\n";

    public static final String resultNECJMemberList =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Employee Store Sales]}\n"
        + "Axis #2:\n"
        + "{[Store Type].[All Store Types].[Deluxe Supermarket], [Employee].[All Employees].[Store Management]}\n"
        + "{[Store Type].[All Store Types].[Gourmet Supermarket], [Employee].[All Employees].[Store Management]}\n"
        + "{[Store Type].[All Store Types].[Mid-Size Grocery], [Employee].[All Employees].[Store Management]}\n"
        + "{[Store Type].[All Store Types].[Small Grocery], [Employee].[All Employees].[Store Management]}\n"
        + "{[Store Type].[All Store Types].[Supermarket], [Employee].[All Employees].[Store Management]}\n"
        + "Row #0: $61,860\n"
        + "Row #1: $10,156\n"
        + "Row #2: $10,212\n"
        + "Row #3: $5,932\n"
        + "Row #4: $108,610\n";

    public static final String resultNECJMultiLevelMemberList =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Employee Store Sales]}\n"
        + "Axis #2:\n"
        + "{[Store Type].[All Store Types].[Deluxe Supermarket], [Employee].[All Employees].[Store Management].[Store Manager]}\n"
        + "{[Store Type].[All Store Types].[Gourmet Supermarket], [Employee].[All Employees].[Store Management].[Store Manager]}\n"
        + "{[Store Type].[All Store Types].[Supermarket], [Employee].[All Employees].[Store Management].[Store Manager]}\n"
        + "Row #0: $1,783\n"
        + "Row #1: $286\n"
        + "Row #2: $1,020\n";

    public static final String resultSF1711865 =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Product].[All Products].[Drink]}\n"
        + "{[Product].[All Products].[Food]}\n"
        + "{[Product].[All Products].[Non-Consumable]}\n"
        + "Row #0: 7,978\n"
        + "Row #0: 62,445\n"
        + "Row #0: 16,414\n";

    public SharedDimensionTest() {
    }

    public SharedDimensionTest(String name) {
        super(name);
    }

    public void testA() {
        // Schema has two cubes sharing a dimension.
        // Query from the first cube.
        TestContext testContext = getTestContextForSharedDimCubeACubeB();

        testContext.assertQueryReturns(queryCubeA, resultCubeA);
    }

    public void testB() {
        // Schema has two cubes sharing a dimension.
        // Query from the second cube.
        TestContext testContext = getTestContextForSharedDimCubeACubeB();

        testContext.assertQueryReturns(queryCubeB, resultCubeB);
    }

    public void testVirtualCube() {
        // Schema has two cubes sharing a dimension, and a virtual cube built
        // over these two cubes.
        // Query from the virtual cube.

        TestContext testContext =
            TestContext.create(
                sharedDimension,
                cubeA + "\n" + cubeB,
                virtualCube,
                null,
                null,
                null);

        testContext.assertQueryReturns(queryVirtualCube, resultVirtualCube);
    }

    public void testNECJMemberList() {
        // Schema has two cubes sharing a dimension.
        // Query from the second cube.
        TestContext testContext = getTestContextForSharedDimCubeACubeB();

        testContext.assertQueryReturns(
            queryNECJMemberList,
            resultNECJMemberList);
    }

    public void testNECJMultiLevelMemberList() {
        // Schema has two cubes sharing a dimension.
        // Query from the first cube.
        // This is a case where not using alias not only affects performance,
        // but also produces incorrect result.
        TestContext testContext = getTestContextForSharedDimCubeACubeB();

        testContext.assertQueryReturns(
            queryNECJMultiLevelMemberList,
            resultNECJMultiLevelMemberList);
    }

    public void testSF1711865() {
        // Test for sourceforge.net bug 1711865
        // Use the default FoodMart schema
        getTestContext().assertQueryReturns(querySF1711865, resultSF1711865);
    }


    public void testStoreCube() {
        // Use the default FoodMart schema
        getTestContext().assertQueryReturns(queryStoreCube, resultStoreCube);
    }

    private TestContext getTestContextForSharedDimCubeACubeB() {
        return TestContext.create(
            sharedDimension,
            cubeA + "\n" + cubeB,
            null,
            null,
            null,
            null);
    }
}

// End SharedDimensionTest.java
