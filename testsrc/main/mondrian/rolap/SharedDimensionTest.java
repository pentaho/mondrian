/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

/**
 * <code>SharedDimensionTest</code> tests for share dimensions.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class SharedDimensionTest  extends FoodMartTestCase {

    public static String sharedDimension = 
        "<Dimension name=\"Employee\">\n" +
        "  <Hierarchy hasAll=\"true\" primaryKey=\"employee_id\" primaryKeyTable=\"employee\">\n" +
        "    <Join leftKey=\"supervisor_id\" rightKey=\"employee_id\">\n" +
        "      <Table name=\"employee\" alias=\"employee\" />\n" +
        "      <Table name=\"employee\" alias=\"employee_manager\" />\n" +
        "    </Join>\n" +
        "    <Level name=\"Manager\" table=\"employee_manager\" column=\"management_role\" uniqueMembers=\"true\"/>\n" +
        "  </Hierarchy>\n" +
        "</Dimension>";

    // Base Cube A: use product_id as foreign key for Employee diemnsion
    // because there exist rows satidfying the join condition
    // "employee.employee_id = inventory_fact_1997.product_id"
    public static String cubeA =
        "<Cube name=\"Employee Store Analysis A\">\n" +
        "  <Table name=\"inventory_fact_1997\" alias=\"inventory\" />\n" +
        "  <DimensionUsage name=\"Employee\" source=\"Employee\" foreignKey=\"product_id\" />\n" +
        "  <DimensionUsage name=\"Store Type\" source=\"Store Type\" foreignKey=\"warehouse_id\" />\n" +
        "  <Measure name=\"Employee Store Sales\" aggregator=\"sum\" formatString=\"$#,##0\" column=\"warehouse_sales\" />\n" +
        "  <Measure name=\"Employee Store Cost\" aggregator=\"sum\" formatString=\"$#,##0\" column=\"warehouse_cost\" />\n" +
        "</Cube>";

    // Base Cube B: use time_id as foreign key for Employee diemnsion
    // because there exist rows satidfying the join condition
    // "employee.employee_id = inventory_fact_1997.time_id"
    public static String cubeB =
        "<Cube name=\"Employee Store Analysis B\">\n" +
        "  <Table name=\"inventory_fact_1997\" alias=\"inventory\" />\n" +
        "  <DimensionUsage name=\"Employee\" source=\"Employee\" foreignKey=\"time_id\" />\n" +
        "  <DimensionUsage name=\"Store Type\" source=\"Store Type\" foreignKey=\"store_id\" />\n" +
        "  <Measure name=\"Employee Store Sales\" aggregator=\"sum\" formatString=\"$#,##0\" column=\"warehouse_sales\" />\n" +
        "  <Measure name=\"Employee Store Cost\" aggregator=\"sum\" formatString=\"$#,##0\" column=\"warehouse_cost\" />\n" +
        "</Cube>";

    public static String virtualCube =
        "<VirtualCube name=\"Employee Store Analysis\">\n" +
        "  <VirtualCubeDimension name=\"Employee\"/>\n" +
        "  <VirtualCubeDimension name=\"Store Type\"/>\n" +
        "  <VirtualCubeMeasure cubeName=\"Employee Store Analysis A\" name=\"[Measures].[Employee Store Sales]\"/>\n" +
        "  <VirtualCubeMeasure cubeName=\"Employee Store Analysis B\" name=\"[Measures].[Employee Store Cost]\"/>\n" +
        "</VirtualCube>";

    public static String queryCubeA =
        "with\n" +
        "  set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Employee], [*BASE_MEMBERS_Store Type])'\n" +
        "  set [*BASE_MEMBERS_Measures] as '{[Measures].[Employee Store Sales], [Measures].[Employee Store Cost]}'\n" +
        "  set [*BASE_MEMBERS_Employee] as '[Employee].[Manager].Members'\n" +
        "  set [*NATIVE_MEMBERS_Employee] as 'Generate([*NATIVE_CJ_SET], {[Employee].CurrentMember})'\n" +
        "  set [*BASE_MEMBERS_Store Type] as '[Store Type].[Store Type].Members'\n" +
        "  set [*NATIVE_MEMBERS_Store Type] as 'Generate([*NATIVE_CJ_SET], {[Store Type].CurrentMember})'\n" +
        "select\n" +
        "  [*BASE_MEMBERS_Measures] ON COLUMNS,\n" +
        "  NON EMPTY Generate([*NATIVE_CJ_SET], {([Employee].CurrentMember, [Store Type].CurrentMember)}) ON ROWS\n" +
        "from\n" +
        "  [Employee Store Analysis A]";

    public static String queryCubeB =
        "with\n" +
        "  set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Employee], [*BASE_MEMBERS_Store Type])'\n" +
        "  set [*BASE_MEMBERS_Measures] as '{[Measures].[Employee Store Sales], [Measures].[Employee Store Cost]}'\n" +
        "  set [*BASE_MEMBERS_Employee] as '[Employee].[Manager].Members'\n" +
        "  set [*NATIVE_MEMBERS_Employee] as 'Generate([*NATIVE_CJ_SET], {[Employee].CurrentMember})'\n" +
        "  set [*BASE_MEMBERS_Store Type] as '[Store Type].[Store Type].Members'\n" +
        "  set [*NATIVE_MEMBERS_Store Type] as 'Generate([*NATIVE_CJ_SET], {[Store Type].CurrentMember})'\n" +
        "select\n" +
        "  [*BASE_MEMBERS_Measures] ON COLUMNS,\n" +
        "  NON EMPTY Generate([*NATIVE_CJ_SET], {([Employee].CurrentMember, [Store Type].CurrentMember)}) ON ROWS\n" +
        "from\n" +
        "  [Employee Store Analysis B]";
    
    public static String queryVirtualCube =
        "with\n" +
        "  set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Employee], [*BASE_MEMBERS_Store Type])'\n" +
        "  set [*BASE_MEMBERS_Measures] as '{[Measures].[Employee Store Sales], [Measures].[Employee Store Cost]}'\n" +
        "  set [*BASE_MEMBERS_Employee] as '[Employee].[Manager].Members'\n" +
        "  set [*NATIVE_MEMBERS_Employee] as 'Generate([*NATIVE_CJ_SET], {[Employee].CurrentMember})'\n" +
        "  set [*BASE_MEMBERS_Store Type] as '[Store Type].[Store Type].Members'\n" +
        "  set [*NATIVE_MEMBERS_Store Type] as 'Generate([*NATIVE_CJ_SET], {[Store Type].CurrentMember})'\n" +
        "select\n" +
        "  [*BASE_MEMBERS_Measures] ON COLUMNS,\n" +
        "  NON EMPTY Generate([*NATIVE_CJ_SET], {([Employee].CurrentMember, [Store Type].CurrentMember)}) ON ROWS\n" +
        "from\n" +
        "  [Employee Store Analysis]";
    
    public static String queryStoreCube =
        "with set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Store Type], [*BASE_MEMBERS_Store])'\n" +
        "set [*BASE_MEMBERS_Measures] as '{[Measures].[Store Sqft]}'\n" +
        "set [*BASE_MEMBERS_Store Type] as '[Store Type].[Store Type].Members'\n" +
        "set [*BASE_MEMBERS_Store] as '[Store].[Store State].Members'\n" +
        "select [*BASE_MEMBERS_Measures] ON COLUMNS,\n" +
        "Generate([*NATIVE_CJ_SET], {[Store Type].CurrentMember}) on rows from [Store]";
    
    public static String resultCubeA =
        "Axis #0:\n" +
        "{}\n" +
        "Axis #1:\n" +
        "{[Measures].[Employee Store Sales]}\n" +
        "{[Measures].[Employee Store Cost]}\n" +
        "Axis #2:\n" +
        "{[Employee].[All Employees].[Middle Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n" +
        "{[Employee].[All Employees].[Middle Management], [Store Type].[All Store Types].[Supermarket]}\n" +
        "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n" +
        "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Gourmet Supermarket]}\n" +
        "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Mid-Size Grocery]}\n" +
        "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Small Grocery]}\n" +
        "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Supermarket]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Gourmet Supermarket]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Mid-Size Grocery]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Small Grocery]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Supermarket]}\n" +
        "Row #0: $200\n" +
        "Row #0: $87\n" +
        "Row #1: $161\n" +
        "Row #1: $68\n" +
        "Row #2: $1,721\n" +
        "Row #2: $739\n" +
        "Row #3: $261\n" +
        "Row #3: $114\n" +
        "Row #4: $257\n" +
        "Row #4: $111\n" +
        "Row #5: $196\n" +
        "Row #5: $101\n" +
        "Row #6: $3,993\n" +
        "Row #6: $1,858\n" +
        "Row #7: $45,014\n" +
        "Row #7: $20,604\n" +
        "Row #8: $7,231\n" +
        "Row #8: $3,211\n" +
        "Row #9: $8,171\n" +
        "Row #9: $3,635\n" +
        "Row #10: $4,471\n" +
        "Row #10: $2,045\n" +
        "Row #11: $77,236\n" +
        "Row #11: $34,842\n";
    
    public static String resultCubeB =
        "Axis #0:\n" +
        "{}\n" +
        "Axis #1:\n" +
        "{[Measures].[Employee Store Sales]}\n" +
        "{[Measures].[Employee Store Cost]}\n" +
        "Axis #2:\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Gourmet Supermarket]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Mid-Size Grocery]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Small Grocery]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Supermarket]}\n" +
        "Row #0: $61,860\n" +
        "Row #0: $28,093\n" +
        "Row #1: $10,156\n" +
        "Row #1: $4,482\n" +
        "Row #2: $10,212\n" +
        "Row #2: $4,576\n" +
        "Row #3: $5,932\n" +
        "Row #3: $2,714\n" +
        "Row #4: $108,610\n" +
        "Row #4: $49,178\n";

    public static String resultVirtualCube =
        "Axis #0:\n" +
        "{}\n" +
        "Axis #1:\n" +
        "{[Measures].[Employee Store Sales]}\n" +
        "{[Measures].[Employee Store Cost]}\n" +
        "Axis #2:\n" +
        "{[Employee].[All Employees].[Middle Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n" +
        "{[Employee].[All Employees].[Middle Management], [Store Type].[All Store Types].[Supermarket]}\n" +
        "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n" +
        "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Gourmet Supermarket]}\n" +
        "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Mid-Size Grocery]}\n" +
        "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Small Grocery]}\n" +
        "{[Employee].[All Employees].[Senior Management], [Store Type].[All Store Types].[Supermarket]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Deluxe Supermarket]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Gourmet Supermarket]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Mid-Size Grocery]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Small Grocery]}\n" +
        "{[Employee].[All Employees].[Store Management], [Store Type].[All Store Types].[Supermarket]}\n" +
        "Row #0: $200\n" +
        "Row #0: \n" +
        "Row #1: $161\n" +
        "Row #1: \n" +
        "Row #2: $1,721\n" +
        "Row #2: \n" +
        "Row #3: $261\n" +
        "Row #3: \n" +
        "Row #4: $257\n" +
        "Row #4: \n" +
        "Row #5: $196\n" +
        "Row #5: \n" +
        "Row #6: $3,993\n" +
        "Row #6: \n" +
        "Row #7: $45,014\n" +
        "Row #7: $28,093\n" +
        "Row #8: $7,231\n" +
        "Row #8: $4,482\n" +
        "Row #9: $8,171\n" +
        "Row #9: $4,576\n" +
        "Row #10: $4,471\n" +
        "Row #10: $2,714\n" +
        "Row #11: $77,236\n" +
        "Row #11: $49,178\n";

    public static String resultStoreCube =
        "Axis #0:\n" +
        "{}\n" +
        "Axis #1:\n" +
        "{[Measures].[Store Sqft]}\n" +
        "Axis #2:\n" +
        "{[Store Type].[All Store Types].[Deluxe Supermarket]}\n" +
        "{[Store Type].[All Store Types].[Gourmet Supermarket]}\n" +
        "{[Store Type].[All Store Types].[HeadQuarters]}\n" +
        "{[Store Type].[All Store Types].[Mid-Size Grocery]}\n" +
        "{[Store Type].[All Store Types].[Small Grocery]}\n" +
        "{[Store Type].[All Store Types].[Supermarket]}\n" +
        "Row #0: 146,045\n" +
        "Row #1: 47,447\n" +
        "Row #2: \n" +
        "Row #3: 109,343\n" +
        "Row #4: 75,281\n" +
        "Row #5: 193,480\n";
        
    public SharedDimensionTest() {
    }

    public SharedDimensionTest(String name) {
        super(name);
    }

    public void testA() {
        // Schema has two cubes sharing a dimension.
        // Query from the first cube.
        TestContext testContext =
            TestContext.create(
             sharedDimension,
             cubeA + "\n" + cubeB,
             null,
             null,
             null);

        testContext.assertQueryReturns(queryCubeA, fold(resultCubeA));
    }   

    public void testB() {
        // Schema has two cubes sharing a dimension.
        // Query from the second cube.
        TestContext testContext =
            TestContext.create(
             sharedDimension,
             cubeA + "\n" + cubeB,
             null,
             null,
             null);

        testContext.assertQueryReturns(queryCubeB, fold(resultCubeB));
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
             null);

        testContext.assertQueryReturns(queryVirtualCube, fold(resultVirtualCube));
    }

    public void testStoreCube() {
        // Use the default FoodMart schema 
        TestContext testContext =
            TestContext.create(
             null,
             null,
             null,
             null,
             null);

        testContext.assertQueryReturns(queryStoreCube, fold(resultStoreCube));
    }
    
}

// End SharedDimensionTest.java
