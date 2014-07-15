/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.util.Bug;

/**
 * Unit test for shared dimensions.
 *
 * @author Rushan Chen
 */
public class SharedDimensionTest extends FoodMartTestCase {

    private static final String employeeManager =
        "  <Table name='employee' alias='employee_manager'>\n"
        + "   <Key name='key$0'>\n"
        + "    <Column table='employee_manager' name='employee_id'/>\n"
        + "   </Key>\n"
        + "  </Table>\n"
        + "  <Link source='employee_manager' target='employee' key='key$0'>\n"
        + "   <ForeignKey>\n"
        + "    <Column table='employee' name='supervisor_id'/>\n"
        + "   </ForeignKey>\n"
        + "  </Link>\n";

    private static final String sharedDimension =
        " <Dimension name='Employee' visible='true' key='$Id' table='employee_manager'>\n"
        + "  <Hierarchies>\n"
        + "   <Hierarchy name='Employee' visible='true' hasAll='true'>\n"
        + "    <Level attribute='Role'/>\n"
        + "    <Level attribute='Title'/>\n"
        + "   </Hierarchy>\n"
        + "  </Hierarchies>\n"
        + "  <Attributes>\n"
        + "   <Attribute name='Role' hasHierarchy='false' keyColumn='management_role'/>\n"
        + "   <Attribute name='Title' hasHierarchy='false'>\n"
        + "    <Key>\n"
        + "     <Column name='management_role'/>\n"
        + "     <Column name='position_title'/>\n"
        + "    </Key>\n"
        + "    <Name>\n"
        + "     <Column name='position_title'/>\n"
        + "    </Name>\n"
        + "   </Attribute>\n"
        + "   <Attribute name='$Id' table='employee' keyColumn='employee_id' hasHierarchy='false'/>\n"
        + "  </Attributes>\n"
        + " </Dimension>\n"
        + " <Dimension name='Store Type' table='store' visible='true' key='$Id'>\n"
        + "  <Hierarchies>\n"
        + "   <Hierarchy name='Store Type' visible='true' hasAll='true'>\n"
        + "    <Level name='Store Type' visible='true' attribute='Store Type'>\n"
        + "    </Level>\n"
        + "   </Hierarchy>\n"
        + "  </Hierarchies>\n"
        + "  <Attributes>\n"
        + "   <Attribute name='Store Type' hasHierarchy='false' keyColumn='store_type'/>\n"
        + "   <Attribute name='$Id' keyColumn='store_id' hasHierarchy='false'>\n"
        + "   </Attribute>\n"
        + "  </Attributes>\n"
        + " </Dimension>\n";


    // Base Cube A: use product_id as foreign key for Employee diemnsion
    // because there exist rows satidfying the join condition
    // "employee.employee_id = inventory_fact_1997.product_id"
    private static final String cubeA =
        " <Cube name='Employee Store Analysis A'>\n"
        + "  <Dimensions>\n"
        + "   <Dimension name='Employee' source='Employee'/>\n"
        + "   <Dimension name='Store Type' source='Store Type'/>\n"
        + "  </Dimensions>\n"
        + "  <MeasureGroups>\n"
        + "   <MeasureGroup name='Employee Store Analysis A' type='fact' table='inventory_fact_1997'>\n"
        + "    <Measures>\n"
        + "     <Measure name='Employee Store Sales' formatString='$#,##0' aggregator='sum' column='warehouse_sales'/>\n"
        + "     <Measure name='Employee Store Cost' formatString='$#,##0' aggregator='sum' column='warehouse_cost'/>\n"
        + "   </Measures>"
        + "    <DimensionLinks>\n"
        + "     <ForeignKeyLink dimension='Employee' foreignKeyColumn='product_id'/>\n"
        + "     <ForeignKeyLink dimension='Store Type' foreignKeyColumn='warehouse_id'/>\n"
        + "    </DimensionLinks>\n"
        + "   </MeasureGroup>\n"
        + "  </MeasureGroups>\n"
        + " </Cube>\n";

    // Base Cube B: use time_id as foreign key for Employee dimension
    // because there exist rows satisfying the join condition
    // "employee.employee_id = inventory_fact_1997.time_id"
    private static final String cubeB =
        " <Cube name='Employee Store Analysis B'>\n"
        + "  <Dimensions>\n"
        + "   <Dimension name='Employee' source='Employee'/>\n"
        + "   <Dimension name='Store Type' source='Store Type'/>"
        + "  </Dimensions>\n"
        + "  <MeasureGroups>\n"
        + "   <MeasureGroup name='Employee Store Analysis B' type='fact' table='inventory_fact_1997'>\n"
        + "    <Measures>\n"
        + "     <Measure name='Employee Store Sales' formatString='$#,##0' aggregator='sum' column='warehouse_sales'/>\n"
        + "     <Measure name='Employee Store Cost' formatString='$#,##0' aggregator='sum' column='warehouse_cost'/>\n"
        + "    </Measures>\n"
        + "    <DimensionLinks>\n"
        + "     <ForeignKeyLink dimension='Employee' foreignKeyColumn='time_id'/>\n"
        + "     <ForeignKeyLink dimension='Store Type' foreignKeyColumn='store_id'/>\n"
        + "    </DimensionLinks>\n"
        + "   </MeasureGroup>\n"
        + "  </MeasureGroups>\n"
        + " </Cube>\n";

    // Some product_id's match store_id. Used to test MONDRIAN-1243
    // without having to alter fact table.
    private static final String cubeAltSales =
        " <Cube name='Alternate Sales'>\n"
        + "  <Dimensions>\n"
        + "   <Dimension name='Store' source='Store'/>\n"
        + "   <Dimension name='Buyer' source='Store'/>\n"
        + "   <Dimension name='BuyerTwo' source='Store'/>\n"
        + "   <Dimension name='Time' source='Time'/>\n"
        + "  </Dimensions>\n"
        + "  <MeasureGroups>\n"
        + "   <MeasureGroup name='Alternate Sales' type='fact' table='sales_fact_1997'>\n"
        + "    <Measures>\n"
        + "     <Measure name='Unit Sales' formatString='Standard' aggregator='sum' column='unit_sales'/>\n"
        + "    </Measures>\n"
        + "    <DimensionLinks>\n"
        + "     <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>\n"
        + "     <ForeignKeyLink dimension='Buyer' foreignKeyColumn='product_id'/>\n"
        + "     <ForeignKeyLink dimension='BuyerTwo' foreignKeyColumn='product_id'/>\n"
        + "     <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>\n"
        + "    </DimensionLinks>\n"
        + "   </MeasureGroup>\n"
        + "  </MeasureGroups>\n"
        + " </Cube>\n";

    private static final String queryCubeA =
        "with\n"
        + "  set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Employee], [*BASE_MEMBERS_Store Type])'\n"
        + "  set [*BASE_MEMBERS_Measures] as '{[Measures].[Employee Store Sales], [Measures].[Employee Store Cost]}'\n"
        + "  set [*BASE_MEMBERS_Employee] as '[Employee].[Role].Members'\n"
        + "  set [*NATIVE_MEMBERS_Employee] as 'Generate([*NATIVE_CJ_SET], {[Employee].CurrentMember})'\n"
        + "  set [*BASE_MEMBERS_Store Type] as '[Store Type].[Store Type].[Store Type].Members'\n"
        + "  set [*NATIVE_MEMBERS_Store Type] as 'Generate([*NATIVE_CJ_SET], {[Store Type].[Store Type].CurrentMember})'\n"
        + "select\n"
        + "  [*BASE_MEMBERS_Measures] ON COLUMNS,\n"
        + "  NON EMPTY Generate([*NATIVE_CJ_SET], {([Employee].CurrentMember, [Store Type].[Store Type].CurrentMember)}) ON ROWS\n"
        + "from\n"
        + "  [Employee Store Analysis A]";

    private static final String queryCubeB =
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

    private static final String queryStoreCube =
        "with set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Store Type], [*BASE_MEMBERS_Store])'\n"
        + "set [*BASE_MEMBERS_Measures] as '{[Measures].[Store Sqft]}'\n"
        + "set [*BASE_MEMBERS_Store Type] as '[Store].[Store Type].[Store Type].Members'\n"
        + "set [*BASE_MEMBERS_Store] as '[Store].[Store State].Members'\n"
        + "select [*BASE_MEMBERS_Measures] ON COLUMNS,\n"
        + "Non Empty Generate([*NATIVE_CJ_SET], {[Store].[Store Type].CurrentMember}) on rows from [Store]";

    private static final String queryNECJMemberList =
        "select {[Measures].[Employee Store Sales]} on columns,\n"
        + "NonEmptyCrossJoin([Store Type].[Store Type].Members,\n"
        + "{[Employee].[All Employees].[Middle Management],\n"
        + " [Employee].[All Employees].[Store Management]})\n"
        + "on rows from [Employee Store Analysis B]";

    private static final String queryNECJMultiLevelMemberList =
        "select {[Employee Store Sales]} on columns, "
        + "NonEmptyCrossJoin([Store Type].[Store Type].Members, "
        + "{[Employee].[Store Management].[Store Manager], "
        + " [Employee].[Senior Management].[President]}) "
        + "on rows from [Employee Store Analysis B]";

    private static final String querySF1711865 =
        "select NON EMPTY {[Product].[Product Family].Members} ON COLUMNS from [Sales 2]";

    private static final String resultCubeA =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Employee Store Sales]}\n"
        + "{[Measures].[Employee Store Cost]}\n"
        + "Axis #2:\n"
        + "{[Employee].[Employee].[Middle Management], [Store Type].[Store Type].[Deluxe Supermarket]}\n"
        + "{[Employee].[Employee].[Middle Management], [Store Type].[Store Type].[Supermarket]}\n"
        + "{[Employee].[Employee].[Senior Management], [Store Type].[Store Type].[Deluxe Supermarket]}\n"
        + "{[Employee].[Employee].[Senior Management], [Store Type].[Store Type].[Gourmet Supermarket]}\n"
        + "{[Employee].[Employee].[Senior Management], [Store Type].[Store Type].[Mid-Size Grocery]}\n"
        + "{[Employee].[Employee].[Senior Management], [Store Type].[Store Type].[Small Grocery]}\n"
        + "{[Employee].[Employee].[Senior Management], [Store Type].[Store Type].[Supermarket]}\n"
        + "{[Employee].[Employee].[Store Management], [Store Type].[Store Type].[Deluxe Supermarket]}\n"
        + "{[Employee].[Employee].[Store Management], [Store Type].[Store Type].[Gourmet Supermarket]}\n"
        + "{[Employee].[Employee].[Store Management], [Store Type].[Store Type].[Mid-Size Grocery]}\n"
        + "{[Employee].[Employee].[Store Management], [Store Type].[Store Type].[Small Grocery]}\n"
        + "{[Employee].[Employee].[Store Management], [Store Type].[Store Type].[Supermarket]}\n"
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

    private static final String resultCubeB =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Employee Store Sales]}\n"
        + "{[Measures].[Employee Store Cost]}\n"
        + "Axis #2:\n"
        + "{[Employee].[Store Management], [Store].[Store Type].[Deluxe Supermarket]}\n"
        + "{[Employee].[Store Management], [Store].[Store Type].[Gourmet Supermarket]}\n"
        + "{[Employee].[Store Management], [Store].[Store Type].[Mid-Size Grocery]}\n"
        + "{[Employee].[Store Management], [Store].[Store Type].[Small Grocery]}\n"
        + "{[Employee].[Store Management], [Store].[Store Type].[Supermarket]}\n"
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

    // This result is actually incorrect for native evaluation.
    // Keep the test case here to test the SQL generation.
    private static final String resultStoreCube =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Store Sqft]}\n"
        + "Axis #2:\n"
        + "{[Store].[Store Type].[Deluxe Supermarket]}\n"
        + "{[Store].[Store Type].[Gourmet Supermarket]}\n"
        + "{[Store].[Store Type].[Mid-Size Grocery]}\n"
        + "{[Store].[Store Type].[Small Grocery]}\n"
        + "{[Store].[Store Type].[Supermarket]}\n"
        + "Row #0: 146,045\n"
        + "Row #1: 47,447\n"
        + "Row #2: 109,343\n"
        + "Row #3: 75,281\n"
        + "Row #4: 193,480\n";

    private static final String resultNECJMemberList =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Employee Store Sales]}\n"
        + "Axis #2:\n"
        + "{[Store].[Store Type].[Deluxe Supermarket], [Employee].[Store Management]}\n"
        + "{[Store].[Store Type].[Gourmet Supermarket], [Employee].[Store Management]}\n"
        + "{[Store].[Store Type].[Mid-Size Grocery], [Employee].[Store Management]}\n"
        + "{[Store].[Store Type].[Small Grocery], [Employee].[Store Management]}\n"
        + "{[Store].[Store Type].[Supermarket], [Employee].[Store Management]}\n"
        + "Row #0: $61,860\n"
        + "Row #1: $10,156\n"
        + "Row #2: $10,212\n"
        + "Row #3: $5,932\n"
        + "Row #4: $108,610\n";

    private static final String resultNECJMultiLevelMemberList =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Employee Store Sales]}\n"
        + "Axis #2:\n"
        + "{[Store].[Store Type].[Deluxe Supermarket], [Employee].[Store Management].[Store Manager]}\n"
        + "{[Store].[Store Type].[Gourmet Supermarket], [Employee].[Store Management].[Store Manager]}\n"
        + "{[Store].[Store Type].[Supermarket], [Employee].[Store Management].[Store Manager]}\n"
        + "Row #0: $1,783\n"
        + "Row #1: $286\n"
        + "Row #2: $1,020\n";

    private static final String resultSF1711865 =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Product].[Products].[Drink]}\n"
        + "{[Product].[Products].[Food]}\n"
        + "{[Product].[Products].[Non-Consumable]}\n"
        + "Row #0: 7,978\n"
        + "Row #0: 62,445\n"
        + "Row #0: 16,414\n";

    private static final String queryIssue1243 =
        "select [Measures].[Unit Sales] on columns,\n"
        + "non empty [Buyer].[USA].[OR].[Portland].children on rows\n"
        + "from [Alternate Sales]";

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
        if (!Bug.BugMondrian1324Fixed) {
            return;
        }
        // Schema has two cubes sharing a dimension.
        // Query from the second cube.
        TestContext testContext = getTestContextForSharedDimCubeACubeB();

        testContext.assertQueryReturns(queryCubeB, resultCubeB);
    }

    public void testNECJMemberList() {
        if (!Bug.BugMondrian1324Fixed) {
            return;
        }
        // Schema has two cubes sharing a dimension.
        // Query from the second cube.
        TestContext testContext = getTestContextForSharedDimCubeACubeB();

        testContext.assertQueryReturns(
            queryNECJMemberList,
            resultNECJMemberList);
    }

    public void testNECJMultiLevelMemberList() {
        if (!Bug.BugMondrian1324Fixed) {
            return;
        }
        // Schema has two cubes sharing a dimension.
        // Query from the first cube.
        // This is a case where not using alias not only affects performance,
        // but also produces incorrect result.
        TestContext testContext = getTestContextForSharedDimCubeACubeB();

        testContext.assertQueryReturns(
            queryNECJMultiLevelMemberList,
            resultNECJMultiLevelMemberList);
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-286">
     * MONDRIAN-286, "NullPointerException for certain mdx using [Sales 2]"</a>.
     */
    public void testBugMondrian286() {
        // Test for sourceforge.net bug 1711865 (MONDRIAN-286).
        // Use the default FoodMart schema
        getTestContext().assertQueryReturns(querySF1711865, resultSF1711865);
    }

    public void testStoreCube() {
        // Use the default FoodMart schema
        getTestContext().assertQueryReturns(queryStoreCube, resultStoreCube);
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-1243">
     * MONDRIAN-1243, "Wrong table alias in SQL generated to populate member
     * cache"</a>.
     */
    public void testBugMondrian1243WrongAlias() {
        getTestContextForSharedDimCubeAltSales().executeQuery(queryIssue1243);
    }

    public void testMemberUniqueNameForSharedWithChangedName() {
        if (Bug.BugMondrian1416Fixed) {
            getTestContextForSharedDimCubeAltSales().assertQueryReturns(
                "with "
                + " member [BuyerTwo].[Stores].[Mexico].[calc] as '[BuyerTwo].[Stores].[Mexico]' "
                + "select [BuyerTwo].[Stores].[Mexico].[calc] on 0 from [Alternate Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[BuyerTwo].[Stores].[Mexico].[calc]}\n"
                + "Row #0: 1,389\n");
        } else {
            getTestContextForSharedDimCubeAltSales().assertQueryReturns(
                "with "
                + " member [BuyerTwo].[Stores].[Mexico].[calc] as '[BuyerTwo].[Stores].[Mexico]' "
                + "select [BuyerTwo].[Stores].[calc] on 0 from [Alternate Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[BuyerTwo].[Stores].[Mexico].[calc]}\n"
                + "Row #0: 1,389\n");
        }
    }

    private TestContext getTestContextForSharedDimCubeACubeB() {
        return TestContext.instance()
            .insertPhysTable(employeeManager)
            .insertCube(cubeA)
            .insertCube(cubeB)
            .insertSharedDimension(sharedDimension);
    }

    private TestContext getTestContextForSharedDimCubeAltSales() {
        return getTestContext().insertCube(cubeAltSales);
    }
}

// End SharedDimensionTest.java
