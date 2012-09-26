/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;

import junit.framework.Assert;

/**
 * Unit tests on the legacy (mondrian version 3) schema.
 *
 * @author jhyde
 */
public class LegacySchemaTest extends FoodMartTestCase {

    public LegacySchemaTest(String name) {
        super(name);
    }

    @Override
    public TestContext getTestContext() {
        return TestContext.instance().with(TestContext.DataSet.LEGACY_FOODMART);
    }

    public void testAllLevelName() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Gender4\" foreignKey=\"customer_id\">\n"
            + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Gender\"\n"
            + " allLevelName=\"GenderLevel\" primaryKey=\"customer_id\">\n"
            + "  <Table name=\"customer\"/>\n"
            + "    <Level name=\"Gender\" column=\"gender\" "
            + "uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");
        String mdx = "select {[Gender4].[All Gender]} on columns from Sales";
        Result result = testContext.executeQuery(mdx);
        Axis axis0 = result.getAxes()[0];
        Position pos0 = axis0.getPositions().get(0);
        Member allGender = pos0.get(0);
        String caption = allGender.getLevel().getName();
        Assert.assertEquals(caption, "GenderLevel");
    }

    public void testCatalogHierarchyBasedOnView() {
        // Don't run this test if aggregates are enabled: two levels mapped to
        // the "gender" column confuse the agg engine.
        if (MondrianProperties.instance().ReadAggregates.get()) {
            return;
        }
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Gender2\" foreignKey=\"customer_id\">\n"
            + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Gender\" primaryKey=\"customer_id\">\n"
            + "    <View alias=\"gender2\">\n"
            + "      <SQL dialect=\"generic\">\n"
            + "        <![CDATA[SELECT * FROM customer]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"oracle\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"hsqldb\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"derby\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"luciddb\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"db2\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"neoview\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"netezza\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "    </View>\n"
            + "    <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>",
            null);
        if (!testContext.getDialect().allowsFromQuery()) {
            return;
        }
        testContext.assertAxisReturns(
            "[Gender2].members",
            "[Gender2].[All Gender]\n"
            + "[Gender2].[F]\n"
            + "[Gender2].[M]");
    }

    /**
     * Run a query against a large hierarchy, to make sure that we can generate
     * joins correctly. This probably won't work in MySQL.
     */
    public void testCatalogHierarchyBasedOnView2() {
        // Don't run this test if aggregates are enabled: two levels mapped to
        // the "gender" column confuse the agg engine.
        if (MondrianProperties.instance().ReadAggregates.get()) {
            return;
        }
        if (getTestContext().getDialect().allowsFromQuery()) {
            return;
        }
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "<Dimension name=\"ProductView\" foreignKey=\"product_id\">\n"
            + "   <Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"productView\">\n"
            + "       <View alias=\"productView\">\n"
            + "           <SQL dialect=\"db2\"><![CDATA[\n"
            + "SELECT *\n"
            + "FROM \"product\", \"product_class\"\n"
            + "WHERE \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "]]>\n"
            + "           </SQL>\n"
            + "           <SQL dialect=\"mssql\"><![CDATA[\n"
            + "SELECT \"product\".\"product_id\",\n"
            + "\"product\".\"brand_name\",\n"
            + "\"product\".\"product_name\",\n"
            + "\"product\".\"SKU\",\n"
            + "\"product\".\"SRP\",\n"
            + "\"product\".\"gross_weight\",\n"
            + "\"product\".\"net_weight\",\n"
            + "\"product\".\"recyclable_package\",\n"
            + "\"product\".\"low_fat\",\n"
            + "\"product\".\"units_per_case\",\n"
            + "\"product\".\"cases_per_pallet\",\n"
            + "\"product\".\"shelf_width\",\n"
            + "\"product\".\"shelf_height\",\n"
            + "\"product\".\"shelf_depth\",\n"
            + "\"product_class\".\"product_class_id\",\n"
            + "\"product_class\".\"product_subcategory\",\n"
            + "\"product_class\".\"product_category\",\n"
            + "\"product_class\".\"product_department\",\n"
            + "\"product_class\".\"product_family\"\n"
            + "FROM \"product\" inner join \"product_class\"\n"
            + "ON \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "]]>\n"
            + "           </SQL>\n"
            + "           <SQL dialect=\"mysql\"><![CDATA[\n"
            + "SELECT `product`.`product_id`,\n"
            + "`product`.`brand_name`,\n"
            + "`product`.`product_name`,\n"
            + "`product`.`SKU`,\n"
            + "`product`.`SRP`,\n"
            + "`product`.`gross_weight`,\n"
            + "`product`.`net_weight`,\n"
            + "`product`.`recyclable_package`,\n"
            + "`product`.`low_fat`,\n"
            + "`product`.`units_per_case`,\n"
            + "`product`.`cases_per_pallet`,\n"
            + "`product`.`shelf_width`,\n"
            + "`product`.`shelf_height`,\n"
            + "`product`.`shelf_depth`,\n"
            + "`product_class`.`product_class_id`,\n"
            + "`product_class`.`product_family`,\n"
            + "`product_class`.`product_department`,\n"
            + "`product_class`.`product_category`,\n"
            + "`product_class`.`product_subcategory` \n"
            + "FROM `product`, `product_class`\n"
            + "WHERE `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "]]>\n"
            + "           </SQL>\n"
            + "           <SQL dialect=\"generic\"><![CDATA[\n"
            + "SELECT *\n"
            + "FROM \"product\", \"product_class\"\n"
            + "WHERE \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "]]>\n"
            + "           </SQL>\n"
            + "       </View>\n"
            + "       <Level name=\"Product Family\" column=\"product_family\" uniqueMembers=\"true\"/>\n"
            + "       <Level name=\"Product Department\" column=\"product_department\" uniqueMembers=\"false\"/>\n"
            + "       <Level name=\"Product Category\" column=\"product_category\" uniqueMembers=\"false\"/>\n"
            + "       <Level name=\"Product Subcategory\" column=\"product_subcategory\" uniqueMembers=\"false\"/>\n"
            + "       <Level name=\"Brand Name\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
            + "       <Level name=\"Product Name\" column=\"product_name\" uniqueMembers=\"true\"/>\n"
            + "   </Hierarchy>\n"
            + "</Dimension>");
        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " {[ProductView].[Drink].[Beverages].children} on rows\n"
            + "from Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[ProductView].[Drink].[Beverages].[Carbonated Beverages]}\n"
            + "{[ProductView].[Drink].[Beverages].[Drinks]}\n"
            + "{[ProductView].[Drink].[Beverages].[Hot Beverages]}\n"
            + "{[ProductView].[Drink].[Beverages].[Pure Juice Beverages]}\n"
            + "Row #0: 3,407\n"
            + "Row #1: 2,469\n"
            + "Row #2: 4,301\n"
            + "Row #3: 3,396\n");
    }

    public void testBadMeasure1() {
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"SalesWithBadMeasure\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <Measure name=\"Bad Measure\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);
        Throwable throwable = null;
        try {
            testContext.assertSimpleQuery();
        } catch (Throwable e) {
            throwable = e;
        }
        // neither a source column or source expression specified
        TestContext.checkThrowable(
            throwable,
            "must contain either a source column or a source expression, but not both");
    }

    public void testBadMeasure2() {
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"SalesWithBadMeasure2\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <Measure name=\"Bad Measure\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\">\n"
            + "    <MeasureExpression>\n"
            + "       <SQL dialect=\"generic\">\n"
            + "         unit_sales\n"
            + "       </SQL>\n"
            + "    </MeasureExpression>\n"
            + "  </Measure>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);
        Throwable throwable = null;
        try {
            testContext.assertSimpleQuery();
        } catch (Throwable e) {
            throwable = e;
        }
        // both a source column and source expression specified
        TestContext.checkThrowable(
            throwable,
            "must contain either a source column or a source expression, but not both");
    }

    /**
     * Tests bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-105">MONDRIAN-105,
     * "bug with hierarchy with no all member when in query"</a>. It caused a
     * dimension with no 'all' member to be constrained twice.
     */
    public void testDimWithoutAll() {
        // Create a test context with a new ""Sales_DimWithoutAll" cube, and
        // which evaluates expressions against that cube.
        final String schema = TestContext.instance().getSchema(
            null,
            "<Cube name='Sales_DimWithoutAll'>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "  <Dimension name='Product' foreignKey='product_id'>\n"
            + "    <Hierarchy hasAll='false' primaryKey='product_id' primaryKeyTable='product'>\n"
            + "      <Join leftKey='product_class_id' rightKey='product_class_id'>\n"
            + "        <Table name='product'/>\n"
            + "        <Table name='product_class'/>\n"
            + "      </Join>\n"
            + "      <Level name='Product Family' table='product_class' column='product_family'\n"
            + "          uniqueMembers='true'/>\n"
            + "      <Level name='Product Department' table='product_class' column='product_department'\n"
            + "          uniqueMembers='false'/>\n"
            + "      <Level name='Product Category' table='product_class' column='product_category'\n"
            + "          uniqueMembers='false'/>\n"
            + "      <Level name='Product Subcategory' table='product_class' column='product_subcategory'\n"
            + "          uniqueMembers='false'/>\n"
            + "      <Level name='Brand Name' table='product' column='brand_name' uniqueMembers='false'/>\n"
            + "      <Level name='Product Name' table='product' column='product_name'\n"
            + "          uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name='Gender' foreignKey='customer_id'>\n"
            + "    <Hierarchy hasAll='false' primaryKey='customer_id'>\n"
            + "    <Table name='customer'/>\n"
            + "      <Level name='Gender' column='gender' uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
            + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
            + "      formatString='Standard' visible='false'/>\n"
            + "  <Measure name='Store Cost' column='store_cost' aggregator='sum'\n"
            + "      formatString='#,###.00'/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);
        TestContext testContext =
            TestContext.instance()
                .withSchema(schema)
                .withCube("Sales_DimWithoutAll");
        // the default member of the Gender dimension is the first member
        testContext.assertExprReturns("[Gender].CurrentMember.Name", "F");
        testContext.assertExprReturns("[Product].CurrentMember.Name", "Drink");
        // There is no all member.
        testContext.assertExprThrows(
            "([Gender].[All Gender], [Measures].[Unit Sales])",
            "MDX object '[Gender].[All Gender]' not found in cube 'Sales_DimWithoutAll'");
        testContext.assertExprThrows(
            "([Gender].[All Genders], [Measures].[Unit Sales])",
            "MDX object '[Gender].[All Genders]' not found in cube 'Sales_DimWithoutAll'");
        // evaluated in the default context: [Product].[Drink], [Gender].[F]
        testContext.assertExprReturns("[Measures].[Unit Sales]", "12,202");
        // evaluated in the same context: [Product].[Drink], [Gender].[F]
        testContext.assertExprReturns(
            "([Gender].[F], [Measures].[Unit Sales])", "12,202");
        // evaluated at in the context: [Product].[Drink], [Gender].[M]
        testContext.assertExprReturns(
            "([Gender].[M], [Measures].[Unit Sales])", "12,395");
        // evaluated in the context:
        // [Product].[Food].[Canned Foods], [Gender].[F]
        testContext.assertExprReturns(
            "([Product].[Food].[Canned Foods], [Measures].[Unit Sales])",
            "9,407");
        testContext.assertExprReturns(
            "([Product].[Food].[Dairy], [Measures].[Unit Sales])", "6,513");
        testContext.assertExprReturns(
            "([Product].[Drink].[Dairy], [Measures].[Unit Sales])", "1,987");
    }

    /**
     * Tests whether the agg mgr behaves correctly if a cell request causes
     * a column to be constrained multiple times. This happens if two levels
     * map to the same column via the same join-path. If the constraints are
     * inconsistent, no data will be returned.
     */
    public void testMultipleConstraintsOnSameColumn() {
        final String cubeName = "Sales_withCities";
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name='" + cubeName + "'>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "  <DimensionUsage name='Time' source='Time' foreignKey='time_id'/>\n"
            + "  <Dimension name='Cities' foreignKey='customer_id'>\n"
            + "    <Hierarchy hasAll='true' allMemberName='All Cities' primaryKey='customer_id'>\n"
            + "      <Table name='customer'/>\n"
            + "      <Level name='City' column='city' uniqueMembers='true'/> \n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name='Customers' foreignKey='customer_id'>\n"
            + "    <Hierarchy hasAll='true' allMemberName='All Customers' primaryKey='customer_id'>\n"
            + "      <Table name='customer'/>\n"
            + "      <Level name='Country' column='country' uniqueMembers='true'/>\n"
            + "      <Level name='State Province' column='state_province' uniqueMembers='true'/>\n"
            + "      <Level name='City' column='city' uniqueMembers='false'/>\n"
            + "      <Level name='Name' column='fullname' uniqueMembers='true'>\n"
            + "        <Property name='Gender' column='gender'/>\n"
            + "        <Property name='Marital Status' column='marital_status'/>\n"
            + "        <Property name='Education' column='education'/>\n"
            + "        <Property name='Yearly Income' column='yearly_income'/>\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name='Gender' foreignKey='customer_id'>\n"
            + "    <Hierarchy hasAll='true' primaryKey='customer_id'>\n"
            + "    <Table name='customer'/>\n"
            + "      <Level name='Gender' column='gender' uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
            + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
            + "      formatString='Standard' visible='false'/>\n"
            + "  <Measure name='Store Sales' column='store_sales' aggregator='sum'\n"
            + "      formatString='#,###.00'/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select {\n"
            + " [Customers].[USA],\n"
            + " [Customers].[USA].[OR],\n"
            + " [Customers].[USA].[CA],\n"
            + " [Customers].[USA].[CA].[Altadena],\n"
            + " [Customers].[USA].[CA].[Burbank],\n"
            + " [Customers].[USA].[CA].[Burbank].[Alma Son]} ON COLUMNS\n"
            + "from ["
            + cubeName
            + "] \n"
            + "where ([Cities].[All Cities].[Burbank], [Measures].[Store Sales])",
            "Axis #0:\n"
            + "{[Cities].[Burbank], [Measures].[Store Sales]}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "{[Customer].[Customers].[USA].[OR]}\n"
            + "{[Customer].[Customers].[USA].[CA]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Altadena]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Burbank]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Burbank].[Alma Son]}\n"
            + "Row #0: 6,577.33\n"
            + "Row #0: \n"
            + "Row #0: 6,577.33\n"
            + "Row #0: \n"
            + "Row #0: 6,577.33\n"
            + "Row #0: 36.50\n");
    }

    // TODO: Test that mondrian gives a warning if a legacy schema contains
    // AggPattern (or any other aggregate elements that cannot be converted)


    public void testHierarchyDefaultMember() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Gender with default\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" "
            + "primaryKey=\"customer_id\" "
            // Define a default member's whose unique name includes the
            // 'all' member.
            + "defaultMember=\"[Gender with default].[All Gender with defaults].[M]\" >\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\" />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>");
        testContext.assertQueryReturns(
            "select {[Gender with default]} on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender with default].[M]}\n"
            + "Row #0: 135,215\n");
    }

    public void testLevelUniqueMembers() {
        // If uniqueMembers is not specified for first level of hierarchy,
        // defaults to false.
        createLevelUniqueMembersTestContext("")
            .assertSimpleQuery();
        createLevelUniqueMembersTestContext(" uniqueMembers='true'")
            .assertSimpleQuery();
        createLevelUniqueMembersTestContext(" uniqueMembers='false'")
            .assertSchemaError(
                "First level of a hierarchy must have unique members \\(at ${pos}\\)",
                "<Level name='Store Country' column='store_country' uniqueMembers='false'/>");
    }

    private TestContext createLevelUniqueMembersTestContext(final String s) {
        return TestContext.instance().createSubstitutingCube(
            "Sales",
            "    <Dimension name='Store' foreignKey='store_id'>\n"
            + "    <Hierarchy hasAll='true' primaryKey='store_id'>\n"
            + "      <Table name='store'/>\n"
            + "      <Level name='Store Country' column='store_country'"
            + s
            + "/>\n"
            + "      <Level name='Store State' column='store_state' uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "</Dimension>");
    }

    public void testDimensionRequiresForeignKey() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "    <Dimension name='Store'>\n"
                + "    <Hierarchy hasAll='true' primaryKey='store_id'>\n"
                + "      <Table name='store'/>\n"
                + "      <Level name='Store Country' column='store_country'/>\n"
                + "      <Level name='Store State' column='store_state' uniqueMembers='true'/>\n"
                + "    </Hierarchy>\n"
                + "</Dimension>");
        testContext.assertSchemaError(
            "Dimension or DimensionUsage must have foreignKey \\(at ${pos}\\)",
            "<Dimension name='Store'>");
    }
}

// End LegacySchemaTest.java
