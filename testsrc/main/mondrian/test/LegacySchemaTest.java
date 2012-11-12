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
        TestContext testContext =
            TestContext.instance().legacy().createSubstitutingCube(
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
                + "  </Hierarchy>"
                + "</Dimension>",
                null);
        if (!testContext.getDialect().allowsFromQuery()) {
            return;
        }
        testContext.assertAxisReturns(
            "[Gender2].[Gender2].members",
            "[Gender2].[Gender2].[All Gender]\n"
            + "[Gender2].[Gender2].[F]\n"
            + "[Gender2].[Gender2].[M]");
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
        final TestContext testContext = TestContext.instance().legacy().create(
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

    /**
     * Tests bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-105">MONDRIAN-105,
     * "bug with hierarchy with no all member when in query"</a>. It caused a
     * dimension with no 'all' member to be constrained twice.
     */
    public void testDimWithoutAll() {
        // Create a test context with a new ""Sales_DimWithoutAll" cube, and
        // which evaluates expressions against that cube.
        final String schema = TestContext.instance().legacy().getSchema(
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
                .legacy()
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
        final TestContext testContext = TestContext.instance().legacy().create(
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
            + "{[Cities].[Cities].[Burbank], [Measures].[Store Sales]}\n"
            + "Axis #1:\n"
            + "{[Customers].[Customers].[USA]}\n"
            + "{[Customers].[Customers].[USA].[OR]}\n"
            + "{[Customers].[Customers].[USA].[CA]}\n"
            + "{[Customers].[Customers].[USA].[CA].[Altadena]}\n"
            + "{[Customers].[Customers].[USA].[CA].[Burbank]}\n"
            + "{[Customers].[Customers].[USA].[CA].[Burbank].[Alma Son]}\n"
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
        TestContext testContext =
            getTestContext().legacy().createSubstitutingCube(
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
            + "{[Gender with default].[Gender with default].[M]}\n"
            + "Row #0: 135,215\n");
    }

    public void testLevelUniqueMembers() {
        // If uniqueMembers is not specified for first level of hierarchy,
        // defaults to false.
        createLevelUniqueMembersTestContext("")
            .assertSimpleQuery();
        createLevelUniqueMembersTestContext(" uniqueMembers='true'")
            .assertSimpleQuery();
    }

    private TestContext createLevelUniqueMembersTestContext(final String s) {
        return TestContext.instance().legacy().createSubstitutingCube(
            "Sales",
            "    <Dimension name='Store2' foreignKey='store_id'>\n"
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
            getTestContext().legacy().createSubstitutingCube(
                "Sales",
                "    <Dimension name='Store'>\n"
                + "    <Hierarchy hasAll='true' primaryKey='store_id'>\n"
                + "      <Table name='store'/>\n"
                + "      <Level name='Store Country' column='store_country'/>\n"
                + "      <Level name='Store State' column='store_state' uniqueMembers='true'/>\n"
                + "    </Hierarchy>\n"
                + "</Dimension>");
        testContext.assertSchemaError(
            "Dimension or DimensionUsage must have foreignKey \\(in Dimension\\) \\(at ${pos}\\)",
            "<Dimension name='Store'>");
    }

    public void testDimensionUsageWithInvalidForeignKey() {
        final TestContext testContext = getTestContext().legacy().create(
            null,
            "<Cube name='Sales77'>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "  <DimensionUsage name='Time2' source='Time' foreignKey='time_id'/>\n"
            + "  <DimensionUsage name='Store' source='Store' foreignKey='invalid_column'/>\n"
            + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum' "
            + "   formatString='Standard'/>\n"
            + "</Cube>", null, null, null, null);
        testContext.assertSchemaError(
            ".*Relation sales_fact_1997 does not contain column invalid_column",
            "");
    }

    /**
     * Test Multiple DimensionUsages on same Dimension.
     * Alias the fact table to avoid issues with aggregation rules
     * and multiple column names
     */
    public void testMultipleDimensionHierarchyCaptionUsages() {
        final TestContext testContext = getTestContext().legacy().create(
            null,
            "<Cube name='Sales Two Dimensions'>\n"
            + "  <Table name='sales_fact_1997' alias='sales_fact_1997_mdu'/>\n"
            + "  <DimensionUsage name='Time' caption='TimeOne' source='Time' foreignKey='time_id'/>\n"
            + "  <DimensionUsage name='Time2' caption='TimeTwo' source='Time' foreignKey='product_id'/>\n"
            + "  <DimensionUsage name='Store' source='Store' foreignKey='store_id'/>\n"
            + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum' "
            + "   formatString='Standard'/>\n"
            + "  <Measure name='Store Cost' column='store_cost' aggregator='sum'"
            + "   formatString='#,###.00'/>\n"
            + "</Cube>", null, null, null, null);

        String query =
            "select\n"
            + " {[Time2].[1997]} on columns,\n"
            + " {[Time].[1997].[Q3]} on rows\n"
            + "From [Sales Two Dimensions]";

        Result result = testContext.executeQuery(query);

        // Time2.1997 Member
        Member member1 =
            result.getAxes()[0].getPositions().iterator().next().iterator()
                .next();

        // NOTE: The caption is modified at the dimension, not the hierarchy
        assertEquals("TimeTwo", member1.getLevel().getDimension().getCaption());

        Member member2 =
            result.getAxes()[1].getPositions().iterator().next().iterator()
                .next();
        assertEquals("TimeOne", member2.getLevel().getDimension().getCaption());
    }

    public void testPropertyFormatter() {
        final TestContext testContext =
            getTestContext().legacy().createSubstitutingCube(
                "Sales",
                "  <Dimension name='Store2' foreignKey='store_id'>\n"
                + "    <Hierarchy name='Store2' hasAll='true' allMemberName='All Stores' primaryKey='store_id'>\n"
                + "      <Table name='store_ragged'/>\n"
                + "      <Level name='Store2' table='store_ragged' column='store_id' captionColumn='store_name' uniqueMembers='true'>\n"
                + "           <Property name='Store Type' column='store_type' formatter='"
                + SchemaTest.DummyPropertyFormatter.class.getName()
                + "'/>"
                + "           <Property name='Store Manager' column='store_manager'/>"
                + "     </Level>"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n");
        try {
            testContext.assertSimpleQuery();
            fail("expected exception");
        } catch (RuntimeException e) {
            TestContext.checkThrowable(
                e,
                "Failed to load formatter class 'mondrian.test.SchemaTest$DummyPropertyFormatter' for property 'Store Type'.");
        }
    }

    /**
     * Tests that an invalid aggregator causes an error.
     */
    public void testInvalidAggregator() {
        TestContext testContext =
            getTestContext().legacy().createSubstitutingCube(
                "Sales",
                null,
                "  <Measure name='Customer Count3' column='customer_id'\n"
                + "      aggregator='invalidAggregator' formatString='#,###'/>\n"
                + "  <CalculatedMember\n"
                + "      name='Half Customer Count'\n"
                + "      dimension='Measures'\n"
                + "      visible='false'\n"
                + "      formula='[Measures].[Customer Count2] / 2'>\n"
                + "  </CalculatedMember>");
        testContext.assertQueryThrows(
            "select from [Sales]",
            "Unknown aggregator 'invalidAggregator'; valid aggregators are: 'sum', 'count', 'min', 'max', 'avg', 'distinct-count'");
    }

    /**
     * Tests that the deprecated "distinct count" value for the
     * Measure@aggregator attribute still works. The preferred value these days
     * is "distinct-count".
     */
    public void testDeprecatedDistinctCountAggregator() {
        TestContext testContext =
            getTestContext().legacy().createSubstitutingCube(
                "Sales",
                null,
                "  <Measure name='Customer Count2' column='customer_id'\n"
                + "      aggregator='distinct count' formatString='#,###'/>\n"
                + "  <CalculatedMember\n"
                + "      name='Half Customer Count'\n"
                + "      dimension='Measures'\n"
                + "      visible='false'\n"
                + "      formula='[Measures].[Customer Count2] / 2'>\n"
                + "  </CalculatedMember>");
        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales],"
            + "    [Measures].[Customer Count], "
            + "    [Measures].[Customer Count2], "
            + "    [Measures].[Half Customer Count]} on 0,\n"
            + " {[Store].[USA].Children} ON 1\n"
            + "FROM [Sales]\n"
            + "WHERE ([Gender].[M])",
            "Axis #0:\n"
            + "{[Gender].[Gender].[M]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "{[Measures].[Customer Count2]}\n"
            + "{[Measures].[Half Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[USA].[CA]}\n"
            + "{[Store].[Store].[USA].[OR]}\n"
            + "{[Store].[Store].[USA].[WA]}\n"
            + "Row #0: 37,989\n"
            + "Row #0: 1,389\n"
            + "Row #0: 1,389\n"
            + "Row #0: 695\n"
            + "Row #1: 34,623\n"
            + "Row #1: 536\n"
            + "Row #1: 536\n"
            + "Row #1: 268\n"
            + "Row #2: 62,603\n"
            + "Row #2: 901\n"
            + "Row #2: 901\n"
            + "Row #2: 451\n");
    }

    /**
     * Test Multiple DimensionUsages on same Dimension.
     * Alias the fact table to avoid issues with aggregation rules
     * and multiple column names
     */
    public void testMultipleDimensionUsages() {
        final TestContext testContext = getTestContext().legacy().create(
            null,

            "<Cube name='Sales Two Dimensions'>\n"
            + "  <Table name='sales_fact_1997' alias='sales_fact_1997_mdu'/>\n"
            + "  <DimensionUsage name='Time' source='Time' foreignKey='time_id'/>\n"
            + "  <DimensionUsage name='Time2' source='Time' foreignKey='product_id'/>\n"
            + "  <DimensionUsage name='Store' source='Store' foreignKey='store_id'/>\n"
            + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum' "
            + "   formatString='Standard'/>\n"
            + "  <Measure name='Store Cost' column='store_cost' aggregator='sum'"
            + "   formatString='#,###.00'/>\n"
            + "</Cube>", null, null, null, null);

        testContext.assertQueryReturns(
            "select\n"
            + " {[Time2].[Time2].[1997]} on columns,\n"
            + " {[Time].[Time].[1997].[Q3]} on rows\n"
            + "From [Sales Two Dimensions]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + (MondrianProperties.instance().SsasCompatibleNaming.get()
                ? "{[Time2].[Time].[1997]}\n"
                : "{[Time2].[1997]}\n")
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "Row #0: 16,266\n");
    }

    /**
     * Tests two dimensions using same table with same foreign key.
     * both using a table alias.
     */
    public void testTwoAliasesDimensionsShareTableSameForeignKeys() {
        final TestContext testContext = getTestContext().legacy().create(
            null,
            "<Cube name='AliasedDimensionsTesting' defaultMeasure='Supply Time'>\n"
            + "  <Table name='inventory_fact_1997'/>\n"
            + "  <Dimension name='StoreA' foreignKey='store_id'>"
            + "    <Hierarchy hasAll='true' primaryKey='store_id'>"
            + "      <Table name='store' alias='storea'/>"
            + "      <Level name='Store Country' column='store_country' uniqueMembers='true'/>"
            + "      <Level name='Store Name' column='store_name' uniqueMembers='true'/>"
            + "    </Hierarchy>"
            + "  </Dimension>"
            + "  <Dimension name='StoreB' foreignKey='store_id'>"
            + "    <Hierarchy hasAll='true' primaryKey='store_id'>"
            + "      <Table name='store'  alias='storeb'/>"
            + "      <Level name='Store Country' column='store_country' uniqueMembers='true'/>"
            + "      <Level name='Store Name' column='store_name' uniqueMembers='true'/>"
            + "    </Hierarchy>"
            + "  </Dimension>"
            + "  <Measure name='Store Invoice' column='store_invoice' "
            + "aggregator='sum'/>\n"
            + "  <Measure name='Supply Time' column='supply_time' "
            + "aggregator='sum'/>\n"
            + "  <Measure name='Warehouse Cost' column='warehouse_cost' "
            + "aggregator='sum'/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select {[StoreA].[USA]} on rows,"
            + "{[StoreB].[USA]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[StoreB].[StoreB].[USA]}\n"
            + "Axis #2:\n"
            + "{[StoreA].[StoreA].[USA]}\n"
            + "Row #0: 10,425\n");
    }

    public void testCubeWithNoDimensions() {
        final TestContext testContext = getTestContext().legacy().create(
            null,
            "<Cube name='NoDim' defaultMeasure='Unit Sales'>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
            + "      formatString='Standard'/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);
        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns from [NoDim]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n");
    }

    public void testAllMemberNoStringReplace() {
        final TestContext testContext = getTestContext().legacy().create(
            null,
            "<Cube name='Sales Special Time'>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "<Dimension name='TIME' foreignKey='time_id' type='TimeDimension'>"
            + "<Hierarchy name='CALENDAR' hasAll='true' allMemberName='All TIME(CALENDAR)' primaryKey='time_id'>"
            + "  <Table name='time_by_day'/>"
            + "  <Level name='Years' column='the_year' uniqueMembers='true' levelType='TimeYears'/>"
            + "  <Level name='Quarters' column='quarter' uniqueMembers='false' levelType='TimeQuarters'/>"
            + "  <Level name='Months' column='month_of_year' uniqueMembers='false' levelType='TimeMonths'/>"
            + "</Hierarchy>"
            + "</Dimension>"
            + "  <DimensionUsage name='Store' source='Store' foreignKey='store_id'/>\n"
            + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum' "
            + "   formatString='Standard'/>\n"
            + "  <Measure name='Store Cost' column='store_cost' aggregator='sum'"
            + "   formatString='#,###.00'/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select [TIME.CALENDAR].[All TIME(CALENDAR)] on columns\n"
            + "from [Sales Special Time]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[TIME].[CALENDAR].[All TIME(CALENDAR)]}\n"
            + "Row #0: 266,773\n");
    }

    public void testCubeHasFact() {
        final TestContext testContext = getTestContext().legacy().create(
            null,
            "<Cube name='Cube with caption' caption='Cube with name'/>\n",
            null, null, null, null);
        testContext.assertSchemaError(
            "Cube 'Cube with caption' requires fact table \\(in Cube\\) \\(at ${pos}\\)",
            "<Cube name='Cube with caption' caption='Cube with name'/>");
    }

}

// End LegacySchemaTest.java
