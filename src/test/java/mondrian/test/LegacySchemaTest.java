/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;

import mondrian.olap.Cube;
import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.NamedSet;
import mondrian.olap.Property;
import mondrian.olap.Schema;
import mondrian.spi.Dialect;
import mondrian.util.Bug;

import junit.framework.Assert;

import org.olap4j.metadata.*;

import java.sql.SQLException;
import java.util.*;

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
        getTestContext().legacy().createSubstitutingCube(
            "Sales",
            "    <Dimension name='Store'>\n"
            + "    <Hierarchy hasAll='true' primaryKey='store_id'>\n"
            + "      <Table name='store'/>\n"
            + "      <Level name='Store Country' column='store_country'/>\n"
            + "      <Level name='Store State' column='store_state' uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "</Dimension>")
            .assertErrorList()
            .containsError(
                "Dimension or DimensionUsage must have foreignKey \\(in Dimension\\) \\(at ${pos}\\)",
                "<Dimension name='Store'>");
    }

    public void testDimensionUsageWithInvalidForeignKey() {
        getTestContext().legacy().create(
            null,
            "<Cube name='Sales77'>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "  <DimensionUsage name='Time2' source='Time' foreignKey='time_id'/>\n"
            + "  <DimensionUsage name='Store' source='Store' foreignKey='invalid_column'/>\n"
            + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum' "
            + "   formatString='Standard'/>\n"
            + "</Cube>", null, null, null, null)
            .assertErrorList()
            .containsError(
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
            + "{[Time2].[Time].[1997]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "Row #0: 16,266\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testTwoAliasesDimensionsShareTable() {
        final TestContext testContext = getTestContext().create(
            null,
            "<Cube name='AliasedDimensionsTesting' defaultMeasure='Supply Time'>\n"
            + "  <Table name='inventory_fact_1997'/>\n"
            + "  <Dimension name='StoreA' foreignKey='store_id'>"
            + "    <Hierarchy hasAll='true' primaryKey='store_id'>"
            + "      <Table name='store' alias='storea'/>"
            + "      <Level name='Store Country' column='store_country' uniqueMembers='true'/>"
            + "      <Level name='Store Name'  column='store_name' uniqueMembers='true'/>"
            + "    </Hierarchy>"
            + "  </Dimension>"

            + "  <Dimension name='StoreB' foreignKey='warehouse_id'>"
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
        getTestContext().legacy().create(
            null,
            "<Cube name='Cube with caption' caption='Cube with name'/>\n",
            null, null, null, null)
            .assertErrorList()
            .containsError(
                "Cube 'Cube with caption' requires fact table \\(in Cube\\) \\(at ${pos}\\)",
                "<Cube name='Cube with caption' caption='Cube with name'/>");
    }

    /**
     * This result is somewhat peculiar. If two dimensions share a foreign key,
     * what is the expected result?  Also, in this case, they share the same
     * table without an alias, and the system doesn't complain.
     */
    public void testDuplicateTableAliasSameForeignKey() {
        TestContext testContext =
            getTestContext().legacy().createSubstitutingCube(
                "Sales",
                "<Dimension name='Yearly Income2' foreignKey='customer_id'>\n"
                + "  <Hierarchy hasAll='true' primaryKey='customer_id'>\n"
                + "    <Table name='customer'/>\n"
                + "    <Level name='Yearly Income' column='yearly_income' uniqueMembers='true'/>\n"
                + "  </Hierarchy>\n"
                + "</Dimension>");
        testContext.assertQueryReturns(
            "select from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "266,773");

        // NonEmptyCrossJoin Fails
        if (false) {
            testContext.assertQueryReturns(
                "select NonEmptyCrossJoin({[Yearly Income2].[All Yearly Income2s]},{[Customers].[All Customers]}) on rows,"
                + "NON EMPTY {[Measures].[Unit Sales]} on columns"
                + " from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "266,773");
        }
    }

    /**
     * Tests a cube whose fact table is a &lt;View&gt; element.
     */
    public void testViewFactTable() {
        final TestContext testContext = getTestContext().legacy().create(
            null,
            // Warehouse cube where the default member in the Warehouse
            // dimension is USA.
            "<Cube name='Warehouse (based on view)'>\n"
            + "  <View alias='FACT'>\n"
            + "    <SQL dialect='generic'>\n"
            + "     <![CDATA[select * from 'inventory_fact_1997' as 'FOOBAR']]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect='oracle'>\n"
            + "     <![CDATA[select * from 'inventory_fact_1997' 'FOOBAR']]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect='mysql'>\n"
            + "     <![CDATA[select * from `inventory_fact_1997` as `FOOBAR`]]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect='infobright'>\n"
            + "     <![CDATA[select * from `inventory_fact_1997` as `FOOBAR`]]>\n"
            + "    </SQL>\n"
            + "  </View>\n"
            + "  <DimensionUsage name='Time' source='Time' foreignKey='time_id'/>\n"
            + "  <DimensionUsage name='Product' source='Product' foreignKey='product_id'/>\n"
            + "  <DimensionUsage name='Store' source='Store' foreignKey='store_id'/>\n"
            + "  <Dimension name='Warehouse' foreignKey='warehouse_id'>\n"
            + "    <Hierarchy hasAll='false' defaultMember='[USA]' primaryKey='warehouse_id'> \n"
            + "      <Table name='warehouse'/>\n"
            + "      <Level name='Country' column='warehouse_country' uniqueMembers='true'/>\n"
            + "      <Level name='State Province' column='warehouse_state_province'\n"
            + "          uniqueMembers='true'/>\n"
            + "      <Level name='City' column='warehouse_city' uniqueMembers='false'/>\n"
            + "      <Level name='Warehouse Name' column='warehouse_name' uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name='Warehouse Cost' column='warehouse_cost' aggregator='sum'/>\n"
            + "  <Measure name='Warehouse Sales' column='warehouse_sales' aggregator='sum'/>\n"
            + "</Cube>", null, null, null, null);

        testContext.assertQueryReturns(
            "select\n"
            + " {[Time].[1997], [Time].[1997].[Q3]} on columns,\n"
            + " {[Store].[USA].Children} on rows\n"
            + "From [Warehouse (based on view)]\n"
            + "where [Warehouse].[USA]",
            "Axis #0:\n"
            + "{[Warehouse].[Warehouses].[USA]}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "Row #0: 25,789.086\n"
            + "Row #0: 8,624.791\n"
            + "Row #1: 17,606.904\n"
            + "Row #1: 3,812.023\n"
            + "Row #2: 45,647.262\n"
            + "Row #2: 12,664.162\n");
    }

    /**
     * Tests a cube whose fact table is a &lt;View&gt; element, and which
     * has dimensions based on the fact table.
     */
    public void testViewFactTable2() {
        final TestContext testContext = getTestContext().legacy().create(
            null,
            // Similar to "Store" cube in FoodMart.mondrian.xml.
            "<Cube name='Store2'>\n"
            + "  <View alias='FACT'>\n"
            + "    <SQL dialect='generic'>\n"
            + "     <![CDATA[select * from 'store' as 'FOOBAR']]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect='oracle'>\n"
            + "     <![CDATA[select * from 'store' 'FOOBAR']]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect='mysql'>\n"
            + "     <![CDATA[select * from `store` as `FOOBAR`]]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect='infobright'>\n"
            + "     <![CDATA[select * from `store` as `FOOBAR`]]>\n"
            + "    </SQL>\n"
            + "  </View>\n"
            + "  <!-- We could have used the shared dimension 'Store Type', but we\n"
            + "     want to test private dimensions without primary key. -->\n"
            + "  <Dimension name='Store Type'>\n"
            + "    <Hierarchy hasAll='true'>\n"
            + "      <Level name='Store Type' column='store_type' uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "\n"
            + "  <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
            + "      formatString='#,###'/>\n"
            + "  <Measure name='Grocery Sqft' column='grocery_sqft' aggregator='sum'\n"
            + "      formatString='#,###'/>\n"
            + "\n"
            + "</Cube>", null, null, null, null);
        testContext.assertQueryReturns(
            "select {[Store Type].Children} on columns from [Store2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Store Type].[Deluxe Supermarket]}\n"
            + "{[Store].[Store Type].[Gourmet Supermarket]}\n"
            + "{[Store].[Store Type].[HeadQuarters]}\n"
            + "{[Store].[Store Type].[Mid-Size Grocery]}\n"
            + "{[Store].[Store Type].[Small Grocery]}\n"
            + "{[Store].[Store Type].[Supermarket]}\n"
            + "Row #0: 146,045\n"
            + "Row #0: 47,447\n"
            + "Row #0: \n"
            + "Row #0: 109,343\n"
            + "Row #0: 75,281\n"
            + "Row #0: 193,480\n");
    }

    /**
     * Tests a cube whose fact table is a &lt;View&gt; element, and which
     * has dimensions based on the fact table.
     */
    public void testViewFactTableInvalid() {
        TestContext testContext = getTestContext().legacy().create(
            null,
            // Similar to "Store" cube in FoodMart.mondrian.xml.
            "<Cube name='Store2'>\n"
            + "  <View alias='FACT'>\n"
            + "    <SQL dialect='generic'>\n"
            + "     <![CDATA[select wrong from wronger]]>\n"
            + "    </SQL>\n"
            + "  </View>\n"
            + "  <Dimension name='Store Type'>\n"
            + "    <Hierarchy hasAll='true'>\n"
            + "      <Level name='Store Type' column='store_type' uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "\n"
            + "  <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
            + "      formatString='#,###'/>\n"
            + "\n"
            + "</Cube>",
            null, null, null, null);
        testContext.assertQueryThrows(
            "select {[Store Type].Children} on columns from [Store2]",
            "View is invalid: ");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-303">
     * MONDRIAN-303, "Property column shifting when use captionColumn"</a>.
     */
    public void testBugMondrian303() {
        // In order to reproduce the problem a dimension specifying
        // captionColumn and Properties were required.
        TestContext testContext =
            getTestContext().legacy().createSubstitutingCube(
                "Sales",
                "  <Dimension name='Store2' foreignKey='store_id'>\n"
                + "    <Hierarchy name='Store2' hasAll='true' allMemberName='All Stores' primaryKey='store_id'>\n"
                + "      <Table name='store_ragged'/>\n"
                + "      <Level name='Store2' table='store_ragged' column='store_id' captionColumn='store_name' uniqueMembers='true'>\n"
                + "           <Property name='Store Type' column='store_type'/>"
                + "           <Property name='Store Manager' column='store_manager'/>"
                + "     </Level>"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n");

        // In the query below Mondrian (prior to the fix) would
        // return the store name instead of the store type.
        testContext.assertQueryReturns(
            "WITH\n"
            + "   MEMBER [Measures].[StoreType] AS \n"
            + "   '[Store2].CurrentMember.Properties(\"Store Type\")'\n"
            + "SELECT\n"
            + "   NonEmptyCrossJoin({[Store2].[All Stores].children}, {[Product].[All Products]}) ON ROWS,\n"
            + "   { [Measures].[Store Sales], [Measures].[StoreType]} ON COLUMNS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[StoreType]}\n"
            + "Axis #2:\n"
            + "{[Store2].[Store2].[2], [Product].[Product].[All Products]}\n"
            + "{[Store2].[Store2].[3], [Product].[Product].[All Products]}\n"
            + "{[Store2].[Store2].[6], [Product].[Product].[All Products]}\n"
            + "{[Store2].[Store2].[7], [Product].[Product].[All Products]}\n"
            + "{[Store2].[Store2].[11], [Product].[Product].[All Products]}\n"
            + "{[Store2].[Store2].[13], [Product].[Product].[All Products]}\n"
            + "{[Store2].[Store2].[14], [Product].[Product].[All Products]}\n"
            + "{[Store2].[Store2].[15], [Product].[Product].[All Products]}\n"
            + "{[Store2].[Store2].[16], [Product].[Product].[All Products]}\n"
            + "{[Store2].[Store2].[17], [Product].[Product].[All Products]}\n"
            + "{[Store2].[Store2].[22], [Product].[Product].[All Products]}\n"
            + "{[Store2].[Store2].[23], [Product].[Product].[All Products]}\n"
            + "{[Store2].[Store2].[24], [Product].[Product].[All Products]}\n"
            + "Row #0: 4,739.23\n"
            + "Row #0: Small Grocery\n"
            + "Row #1: 52,896.30\n"
            + "Row #1: Supermarket\n"
            + "Row #2: 45,750.24\n"
            + "Row #2: Gourmet Supermarket\n"
            + "Row #3: 54,545.28\n"
            + "Row #3: Supermarket\n"
            + "Row #4: 55,058.79\n"
            + "Row #4: Supermarket\n"
            + "Row #5: 87,218.28\n"
            + "Row #5: Deluxe Supermarket\n"
            + "Row #6: 4,441.18\n"
            + "Row #6: Small Grocery\n"
            + "Row #7: 52,644.07\n"
            + "Row #7: Supermarket\n"
            + "Row #8: 49,634.46\n"
            + "Row #8: Supermarket\n"
            + "Row #9: 74,843.96\n"
            + "Row #9: Deluxe Supermarket\n"
            + "Row #10: 4,705.97\n"
            + "Row #10: Small Grocery\n"
            + "Row #11: 24,329.23\n"
            + "Row #11: Mid-Size Grocery\n"
            + "Row #12: 54,431.14\n"
            + "Row #12: Supermarket\n");
    }

    public void testCubeWithOneDimensionUsageOneMeasure() {
        final TestContext testContext = getTestContext().legacy().create(
            null,
            "<Cube name='OneDimUsage' defaultMeasure='Unit Sales'>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "  <DimensionUsage name='Product' source='Product' foreignKey='product_id'/>\n"
            + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
            + "      formatString='Standard'/>\n"
            + "</Cube>",
            null, null, null, null);
        testContext.assertQueryReturns(
            "select {[Product].Children} on columns from [OneDimUsage]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Product].[Drink]}\n"
            + "{[Product].[Product].[Food]}\n"
            + "{[Product].[Product].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #0: 191,940\n"
            + "Row #0: 50,236\n");
    }

    public void testCubeCaption() throws SQLException {
        final TestContext testContext = getTestContext().legacy().create(
            null,
            "<Cube name='Cube with caption' caption='Cube with name'>"
            + "  <Table name='sales_fact_1997'/>"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "   formatString=\"Standard\"/>"
            + "</Cube>\n",
            "<VirtualCube name='Warehouse and Sales with caption' "
            + "caption='Warehouse and Sales with name'  defaultMeasure='Store Sales'>\n"
            + "  <VirtualCubeDimension cubeName='Sales' name='Customers'/>\n"
            + "  <VirtualCubeDimension cubeName='Sales' name='Time'/>\n"
            + "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n"
            + "   formatString=\"#,###.00\"/>"
            + "</VirtualCube>",
            null, null, null);
        final NamedList<org.olap4j.metadata.Cube> cubes =
            testContext.getOlap4jConnection().getOlapSchema().getCubes();
        final org.olap4j.metadata.Cube cube = cubes.get("Cube with caption");
        assertEquals("Cube with name", cube.getCaption());
        final org.olap4j.metadata.Cube cube2 =
            cubes.get("Warehouse and Sales with caption");
        assertEquals("Warehouse and Sales with name", cube2.getCaption());
    }

    public void testHierarchiesWithDifferentPrimaryKeysThrows() {
        final TestContext testContext =
            getTestContext().legacy().createSubstitutingCube(
                "Sales",
                "  <Dimension name='Time' type='TimeDimension' foreignKey='time_id'>\n"
                + "    <Hierarchy hasAll='false' primaryKey='time_id'>\n"
                + "      <Table name='time_by_day'/>\n"
                + "      <Level name='Year' column='the_year' type='Numeric' uniqueMembers='true'\n"
                + "          levelType='TimeYears'/>\n"
                + "      <Level name='Quarter' column='quarter' uniqueMembers='false'\n"
                + "          levelType='TimeQuarters'/>\n"
                + "      <Level name='Month' column='month_of_year' uniqueMembers='false' type='Numeric'\n"
                + "          levelType='TimeMonths'/>\n"
                + "    </Hierarchy>\n"
                + "    <Hierarchy hasAll='true' name='Weekly' primaryKey='store_id'>\n"
                + "      <Table name='time_by_day'/>\n"
                + "      <Level name='Year' column='the_year' type='Numeric' uniqueMembers='true'\n"
                + "          levelType='TimeYears'/>\n"
                + "      <Level name='Week' column='week_of_year' type='Numeric' uniqueMembers='false'\n"
                + "          levelType='TimeWeeks'/>\n"
                + "      <Level name='Day' column='day_of_month' uniqueMembers='false' type='Numeric'\n"
                + "          levelType='TimeDays'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>",
                null);
        try {
            testContext.assertSimpleQuery();
        } catch (RuntimeException e) {
            TestContext.checkThrowable(
                e,
                "Cannot convert schema: hierarchies in dimension 'Time' do not have consistent primary keys");
        }
    }

    public void testVirtualCubeDimensionMustJoinToAtLeastOneCube() {
        TestContext testContext = getTestContext().legacy().create(
            null,
            null,
            "<VirtualCube name='Sales vs HR'>\n"
            + "<VirtualCubeDimension name='Store'/>\n"
            + "<VirtualCubeDimension cubeName='HR' name='Position'/>\n"
            + "<VirtualCubeDimension cubeName='HR' name='Employees'/>\n"
            + "<VirtualCubeMeasure cubeName='HR' name='[Measures].[Org Salary]'/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);
        RuntimeException throwable = null;
        try {
            testContext.assertSimpleQuery();
        } catch (RuntimeException e) {
            throwable = e;
        }
        TestContext.checkThrowable(
            throwable,
            "Virtual cube dimension must join to at least one cube: dimension 'Store' in cube 'Sales vs HR'");
    }

    public void testStoredMeasureMustHaveColumns() {
        final TestContext testContext = getTestContext().legacy().create(
            null,
            "<Cube name='Warehouse-old'>\n"
            + "  <Table name='inventory_fact_1997'/>\n"
            + "  <DimensionUsage name='Time' source='Time' foreignKey='time_id'/>\n"
            + "  <DimensionUsage name='Product' source='Product' foreignKey='product_id'/>\n"
            + "  <DimensionUsage name='Warehouse' source='Warehouse' foreignKey='warehouse_id'/>\n"
            + "  <Measure name='Units Ordered' column='units_ordered' aggregator='sum' formatString='#.0'/>\n"
            + "  <Measure name='Warehouse Profit' aggregator='sum'>\n"
            + "    <MeasureExpression>\n"
            + "      <SQL dialect='generic'>\n"
            + "       &quot;warehouse_sales&quot; - &quot;inventory_fact_1997&quot;.&quot;warehouse_cost&quot;\n"
            + "      </SQL>\n"
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
        } catch (RuntimeException e) {
            throwable = e;
        }
        TestContext.checkThrowable(
            throwable,
            "Expression must belong to one and only one relation (at line 177, column 8)");
    }

    public void testCubesVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name='Foo' visible='@REPLACE_ME@'>\n"
                + "  <Table name='store'/>\n"
                + "  <Dimension name='Store Type'>\n"
                + "    <Hierarchy hasAll='true'>\n"
                + "      <Level name='Store Type' column='store_type' uniqueMembers='true'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
                + "      formatString='#,###'/>\n"
                + "</Cube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                getTestContext().legacy().create(
                    null, cubeDef, null, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            assertTrue(testValue.equals(cube.isVisible()));
        }
    }

    public void testLevelVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name='Foo'>\n"
                + "  <Table name='store'/>\n"
                + "  <Dimension name='Bar'>\n"
                + "    <Hierarchy name='Bacon' hasAll='false'>\n"
                + "      <Level name='Samosa' column='store_type' uniqueMembers='true' visible='@REPLACE_ME@'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
                + "      formatString='#,###'/>\n"
                + "</Cube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                getTestContext().legacy().create(
                    null, cubeDef, null, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            Dimension dim = null;
            for (Dimension dimCheck : cube.getDimensionList()) {
                if (dimCheck.getName().equals("Bar")) {
                    dim = dimCheck;
                }
            }
            assertNotNull(dim);
            final Hierarchy hier = dim.getHierarchy();
            assertNotNull(hier);
            assertEquals(
                "Bacon",
                hier.getName());
            final mondrian.olap.Level level = hier.getLevelList().get(0);
            assertEquals("Samosa", level.getName());
            assertTrue(testValue.equals(level.isVisible()));
        }
    }

    public void testInvalidRoleError() {
        String schema =
            TestContext.getRawSchema(TestContext.DataSet.LEGACY_FOODMART);
        schema =
            schema.replaceFirst(
                "<Schema name=\"FoodMart\"",
                "<Schema name='FoodMart' defaultRole='Unknown'");
        final TestContext testContext =
            getTestContext().withSchema(schema);
        testContext.assertErrorList().containsError(
            "Role 'Unknown' not found \\(in Schema 'FoodMart'\\) \\(at ${pos}\\)",
            "<Schema name='FoodMart' defaultRole='Unknown'>");
    }

    public void testVirtualCubeNamedSetSupportInSchema() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Warehouse and Sales",
            null,
            null,
            null,
            "<NamedSet name='Non CA State Stores' "
            + "formula='EXCEPT({[Store].[Store Country].[USA].children},{[Store].[Store Country].[USA].[CA]})'/>");
        testContext.assertQueryReturns(
            "WITH "
            + "SET [Non CA State Stores] AS 'EXCEPT({[Store].[Store Country].[USA].children},"
            + "{[Store].[Store Country].[USA].[CA]})'\n"
            + "MEMBER "
            + "[Store].[Total Non CA State] AS \n"
            + "'SUM({[Non CA State Stores]})'\n"
            + "SELECT {[Store].[Store Country].[USA],[Store].[Total Non CA State]} ON 0,"
            + "{[Measures].[Unit Sales]} ON 1 FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Store].[USA]}\n"
            + "{[Store].[Store].[Total Non CA State]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 192,025\n");

        testContext.assertQueryReturns(
            "WITH "
            + "MEMBER "
            + "[Store].[Total Non CA State] AS \n"
            + "'SUM({[Non CA State Stores]})'\n"
            + "SELECT {[Store].[Store Country].[USA],[Store].[Total Non CA State]} ON 0,"
            + "{[Measures].[Unit Sales]} ON 1 FROM [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Store].[USA]}\n"
            + "{[Store].[Store].[Total Non CA State]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 192,025\n");
    }

    public void testVirtualCubeNamedSetSupportInSchemaError() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Warehouse and Sales",
            null,
            null,
            null,
            "<NamedSet name='Non CA State Stores' "
            + "formula='EXCEPT({[Store].[Store State].[USA].children},{[Store].[Store Country].[USA].[CA]})'/>");
        try {
            testContext.assertQueryReturns(
                "WITH "
                + "SET [Non CA State Stores] AS 'EXCEPT({[Store].[Store Country].[USA].children},"
                + "{[Store].[Store Country].[USA].[CA]})'\n"
                + "MEMBER "
                + "[Store].[Total Non CA State] AS \n"
                + "'SUM({[Non CA State Stores]})'\n"
                + "SELECT {[Store].[Store Country].[USA],[Store].[Total Non CA State]} ON 0,"
                + "{[Measures].[Unit Sales]} ON 1 FROM [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA]}\n"
                + "{[Store].[Total Non CA State]}\n"
                + "Axis #2:\n"
                + "{[Measures].[Unit Sales]}\n"
                + "Row #0: 266,773\n"
                + "Row #0: 192,025\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(e.getMessage().indexOf("bad formula") >= 0);
        }
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-413">
     * MONDRIAN-413, "RolapMember causes ClassCastException in compare()"</a>,
     * caused by binary column value.
     */
    public void testBinaryLevelKey() {
        if (!Bug.BugMondrian1330Fixed) {
            return;
        }
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case DERBY:
        case MYSQL:
            break;
        default:
            // Not all databases support binary literals (e.g. X'AB01'). Only
            // Derby returns them as byte[] values from its JDBC driver and
            // therefore experiences bug MONDRIAN-413.
            return;
        }
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "  <Dimension name='Binary' foreignKey='promotion_id'>\n"
            + "    <Hierarchy hasAll='false' primaryKey='id'>\n"
            + "      <InlineTable alias='binary'>\n"
            + "        <ColumnDefs>\n"
            + "          <ColumnDef name='id' type='Integer'/>\n"
            + "          <ColumnDef name='bin' type='Integer'/>\n"
            + "          <ColumnDef name='name' type='String'/>\n"
            + "        </ColumnDefs>\n"
            + "        <Rows>\n"
            + "          <Row>\n"
            + "            <Value column='id'>2</Value>\n"
            + "            <Value column='bin'>X'4546'</Value>\n"
            + "            <Value column='name'>Ben</Value>\n"
            + "          </Row>\n"
            + "          <Row>\n"
            + "            <Value column='id'>3</Value>\n"
            + "            <Value column='bin'>X'424344'</Value>\n"
            + "            <Value column='name'>Bill</Value>\n"
            + "          </Row>\n"
            + "          <Row>\n"
            + "            <Value column='id'>4</Value>\n"
            + "            <Value column='bin'>X'424344'</Value>\n"
            + "            <Value column='name'>Bill</Value>\n"
            + "          </Row>\n"
            + "        </Rows>\n"
            + "      </InlineTable>\n"
            + "      <Level name='Level1' column='bin' nameColumn='name' ordinalColumn='name' />\n"
            + "      <Level name='Level2' column='id'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n");
        testContext.assertQueryReturns(
            "select {[Binary].members} on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Binary].[Ben]}\n"
            + "{[Binary].[Ben].[2]}\n"
            + "{[Binary].[Bill]}\n"
            + "{[Binary].[Bill].[3]}\n"
            + "{[Binary].[Bill].[4]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n");
        testContext.assertQueryReturns(
            "select hierarchize({[Binary].members}) on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Binary].[Ben]}\n"
            + "{[Binary].[Ben].[2]}\n"
            + "{[Binary].[Bill]}\n"
            + "{[Binary].[Bill].[3]}\n"
            + "{[Binary].[Bill].[4]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n");
    }

    /**
     * Testcase for a problem which involved a slowly changing dimension.
     * Not actually a slowly-changing dimension - we don't have such a thing in
     * the foodmart schema - but the same structure. The dimension is a two
     * table snowflake, and the table nearer to the fact table is not used by
     * any level.
     */
    public void testScdJoin() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "  <Dimension name='Product truncated' foreignKey='product_id'>\n"
                + "    <Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>\n"
                + "      <Join leftKey='product_class_id' rightKey='product_class_id'>\n"
                + "        <Table name='product'/>\n"
                + "        <Table name='product_class'/>\n"
                + "      </Join>\n"
                + "      <Level name='Product Class' table='product_class' nameColumn='product_subcategory'\n"
                + "          column='product_class_id' type='Numeric' uniqueMembers='true'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n",
                null,
                null,
                null);
        testContext.assertQueryReturns(
            "select non empty {[Measures].[Unit Sales]} on 0,\n"
            + " non empty Filter({[Product truncated].Members}, [Measures].[Unit Sales] > 10000) on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product truncated].[Product truncated].[All Product truncateds]}\n"
            + "{[Product truncated].[Product truncated].[Fresh Vegetables]}\n"
            + "{[Product truncated].[Product truncated].[Fresh Fruit]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 20,739\n"
            + "Row #2: 11,767\n");
    }


    public void testCubeRequiresFactTable() {
        getTestContext().create(
            null, "<Cube name='cube without fact table'/>",
            null, null, null, null)
            .assertErrorList()
            .containsError(
                "Cube 'cube without fact table' requires fact table \\(in Cube\\) \\(at ${pos}\\)",
                "<Cube name='cube without fact table'/>");
    }

    /**
     * Test case for the Level@internalType attribute.
     *
     * <p>See bug <a href="http://jira.pentaho.com/browse/MONDRIAN-896">
     * MONDRIAN-896, "Oracle integer columns overflow if value &gt;>2^31"</a>.
     */
    public void testLevelInternalType() {
        // One of the keys is larger than Integer.MAX_VALUE (2 billion), so
        // will only work if we use long values.
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "  <Dimension name='Big numbers' foreignKey='promotion_id'>\n"
            + "    <Hierarchy hasAll='false' primaryKey='id'>\n"
            + "      <InlineTable alias='t'>\n"
            + "        <ColumnDefs>\n"
            + "          <ColumnDef name='id' type='Integer'/>\n"
            + "          <ColumnDef name='big_num' type='Integer'/>\n"
            + "          <ColumnDef name='name' type='String'/>\n"
            + "        </ColumnDefs>\n"
            + "        <Rows>\n"
            + "          <Row>\n"
            + "            <Value column='id'>0</Value>\n"
            + "            <Value column='big_num'>1234</Value>\n"
            + "            <Value column='name'>Ben</Value>\n"
            + "          </Row>\n"
            + "          <Row>\n"
            + "            <Value column='id'>519</Value>\n"
            + "            <Value column='big_num'>1234567890123</Value>\n"
            + "            <Value column='name'>Bill</Value>\n"
            + "          </Row>\n"
            + "        </Rows>\n"
            + "      </InlineTable>\n"
            + "      <Level name='Level1' column='big_num' internalType='long'/>\n"
            + "      <Level name='Level2' column='id'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n");
        testContext.assertQueryReturns(
            "select {[Big numbers].members} on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Big numbers].[Big numbers].[1234]}\n"
            + "{[Big numbers].[Big numbers].[1234].[0]}\n"
            + "{[Big numbers].[Big numbers].[1234567890123]}\n"
            + "{[Big numbers].[Big numbers].[1234567890123].[519]}\n"
            + "Row #0: 195,448\n"
            + "Row #0: 195,448\n"
            + "Row #0: 739\n"
            + "Row #0: 739\n");
    }

    /**
     * Negative test for Level@internalType attribute.
     */
    public void testLevelInternalTypeErr() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "  <Dimension name='Big numbers' foreignKey='promotion_id'>\n"
            + "    <Hierarchy hasAll='false' primaryKey='id'>\n"
            + "      <InlineTable alias='t'>\n"
            + "        <ColumnDefs>\n"
            + "          <ColumnDef name='id' type='Integer'/>\n"
            + "          <ColumnDef name='big_num' type='Integer'/>\n"
            + "          <ColumnDef name='name' type='String'/>\n"
            + "        </ColumnDefs>\n"
            + "        <Rows>\n"
            + "          <Row>\n"
            + "            <Value column='id'>0</Value>\n"
            + "            <Value column='big_num'>1234</Value>\n"
            + "            <Value column='name'>Ben</Value>\n"
            + "          </Row>\n"
            + "        </Rows>\n"
            + "      </InlineTable>\n"
            + "      <Level name='Level1' column='big_num' type='Integer' internalType='char'/>\n"
            + "      <Level name='Level2' column='id'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n");
        testContext.assertQueryThrows(
            "select {[Big numbers].members} on 0 from [Sales]",
            "In Schema: In Cube: In Dimension: In Hierarchy: In Level: Value 'char' of attribute 'internalType' has illegal value 'char'.  Legal values: {int, long, Object, String}");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-482">
     * MONDRIAN-482, "ClassCastException when obtaining RolapCubeLevel"</a>.
     */
    public void testBugMondrian482() {
        // until bug MONDRIAN-495, "Table filter concept does not support
        // dialects." is fixed, this test case only works on MySQL
        if (!Bug.BugMondrian495Fixed
            && getTestContext().getDialect().getDatabaseProduct()
               != Dialect.DatabaseProduct.MYSQL)
        {
            return;
        }

        // skip this test if using aggregates, the agg tables do not
        // enforce the SQL element in the fact table
        if (MondrianProperties.instance().UseAggregates.booleanValue()) {
            return;
        }

        // In order to reproduce the problem it was necessary to only have one
        // non empty member under USA. In the cube definition below we create a
        // cube with only CA data to achieve this.
        String salesCube1 =
            "<Cube name='Sales2' defaultMeasure='Unit Sales'>\n"
            + "  <Table name='sales_fact_1997' >\n"
            + "    <SQL dialect='default'>\n"
            + "     <![CDATA[`sales_fact_1997`.`store_id` in (select distinct `store_id` from `store` where `store`.`store_state` = 'CA')]]>\n"
            + "    </SQL>\n"
            + "  </Table>\n"
            + "  <DimensionUsage name='Store' source='Store' foreignKey='store_id'/>\n"
            + "  <DimensionUsage name='Product' source='Product' foreignKey='product_id'/>\n"
            + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "  <Measure name='Store Sales' column='store_sales' aggregator='sum' formatString='Standard'/>\n"
            + "</Cube>\n";

        final TestContext testContext = getTestContext().create(
            null,
            salesCube1,
            null,
            null,
            null,
            null);

        // First query all children of the USA. This should only return CA since
        // all the other states were filtered out. CA will be put in the member
        // cache
        String query1 =
            "WITH SET [#DataSet#] as "
            + "'NonEmptyCrossjoin({[Product].[All Products]}, {[Store].[All Stores].[USA].Children})' "
            + "SELECT {[Measures].[Unit Sales]} on columns, "
            + "NON EMPTY Hierarchize({[#DataSet#]}) on rows FROM [Sales2]";

        testContext.assertQueryReturns(
            query1,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[All Products], [Store].[USA].[CA]}\n"
            + "Row #0: 74,748\n");

        // Now query the children of CA using the descendants function
        // This is where the ClassCastException occurs
        String query2 =
            "WITH SET [#DataSet#] as "
            + "'{Descendants([Store].[All Stores], 3)}' "
            + "SELECT {[Measures].[Unit Sales]} on columns, "
            + "NON EMPTY Hierarchize({[#DataSet#]}) on rows FROM [Sales2]";

        testContext.assertQueryReturns(
            query2,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "Row #0: 21,333\n"
            + "Row #1: 25,663\n"
            + "Row #2: 25,635\n"
            + "Row #3: 2,117\n");
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-355">Bug MONDRIAN-355,
     * "adding hours/mins as levelType for level of type Dimension"</a>.
     */
    public void testBugMondrian355() {
        if (!Bug.BugMondrian1329Fixed) {
            return;
        }
        checkBugMondrian355("TimeHalfYears");

        // make sure that the deprecated name still works
        checkBugMondrian355("TimeHalfYear");
    }

    public void checkBugMondrian355(String timeHalfYear) {
        final String xml =
            "<Dimension name='Time2' foreignKey='time_id' type='TimeDimension'>\n"
            + "<Hierarchy hasAll='true' primaryKey='time_id'>\n"
            + "  <Table name='time_by_day'/>\n"
            + "  <Level name='Years' column='the_year' uniqueMembers='true' type='Numeric' levelType='TimeYears'/>\n"
            + "  <Level name='Half year' column='quarter' uniqueMembers='false' levelType='"
            + timeHalfYear
            + "'/>\n"
            + "  <Level name='Hours' column='month_of_year' uniqueMembers='false' type='Numeric' levelType='TimeHours'/>\n"
            + "  <Level name='Quarter hours' column='time_id' uniqueMembers='false' type='Numeric' levelType='TimeUndefined'/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>";
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales", xml);

        testContext.assertQueryReturns(
            "select Head([Time2].[Quarter hours].Members, 3) on columns\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time2].[1997].[Q1].[1].[367]}\n"
            + "{[Time2].[1997].[Q1].[1].[368]}\n"
            + "{[Time2].[1997].[Q1].[1].[369]}\n"
            + "Row #0: 348\n"
            + "Row #0: 635\n"
            + "Row #0: 589\n");

        // Check that can apply ParallelPeriod to a TimeUndefined level.
        testContext.assertAxisReturns(
            "PeriodsToDate([Time2].[Quarter hours], [Time2].[1997].[Q1].[1].[368])",
            "[Time2].[1997].[Q1].[1].[368]");

        testContext.assertAxisReturns(
            "PeriodsToDate([Time2].[Half year], [Time2].[1997].[Q1].[1].[368])",
            "[Time2].[1997].[Q1].[1].[367]\n"
            + "[Time2].[1997].[Q1].[1].[368]");

        // Check that get an error if give invalid level type
        try {
            getTestContext()
                .createSubstitutingCube(
                    "Sales",
                    Util.replace(xml, "TimeUndefined", "TimeUnspecified"))
                .assertSimpleQuery();
            fail("expected error");
        } catch (Throwable e) {
            TestContext.checkThrowable(
                e,
                "Value 'TimeUnspecified' of attribute 'levelType' has illegal value 'TimeUnspecified'.  Legal values: {Regular, TimeYears, ");
        }
    }

    /**
     * Unit test for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-463">
     * MONDRIAN-463, "Snowflake dimension with 3-way join."</a>.
     */
    public void testBugMondrian463() {
        if (!Bug.BugMondrian1335Fixed) {
            return;
        }
        if (!propSaver.props.FilterChildlessSnowflakeMembers.get()) {
            // Similar to aggregates. If we turn off filtering,
            // we get wild stuff because of referential integrity.
            return;
        }
        // To build a dimension that is a 3-way snowflake, take the 2-way
        // product -> product_class join and convert to product -> store ->
        // product_class.
        //
        // It works because product_class_id covers the range 1 .. 110;
        // store_id covers every value in 0 .. 24;
        // region_id has 24 distinct values in the range 0 .. 106 (region_id 25
        // occurs twice).
        // Therefore in store, store_id -> region_id is a 25 to 24 mapping.
        checkBugMondrian463(
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension name='Product3' foreignKey='product_id'>\n"
                + "  <Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>\n"
                + "    <Join leftKey='product_class_id' rightKey='store_id'>\n"
                + "      <Table name='product'/>\n"
                + "      <Join leftKey='region_id' rightKey='product_class_id'>\n"
                + "        <Table name='store'/>\n"
                + "        <Table name='product_class'/>\n"
                + "      </Join>\n"
                + "    </Join>\n"
                + "    <Level name='Product Family' table='product_class' column='product_family' uniqueMembers='true'/>\n"
                + "    <Level name='Product Department' table='product_class' column='product_department' uniqueMembers='false'/>\n"
                + "    <Level name='Product Category' table='product_class' column='product_category' uniqueMembers='false'/>\n"
                + "    <Level name='Product Subcategory' table='product_class' column='product_subcategory' uniqueMembers='false'/>\n"
                + "    <Level name='Product Class' table='store' column='store_id' type='Numeric' uniqueMembers='true'/>\n"
                + "    <Level name='Brand Name' table='product' column='brand_name' uniqueMembers='false'/>\n"
                + "    <Level name='Product Name' table='product' column='product_name' uniqueMembers='true'/>\n"
                + "  </Hierarchy>\n"
                + "</Dimension>"));

        // As above, but using shared dimension.
        if (MondrianProperties.instance().ReadAggregates.get()
            && MondrianProperties.instance().UseAggregates.get())
        {
            // With aggregates enabled, query gives different answer. This is
            // expected because some of the foreign keys have referential
            // integrity problems.
            return;
        }
        checkBugMondrian463(
            getTestContext().withSchema(
                "<?xml version='1.0'?>\n"
                + "<Schema name='FoodMart'>\n"
                + "<Dimension name='Product3'>\n"
                + "  <Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>\n"
                + "    <Join leftKey='product_class_id' rightKey='store_id'>\n"
                + "      <Table name='product'/>\n"
                + "      <Join leftKey='region_id' rightKey='product_class_id'>\n"
                + "        <Table name='store'/>\n"
                + "        <Table name='product_class'/>\n"
                + "      </Join>\n"
                + "    </Join>\n"
                + "    <Level name='Product Family' table='product_class' column='product_family' uniqueMembers='true'/>\n"
                + "    <Level name='Product Department' table='product_class' column='product_department' uniqueMembers='false'/>\n"
                + "    <Level name='Product Category' table='product_class' column='product_category' uniqueMembers='false'/>\n"
                + "    <Level name='Product Subcategory' table='product_class' column='product_subcategory' uniqueMembers='false'/>\n"
                + "    <Level name='Product Class' table='store' column='store_id' type='Numeric' uniqueMembers='true'/>\n"
                + "    <Level name='Brand Name' table='product' column='brand_name' uniqueMembers='false'/>\n"
                + "    <Level name='Product Name' table='product' column='product_name' uniqueMembers='true'/>\n"
                + "  </Hierarchy>\n"
                + "</Dimension>\n"
                + "<Cube name='Sales'>\n"
                + "  <Table name='sales_fact_1997'/>\n"
                + "  <Dimension name='Time' type='TimeDimension' foreignKey='time_id'>\n"
                + "    <Hierarchy hasAll='false' primaryKey='time_id'>\n"
                + "      <Table name='time_by_day'/>\n"
                + "      <Level name='Year' column='the_year' type='Numeric' uniqueMembers='true'\n"
                + "          levelType='TimeYears'/>\n"
                + "      <Level name='Quarter' column='quarter' uniqueMembers='false'\n"
                + "          levelType='TimeQuarters'/>\n"
                + "      <Level name='Month' column='month_of_year' uniqueMembers='false' type='Numeric'\n"
                + "          levelType='TimeMonths'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <DimensionUsage source='Product3' name='Product3' foreignKey='product_id'/>\n"
                + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
                + "      formatString='#,###'/>\n"
                + "</Cube>\n"
                + "</Schema>"));
    }

    private void checkBugMondrian463(TestContext testContext) {
        testContext.assertQueryReturns(
            "select [Measures] on 0,\n"
            + " head([Product3].members, 10) on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product3].[All Product3s]}\n"
            + "{[Product3].[Drink]}\n"
            + "{[Product3].[Drink].[Baking Goods]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods].[Coffee]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24].[Amigo]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24].[Amigo].[Amigo Lox]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24].[Curlew]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24].[Curlew].[Curlew Lox]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 2,647\n"
            + "Row #2: 835\n"
            + "Row #3: 835\n"
            + "Row #4: 835\n"
            + "Row #5: 835\n"
            + "Row #6: 175\n"
            + "Row #7: 175\n"
            + "Row #8: 186\n"
            + "Row #9: 186\n");
    }

    public void testVirtualCubesVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<VirtualCube name='Foo' defaultMeasure='Store Sales' visible='@REPLACE_ME@'>\n"
                + "  <VirtualCubeDimension cubeName='Sales' name='Customers'/>\n"
                + "  <VirtualCubeDimension cubeName='Sales' name='Time'/>\n"
                + "  <VirtualCubeMeasure cubeName='Sales' name='[Measures].[Store Sales]'/>\n"
                + "</VirtualCube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                getTestContext().create(
                    null, null, cubeDef, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            assertTrue(testValue.equals(cube.isVisible()));
        }
    }

    public void testVirtualDimensionVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<VirtualCube name='Foo' defaultMeasure='Store Sales'>\n"
                + "  <VirtualCubeDimension cubeName='Sales' name='Customers' visible='@REPLACE_ME@'/>\n"
                + "  <VirtualCubeDimension cubeName='Sales' name='Time'/>\n"
                + "  <VirtualCubeMeasure cubeName='Sales' name='[Measures].[Store Sales]'/>\n"
                + "</VirtualCube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                getTestContext().create(
                    null, null, cubeDef, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            Dimension dim = null;
            for (Dimension dimCheck : cube.getDimensionList()) {
                if (dimCheck.getName().equals("Customers")) {
                    dim = dimCheck;
                }
            }
            assertNotNull(dim);
            assertTrue(testValue.equals(dim.isVisible()));
        }
    }

    public void testDimensionVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name='Foo'>\n"
                + "  <Table name='store'/>\n"
                + "  <Dimension name='Bar' visible='@REPLACE_ME@'>\n"
                + "    <Hierarchy hasAll='true'>\n"
                + "      <Level name='Store Type' column='store_type' uniqueMembers='true'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
                + "      formatString='#,###'/>\n"
                + "</Cube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                getTestContext().create(
                    null, cubeDef, null, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            Dimension dim = null;
            for (Dimension dimCheck : cube.getDimensionList()) {
                if (dimCheck.getName().equals("Bar")) {
                    dim = dimCheck;
                }
            }
            assertNotNull(dim);
            assertTrue(testValue.equals(dim.isVisible()));
        }
    }

    /**
     * Test for MONDRIAN-943 and MONDRIAN-465.
     */
    public void testCaptionWithOrdinalColumn() {
        final TestContext tc =
            getTestContext().createSubstitutingCube(
                "HR",
                "<Dimension name='Position2' foreignKey='employee_id'>\n"
                + "  <Hierarchy hasAll='true' allMemberName='All Position' primaryKey='employee_id'>\n"
                + "    <Table name='employee'/>\n"
                + "    <Level name='Management Role' uniqueMembers='true' column='management_role'/>\n"
                + "    <Level name='Position Title' uniqueMembers='false' column='position_title' ordinalColumn='position_id' captionColumn='position_title'/>\n"
                + "  </Hierarchy>\n"
                + "</Dimension>\n");
        String mdxQuery =
            "WITH SET [#DataSet#] as '{Descendants([Position2].[All Position], 2)}' "
            + "SELECT {[Measures].[Org Salary]} on columns, "
            + "NON EMPTY Hierarchize({[#DataSet#]}) on rows FROM [HR]";
        Result result = tc.executeQuery(mdxQuery);
        Axis[] axes = result.getAxes();
        List<Position> positions = axes[1].getPositions();
        Member mall = positions.get(0).get(0);
        String caption = mall.getHierarchy().getCaption();
        assertEquals("Position2", caption);
        String captionValue = mall.getCaption();
        assertEquals("HQ Information Systems", captionValue);
        mall = positions.get(14).get(0);
        captionValue = mall.getCaption();
        assertEquals("Store Manager", captionValue);
        mall = positions.get(15).get(0);
        captionValue = mall.getCaption();
        assertEquals("Store Assistant Manager", captionValue);
    }


    public void testHierarchyVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name='Foo'>\n"
                + "  <Table name='store'/>\n"
                + "  <Dimension name='Bar'>\n"
                + "    <Hierarchy name='Bacon' hasAll='true' visible='@REPLACE_ME@'>\n"
                + "      <Level name='Store Type' column='store_type' uniqueMembers='true'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
                + "      formatString='#,###'/>\n"
                + "</Cube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                getTestContext().create(
                    null, cubeDef, null, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            Dimension dim = null;
            for (Dimension dimCheck : cube.getDimensionList()) {
                if (dimCheck.getName().equals("Bar")) {
                    dim = dimCheck;
                }
            }
            assertNotNull(dim);
            final Hierarchy hier = dim.getHierarchy();
            assertNotNull(hier);
            assertEquals(
                "Bacon",
                hier.getName());
            assertTrue(testValue.equals(hier.isVisible()));
        }
    }

    /**
     * This is a test case for bug Mondrian-923. When a virtual cube included
     * calculated members in its schema, they were not included in the list of
     * existing measures because of an override of the hierarchy schema reader
     * which was done at cube init time when resolving the calculated members
     * of the base cubes.
     */
    public void testBugMondrian923() throws Exception {
        if (!Bug.VirtualCubeConversionMissesHiddenFixed) {
            return;
        }
        TestContext context =
            getTestContext().createSubstitutingCube(
                "Warehouse and Sales",
                null,
                null,
                "<CalculatedMember name='Image Unit Sales' dimension='Measures'>"
                + "  <Formula>[Measures].[Unit Sales]</Formula>"
                + "  <CalculatedMemberProperty name='FORMAT_STRING' value='|$#,###.00|image=icon_chart\\.gif|link=http://www\\.pentaho\\.com'/>"
                + "</CalculatedMember>"
                + "<CalculatedMember name='Arrow Unit Sales' dimension='Measures'>"
                + "  <Formula>[Measures].[Unit Sales]</Formula>"
                + "  <CalculatedMemberProperty name='FORMAT_STRING' "
                + "   expression='IIf([Measures].[Unit Sales] &gt; 10000,&apos;|#,###|arrow=up&apos;,"
                + "               IIf([Measures].[Unit Sales] &gt; 5000,&apos;|#,###|arrow=down&apos;,&apos;|#,###|arrow=none&apos;))'/>"
                + "</CalculatedMember>"
                + "<CalculatedMember name='Style Unit Sales' dimension='Measures'>"
                + "  <Formula>[Measures].[Unit Sales]</Formula>"
                + "  <CalculatedMemberProperty name='FORMAT_STRING' "
                + "   expression='IIf([Measures].[Unit Sales] &gt; 100000,&apos;|#,###|style=green&apos;,"
                + "               IIf([Measures].[Unit Sales] &gt; 50000,&apos;|#,###|style=yellow&apos;,&apos;|#,###|style=red&apos;))'/>"
                + "</CalculatedMember>",
                null);
        for (Cube cube : context.getConnection().getSchemaReader().getCubes()) {
            if (cube.getName().equals("Warehouse and Sales")) {
                for (Dimension dim : cube.getDimensionList()) {
                    if (dim.isMeasures()) {
                        List<Member> members =
                            context.getConnection()
                                .getSchemaReader().getLevelMembers(
                                    dim.getHierarchy().getLevelList().get(0),
                                true);
                        assertTrue(
                            members.toString().contains(
                                "[Measures].[Profit Per Unit Shipped]"));
                        assertTrue(
                            members.toString().contains(
                                "[Measures].[Image Unit Sales]"));
                        assertTrue(
                            members.toString().contains(
                                "[Measures].[Arrow Unit Sales]"));
                        assertTrue(
                            members.toString().contains(
                                "[Measures].[Style Unit Sales]"));
                        assertTrue(
                            members.toString().contains(
                                "[Measures].[Average Warehouse Sale]"));
                        return;
                    }
                }
            }
        }
        fail("Didn't find measures in sales cube.");
    }

    /**
     * Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1047">MONDRIAN-1047,
     * "IllegalArgumentException when cube has closure tables and many
     * levels"</a>.
     */
    public void testBugMondrian1047() {
        // Test case only works under MySQL, due to how columns are quoted.
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case MYSQL:
            break;
        default:
            return;
        }
        TestContext testContext =
            getTestContext().createSubstitutingCube(
                "HR",
                TestContext.repeatString(
                    100,
                    "<Dimension name='Position %1$d' foreignKey='employee_id'>\n"
                    + "  <Hierarchy hasAll='true' allMemberName='All Position' primaryKey='employee_id'>\n"
                    + "    <Table name='employee'/>\n"
                    + "    <Level name='Position Title' uniqueMembers='false' ordinalColumn='position_id'>\n"
                    + "      <KeyExpression><SQL dialect='generic'>`position_title` + %1$d</SQL></KeyExpression>\n"
                    + "    </Level>\n"
                    + "  </Hierarchy>\n"
                    + "</Dimension>"),
                null);
        testContext.assertQueryReturns(
            "select from [HR]",
            "Axis #0:\n"
            + "{}\n"
            + "$39,431.67");
    }

    /**
     * Test for descriptions, captions and annotations of various schema
     * elements.
     */
    public void testCaptionDescriptionAndAnnotation() {
        if (!Bug.VirtualCubeConversionMissesHiddenFixed) {
            return;
        }
        final String schemaName = "Description schema";
        final String salesCubeName = "DescSales";
        final String virtualCubeName = "DescWarehouseAndSales";
        final String warehouseCubeName = "Warehouse";
        final TestContext testContext = getTestContext().withSchema(
            "<Schema name='" + schemaName + "'\n"
            + " description='Schema to test descriptions and captions'>\n"
            + "  <Annotations>\n"
            + "    <Annotation name='a'>Schema</Annotation>\n"
            + "    <Annotation name='b'>Xyz</Annotation>\n"
            + "  </Annotations>\n"
            + "  <Dimension name='Time' type='TimeDimension'\n"
            + "      caption='Time shared caption'\n"
            + "      description='Time shared description'>\n"
            + "    <Annotations><Annotation name='a'>Time shared</Annotation></Annotations>\n"
            + "    <Hierarchy hasAll='false' primaryKey='time_id'\n"
            + "        caption='Time shared hierarchy caption'\n"
            + "        description='Time shared hierarchy description'>\n"
            + "      <Table name='time_by_day'/>\n"
            + "      <Level name='Year' column='the_year' type='Numeric' uniqueMembers='true'\n"
            + "          levelType='TimeYears'/>\n"
            + "      <Level name='Quarter' column='quarter' uniqueMembers='false'\n"
            + "          levelType='TimeQuarters'/>\n"
            + "      <Level name='Month' column='month_of_year' uniqueMembers='false' type='Numeric'\n"
            + "          levelType='TimeMonths'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name='Warehouse'>\n"
            + "    <Hierarchy hasAll='true' primaryKey='warehouse_id'>\n"
            + "      <Table name='warehouse'/>\n"
            + "      <Level name='Country' column='warehouse_country' uniqueMembers='true'/>\n"
            + "      <Level name='State Province' column='warehouse_state_province'\n"
            + "          uniqueMembers='true'/>\n"
            + "      <Level name='City' column='warehouse_city' uniqueMembers='false'/>\n"
            + "      <Level name='Warehouse Name' column='warehouse_name' uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Cube name='" + salesCubeName + "'\n"
            + "    description='Cube description'>\n"
            + "  <Annotations><Annotation name='a'>Cube</Annotation></Annotations>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "  <Dimension name='Store' foreignKey='store_id'\n"
            + "      caption='Dimension caption'\n"
            + "      description='Dimension description'>\n"
            + "    <Annotations><Annotation name='a'>Dimension</Annotation></Annotations>\n"
            + "    <Hierarchy hasAll='true' primaryKeyTable='store' primaryKey='store_id'\n"
            + "        caption='Hierarchy caption'\n"
            + "        description='Hierarchy description'>\n"
            + "      <Annotations><Annotation name='a'>Hierarchy</Annotation></Annotations>\n"
            + "      <Join leftKey='region_id' rightKey='region_id'>\n"
            + "        <Table name='store'/>\n"
            + "        <Join leftKey='sales_district_id' rightKey='promotion_id'>\n"
            + "          <Table name='region'/>\n"
            + "          <Table name='promotion'/>\n"
            + "        </Join>\n"
            + "      </Join>\n"
            + "      <Level name='Store Country' table='store' column='store_country'\n"
            + "          description='Level description'"
            + "          caption='Level caption'>\n"
            + "        <Annotations><Annotation name='a'>Level</Annotation></Annotations>\n"
            + "      </Level>\n"
            + "      <Level name='Store Region' table='region' column='sales_region' />\n"
            + "      <Level name='Store Name' table='store' column='store_name' />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <DimensionUsage name='Time1'\n"
            + "    caption='Time usage caption'\n"
            + "    description='Time usage description'\n"
            + "    source='Time' foreignKey='time_id'>\n"
            + "    <Annotations><Annotation name='a'>Time usage</Annotation></Annotations>\n"
            + "  </DimensionUsage>\n"
            + "  <DimensionUsage name='Time2'\n"
            + "    source='Time' foreignKey='time_id'/>\n"
            + "<Measure name='Unit Sales' column='unit_sales'\n"
            + "    aggregator='sum' formatString='Standard'\n"
            + "    caption='Measure caption'\n"
            + "    description='Measure description'>\n"
            + "  <Annotations><Annotation name='a'>Measure</Annotation></Annotations>\n"
            + "</Measure>\n"
            + "<CalculatedMember name='Foo' dimension='Measures' \n"
            + "    caption='Calc member caption'\n"
            + "    description='Calc member description'>\n"
            + "    <Annotations><Annotation name='a'>Calc member</Annotation></Annotations>\n"
            + "    <Formula>[Measures].[Unit Sales] + 1</Formula>\n"
            + "    <CalculatedMemberProperty name='FORMAT_STRING' value='$#,##0.00'/>\n"
            + "  </CalculatedMember>\n"
            + "  <NamedSet name='Top Periods'\n"
            + "      caption='Named set caption'\n"
            + "      description='Named set description'>\n"
            + "    <Annotations><Annotation name='a'>Named set</Annotation></Annotations>\n"
            + "    <Formula>TopCount([Time1].MEMBERS, 5, [Measures].[Foo])</Formula>\n"
            + "  </NamedSet>\n"
            + "</Cube>\n"
            + "<Cube name='" + warehouseCubeName + "'>\n"
            + "  <Table name='inventory_fact_1997'/>\n"
            + "\n"
            + "  <DimensionUsage name='Time' source='Time' foreignKey='time_id'/>\n"
            + "  <DimensionUsage name='Warehouse' source='Warehouse' foreignKey='warehouse_id'/>\n"
            + "\n"
            + "  <Measure name='Units Shipped' column='units_shipped' aggregator='sum' formatString='#.0'/>\n"
            + "</Cube>\n"
            + "<VirtualCube name='" + virtualCubeName + "'\n"
            + "    caption='Virtual cube caption'\n"
            + "    description='Virtual cube description'>\n"
            + "  <Annotations><Annotation name='a'>Virtual cube</Annotation></Annotations>\n"
            + "  <VirtualCubeDimension name='Time1' cubeName='"
            + salesCubeName
            + "'/>\n"
            + "  <VirtualCubeDimension cubeName='" + warehouseCubeName
            + "' name='Warehouse'/>\n"
            + "  <VirtualCubeMeasure cubeName='" + salesCubeName
            + "' name='[Measures].[Unit Sales]'>\n"
            + "    <Annotations><Annotation name='a'>Virtual cube measure</Annotation></Annotations>\n"
            + "  </VirtualCubeMeasure>\n"
            + "  <VirtualCubeMeasure cubeName='" + warehouseCubeName
            + "' name='[Measures].[Units Shipped]'/>\n"
            + "  <CalculatedMember name='Profit Per Unit Shipped' dimension='Measures'>\n"
            + "    <Formula>1 / [Measures].[Units Shipped]</Formula>\n"
            + "  </CalculatedMember>\n"
            + "</VirtualCube>"
            + "</Schema>");
        final Result result =
            testContext.executeQuery("select from [" + salesCubeName + "]");
        final Cube cube = result.getQuery().getCube();
        assertEquals("Cube description", cube.getDescription());
        checkAnnotations(cube.getAnnotationMap(), "a", "Cube");

        final Schema schema = cube.getSchema();
        checkAnnotations(schema.getAnnotationMap(), "a", "Schema", "b", "Xyz");

        final Dimension dimension = cube.getDimensionList().get(1);
        assertEquals("Dimension description", dimension.getDescription());
        assertEquals("Dimension caption", dimension.getCaption());
        checkAnnotations(dimension.getAnnotationMap(), "a", "Dimension");

        final Hierarchy hierarchy = dimension.getHierarchyList().get(0);
        assertEquals("Hierarchy description", hierarchy.getDescription());
        assertEquals("Hierarchy caption", hierarchy.getCaption());
        checkAnnotations(hierarchy.getAnnotationMap(), "a", "Hierarchy");

        final mondrian.olap.Level level = hierarchy.getLevelList().get(1);
        assertEquals("Level description", level.getDescription());
        assertEquals("Level caption", level.getCaption());
        checkAnnotations(level.getAnnotationMap(), "a", "Level");

        // Caption comes from the CAPTION member property, defaults to name.
        // Description comes from the DESCRIPTION member property.
        // Annotations are always empty for regular members.
        final List<Member> memberList =
            cube.getSchemaReader(null).withLocus()
                .getLevelMembers(level, false);
        final Member member = memberList.get(0);
        assertEquals("Canada", member.getName());
        assertEquals("Canada", member.getCaption());
        assertNull(member.getDescription());
        checkAnnotations(member.getAnnotationMap());

        // All member. Caption defaults to name; description is null.
        final Member allMember = member.getParentMember();
        assertEquals("All Stores", allMember.getName());
        assertEquals("All Stores", allMember.getCaption());
        assertNull(allMember.getDescription());

        // All level.
        final mondrian.olap.Level allLevel = hierarchy.getLevelList().get(0);
        assertEquals("(All)", allLevel.getName());
        assertNull(allLevel.getDescription());
        assertEquals(allLevel.getName(), allLevel.getCaption());
        checkAnnotations(allLevel.getAnnotationMap());

        // the first time dimension overrides the caption and description of the
        // shared time dimension
        final Dimension timeDimension = cube.getDimensionList().get(2);
        assertEquals("Time1", timeDimension.getName());
        assertEquals("Time usage description", timeDimension.getDescription());
        assertEquals("Time usage caption", timeDimension.getCaption());
        checkAnnotations(timeDimension.getAnnotationMap(), "a", "Time usage");

        // Time1 is a usage of a shared dimension Time.
        // Now look at the hierarchy usage within that dimension usage.
        // Because the dimension usage has a name, use that as a prefix for
        // name, caption and description of the hierarchy usage.
        final Hierarchy timeHierarchy = timeDimension.getHierarchyList().get(0);
        // The hierarchy in the shared dimension does not have a name, so the
        // hierarchy usage inherits the name of the dimension usage, Time1.
        assertEquals("Time", timeHierarchy.getName());
        assertEquals("Time1", timeHierarchy.getDimension().getName());
        // The description is prefixed by the dimension usage name.
        assertEquals(
            "Time usage caption.Time shared hierarchy description",
            timeHierarchy.getDescription());
        // The hierarchy caption is prefixed by the caption of the dimension
        // usage.
        assertEquals(
            "Time usage caption.Time shared hierarchy caption",
            timeHierarchy.getCaption());
        // No annotations.
        checkAnnotations(timeHierarchy.getAnnotationMap());

        // the second time dimension does not overrides caption and description
        final Dimension time2Dimension = cube.getDimensionList().get(3);
        assertEquals("Time2", time2Dimension.getName());
        assertEquals(
            "Time shared description", time2Dimension.getDescription());
        assertEquals("Time shared caption", time2Dimension.getCaption());
        checkAnnotations(time2Dimension.getAnnotationMap());

        final Hierarchy time2Hierarchy =
            time2Dimension.getHierarchyList().get(0);
        // The hierarchy in the shared dimension does not have a name, so the
        // hierarchy usage inherits the name of the dimension usage, Time2.
        assertEquals("Time", time2Hierarchy.getName());
        assertEquals("Time2", time2Hierarchy.getDimension().getName());
        // The description is prefixed by the dimension usage name (because
        // dimension usage has no caption).
        assertEquals(
            "Time2.Time shared hierarchy description",
            time2Hierarchy.getDescription());
        // The hierarchy caption is prefixed by the dimension usage name
        // (because the dimension usage has no caption.
        assertEquals(
            "Time2.Time shared hierarchy caption",
            time2Hierarchy.getCaption());
        // No annotations.
        checkAnnotations(time2Hierarchy.getAnnotationMap());

        final Dimension measuresDimension = cube.getDimensionList().get(0);
        final Hierarchy measuresHierarchy =
            measuresDimension.getHierarchyList().get(0);
        final mondrian.olap.Level measuresLevel =
            measuresHierarchy.getLevelList().get(0);
        final SchemaReader schemaReader = cube.getSchemaReader(null);
        final List<Member> measures =
            schemaReader.getLevelMembers(measuresLevel, true);
        final Member measure = measures.get(0);
        assertEquals("Unit Sales", measure.getName());
        assertEquals("Measure caption", measure.getCaption());
        assertEquals("Measure description", measure.getDescription());
        assertEquals(
            measure.getDescription(),
            measure.getPropertyValue(Property.DESCRIPTION));
        assertEquals(
            measure.getCaption(),
            measure.getPropertyValue(Property.CAPTION));
        assertEquals(
            measure.getCaption(),
            measure.getPropertyValue(Property.MEMBER_CAPTION));
        checkAnnotations(measure.getAnnotationMap(), "a", "Measure");

        // The implicitly created [Fact Count] measure
        final Member factCountMeasure = measures.get(1);
        assertEquals("Fact Count", factCountMeasure.getName());
        assertEquals(
            false,
            factCountMeasure.getPropertyValue(Property.VISIBLE));

        final Member calcMeasure = measures.get(2);
        assertEquals("Foo", calcMeasure.getName());
        assertEquals("Calc member caption", calcMeasure.getCaption());
        assertEquals("Calc member description", calcMeasure.getDescription());
        assertEquals(
            calcMeasure.getDescription(),
            calcMeasure.getPropertyValue(Property.DESCRIPTION));
        assertEquals(
            calcMeasure.getCaption(),
            calcMeasure.getPropertyValue(Property.CAPTION));
        assertEquals(
            calcMeasure.getCaption(),
            calcMeasure.getPropertyValue(Property.MEMBER_CAPTION));
        checkAnnotations(calcMeasure.getAnnotationMap(), "a", "Calc member");

        final NamedSet namedSet = cube.getNamedSets()[0];
        assertEquals("Top Periods", namedSet.getName());
        assertEquals("Named set caption", namedSet.getCaption());
        assertEquals("Named set description", namedSet.getDescription());
        checkAnnotations(namedSet.getAnnotationMap(), "a", "Named set");

        final Result result2 =
            testContext.executeQuery("select from [" + virtualCubeName + "]");
        final Cube cube2 = result2.getQuery().getCube();
        assertEquals("Virtual cube description", cube2.getDescription());
        checkAnnotations(cube2.getAnnotationMap(), "a", "Virtual cube");

        final SchemaReader schemaReader2 = cube2.getSchemaReader(null);
        final Dimension measuresDimension2 = cube2.getDimensionList().get(0);
        final Hierarchy measuresHierarchy2 =
            measuresDimension2.getHierarchyList().get(0);
        final mondrian.olap.Level measuresLevel2 =
            measuresHierarchy2.getLevelList().get(0);
        final List<Member> measures2 =
            schemaReader2.getLevelMembers(measuresLevel2, true);
        final Member measure2 = measures2.get(0);
        assertEquals("Unit Sales", measure2.getName());
        assertEquals("Measure caption", measure2.getCaption());
        assertEquals("Measure description", measure2.getDescription());
        assertEquals(
            measure2.getDescription(),
            measure2.getPropertyValue(Property.DESCRIPTION));
        assertEquals(
            measure2.getCaption(),
            measure2.getPropertyValue(Property.CAPTION));
        assertEquals(
            measure2.getCaption(),
            measure2.getPropertyValue(Property.MEMBER_CAPTION));
        checkAnnotations(
            measure2.getAnnotationMap(), "a", "Virtual cube measure");
    }

    private static void checkAnnotations(
        Map<String, Annotation> annotationMap,
        String... nameVal)
    {
        assertNotNull(annotationMap);
        assertEquals(0, nameVal.length % 2);
        assertEquals(nameVal.length / 2, annotationMap.size());
        int i = 0;
        for (Map.Entry<String, Annotation> entry : annotationMap.entrySet()) {
            assertEquals(nameVal[i++], entry.getKey());
            assertEquals(nameVal[i++], entry.getValue().getValue());
        }
    }

    public void testCaptionExpression() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "  <Dimension name='Gender2' foreignKey='customer_id'>\n"
            + "    <Hierarchy hasAll='true' primaryKey='customer_id' >\n"
            + "      <Table name='customer'/>\n"
            + "      <Level name='Gender' column='gender' uniqueMembers='true' >\n"
            + "        <CaptionExpression>\n"
            + "          <SQL dialect='generic'>'foobar'</SQL>\n"
            + "        </CaptionExpression>\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>");
        switch (testContext.getDialect().getDatabaseProduct()) {
        case POSTGRESQL:
            // Postgres fails with:
            //   Internal error: while building member cache; sql=[select
            //     "customer"."gender" as "c0", 'foobar' as "c1" from "customer"
            //     as "customer" group by "customer"."gender", 'foobar' order by
            //     "customer"."\ gender" ASC NULLS LAST]
            //   Caused by: org.postgresql.util.PSQLException: ERROR:
            //     non-integer constant in GROUP BY
            //
            // It's difficult for mondrian to spot that it's been given a
            // constant expression. We can live with this bug. Postgres
            // shouldn't be so picky, and people shouldn't be so daft.
            return;
        }
        Result result = testContext.executeQuery(
            "select {[Gender2].Children} on columns from [Sales]");
        assertEquals(
            "foobar",
            result.getAxes()[0].getPositions().get(0).get(0).getCaption());
    }

    /**
     * Tests that mondrian gives an error if a level is not functionally
     * dependent on the level immediately below it.
     */
    public void testSnowflakeNotFunctionallyDependent() {
        final String cubeName = "SalesNotFD";
        final TestContext testContext = getTestContext().create(
            null,
            "<Cube name='" + cubeName + "' defaultMeasure='Unit Sales'>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "  <Dimension name='Store' foreignKey='store_id'>\n"
            + "    <Hierarchy hasAll='true' primaryKeyTable='store' primaryKey='store_id'>\n"
            + "      <Join leftKey='region_id' rightKey='region_id'>\n"
            + "        <Table name='store'/>\n"
            + "        <Join leftKey='sales_district_id' rightKey='promotion_id'>\n"
            + "          <Table name='region'/>\n"
            + "          <Table name='promotion'/>\n"
            + "        </Join>\n"
            + "      </Join>\n"
            + "      <Level name='Store Country' table='store' column='store_country' uniqueMembers='true'/>\n"
            + "      <Level name='Store Region' table='region' column='sales_region' />\n"
            + "      <Level name='Store Name' table='store' column='store_name' />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "<Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "</Cube>",
            null, null, null, null);

        // TODO: convert this exception from fatal to warning
        testContext.assertQueryThrows(
            "select from [SalesNotFD]",
            "Key of level [Store].[Store Region] is not functionally dependent "
            + "on key of parent level: Needed to find exactly one path from "
            + "region to store, but found 0");
    }

    /**
     * test hierarchy with slightly different join path to fact table than
     * first hierarchy. tables from first and second hierarchy should contain
     * the same join aliases to the fact table.
     */
    public void testSnowflakeHierarchyValidationNotNeeded() {
        if (!Bug.BugMondrian1324Fixed) {
            return;
        }
        final TestContext testContext = getTestContext().create(
            null,
            "<Cube name='AliasedDimensionsTesting' defaultMeasure='Unit Sales'>\n"
            + "  <Table name='sales_fact_1997'>\n"
            + "    <AggExclude pattern='agg_lc_06_sales_fact_1997'/>\n"
            + "  </Table>"
            + "  <Dimension name='Store' foreignKey='store_id'>\n"
            + "    <Hierarchy hasAll='true' primaryKeyTable='store' primaryKey='store_id'>\n"
            + "      <Join leftKey='region_id' rightKey='region_id'>\n"
            + "        <Table name='store'/>\n"
            + "        <Join leftKey='sales_district_id' rightKey='promotion_id'>\n"
            + "          <Table name='region'/>\n"
            + "          <Table name='promotion'/>\n"
            + "        </Join>\n"
            + "      </Join>\n"
            + "      <Level name='Store Country' table='store' column='store_country' uniqueMembers='true'/>\n"
            + "      <Level name='Store Region' table='region' column='sales_region' />\n"
            + "      <Level name='Store Name' table='store' column='store_name' />\n"
            + "    </Hierarchy>\n"
            + "    <Hierarchy name='MyHierarchy' hasAll='true' primaryKeyTable='store' primaryKey='store_id'>\n"
            + "      <Join leftKey='region_id' rightKey='region_id'>\n"
            + "        <Table name='store'/>\n"
            + "        <Table name='region'/>\n"
            + "      </Join>\n"
            + "      <Level name='Store Country' table='store' column='store_country' uniqueMembers='true'/>\n"
            + "      <Level name='Store Region' table='region' column='sales_region' />\n"
            + "      <Level name='Store Name' table='store' column='store_name' />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name='Customers' foreignKey='customer_id'>\n"
            + "    <Hierarchy hasAll='true' allMemberName='All Customers' primaryKeyTable='customer' primaryKey='customer_id'>\n"
            + "    <Join leftKey='customer_region_id' rightKey='region_id'>\n"
            + "      <Table name='customer'/>\n"
            + "      <Table name='region'/>\n"
            + "    </Join>\n"
            + "    <Level name='Country' table='customer' column='country' uniqueMembers='true'/>\n"
            + "    <Level name='Region' table='region' column='sales_region' uniqueMembers='true'/>\n"
            + "    <Level name='City' table='customer' column='city' uniqueMembers='false'/>\n"
            + "    <Level name='Name' table='customer' column='customer_id' type='Numeric' uniqueMembers='true'/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select  {[Store.MyHierarchy].[USA].[South West]} on rows,"
            + "{[Customers].[USA].[South West]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[MyHierarchy].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTable() {
        if (!Bug.BugMondrian1324Fixed) {
            return;
        }
        final TestContext testContext = getTestContext().legacy().create(
            null,
            "<Cube name='AliasedDimensionsTesting' defaultMeasure='Unit Sales'>\n"
            + "  <Table name='sales_fact_1997'>\n"
            + "    <AggExclude pattern='agg_lc_06_sales_fact_1997'/>\n"
            + "  </Table>"
            + "<Dimension name='Store' foreignKey='store_id'>\n"
            + "<Hierarchy hasAll='true' primaryKeyTable='store' primaryKey='store_id'>\n"
            + "    <Join leftKey='region_id' rightKey='region_id'>\n"
            + "      <Table name='store'/>\n"
            + "      <Table name='region'/>\n"
            + "    </Join>\n"
            + " <Level name='Store Country' table='store'  column='store_country' uniqueMembers='true'/>\n"
            + " <Level name='Store Region'  table='region' column='sales_region'  uniqueMembers='true'/>\n"
            + " <Level name='Store Name'    table='store'  column='store_name'    uniqueMembers='true'/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Dimension name='Customers' foreignKey='customer_id'>\n"
            + "<Hierarchy hasAll='true' allMemberName='All Customers' primaryKeyTable='customer' primaryKey='customer_id'>\n"
            + "    <Join leftKey='customer_region_id' rightKey='region_id'>\n"
            + "      <Table name='customer'/>\n"
            + "      <Table name='region'/>\n"
            + "    </Join>\n"
            + "  <Level name='Country' table='customer' column='country'                      uniqueMembers='true'/>\n"
            + "  <Level name='Region'  table='region'   column='sales_region'                 uniqueMembers='true'/>\n"
            + "  <Level name='City'    table='customer' column='city'                         uniqueMembers='false'/>\n"
            + "  <Level name='Name'    table='customer' column='customer_id' type='Numeric' uniqueMembers='true'/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "<Measure name='Store Sales' column='store_sales' aggregator='sum' formatString='#,###.00'/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select  {[Store].[USA].[South West]} on rows,"
            + "{[Customers].[USA].[South West]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customers].[Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTableOneAlias() {
        final TestContext testContext = getTestContext().legacy().create(
            null,
            "<Cube name='AliasedDimensionsTesting' defaultMeasure='Unit Sales'>\n"
            + "  <Table name='sales_fact_1997'>\n"
            + "    <AggExclude pattern='agg_lc_06_sales_fact_1997'/>\n"
            + "  </Table>"
            + "<Dimension name='Store' foreignKey='store_id'>\n"
            + "<Hierarchy hasAll='true' primaryKeyTable='store' primaryKey='store_id'>\n"
            + "    <Join leftKey='region_id' rightKey='region_id'>\n"
            + "      <Table name='store'/>\n"
            + "      <Table name='region'/>\n"
            + "    </Join>\n"
            + " <Level name='Store Country' table='store'  column='store_country' uniqueMembers='true'/>\n"
            + " <Level name='Store Region'  table='region' column='sales_region'  uniqueMembers='true'/>\n"
            + " <Level name='Store Name'    table='store'  column='store_name'    uniqueMembers='true'/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Dimension name='Customers' foreignKey='customer_id'>\n"
            + "<Hierarchy hasAll='true' allMemberName='All Customers' primaryKeyTable='customer' primaryKey='customer_id'>\n"
            + "    <Join leftKey='customer_region_id' rightKey='region_id'>\n"
            + "      <Table name='customer'/>\n"
            + "      <Table name='region' alias='customer_region'/>\n"
            + "    </Join>\n"
            + "  <Level name='Country' table='customer' column='country'                      uniqueMembers='true'/>\n"
            + "  <Level name='Region'  table='customer_region'   column='sales_region'                 uniqueMembers='true'/>\n"
            + "  <Level name='City'    table='customer' column='city'                         uniqueMembers='false'/>\n"
            + "  <Level name='Name'    table='customer' column='customer_id' type='Numeric' uniqueMembers='true'/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "<Measure name='Store Sales' column='store_sales' aggregator='sum' formatString='#,###.00'/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select  {[Store].[USA].[South West]} on rows,"
            + "{[Customers].[USA].[South West]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customers].[Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTableTwoAliases() {
        final TestContext testContext = getTestContext().legacy().create(
            null,
            "<Cube name='AliasedDimensionsTesting' defaultMeasure='Unit Sales'>\n"
            + "  <Table name='sales_fact_1997'>\n"
            + "    <AggExclude pattern='agg_lc_06_sales_fact_1997'/>\n"
            + "  </Table>"
            + "<Dimension name='Store' foreignKey='store_id'>\n"
            + "<Hierarchy hasAll='true' primaryKeyTable='store' primaryKey='store_id'>\n"
            + "    <Join leftKey='region_id' rightKey='region_id'>\n"
            + "      <Table name='store'/>\n"
            + "      <Table name='region' alias='store_region'/>\n"
            + "    </Join>\n"
            + " <Level name='Store Country' table='store'  column='store_country' uniqueMembers='true'/>\n"
            + " <Level name='Store Region'  table='store_region' column='sales_region'  uniqueMembers='true'/>\n"
            + " <Level name='Store Name'    table='store'  column='store_name'    uniqueMembers='true'/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Dimension name='Customers' foreignKey='customer_id'>\n"
            + "<Hierarchy hasAll='true' allMemberName='All Customers' primaryKeyTable='customer' primaryKey='customer_id'>\n"
            + "    <Join leftKey='customer_region_id' rightKey='region_id'>\n"
            + "      <Table name='customer'/>\n"
            + "      <Table name='region' alias='customer_region'/>\n"
            + "    </Join>\n"
            + "  <Level name='Country' table='customer' column='country'                      uniqueMembers='true'/>\n"
            + "  <Level name='Region'  table='customer_region'   column='sales_region'                 uniqueMembers='true'/>\n"
            + "  <Level name='City'    table='customer' column='city'                         uniqueMembers='false'/>\n"
            + "  <Level name='Name'    table='customer' column='customer_id' type='Numeric' uniqueMembers='true'/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "<Measure name='Store Sales' column='store_sales' aggregator='sum' formatString='#,###.00'/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select  {[Store].[USA].[South West]} on rows,"
            + "{[Customers].[USA].[South West]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customers].[Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }


    /**
     * Unit test for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-661">
     * MONDRIAN-661, "Name expressions in snowflake hierarchies do not work,
     * unfriendly exception occurs"</a>.
     *
     * <p>NOTE: bug is not marked fixed yet.</p>
     */
    public void testSnowFlakeNameExpressions() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension name='Product with inline' foreignKey='product_id'>"
                + "  <Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>"
                + "    <Join leftKey='product_class_id' rightKey='product_class_id'>"
                + "      <Table name='product'/>"
                + "      <Table name='product_class'/>"
                + "    </Join>"
                + "    <Level name='Product Family' table='product_class' column='product_family' uniqueMembers='true'/>"
                + "    <Level name='Product Department' table='product_class' column='product_department' uniqueMembers='false'/>"
                + "    <Level name='Product Category' table='product_class' column='product_category' uniqueMembers='false'/>"
                + "    <Level name='Product Subcategory' table='product_class' column='product_subcategory' uniqueMembers='false'/>"
                + "    <Level name='Brand Name' table='product' column='brand_name' uniqueMembers='false'/>"
                + "    <Level name='Product Name' table='product' column='product_name' uniqueMembers='true'>"
                + "      <NameExpression>"
                + "        <SQL dialect='mysql'>concat(`product_name`,'_bar')</SQL>"
                + "        <SQL dialect='oracle'>`product_name` || '_bar'</SQL>"
                + "      </NameExpression>"
                + "    </Level>"
                + "  </Hierarchy>"
                + "</Dimension>");
        testContext.assertQueryReturns(
            "select {[Product with inline].[All Product with inlines].[Drink].[Dairy].[Dairy].[Milk].[Club].Children} "
            + "on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product with inline].[Product with inline].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club 1% Milk_bar]}\n"
            + "{[Product with inline].[Product with inline].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club 2% Milk_bar]}\n"
            + "{[Product with inline].[Product with inline].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Buttermilk_bar]}\n"
            + "{[Product with inline].[Product with inline].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Chocolate Milk_bar]}\n"
            + "{[Product with inline].[Product with inline].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Whole Milk_bar]}\n"
            + "Row #0: 155\n"
            + "Row #0: 145\n"
            + "Row #0: 140\n"
            + "Row #0: 159\n"
            + "Row #0: 168\n");
    }

    /**
     * Tests that a join nested left-deep, that is (Join (Join A B) C), fails.
     * The correct way to use a join is right-deep, that is (Join A (Join B C)).
     * Same schema as {@link #testBugMondrian463}, except left-deep.
     */
    public void testLeftDeepJoinFails() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name='Product3' foreignKey='product_id'>\n"
            + "  <Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>\n"
            + "    <Join leftKey='store_id' rightKey='product_class_id'>\n"
            + "      <Join leftKey='product_class_id' rightKey='region_id'>\n"
            + "        <Table name='product'/>\n"
            + "        <Table name='store'/>\n"
            + "      </Join>\n"
            + "      <Table name='product_class'/>\n"
            + "    </Join>\n"
            + "    <Level name='Product Family' table='product_class' column='product_family' uniqueMembers='true'/>\n"
            + "    <Level name='Product Department' table='product_class' column='product_department' uniqueMembers='false'/>\n"
            + "    <Level name='Product Category' table='product_class' column='product_category' uniqueMembers='false'/>\n"
            + "    <Level name='Product Subcategory' table='product_class' column='product_subcategory' uniqueMembers='false'/>\n"
            + "    <Level name='Product Class' table='store' column='store_id' uniqueMembers='true'/>\n"
            + "    <Level name='Brand Name' table='product' column='brand_name' uniqueMembers='false'/>\n"
            + "    <Level name='Product Name' table='product' column='product_name' uniqueMembers='true'/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");
        try {
            testContext.assertSimpleQuery();
            fail("expected error");
        } catch (MondrianException e) {
            assertEquals(
                "Mondrian Error:Left side of join must not be a join; mondrian only supports right-deep joins.",
                e.getMessage());
        }
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * native non empty cross join sql generation returns empty query.
     * note that this works when native cross join is disabled
     */
    public void testDimensionsShareTableNativeNonEmptyCrossJoin() {
        final TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name='Yearly Income2' foreignKey='product_id'>\n"
            + "  <Hierarchy hasAll='true' primaryKey='customer_id'>\n"
            + "    <Table name='customer' alias='customerx' />\n"
            + "    <Level name='Yearly Income' column='yearly_income' uniqueMembers='true'/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");

        testContext.assertQueryReturns(
            "select NonEmptyCrossJoin({[Yearly Income2].[All Yearly Income2s]},{[Customers].[All Customers]}) on rows,"
            + "NON EMPTY {[Measures].[Unit Sales]} on columns"
            + " from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income2].[Yearly Income2].[All Yearly Income2s], [Customers].[Customers].[All Customers]}\n"
            + "Row #0: 266,773\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * Without the table alias, generates SQL which is missing a join condition.
     */
    public void testDimensionsShareTable() {
        final TestContext legacy = getTestContext().legacy();
        final TestContext testContext = legacy.createSubstitutingCube(
            "Sales",
            "<Dimension name='Yearly Income2' foreignKey='product_id'>\n"
            + "  <Hierarchy hasAll='true' primaryKey='customer_id'>\n"
            + "    <Table name='customer' alias='customerx' />\n"
            + "    <Level name='Yearly Income' column='yearly_income' uniqueMembers='true'/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");

        testContext.assertQueryReturns(
            "select {[Yearly Income].[$10K - $30K]} on columns,"
            + "{[Yearly Income2].[$150K +]} on rows from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income2].[Yearly Income2].[$150K +]}\n"
            + "Row #0: 918\n");

        testContext.assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "NON EMPTY Crossjoin({[Yearly Income].[All Yearly Incomes].Children},\n"
            + "                     [Yearly Income2].[All Yearly Income2s].Children) ON ROWS\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income2].[$90K - $110K]}\n"
            + "Row #0: 12,824\n"
            + "Row #1: 2,822\n"
            + "Row #2: 2,933\n"
            + "Row #3: 918\n"
            + "Row #4: 18,381\n"
            + "Row #5: 10,436\n"
            + "Row #6: 6,777\n"
            + "Row #7: 2,859\n"
            + "Row #8: 2,432\n"
            + "Row #9: 532\n"
            + "Row #10: 566\n"
            + "Row #11: 177\n"
            + "Row #12: 3,877\n"
            + "Row #13: 2,131\n"
            + "Row #14: 1,319\n"
            + "Row #15: 527\n"
            + "Row #16: 3,331\n"
            + "Row #17: 643\n"
            + "Row #18: 703\n"
            + "Row #19: 187\n"
            + "Row #20: 4,497\n"
            + "Row #21: 2,629\n"
            + "Row #22: 1,681\n"
            + "Row #23: 721\n"
            + "Row #24: 1,123\n"
            + "Row #25: 224\n"
            + "Row #26: 257\n"
            + "Row #27: 109\n"
            + "Row #28: 1,924\n"
            + "Row #29: 1,026\n"
            + "Row #30: 675\n"
            + "Row #31: 291\n"
            + "Row #32: 19,067\n"
            + "Row #33: 4,078\n"
            + "Row #34: 4,235\n"
            + "Row #35: 1,569\n"
            + "Row #36: 28,160\n"
            + "Row #37: 15,368\n"
            + "Row #38: 10,329\n"
            + "Row #39: 4,504\n"
            + "Row #40: 9,708\n"
            + "Row #41: 2,353\n"
            + "Row #42: 2,243\n"
            + "Row #43: 748\n"
            + "Row #44: 14,469\n"
            + "Row #45: 7,966\n"
            + "Row #46: 5,272\n"
            + "Row #47: 2,208\n"
            + "Row #48: 7,320\n"
            + "Row #49: 1,630\n"
            + "Row #50: 1,602\n"
            + "Row #51: 541\n"
            + "Row #52: 10,550\n"
            + "Row #53: 5,843\n"
            + "Row #54: 3,997\n"
            + "Row #55: 1,562\n"
            + "Row #56: 2,722\n"
            + "Row #57: 597\n"
            + "Row #58: 568\n"
            + "Row #59: 193\n"
            + "Row #60: 3,800\n"
            + "Row #61: 2,192\n"
            + "Row #62: 1,324\n"
            + "Row #63: 523\n");
    }
}

// End LegacySchemaTest.java
