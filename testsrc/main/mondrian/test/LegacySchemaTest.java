/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import junit.framework.Assert;

import mondrian.olap.*;

/**
 * Unit tests on the legacy (mondrian version 3) schema.
 *
 * @author jhyde
 * @version $Id$
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
}

// End LegacySchemaTest.java
