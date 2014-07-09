/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.util.Bug;

import java.sql.SQLException;

/**
 * Unit tests that check compatibility with Microsoft SQL Server Analysis
 * Services 2005.
 *
 * <p>This suite contains a MDX collection of queries that were run on SSAS. The
 * queries cover a variety of issues, including multiple hierarchies in a
 * dimension, attribute hierarchies, and name resolution. Expect to find tests
 * for these areas in dedicated tests also.
 *
 * <p>There are tests for features which are unimplemented or where mondrian's
 * behavior differs from SSAS2005. These tests will appear in this file
 * disabled or with (clearly marked) incorrect results.
 *
 * @author jhyde
 * @since December 15, 2008
 */
public class Ssas2005CompatibilityTest extends FoodMartTestCase {

    /**
     * Whether member naming rules are implemented.
     */
    private static final boolean MEMBER_NAMING_IMPL = Bug.BugMondrian951Fixed;

    /**
     * Whether attribute hierarchies are implemented.
     */
    public static final boolean ATTR_HIER_IMPL = false;

    /**
     * Whether the AXIS function has been are implemented.
     */
    public static final boolean AXIS_IMPL = false;

    /**
     * Catch-all for tests that depend on something that hasn't been
     * implemented.
     */
    private static final boolean IMPLEMENTED = false;

    /**
     * Creates a Ssas2005CompatibilityTest.
     *
     * @param name Testcase name
     */
    public Ssas2005CompatibilityTest(String name) {
        super(name);
    }

    private void runQ(String s) {
        Result result = getTestContext().executeQuery(s);
        Util.discard(TestContext.toString(result));
    }

    @Override
    public TestContext getTestContext() {
        // Key features:
        // 1. Dimension [Product] has hierarchies [Products] and at least one
        //    other.
        // 2. Dimension [Currency] has one unnamed hierarchy
        // 3. Dimension [Time] has hierarchies [Time2] and [Time by Week]
        //    (intentionally named hierarchy differently from dimension)
        return TestContext.instance().legacy().withSchema(
            "<Schema name=\"FoodMart\">\n"
            + "<Cube name=\"Warehouse and Sales\" defaultMeasure=\"Unit Sales\">\n"
            + "  <Table name=\"sales_fact_1997\" />\n"
            + "  <Dimension name=\"Store\" foreignKey=\"store_id\">\n"
            + "    <Hierarchy name=\"Stores\" hasAll=\"true\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\">\n"
            + "        <Property name=\"Store Type\" column=\"store_type\"/>\n"
            + "        <Property name=\"Store Sqft\" column=\"store_sqft\" type=\"Numeric\"/>\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Time\" type=\"TimeDimension\" foreignKey=\"time_id\">\n"
            + "    <Hierarchy hasAll=\"true\" name=\"Time By Week\" primaryKey=\"time_id\" >\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year2\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
            + "          levelType=\"TimeYears\"/>\n"
            + "      <Level name=\"Week\" column=\"week_of_year\" type=\"Numeric\" uniqueMembers=\"false\"\n"
            + "          levelType=\"TimeWeeks\"/>\n"
            + "      <Level name=\"Date2\" column=\"day_of_month\" uniqueMembers=\"false\" type=\"Numeric\"\n"
            + "          levelType=\"TimeDays\"/>\n"
            + "    </Hierarchy>\n"
            + "    <Hierarchy name=\"Time2\" hasAll=\"false\" primaryKey=\"time_id\">\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year2\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
            + "          levelType=\"TimeYears\"/>\n"
            + "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n"
            + "          levelType=\"TimeQuarters\"/>\n"
            + "      <Level name=\"Month\" column=\"month_of_year\" nameColumn=\"the_month\" uniqueMembers=\"false\" type=\"Numeric\"\n"
            + "          levelType=\"TimeMonths\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Product\" foreignKey=\"product_id\">\n"
            + "    <Hierarchy name=\"Products\" hasAll=\"true\" allMemberName=\"All Products\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
            + "      <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
            + "        <Table name=\"product\"/>\n"
            + "        <Table name=\"product_class\"/>\n"
            + "      </Join>\n"
            + "      <Level name=\"Product Family\" table=\"product_class\" column=\"product_family\"\n"
            + "          uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Product Department\" table=\"product_class\" column=\"product_department\"\n"
            + "          uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Product Category\" table=\"product_class\" column=\"product_category\"\n"
            + "          uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Product Subcategory\" table=\"product_class\" column=\"product_subcategory\"\n"
            + "          uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Product Name\" table=\"product\" nameColumn=\"product_name\" column=\"product_id\" \n"
            + "          uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "    <Hierarchy name=\"Product Name\" hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
            + "      <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
            + "        <Table name=\"product\"/>\n"
            + "        <Table name=\"product_class\"/>\n"
            + "      </Join>\n"
            + "      <Level name=\"Product Name\" table=\"product\" column=\"product_name\"\n"
            + "          uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Promotion\" foreignKey=\"promotion_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Promotions\" primaryKey=\"promotion_id\" defaultMember=\"[All Promotions]\">\n"
            + "      <Table name=\"promotion\"/>\n"
            + "      <Level name=\"Promotion Name\" column=\"promotion_name\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Currency\" foreignKey=\"promotion_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"promotion_id\">\n"
            + "      <Table name=\"promotion\"/>\n"
            + "      <Level name=\"Currency\" column=\"media_type\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
/*
            + "  <Dimension name=\"Customer2\" foreignKey=\"promotion_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"promotion_id\">\n"
            + "      <Table name=\"promotion\"/>\n"
            + "      <Level name=\"Customer\" column=\"media_type\" "
            + "uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
*/
            + "  <Dimension name=\"Customer\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKey=\"customer_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"Country\" column=\"country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"State Province\" column=\"state_province\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"City\" column=\"city\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Name\" column=\"customer_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
/*
            + "    <Hierarchy name=\"Gender\" hasAll=\"true\" "
            + "primaryKey=\"customer_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"Gender\" column=\"gender\" "
            + "uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
*/
            + "  </Dimension>\n"
            + "  <Dimension name='Store Size in SQFT' foreignKey='store_id'>\n"
            + "    <Hierarchy hasAll='true' primaryKey='store_id'>\n"
            + "      <Table name='store'/>\n"
            + "      <Level name='Store Sqft' column='store_sqft' type='Numeric' uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "</Cube>\n"
            + "</Schema>").withCube("Warehouse and Sales");
    }

    public void testUniqueName() {
        // TODO:
        // Unique mmbers:
        // [Time].[Time2].[Year2].[1997]
        // Non unique:
        // [Time].[Time2].[Quarter].&[Q1]&[1997]
        // All:
        // [Time].[Time2].[All]
        // Unique id:
        // [Currency].[Currency].&[1]
    }

    public void testDimensionDotHierarchyAmbiguous() {
        // If there is a dimension, hierarchy, level with the same name X,
        // then [X].[X] might reasonably resolve to hierarchy or the level.
        // SSAS resolves to hierarchy, old mondrian resolves to level.

        // SSAS gives error with the <Level>.Ordinal function:
        //   The ORDINAL function expects a level expression for
        //   the  argument. A hierarchy expression was used.
        getTestContext().assertExprThrows(
            "[Currency].[Currency].Ordinal",
            "No function matches signature '<Hierarchy>.Ordinal'");

        // SSAS succeeds with the '<Hierarchy>.Levels(<Numeric Expression>)'
        // function, returns 2
        getTestContext().assertExprReturns(
            "[Currency].[Currency].Levels(0).Name",
            "(All)");

        // There are 4 hierarchy members (including 'Any currency')
        getTestContext().assertExprReturns(
            "[Currency].[Currency].Members.Count",
            "15");

        // There are 3 level members
        getTestContext().assertExprReturns(
            "[Currency].[Currency].[Currency].Members.Count",
            "14");
    }

    public void testHierarchyLevelsFunction() {
        if (!IMPLEMENTED) {
            return;
        }

        // The <Hierarchy>.Levels function is not implemented in mondrian;
        // only <Hierarchy>.Levels(<Numeric Expression>)
        // and <Hierarchy>.Levels(<String Expression>)
        // SSAS returns 7.
        getTestContext().assertExprReturns(
            "[Product].[Products].Levels.Count",
            "7");
    }

    public void testDimensionDotHierarchyDotLevelDotMembers() {
        // [dimension].[hierarchy].[level] is valid on dimension with multiple
        // hierarchies;
        // SSAS2005 succeeds
        runQ(
            "select [Time].[Time by Week].[Week].MEMBERS on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testDimensionDotHierarchyDotLevel() {
        // [dimension].[hierarchy].[level] is valid on dimension with single
        // hierarchy
        // SSAS2005 succeeds
        assertQueryReturns(
            "select [Store].[Stores].[Store State].MEMBERS on 0\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[Canada].[BC]}\n"
            + "{[Store].[Stores].[Mexico].[DF]}\n"
            + "{[Store].[Stores].[Mexico].[Guerrero]}\n"
            + "{[Store].[Stores].[Mexico].[Jalisco]}\n"
            + "{[Store].[Stores].[Mexico].[Veracruz]}\n"
            + "{[Store].[Stores].[Mexico].[Yucatan]}\n"
            + "{[Store].[Stores].[Mexico].[Zacatecas]}\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 74,748\n"
            + "Row #0: 67,659\n"
            + "Row #0: 124,366\n");
    }

    public void testNamingDimensionDotLevel() {
        // [dimension].[level] is valid if level name is unique within all
        // hierarchies. (Note that [Week] is a level in hierarchy
        // [Time].[Time by Week]; here is no attribute [Time].[Week].)
        // SSAS2005 succeeds
        runQ(
            "select [Time].[Week].MEMBERS on 0\n"
            + "from [Warehouse and Sales]");

        // [dimension].[level] is valid if level name is unique within all
        // hierarchies. (Note that [Week] is a level in hierarchy
        // [Time].[Time by Week]; here is no attribute [Time].[Week].)
        // SSAS returns "[Time].[Time By Week].[Year2]".
        assertQueryReturns(
            "with member [Measures].[Foo] as ' [Time].[Year2].UniqueName '\n"
            + "select [Measures].[Foo] on 0\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: [Time].[Time By Week].[Year2]\n");
    }

    public void testNamingDimensionDotLevel2() {
        // Date2 is a level that occurs in only 1 hierarchy
        // There is no attribute called Date2
        runQ(
            "select [Time].[Date2].MEMBERS on 0 from [Warehouse and Sales]");

        // SSAS returns [Time].[Time By Week].[Date2]
        runQ(
            "with member [Measures].[Foo] as ' [Time].[Date2].UniqueName '\n"
            + "select [Measures].[Foo] on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testNamingDimensionDotLevelNotUnique() {
        if (!IMPLEMENTED) {
            return;
        }

        // Year2 is a level that occurs in only 2 hierarchies:
        // [Time].[Time2].[Year2] and [Time].[Time By Week].[Year2].
        // There is no attribute called Year2
        runQ(
            "select [Time].[Year2].MEMBERS on 0 from [Warehouse and Sales]");

        // SSAS2005 returns [Time].[Time By Week].[Year2]
        // (Presumably because it comes first.)
        runQ(
            "with member [Measures].[Foo] as ' [Time].[Year2].UniqueName '\n"
            + "select [Measures].[Foo] on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testDimensionDotMemberName() {
        // Note that 'Product' is a dimension with multiple hierarchies
        // but only one hierarchy has a member called 'All Products'.
        // Works on SSAS 2005.
        assertAxisReturns(
            "[Product].[All Products].Children",
            "[Product].[Products].[Drink]\n"
            + "[Product].[Products].[Food]\n"
            + "[Product].[Products].[Non-Consumable]");
    }

    public void testDimensionMembersOnSingleHierarchyDimension() {
        // [dimension].members for a dimension with one hierarchy
        // (and no attributes)
        // SSAS2005 succeeds
        assertQueryReturns(
            "select [Currency].Members on 0\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Currency].[Currency].[All Currencys]}\n"
            + "{[Currency].[Currency].[Bulk Mail]}\n"
            + "{[Currency].[Currency].[Cash Register Handout]}\n"
            + "{[Currency].[Currency].[Daily Paper]}\n"
            + "{[Currency].[Currency].[Daily Paper, Radio]}\n"
            + "{[Currency].[Currency].[Daily Paper, Radio, TV]}\n"
            + "{[Currency].[Currency].[In-Store Coupon]}\n"
            + "{[Currency].[Currency].[No Media]}\n"
            + "{[Currency].[Currency].[Product Attachment]}\n"
            + "{[Currency].[Currency].[Radio]}\n"
            + "{[Currency].[Currency].[Street Handout]}\n"
            + "{[Currency].[Currency].[Sunday Paper]}\n"
            + "{[Currency].[Currency].[Sunday Paper, Radio]}\n"
            + "{[Currency].[Currency].[Sunday Paper, Radio, TV]}\n"
            + "{[Currency].[Currency].[TV]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 4,320\n"
            + "Row #0: 6,697\n"
            + "Row #0: 7,738\n"
            + "Row #0: 6,891\n"
            + "Row #0: 9,513\n"
            + "Row #0: 3,798\n"
            + "Row #0: 195,448\n"
            + "Row #0: 7,544\n"
            + "Row #0: 2,454\n"
            + "Row #0: 5,753\n"
            + "Row #0: 4,339\n"
            + "Row #0: 5,945\n"
            + "Row #0: 2,726\n"
            + "Row #0: 3,607\n");
    }

    public void testMultipleHierarchyRequiresQualification() {
        // [dimension].members for a dimension with one hierarchy
        // (and some attributes)
        // SSAS2005 gives error:
        //   Query (1, 8) The 'Product' dimension contains more than
        //   one hierarchy, therefore the hierarchy must be explicitly
        //   specified.
        assertQueryThrows(
            "select [Product].Members on 0\n"
            + "from [Warehouse and Sales]",
            "The 'Product' dimension contains more than one hierarchy, "
            + "therefore the hierarchy must be explicitly specified.");
    }

    /**
     * Tests that it is an error to define a calc member in a dimension
     * with multiple hierarchies without specifying hierarchy.
     * Based on {@link mondrian.test.BasicQueryTest#testHalfYears()}.
     */
    public void testCalcMemberAmbiguousHierarchy() {
        String mdx =
            "WITH MEMBER [Measures].[ProfitPercent] AS\n"
            + "     '([Measures].[Store Sales]-[Measures].[Store Cost])/"
            + "([Measures].[Store Cost])',\n"
            + " FORMAT_STRING = '#.00%', SOLVE_ORDER = 1\n"
            + " MEMBER [Time].[First Half 97] AS  '[Time].[1997].[Q1] + "
            + "[Time].[1997].[Q2]'\n"
            + " MEMBER [Time].[Second Half 97] AS '[Time].[1997].[Q3] + "
            + "[Time].[1997].[Q4]'\n"
            + " SELECT {[Time].[First Half 97],\n"
            + "     [Time].[Second Half 97],\n"
            + "     [Time].[1997].CHILDREN} ON COLUMNS,\n"
            + " {[Store].[Store Country].[USA].CHILDREN} ON ROWS\n"
            + " FROM [Sales]\n"
            + " WHERE ([Measures].[ProfitPercent])";
        TestContext.instance().assertQueryThrows(
            mdx,
            "Hierarchy for calculated member '[Time].[First Half 97]' not found");
    }

    /**
     * Tests accessing members of a dimension with one hierarchy.
     *
     * <p>Relates to bug mondrian-960, because Measures only has one
     * hierarchy.</p>
     */
    public void testUnqualifiedHierarchy() {
        // [hierarchy].members for a dimension with one hierarchy
        // (and some attributes)

        // SSAS2005 fails of the dim has more than 1 hierarchy.
        // Note that 'Product' is the dimension, 'Products' is the hierarchy
        assertQueryThrows(
            "select [Product].Members on 0\n"
            + "from [Warehouse and Sales]",
            "The 'Product' dimension contains more than one hierarchy, therefore the hierarchy must be explicitly specified.");

        // Same but with Hierarchy.Members. Should work.
        runQ(
            "select {[Products].Members} on 0\n"
            + "from [Warehouse and Sales]");

        // Now try Dimension.Members on a single hierarchy dim.
        // Should work.
        runQ(
            "select {[Store].Members} on 0\n"
            + "from [Warehouse and Sales]");

        // TODO: run this in SSAS
        // [Measures] is both a dimension and a hierarchy;
        // [Products] is just a hierarchy.
        // SSAS returns 557863
        runQ(
            "select [Measures].[Unit Sales] on 0,\n"
            + "  [Products].[Food] on 1\n"
            + "from [Warehouse and Sales]");
    }

    /**
     * Tests that time functions such as Ytd behave correctly when there are
     * multiple time hierarchies.
     */
    public void testYtd() {
        // We use 'Generate' to establish context for Ytd without passing it
        // an explicit argument.
        // SSAS returns [Q1], [Q2], [Q3].
        assertQueryReturns(
            "select Generate(\n"
            + "  {[Time].[Time2].[1997].[Q3]},\n"
            + "  {Ytd()}) on 0,\n"
            + " [Products].Children on 1\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n");
    }

    public void testAxesOutOfOrder() {
        // TODO: run this in SSAS
        // Ssas2000 disallowed out-of-order axes. Don't know about Ssas2005.
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 1,\n"
            + "[Products].Children on 0\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 24,597\n"
            + "Row #0: 191,940\n"
            + "Row #0: 50,236\n");
    }

    public void testDimensionMembersRequiresHierarchyQualification() {
        // [dimension].members for a dimension with multiple hierarchies
        // SSAS2005 gives error:
        //    Query (1, 8) The 'Time' dimension contains more than one
        //    hierarchy, therefore the hierarchy must be explicitly
        //    specified.
        assertQueryThrows(
            "select [Time].Members on 0\n"
            + "from [Warehouse and Sales]",
            "The 'Time' dimension contains more than one hierarchy, therefore the hierarchy must be explicitly specified.");
    }

    public void testDimensionMemberRequiresHierarchyQualification() {
        // [dimension].CurrentMember
        // SSAS2005 gives error:
        //   Query (1, 8) The 'Product' dimension contains more than
        //   one hierarchy, therefore the hierarchy must be explicitly
        //   specified.
        final String[] exprs = {
            "[Product].CurrentMember",
            // TODO: Verify that this does indeed fail on SSAS
            "[Product].DefaultMember",
            // TODO: Verify that this does indeed fail on SSAS
            "[Product].AllMembers",
            "Dimensions(3).CurrentMember",
            "Dimensions(3).DefaultMember",
            "Dimensions(3).AllMembers",
        };
        final String expectedException =
            "The 'Product' dimension contains more than one hierarchy, "
            + "therefore the hierarchy must be explicitly specified.";
        assertQueryThrows(
            "select [Product].CurrentMember on 0\n"
            + "from [Warehouse and Sales]",
            expectedException);
        assertQueryThrows(
            "select [Product].DefaultMember on 0\n"
            + "from [Warehouse and Sales]",
            expectedException);
        assertQueryThrows(
            "select [Product].AllMembers on 0\n"
            + "from [Warehouse and Sales]",
            expectedException);

        // The following are OK because Dimensions(<n>) returns a hierarchy.
        final String expectedResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time2].[1997]}\n"
            + "Row #0: 266,773\n";
        assertQueryReturns(
            "select Dimensions(3).CurrentMember on 0\n"
            + "from [Warehouse and Sales]",
            expectedResult);
        assertQueryReturns(
            "select Dimensions(3).DefaultMember on 0\n"
            + "from [Warehouse and Sales]",
            expectedResult);
        assertQueryReturns(
            "select Head(Dimensions(7).AllMembers, 3) on 0\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Currency].[Currency].[All Currencys]}\n"
            + "{[Currency].[Currency].[Bulk Mail]}\n"
            + "{[Currency].[Currency].[Cash Register Handout]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 4,320\n"
            + "Row #0: 6,697\n");
    }

    public void testImplicitCurrentMemberRequiresHierarchyQualification() {
        // a function that causes an implicit call to CurrentMember
        // SSAS2005 gives error:
        //   Query (1, 8) The 'Product' dimension contains more than
        //   one hierarchy, therefore the hierarchy must be explicitly
        //   specified.
        assertQueryThrows(
            "select Ascendants([Product]) on 0\n"
            + "from [Warehouse and Sales]",
            "The 'Product' dimension contains more than one hierarchy, therefore the hierarchy must be explicitly specified.");
        // Works for [Store], which has only one hierarchy.
        // TODO: check SSAS
        assertQueryReturns(
            "select Ascendants([Store]) on 0\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[All Storess]}\n"
            + "Row #0: 266,773\n");
    }

    public void testUnqualifiedHierarchyCurrentMember() {
        // [hierarchy].CurrentMember
        // SSAS2005 succeeds
        runQ(
            "select [Products].CurrentMember on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testCannotDistinguishMdxFromSql() {
        // Cannot tell whether statement is MDX or SQL
        // SSAS2005 gives error:
        //   Parser: The statement dialect could not be resolved due
        //   to ambiguity.
        assertQueryThrows(
            "select [Time].Members\n"
            + "from [Warehouse and Sales]",
            "Syntax error at line 2, column 1, token 'from'");
    }

    public void testNamingDimensionAttr() {
        if (!ATTR_HIER_IMPL) {
            return;
        }
        // [dimension].[attribute] succeeds
        // (There is no level called [Store Manager])
        runQ(
            "select [Store].[Store Manager].Members on 0 from [Warehouse and Sales]");
    }

    public void testNamingDimensionAttrVsLevel() {
        if (!ATTR_HIER_IMPL) {
            return;
        }
        // [dimension].[attribute]
        // (There is a level called [Store City], but the attribute is chosen in
        // preference.)
        // SSAS2005 succeeds
        runQ(
            "select [Store].[Store City].Members on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testAttrHierarchyMemberParent() {
        if (!ATTR_HIER_IMPL) {
            return;
        }
        // parent of member of attribute hierarchy
        // SSAS2005 returns "[Store].[Store City].[All]"
        runQ(
            "with member [Measures].[Foo] as ' [Store].[Store City].[San Francisco].Parent.UniqueName '\n"
            + "select [Measures].[Foo] on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testAttrHierarchyMemberChildren() {
        if (!ATTR_HIER_IMPL) {
            return;
        }
        // children of member of attribute hierarchy
        // SSAS2005 returns empty set
        runQ(
            "select [Store].[Store City].[San Francisco].Children on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testAttrHierarchyAllMemberChildren() {
        if (!ATTR_HIER_IMPL) {
            return;
        }
        // children of all member of attribute hierarchy
        // SSAS2005 succeeds
        runQ(
            "select [Store].[Store City].Children on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testAttrHierarchyMemberLevel() {
        if (!ATTR_HIER_IMPL) {
            return;
        }
        // level of member of attribute hierarchy
        // SSAS2005 returns "[Store].[Store City].[Store City]"
        runQ(
            "with member [Measures].[Foo] as [Store].[Store City].[San Francisco].Level.UniqueName\n"
            + "select [Measures].[Foo] on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testAttrHierarchyUniqueName() {
        if (!ATTR_HIER_IMPL) {
            return;
        }
        // Returns [Store].[Store City]
        runQ(
            "with member [Measures].[Foo] as [Store].[Store City].UniqueName\n"
            + "select [Measures].[Foo] on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testMemberAddressedByLevelAndKey() {
        if (!MEMBER_NAMING_IMPL) {
            return;
        }
        // [dimension].[hierarchy].[level].&[key]
        // (Returns 31, 5368)
        runQ(
            "select {[Time].[Time By Week].[Week].[31]} on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testMemberAddressedByCompoundKey() {
        if (!MEMBER_NAMING_IMPL) {
            return;
        }
        // compound key
        // SSAS2005 returns 1 row
        runQ(
            "select [Time].[Time By Week].[Year2].[1998].&[30]&[1998] on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testMemberAddressedByPartialCompoundKey() {
        if (!MEMBER_NAMING_IMPL) {
            return;
        }
        // compound key, partially specified
        // SSAS2005 returns 0 rows but no error
        runQ(
            "select [Time].[Time By Week].[Year2].[1998].&[30] on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testMemberAddressedByNonUniqueName() {
        if (!MEMBER_NAMING_IMPL) {
            return;
        }
        // address member by non-unique name
        // [dimension].[hierarchy].[level].[name]
        // SSAS2005 returns first member that matches, 1997.January
        runQ(
            "select [Time].[Time2].[Month].[January] on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testMemberAddressedByLevelAndCompoundKey() {
        if (!MEMBER_NAMING_IMPL) {
            return;
        }
        // SSAS2005 returns [Time].[Time2].[Month].&[1]&[1997]
        runQ(
            "with member [Measures].[Foo] as ' [Time].[Time2].[Month].[January].UniqueName '\n"
            + "select [Measures].[Foo] on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testMemberAddressedByLevelAndName() {
        if (!MEMBER_NAMING_IMPL) {
            return;
        }
        // similarly
        // [dimension].[level].[member name]
        runQ(
            "with member [Measures].[Foo] as ' [Store].[Store City].[Month].[January].UniqueName '\n"
            + "select [Measures].[Foo] on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testFoo31() {
        // [dimension].[member name]
        // returns [Product].[Products].[Product Department].[Dairy]
        // note that there are members
        //   [Product].[Drink].[Dairy]
        //   [Product].[Drink].[Dairy].[Dairy]
        //   [Product].[Food].[Dairy]
        //   [Product].[Food].[Dairy].[Dairy]
        runQ(
            "select Measures on 0,[Product].[Product Department].Members on 1\n"
            + "from [Warehouse and Sales]");
    }

    public void testFoo32() {
        if (!IMPLEMENTED) {
            return;
        }
        // returns [Product].[Products].[Product Department].[Dairy]
        // In my opinion this is weird unique name, because there is a
        // Food.Dairy and a Drink.Dairy. But behavior is consistent with
        // returning first member that matches.
        runQ(
            "with member [Measures].[U] as ' [Product].UniqueName '\n"
            + "    member [Measures].[PU] as ' [Product].Parent.UniqueName '\n"
            + "select {[Measures].[U], [Measures].[PU]} on 0,\n"
            + "  [Product].[Dairy] on 1\n"
            + "from [Warehouse and Sales]");
    }

    public void testNamingAttrVsLevel() {
        if (!ATTR_HIER_IMPL) {
            return;
        }
        // [attribute] vs. [level]
        // SSAS2005 succeeds
        runQ(
            "select [Store City].Members on 0\n"
            + "from [Warehouse and Sales]");

        // the attribute hierarchy wins over the level
        // SSAS2005 returns [Store].[Store City]
        assertQueryReturns(
            "with member [Measures].[Foo] as [Store City].UniqueName\n"
            + "select [Measures].[Foo] on 0\n"
            + "from [Warehouse and Sales]", "xxxxx");
    }

    public void testUnqualifiedLevel() {
        if (!IMPLEMENTED) {
            return;
        }
        // [level]
        // SSAS2005 succeeds
        runQ(
            "select [Week].Members on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testDimensionAsScalarExpression() {
        if (!IMPLEMENTED) {
            return;
        }
        // Dimension used as scalar expression fails.
        // SSAS2005 gives error:
        //   The function expects a string or numeric expression for
        //    the argument.  A level expression was used.
        runQ(
            "with member [Measures].[Foo] as [Date2]\n"
            + "select [Measures].[Foo] on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testDimensionWithMultipleHierarchiesDotParent() {
        // [Dimension].Parent
        // SSAS2005 returns error:
        //   The 'Product' dimension contains more than one hierarchy,
        //   therefore the hierarchy must be explicitly specified.
        assertExprThrows(
            "[Time].Parent.UniqueName",
            "The 'Time' dimension contains more than one hierarchy, "
            + "therefore the hierarchy must be explicitly specified.");
    }

    public void testDimensionDotHierarchyInBrackets() {
        // [dimension.hierarchy] is valid
        // SSAS2005 succeeds
        runQ(
            "select {[Time.Time By Week].Members} on 0\n"
            + "from [Warehouse and Sales]");
    }

    /**
     * Test case for bug 2688790, "Hierarchy Naming Compatibility issue".
     * Occurs when dimension and hierarchy have the same name and are used with
     * [name.name].
     */
    public void testDimensionDotHierarchySameNameInBrackets() {
        TestContext testContext =
            TestContext.instance().legacy().createSubstitutingCube(
                "Sales",
                "<Dimension name=\"Store Type 2\" foreignKey=\"store_id\">"
                + " <Hierarchy name=\"Store Type 2\" hasAll=\"true\" primaryKey=\"store_id\">"
                + " <Table name=\"store\"/>"
                + " <Level name=\"Store Type\" column=\"store_type\" uniqueMembers=\"true\"/>"
                + " </Hierarchy>"
                + "</Dimension>",
                null);
        testContext.assertQueryReturns(
            "select [Store Type 2.Store Type 2].[Store Type].members ON columns "
            + "from [Sales] where [Time].[1997]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Store Type 2].[Store Type 2].[Deluxe Supermarket]}\n"
            + "{[Store Type 2].[Store Type 2].[Gourmet Supermarket]}\n"
            + "{[Store Type 2].[Store Type 2].[HeadQuarters]}\n"
            + "{[Store Type 2].[Store Type 2].[Mid-Size Grocery]}\n"
            + "{[Store Type 2].[Store Type 2].[Small Grocery]}\n"
            + "{[Store Type 2].[Store Type 2].[Supermarket]}\n"
            + "Row #0: 76,837\n"
            + "Row #0: 21,333\n"
            + "Row #0: \n"
            + "Row #0: 11,491\n"
            + "Row #0: 6,557\n"
            + "Row #0: 150,555\n");
    }

    public void testDimensionDotLevelDotHierarchyInBrackets() {
        // [dimension.hierarchy.level]
        // SSAS2005 gives error:
        //   Query (1, 8) The dimension '[Time.Time2.Quarter]' was not
        //   found in the cube when the string, [Time.Time2.Quarter],
        //   was parsed.
        assertQueryThrows(
            "select [Time.Time2.Quarter].Members on 0\n"
            + "from [Warehouse and Sales]",
            "MDX object '[Time.Time2.Quarter]' not found in cube 'Warehouse and Sales'");
    }

    public void testDimensionDotInvalidHierarchyInBrackets() {
        // invalid hierarchy name
        // SSAS2005 gives error:
        //  Query (1, 9) The dimension '[Time.Time By Week55]' was not
        //  found in the cube when the string, [Time.Time By Week55],
        //  was parsed.
        assertQueryThrows(
            "select {[Time.Time By Week55].Members} on 0\n"
            + "from [Warehouse and Sales]",
            "MDX object '[Time.Time By Week55]' not found in cube 'Warehouse and Sales'");
    }

    public void testDimensionDotDimensionInBrackets() {
        // [dimension.dimension] is invalid.  SSAS2005 gives similar
        // error to above.  (The Time dimension has hierarchies called
        // [Time2] and [Time By Day]. but no hierarchy [Time].)
        assertQueryThrows(
            "select {[Time.Time].Members} on 0\n"
            + "from [Warehouse and Sales]",
            "MDX object '[Time.Time]' not found in cube 'Warehouse and Sales'");
    }

    public void testDimensionDotHierarchyDotNonExistentLevel() {
        if (!IMPLEMENTED) {
            return;
        }
        // Non-existent level of hierarchy.
        // SSAS2005 gives error:
        //  Query (1, 8) The MEMBERS function expects a hierarchy
        //  expression for the argument. A member expression was used.
        //
        // Mondrian currently gives
        //  MDX object '[Time].[Time By Week].[Month]' not found in
        //  cube 'Warehouse and Sales'
        // which is not good enough.
        runQ(
            "select [Time].[Time By Week].[Month].Members on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testDimensionDotHierarchyDotLevelMembers() {
        if (!IMPLEMENTED) {
            return;
        }
        // SSAS2005 returns 8 quarters.
        runQ(
            "select [Time].[Time2].[Quarter].Members on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testDupHierarchyOnAxes() {
        // same hierarchy on both axes
        // SSAS2005 gives error:
        //   The Products hierarchy already appears in the Axis0 axis.
        // SSAS query:
        //   select [Products] on 0,
        //     [Products] on 1
        //   from [Warehouse and Sales]
        assertQueryThrows(
            "select {[Products]} on 0,\n"
            + "  {[Products]} on 1\n"
            + "from [Warehouse and Sales]",
            "Hierarchy '[Product].[Products]' appears in more than one independent axis.");
    }

    public void testDimensionOnAxis() {
        if (!IMPLEMENTED) {
            return;
        }
        // Dimension is implicitly converted to member
        // so is OK on axis.
        runQ("select [Product] on 0 from [Warehouse and Sales]");
    }

    public void testDimensionDotHierarchyOnAxis() {
        // Dimension is implicitly converted to member
        // so is OK on axis.
        runQ(
            "select [Product].[Products] on 0,\n"
            + "[Customer].[Customer] on 1\n"
            + "from [Warehouse and Sales]");
    }

    public void testHierarchiesFromSameDimensionOnAxes() {
        if (!IMPLEMENTED) {
            return;
        }
        // different hierarchies from same dimension
        // SSAS2005 succeeds
        runQ(
            "select [Time].[Time2] on 0,\n"
            + "  [Time].[Time By Week] on 1\n"
            + "from [Warehouse and Sales]");
    }

    // TODO:
    public void testDifferentHierarchiesFromSameDimensionOnAxes() {
        if (!IMPLEMENTED) {
            return;
        }
        // different hierarchies from same dimension
        // SSAS2005 succeeds
        // Note that [Time].[1997] resolves to [Time].[Time2].[1997]
        runQ(
            "select [Time].[Time2] on 0,\n"
            + "  [Time].[Time By Week] on 1\n"
            + "from [Warehouse and Sales]\n"
            + "where [Time].[1997]");
    }

    // TODO:
    public void testDifferentHierarchiesFromSameDimensionInCrossjoin() {
        if (!IMPLEMENTED) {
            return;
        }
        // crossjoin different hierarchies from same dimension
        // SSAS2005 succeeds
        runQ(
            "select Crossjoin([Time].[Time By Week].Children, [Time].[Time2].Members) on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testHierarchyUsedTwiceInCrossjoin() {
        if (!IMPLEMENTED) {
            return;
        }
        // SSAS2005 gives error:
        //   Query (2, 4) The Time By Week hierarchy is used more than
        //   once in the Crossjoin function.
        runQ(
            "select \n"
            + "   [Time].[Time By Week].Children\n"
            + "     * [Time].[Time2].Children\n"
            + "     * [Time].[Time By Week].Children on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testAttributeHierarchyUsedTwiceInCrossjoin() {
        if (!ATTR_HIER_IMPL) {
            return;
        }
        // Attribute hierarchy used more than once in Crossjoin.
        // SSAS2005 gives error:
        //   Query (2, 4) The SKU hierarchy is used more than once in
        //   the Crossjoin function.
        runQ(
            "select \n"
            + "   [Product].[SKU].Children\n"
            + "     * [Product].[Products].Members\n"
            + "     * [Time].[Time By Week].Children\n"
            + "     * [Product].[SKU].Members on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testFoo50() {
        if (!ATTR_HIER_IMPL) {
            return;
        }
        // Mixing attributes in a set
        // SSAS2005 gives error:
        //    Members belong to different hierarchies in the  function.
        runQ(
            "select {[Store].[Store Country].[USA], [Store].[Stores].[Store Country].[USA]} on 0\n"
            + "from [Warehouse and Sales]");
    }

    public void testQuoteInStringInQuotedFormula() {
        // Quoted formulas vs. unquoted formulas
        // Single quote in string
        // SSAS2005 returns 5
        assertQueryReturns(
            "with member [Measures].[Foo] as ' len(\"can''t\") '\n"
            + "select [Measures].[Foo] on 0\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: 5\n");
    }

    public void testQuoteInStringInUnquotedFormula() {
        // SSAS2005 returns 6
        assertQueryReturns(
            "with member [Measures].[Foo] as len(\"can''t\")\n"
            + "select [Measures].[Foo] on 0\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: 6\n");
    }

    public void testMemberIdentifiedByHierarchyAndKey() {
        // Member identified by hierarchy (unique across all dimensions), key;
        // works on SSAS;
        // gives {[Washington Berry Juice], 231}.
        // Presumably, if level is not specified, it defaults to lowest level?
        runQ(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Products].&[1] on 1\n"
            + "from [Warehouse and Sales]");
    }

    public void testDimensionHierarchyKey() {
        // member identified by dimension, hierarchy, key
        // works on SSAS
        // gives {[Washington Berry Juice], 231}
        runQ(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Product].[Products].&[1] on 1\n"
            + "from [Warehouse and Sales]");
    }

    public void testCompoundKey() {
        // compound key
        // succeeds on SSAS
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Time].[Time2].[Month].&[12]&Q4&[1997] on 1\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time2].[1997].[Q4].[December]}\n"
            + "Row #0: 26,796\n");
    }

    public void testCompoundKeySyntaxError() {
        // without [] fails on SSAS (syntax error because a number)
        assertQueryThrows(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Product].[Products].[Brand Name].&43&[Walrus] on 1\n"
            + "from [Warehouse and Sales]",
            "mondrian.parser.TokenMgrError: Lexical error at line 2, column 36.  Encountered: \"4\" (52), after : \"&\"");
    }

    public void testCompoundKeyStringBad() {
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case MYSQL:
            break;
        default:
            // Columns in error string are quoted differently on other
            // databases.
            return;
        }
        // too few values in key
        assertQueryThrows(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Product].[Products].[Brand Name].&[43]&Walrus&Foo on 1\n"
            + "from [Warehouse and Sales]",
            "Wrong number of values in member key; &[43]&Walrus&Foo has 3 "
            + "values, whereas level's key has 5 columns ["
            + "`product`.`brand_name`, "
            + "`product_class`.`product_subcategory`, "
            + "`product_class`.`product_category`, "
            + "`product_class`.`product_department`, "
            + "`product_class`.`product_family`].");

        // too few values in key
        assertQueryThrows(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Time].[Time2].[Quarter].&Q3 on 1\n"
            + "from [Warehouse and Sales]",
            "Wrong number of values in member key; &Q3 has 1 values, whereas "
            + "level's key has 2 columns ["
            + "`time_by_day`.`quarter`, "
            + "`time_by_day`.`the_year`].");

        // too many values in key
        assertQueryThrows(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Time].[Time2].[Quarter].&Q3&[1997]&ABC on 1\n"
            + "from [Warehouse and Sales]",
            "Wrong number of values in member key; &Q3&[1997]&ABC has 3 "
            + "values, whereas level's key has 2 columns ["
            + "`time_by_day`.`quarter`, "
            + "`time_by_day`.`the_year`].");
    }

    public void testCompoundKeyString() {
        // succeeds on SSAS (gives 1 row)
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Store].[Stores].[Store City].&[San Francisco]&CA on 1\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco]}\n"
            + "Row #0: 2,117\n");
    }

    /**
     * Tests a member where a name segments {@code [San Francisco].[Store 14]}
     * occur after a key segment {@code &amp;&amp;CA}.
     *
     * <p>Needs to work regardless of the value of
     * {@code MondrianProperties#SsasCompatibleNaming}. Mondrian-3 had this
     * functionality.</p>
     */
    public void testNameAfterKey() {
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + TestContext.hierarchyName("Store", "Stores")
            + ".[Store State].&CA.[San Francisco].[Store 14] on 1\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{"
            + TestContext.hierarchyName("Store", "Stores")
            + ".[USA].[CA].[San Francisco].[Store 14]}\n"
            + "Row #0: 2,117\n");
    }

    /**
     * Tests a member where a name segment {@code [Store 14]} occurs after a
     * composite key segment {@code &amp;[San Francisco]&amp;CA}.
     */
    public void testNameAfterCompositeKey() {
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Store].[Stores].[Store City].&[San Francisco]&CA.[Store 14] on 1\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "Row #0: 2,117\n");
    }

    public void testCompoundKeyAll() {
        getTestContext()
            .assertExprReturns(
                "[Customer].Level.Name",
                "(All)");
        assertQueryThrows(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Customer].[(All)].&All on 1\n"
            + "from [Warehouse and Sales]",
            "Wrong number of values in member key; &All has 1 values, whereas level's key has 0 columns [].");
    }

    public void testCompoundKeyParent() {
        getTestContext()
            .assertAxisReturns(
                "[Store].[Stores].[Store City].&[San Francisco]&CA.Parent",
                "[Store].[Stores].[USA].[CA]");
    }

    public void testCompoundKeyNull() {
        // Note: [Store Size in SQFT].[#null] is the member whose name is null;
        //   [Store Size in SQFT].&[#null] is the member whose key is null.
        // REVIEW: Does SSAS use the same syntax, '&[#null]', for null key?
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Store Size in SQFT].[Store Size in SQFT].&[#null] on 1\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store Size in SQFT].[Store Size in SQFT].[#null]}\n"
            + "Row #0: 39,329\n");
    }

    public void testFoo56() {
        if (!IMPLEMENTED) {
            return;
        }
        // succeeds on SSAS (gives 1 row)
        runQ(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Product].[Products].[Brand Name].[Walrus] on 1\n"
            + "from [Warehouse and Sales]");
    }

    public void testKeyNonExistent() {
        // SSAS gives 1 row
        runQ(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Time].[Time2].[Quarter].&Q3&[1997] on 1\n"
            + "from [Warehouse and Sales]");

        propSaver.set(propSaver.props.IgnoreInvalidMembersDuringQuery, true);
        // SSAS gives 0 rows
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Time].[Time2].[Quarter].&Q5&[1997] on 1\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n");

        propSaver.set(propSaver.props.IgnoreInvalidMembersDuringQuery, false);
        assertQueryThrows(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Time].[Time2].[Quarter].&Q5&[1997] on 1\n"
            + "from [Warehouse and Sales]",
            "MDX object '[Time].[Time2].[Quarter].&Q5&[1997]' not found in cube 'Warehouse and Sales'");
    }

    public void testAxesLabelsOutOfSequence() {
        // succeeds on SSAS
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 1,\n"
            + "[Product].[Products] on 0\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[All Products]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n");
    }

    public void testAxisLabelsNotContiguousFails() {
        // SSAS gives error:
        //   Query (1, 8) Axis numbers specified in a query must be sequentially
        //   specified, and cannot contain gaps.
        assertQueryThrows(
            "select [Measures].[Unit Sales] on 1,\n"
            + "[Product].[Products].Children on 2\n"
            + "from [Warehouse and Sales]",
            "Axis numbers specified in a query must be sequentially "
            + "specified, and cannot contain gaps. Axis 0 (COLUMNS) is missing.");
    }

    public void testLotsOfAxes() {
        // lots of axes, mixed ways of specifying axes
        // SSAS succeeds, although Studio says:
        //   Results cannot be displayed for cellsets with more than two axes.
        runQ(
            "select [Measures].[Unit Sales] on axis(0),\n"
            + "[Product].[Products] on rows,\n"
            + "[Customer].[Customer] on pages,\n"
            + "[Currency] on 3,\n"
            + "[Promotion] on axis(4),\n"
            + "[Time].[Time2] on 5,\n"
            + "[Time].[Time by Week] on 6\n"
            + "from [Warehouse and Sales]");
    }

    /** Tests Mosha's recommended way to query members; see
     * <a href="http://sqlblog.com/blogs/mosha/archive/2006/10/11/querying-dimensions-in-mdx.aspx">Querying dimensions in MDX</a>.
     */
    public void testEmptyAxis() {
        assertQueryReturns(
            "SELECT {} ON 0,Customer.Customer.Country.Members ON 1\n"
            + "FROM [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "Axis #2:\n"
            + "{[Customer].[Customer].[Canada]}\n"
            + "{[Customer].[Customer].[Mexico]}\n"
            + "{[Customer].[Customer].[USA]}\n");
        // reverse axes
        assertQueryReturns(
            "SELECT Customer.Customer.Country.Members ON 0,{} ON 1\n"
            + "FROM [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customer].[Canada]}\n"
            + "{[Customer].[Customer].[Mexico]}\n"
            + "{[Customer].[Customer].[USA]}\n"
            + "Axis #2:\n");
        // multiple empty axes
        assertQueryReturns(
            "SELECT Customer.Customer.Country.Members ON 0,{} ON 1,{} on 2\n"
            + "FROM [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customer].[Canada]}\n"
            + "{[Customer].[Customer].[Mexico]}\n"
            + "{[Customer].[Customer].[USA]}\n"
            + "Axis #2:\n"
            + "Axis #3:\n");
    }

    public void testOnAxesFails() {
        // axes(n) is not an acceptable alternative to axis(n)
        // SSAS gives:
        //   Query (1, 35) Parser: The syntax for 'axes' is incorrect.
        assertQueryThrows(
            "select [Measures].[Unit Sales] on axes(0)\n"
            + "from [Warehouse and Sales]",
            "Syntax error at line 1, column 35, token 'axes'");
    }

    public void testOnExpression() {
        // SSAS gives syntax error
        assertQueryThrows(
            "select [Measures].[Unit Sales] on 0 + 1\n"
            + "from [Warehouse and Sales]",
            "Syntax error at line 1, column 37, token '+'");
    }

    public void testOnFractionFails() {
        // SSAS gives syntax error
        assertQueryThrows(
            "select [Measures].[Unit Sales] on 0.4\n"
            + "from [Warehouse and Sales]",
            "Invalid axis specification. The axis number must be a non-negative"
            + " integer, but it was 0.4.");
    }

    public void testAxisFunction() {
        // AXIS(n) function as expression
        // SSAS succeeds
        if (!AXIS_IMPL) {
            return;
        }
        runQ(
            "WITH MEMBER MEASURES.AXISDEMO AS\n"
            + "  SUM(AXIS(1), [Measures].[Unit Sales])\n"
            + "SELECT {[Measures].[Unit Sales],MEASURES.AXISDEMO} ON 0,\n"
            + "{[Time].[Time by Week].Children} ON 1\n"
            + "FROM [Warehouse and Sales]");
    }

    public void testAxisAppliedToExpr() {
        // Axis applied to an expression ('3 - 2' in place of '1' above).
        // SSAS succeeds.
        // When we implement Axis, it may be acceptable for Mondrian to fail in
        // this case - or perhaps struggle on with less type information.
        if (!AXIS_IMPL) {
            return;
        }
        assertQueryReturns(
            "WITH MEMBER MEASURES.AXISDEMO AS\n"
            + "  SUM(AXIS(1), [Measures].[Unit Sales])\n"
            + "SELECT {[Measures].[Unit Sales],MEASURES.AXISDEMO} ON 0,\n"
            + "{[Time].[Time by Week].Children} ON 1\n"
            + "FROM [Warehouse and Sales]",
            "xxx");
    }

    public void testAxisFunctionReferencesPreviousAxis() {
        // reference axis 0 while computing axis 1
        // SSAS succeeds
        if (!AXIS_IMPL) {
            return;
        }
        assertQueryReturns(
            "WITH MEMBER MEASURES.AXISDEMO AS\n"
            + "  SUM(AXIS(0), [Measures].CurrentMember)\n"
            + "SELECT {[Measures].[Store Sales],MEASURES.AXISDEMO} ON 0,\n"
            + "{Filter([Time].[Time by Week].Members, Measures.AxisDemo > 0)} ON 1\n"
            + "FROM [Warehouse and Sales]",
            "xxx");
    }

    public void testAxisFunctionReferencesSameAxisFails() {
        // reference axis 1 while computing axis 1, not ok
        // SSAS gives:
        //   Infinite recursion detected. The loop of dependencies is: AXISDEMO
        //   -> AXISDEMO.
        if (!AXIS_IMPL) {
            return;
        }
        assertQueryThrows(
            "WITH MEMBER MEASURES.AXISDEMO AS\n"
            + "  SUM(AXIS(1), [Measures].CurrentMember)\n"
            + "SELECT {[Measures].[Store Sales],MEASURES.AXISDEMO} ON 0,\n"
            + "{Filter([Time].[Time by Week].Members, Measures.AxisDemo > 0)} ON 1\n"
            + "FROM [Warehouse and Sales]",
            "xxx");
    }

    public void testAxisFunctionReferencesSameAxisZeroFails() {
        // reference axis 0 while computing axis 0, not ok
        // SSAS gives:
        //   Infinite recursion detected. The loop of dependencies is: AXISDEMO
        //   -> AXISDEMO.
        if (!AXIS_IMPL) {
            return;
        }
        assertQueryThrows(
            "WITH MEMBER MEASURES.AXISDEMO AS\n"
            + "  SUM(AXIS(0), [Measures].CurrentMember)\n"
            + "SELECT {[Measures].[Store Sales],MEASURES.AXISDEMO} ON 1,\n"
            + "{Filter([Time].[Time by Week].Members, Measures.AxisDemo > 0)} ON 0\n"
            + "FROM [Warehouse and Sales]",
            "xxx");
    }

    public void testAxisFunctionReferencesLaterAxis() {
        // reference axis 1 while computing axis 0, ok
        // The SSAS online doc says:
        //    An axis can reference only a prior axis. For example, Axis(0) must
        //    occur after the COLUMNS axis has been evaluated, such as on a ROW
        //    or PAGE axis.
        // but nevertheless SSAS does the right thing and allows this query.
        if (!AXIS_IMPL) {
            return;
        }
        assertQueryReturns(
            "WITH MEMBER MEASURES.AXISDEMO AS\n"
            + "  SUM(AXIS(1), [Measures].CurrentMember)\n"
            + "SELECT {[Measures].[Store Sales],MEASURES.AXISDEMO} ON 1,\n"
            + "{Filter([Time].[Time by Week].Members, Measures.AxisDemo > 0)} ON 0\n"
            + "FROM [Warehouse and Sales]",
            "xxx");
    }

    public void testAxisFunctionReferencesSameAxisInlineFails() {
        // If we inline the member, SSAS runs out of memory.
        // SSAS gives error:
        //   Memory error: Allocation failure : The paging file is too small for
        //   this operation to complete. .
        // (Should give cyclicity error.)
        if (!AXIS_IMPL) {
            return;
        }
        assertQueryThrows(
            "SELECT [Measures].[Store Sales] ON 1,\n"
            + "{Filter([Time].[Time by Week].Members, SUM(AXIS(0), [Measures].CurrentMember) > 0)} ON 0\n"
            + "FROM [Warehouse and Sales]",
            "xxx cyclic something");
    }

    public void testCrossjoinMember() {
        // Mondrian currently gives error:
        //   No function matches signature 'crossjoin(<Member>, <Set>)'
        if (!IMPLEMENTED) {
            return;
        }
        // Apply crossjoin(Member,Set)
        // SSAS gives 626866, 626866, 626866.
        assertQueryReturns(
            "select crossjoin([Products].DefaultMember, [Gender].Members) on 0\n"
            + "from [Warehouse and Sales]",
            "xx");
    }

    /**
     * Tests the ambiguity between a level and a member of the same name,
     * both in SSAS compatible mode and in regular mode.
     * @throws SQLException If the test fails.
     */
    public void testCanHaveMemberWithSameNameAsLevel() throws SQLException {
        TestContext testContext =
            TestContext.instance().legacy().createSubstitutingCube(
                "Sales",
                 "<Dimension name=\"SameName\" foreignKey=\"customer_id\">\n"
                 + " <Hierarchy hasAll=\"true\" primaryKey=\"id\">\n"
                 + " <InlineTable alias=\"sn\">\n"
                 + " <ColumnDefs>\n"
                 + " <ColumnDef name=\"id\" type=\"Numeric\" />\n"
                 + " <ColumnDef name=\"desc\" type=\"String\" />\n"
                 + " </ColumnDefs>\n"
                 + " <Rows>\n"
                 + " <Row>\n"
                 + " <Value column=\"id\">1</Value>\n"
                 + " <Value column=\"desc\">SameName</Value>\n"
                 + " </Row>\n"
                 + " </Rows>\n"
                 + " </InlineTable>\n"
                 + " <Level name=\"SameName\" column=\"desc\" uniqueMembers=\"true\" />\n"
                 + " </Hierarchy>\n"
                 + "</Dimension>");

        org.olap4j.metadata.Member member = testContext.getOlap4jConnection()
            .getOlapSchema().getCubes().get("Sales").getDimensions()
            .get("SameName").getHierarchies().get("SameName").getLevels()
            .get("SameName").getMembers().get(0);
        assertEquals(
            "[SameName].[SameName].[SameName].[SameName]",
            member.getUniqueName());

        testContext.assertQueryThrows(
            "select {"
            + "[SameName].[SameName].[SameName]"
            + "} on 0 from Sales",
            "Mondrian Error:No function matches signature '{<Level>}'");

        testContext.assertQueryReturns(
            "select {[SameName].[SameName].[SameName].[SameName]} on 0 from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[SameName].[SameName].[SameName].[SameName]}\n"
            + "Row #0: \n");
    }

    public void testMemberNameSortCaseSensitivity()
    {
        // In SSAS, "MacDougal" occurs between "Maccietto" and "Macha". This
        // would not occur if sort was case-sensitive.
        final TestContext testContext =
            TestContext.instance().legacy().createSubstitutingCube(
                "Sales",
                "  <Dimension name=\"Customer Last Name\" "
                + "foreignKey=\"customer_id\">\n"
                + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Customers\""
                + " primaryKey=\"customer_id\">\n"
                + "      <Table name=\"customer\"/>\n"
                + "      <Level name=\"Last Name\" column=\"lname\" keyColumn=\"customer_id\" uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n");
        testContext.assertAxisReturns(
            "head(\n"
            + "  filter(\n"
            + "    [Customer Last Name].[Last Name].Members,"
            + "    Left([Customer Last Name].[Last Name].CurrentMember.Name, "
            + "1) = \"M\"),\n"
            + "  10)",
            "[Customer Last Name].[Customer Last Name].[Mabe]\n"
            + "[Customer Last Name].[Customer Last Name].[Macaluso]\n"
            + "[Customer Last Name].[Customer Last Name].[MacBride]\n"
            + "[Customer Last Name].[Customer Last Name].[Maccietto]\n"
            + "[Customer Last Name].[Customer Last Name].[MacDougal]\n"
            + "[Customer Last Name].[Customer Last Name].[Macha]\n"
            + "[Customer Last Name].[Customer Last Name].[Macias]\n"
            + "[Customer Last Name].[Customer Last Name].[Mack]\n"
            + "[Customer Last Name].[Customer Last Name].[Mackin]\n"
            + "[Customer Last Name].[Customer Last Name].[Maddalena]");

        testContext.assertAxisReturns(
            "order(\n"
            + "  head(\n"
            + "    filter(\n"
            + "      [Customer Last Name].[Last Name].Members,"
            + "      Left([Customer Last Name].[Last Name].CurrentMember.Name, 1) = \"M\"),\n"
            + "  10),\n"
            + " [Customer Last Name].[Last Name].CurrentMember.Name)",
            "[Customer Last Name].[Customer Last Name].[Mabe]\n"
            + "[Customer Last Name].[Customer Last Name].[Macaluso]\n"
            + "[Customer Last Name].[Customer Last Name].[MacBride]\n"
            + "[Customer Last Name].[Customer Last Name].[Maccietto]\n"
            + "[Customer Last Name].[Customer Last Name].[MacDougal]\n"
            + "[Customer Last Name].[Customer Last Name].[Macha]\n"
            + "[Customer Last Name].[Customer Last Name].[Macias]\n"
            + "[Customer Last Name].[Customer Last Name].[Mack]\n"
            + "[Customer Last Name].[Customer Last Name].[Mackin]\n"
            + "[Customer Last Name].[Customer Last Name].[Maddalena]");
    }

    /**
     * SSAS can resolve root members of a hierarchy even if not qualified
     * by hierarchy, and even if the dimension has more than one hierarchy.
     */
    public void testRootMembers() {
        // for member defined in the database
        final String timeByWeek =
            TestContext.hierarchyName("Time", "Time By Week");
        assertExprReturns(
            "[Time].[1997].Level.UniqueName",
            timeByWeek + ".[Year2]");

        // now for a calc member defined in a query
        assertQueryReturns(
            "with member [Time].[Time2].[Foo] as\n"
            + "[Time].[Time2].[1997] + [Time].[Time2].[1997].[Q3]\n"
            + "select [Time].[Foo] on 0\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time2].[Foo]}\n"
            + "Row #0: 332,621\n");
    }

    public void testUnqualifiedMeasures() {
        assertQueryReturns(
            "select from [Warehouse and Sales] where [Unit Sales]",
            "Axis #0:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "266,773");

        // similarly for calc member defined in query
        assertQueryReturns(
            "with member [Measures].[Foo] as 1\n"
            + "select from [Warehouse and Sales] where [Foo]",
            "Axis #0:\n"
            + "{[Measures].[Foo]}\n"
            + "1");
    }
}

// End Ssas2005CompatibilityTest.java
