/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2009-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.util.Bug;

/**
 * Tests the expressions used for calculated members. Please keep in sync
 * with the actual code used by the wizard.
 *
 * @author jhyde
 * @since 15 May, 2009
 */
public class CompoundSlicerTest extends FoodMartTestCase {
    /**
     * Creates a CompoundSlicerTest.
     */
    public CompoundSlicerTest() {
        super();
    }

    /**
     * Creates a CompoundSlicerTest with a given name.
     *
     * @param name Test name
     */
    public CompoundSlicerTest(String name) {
        super(name);
    }

    /**
     * Query that simulates a compound slicer by creating a calculated member
     * that aggregates over a set and places it in the WHERE clause.
     */
    public void testSimulatedCompoundSlicer() {
        assertQueryReturns(
            "with\n"
            + "  member [Measures].[Price per Unit] as\n"
            + "    [Measures].[Store Sales] / [Measures].[Unit Sales]\n"
            + "  set [Top Products] as\n"
            + "    TopCount(\n"
            + "      [Product].[Brand Name].Members,\n"
            + "      3,\n"
            + "      ([Measures].[Unit Sales], [Time].[1997].[Q3]))\n"
            + "  member [Product].[Top] as\n"
            + "    Aggregate([Top Products])\n"
            + "select {\n"
            + "  [Measures].[Unit Sales],\n"
            + "  [Measures].[Price per Unit]} on 0,\n"
            + " [Gender].Children * [Marital Status].Children on 1\n"
            + "from [Sales]\n"
            + "where ([Product].[Top], [Time].[1997].[Q3])",
            "Axis #0:\n"
            + "{[Product].[Products].[Top], [Time].[Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Price per Unit]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S]}\n"
            + "Row #0: 779\n"
            + "Row #0: 2.40\n"
            + "Row #1: 811\n"
            + "Row #1: 2.24\n"
            + "Row #2: 829\n"
            + "Row #2: 2.23\n"
            + "Row #3: 886\n"
            + "Row #3: 2.25\n");

        // Now the equivalent query, using a set in the slicer.
        assertQueryReturns(
            "with\n"
            + "  member [Measures].[Price per Unit] as\n"
            + "    [Measures].[Store Sales] / [Measures].[Unit Sales]\n"
            + "  set [Top Products] as\n"
            + "    TopCount(\n"
            + "      [Product].[Brand Name].Members,\n"
            + "      3,\n"
            + "      ([Measures].[Unit Sales], [Time].[1997].[Q3]))\n"
            + "select {\n"
            + "  [Measures].[Unit Sales],\n"
            + "  [Measures].[Price per Unit]} on 0,\n"
            + " [Gender].Children * [Marital Status].Children on 1\n"
            + "from [Sales]\n"
            + "where [Top Products] * [Time].[1997].[Q3]",
            "Axis #0:\n"
            + "{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos], [Time].[Time].[1997].[Q3]}\n"
            + "{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale], [Time].[Time].[1997].[Q3]}\n"
            + "{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony], [Time].[Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Price per Unit]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S]}\n"
            + "Row #0: 779\n"
            + "Row #0: 2.40\n"
            + "Row #1: 811\n"
            + "Row #1: 2.24\n"
            + "Row #2: 829\n"
            + "Row #2: 2.23\n"
            + "Row #3: 886\n"
            + "Row #3: 2.25\n");
    }

    /**
     * Tests compound slicer with EXCEPT.
     *
     * <p>Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-637">
     * Bug MONDRIAN-637, "Using Except in the slicer makes no sense"</a>.
     */
    public void testCompoundSlicerExcept() {
        final String expected =
            "Axis #0:\n"
            + "{[Promotion].[Media Type].[Bulk Mail]}\n"
            + "{[Promotion].[Media Type].[Cash Register Handout]}\n"
            + "{[Promotion].[Media Type].[Daily Paper, Radio]}\n"
            + "{[Promotion].[Media Type].[Daily Paper, Radio, TV]}\n"
            + "{[Promotion].[Media Type].[In-Store Coupon]}\n"
            + "{[Promotion].[Media Type].[No Media]}\n"
            + "{[Promotion].[Media Type].[Product Attachment]}\n"
            + "{[Promotion].[Media Type].[Radio]}\n"
            + "{[Promotion].[Media Type].[Street Handout]}\n"
            + "{[Promotion].[Media Type].[Sunday Paper]}\n"
            + "{[Promotion].[Media Type].[Sunday Paper, Radio]}\n"
            + "{[Promotion].[Media Type].[Sunday Paper, Radio, TV]}\n"
            + "{[Promotion].[Media Type].[TV]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 259,035\n"
            + "Row #1: 127,871\n"
            + "Row #2: 131,164\n";

        // slicer expression that inherits [Promotion].[Media Type] member from
        // context
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + " [Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where Except(\n"
            + "  [Promotion].[Media Type].Children,\n"
            + "  {[Promotion].[Media Type].[Daily Paper]})", expected);

        // similar query, but don't assume that [Promotion].[Media
        // Type].CurrentMember = [Promotion].[Media Type].[All Media]
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + " [Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where Except(\n"
            + "  [Promotion].[Media Type].[All Media].Children,\n"
            + "  {[Promotion].[Media Type].[Daily Paper]})", expected);

        // reference query, computing the same numbers a different way
        assertQueryReturns(
            "with member [Promotion].[Media Type].[Except Daily Paper] as\n"
            + "  Aggregate(\n"
            + "    Except(\n"
            + "      [Promotion].[Media Type].Children,\n"
            + "      {[Promotion].[Media Type].[Daily Paper]}))\n"
            + "select [Measures].[Unit Sales]\n"
            + " * {[Promotion].[Media Type],\n"
            + "    [Promotion].[Media Type].[Daily Paper],\n"
            + "    [Promotion].[Media Type].[Except Daily Paper]} on 0,\n"
            + " [Gender].Members on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales], [Promotion].[Media Type].[All Media]}\n"
            + "{[Measures].[Unit Sales], [Promotion].[Media Type].[Daily Paper]}\n"
            + "{[Measures].[Unit Sales], [Promotion].[Media Type].[Except Daily Paper]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 7,738\n"
            + "Row #0: 259,035\n"
            + "Row #1: 131,558\n"
            + "Row #1: 3,687\n"
            + "Row #1: 127,871\n"
            + "Row #2: 135,215\n"
            + "Row #2: 4,051\n"
            + "Row #2: 131,164\n");
    }
    public void testCompoundSlicerWithCellFormatter() {
        String xmlMeasure =
            "<Measure name='Unit Sales Foo Bar' column='unit_sales'\n"
            + "    aggregator='sum' formatString='Standard' formatter='"
            + UdfTest.FooBarCellFormatter.class.getName()
            + "'/>";
        TestContext tc =
            TestContext.instance().legacy().createSubstitutingCube(
                "Sales", null, xmlMeasure, null, null);

        // the cell formatter for the measure should still be used
        tc.assertQueryReturns(
            "select from sales where "
            + " measures.[Unit Sales Foo Bar] * {[Gender].F, [Gender].M} ",
            "Axis #0:\n"
            + "{[Measures].[Unit Sales Foo Bar], [Gender].[Gender].[F]}\n"
            + "{[Measures].[Unit Sales Foo Bar], [Gender].[Gender].[M]}\n"
            + "foo266773.0bar");

        tc.assertQueryReturns(
            "select from sales where "
            + " {[Gender].F, [Gender].M}  * measures.[Unit Sales Foo Bar]",
            "Axis #0:\n"
            + "{[Gender].[Gender].[F], [Measures].[Unit Sales Foo Bar]}\n"
            + "{[Gender].[Gender].[M], [Measures].[Unit Sales Foo Bar]}\n"
            + "foo266773.0bar");
    }


    public void testMondrian1226() {
        assertQueryReturns(
            "with \n"
            +    "member Measures.x1 as ([Time].[1997].[Q1],"
            + "[Measures].[Store Sales])\n"
            +    "member Measures.x2 as ([Time].[1997].[Q2],"
            + " [Measures].[Store Sales])\n"
            +    "set products as TopCount("
            + "Product.[Product Name].Members,1,Measures.[Store Sales])\n"
            +    "SELECT\n"
            +    "NON EMPTY products ON 1,\n"
            +    "NON EMPTY {[Measures].[Store Sales], "
            + "Measures.x1, Measures.x2} ON 0\n"
            +    "FROM [Sales]\n"
            +    "where ([Time].[1997].[Q1] : [Time].[1997].[Q2])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[x1]}\n"
            + "{[Measures].[x2]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs]"
            + ".[Urban].[Urban Small Eggs]}\n"
            + "Row #0: 497.42\n"
            + "Row #0: 235.62\n"
            + "Row #0: 261.80\n");
    }

     public void _testMondrian1226Variation() {
         // Currently broke.  Below are two queries with two dimensions
         // in the compound slicer.
         //  The first has a measure which overrides the Time context,
         // and gives expected results (since the Time dimension is
         // the "placeholder" dimension.
         assertQueryReturns(
             "with member measures.HalfTime as '[Time].[1997].[Q1]/2'"
             + " select measures.HalfTime on 0 from sales where "
             + "({[Time].[1997].[Q1] : [Time].[1997].[Q2]} * gender.[All Gender]) ",
             "Axis #0:\n"
             + "{[Time].[Time].[1997].[Q1], [Customer].[Gender].[All Gender]}\n"
             + "{[Time].[Time].[1997].[Q2], [Customer].[Gender].[All Gender]}\n"
             + "Axis #1:\n"
             + "{[Measures].[HalfTime]}\n"
             + "Row #0: 33,146\n");

         // The second query has a measure overriding gender, which
         // fails since context is not set appropriately for gender.
         assertQueryReturns(
             "with member measures.HalfMan as 'Gender.m/2'"
             +    " select measures.HalfMan on 0 from sales where "
             +    "({[Time].[1997].[Q1] : [Time].[1997].[Q2]} "
             + "* gender.[All Gender]) ",
             "Axis #0:\n"
             + "{[Time].[Time].[1997].[Q1], [Customer].[Gender].[M]}\n"
             + "{[Time].[Time].[1997].[Q2], [Customer].[Gender].[M]}\n"
             + "Axis #1:\n"
             + "{[Measures].[HalfMan]}\n"
             + "Row #0: 32,500\n");
     }


    /**
     * Tests a query with a compond slicer over tuples. (Multiple rows, each
     * of which has multiple members.)
     */
    public void testCompoundSlicerOverTuples() {
        // reference query
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "    TopCount(\n"
            + "      [Product].[Product Category].Members\n"
            + "      * [Customers].[City].Members,\n"
            + "      10) on 1\n"
            + "from [Sales]\n"
            + "where [Time].[1997].[Q3]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customer].[Customers].[Canada].[BC].[Burnaby]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customer].[Customers].[Canada].[BC].[Cliffside]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customer].[Customers].[Canada].[BC].[Haney]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customer].[Customers].[Canada].[BC].[Ladner]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customer].[Customers].[Canada].[BC].[Langford]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customer].[Customers].[Canada].[BC].[Langley]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customer].[Customers].[Canada].[BC].[Metchosin]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customer].[Customers].[Canada].[BC].[N. Vancouver]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customer].[Customers].[Canada].[BC].[Newton]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customer].[Customers].[Canada].[BC].[Oak Bay]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n"
            + "Row #3: \n"
            + "Row #4: \n"
            + "Row #5: \n"
            + "Row #6: \n"
            + "Row #7: \n"
            + "Row #8: \n"
            + "Row #9: \n");

        // The actual query. Note that the set in the slicer has two dimensions.
        // This could not be expressed using calculated members and the
        // Aggregate function.
        assertQueryReturns(
            "with\n"
            + "  member [Measures].[Price per Unit] as\n"
            + "    [Measures].[Store Sales] / [Measures].[Unit Sales]\n"
            + "  set [Top Product Cities] as\n"
            + "    TopCount(\n"
            + "      [Product].[Product Category].Members\n"
            + "      * [Customers].[City].Members,\n"
            + "      3,\n"
            + "      ([Measures].[Unit Sales], [Time].[1997].[Q3]))\n"
            + "select {\n"
            + "  [Measures].[Unit Sales],\n"
            + "  [Measures].[Price per Unit]} on 0,\n"
            + " [Gender].Children * [Marital Status].Children on 1\n"
            + "from [Sales]\n"
            + "where [Top Product Cities] * [Time].[1997].[Q3]",
            "Axis #0:\n"
            + "{[Product].[Products].[Food].[Snack Foods].[Snack Foods], [Customer].[Customers].[USA].[WA].[Spokane], [Time].[Time].[1997].[Q3]}\n"
            + "{[Product].[Products].[Food].[Produce].[Vegetables], [Customer].[Customers].[USA].[WA].[Spokane], [Time].[Time].[1997].[Q3]}\n"
            + "{[Product].[Products].[Food].[Snack Foods].[Snack Foods], [Customer].[Customers].[USA].[WA].[Puyallup], [Time].[Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Price per Unit]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S]}\n"
            + "Row #0: 483\n"
            + "Row #0: 2.21\n"
            + "Row #1: 419\n"
            + "Row #1: 2.21\n"
            + "Row #2: 422\n"
            + "Row #2: 2.22\n"
            + "Row #3: 332\n"
            + "Row #3: 2.20\n");
    }

    public void testCompoundSlicerCrossjoinRange() {
        assertQueryReturns(
            "select\n"
            + "from [Sales]\n"
            + "where [Customer].[Gender].[M]\n"
            + " * [Product].[Drink]\n"
            + " * {[Time].[1997].[Q1] : [Time].[1997].[Q3]}",
            "Axis #0:\n"
            + "{[Customer].[Gender].[M], [Product].[Products].[Drink], [Time].[Time].[1997].[Q1]}\n"
            + "{[Customer].[Gender].[M], [Product].[Products].[Drink], [Time].[Time].[1997].[Q2]}\n"
            + "{[Customer].[Gender].[M], [Product].[Products].[Drink], [Time].[Time].[1997].[Q3]}\n"
            + "9,032");
    }

    /**
     * Tests that if the slicer contains zero members, all cells are null.
     */
    public void testEmptySetSlicerReturnsNull() {
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Product].[Products].Children on 1\n"
            + "from [Sales]\n"
            + "where {}",
            "Axis #0:\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n");
    }

    /**
     * Tests that if the slicer is calculated using an expression and contains
     * zero members, all cells are null.
     */
    public void testEmptySetSlicerViaExpressionReturnsNull() {
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Product].[Products].Children on 1\n"
            + "from [Sales]\n"
            + "where filter([Gender].members * [Marital Status].members, 1 = 0)",
            "Axis #0:\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n");
    }

    /**
     * Test case for a basic query with more than one member of the same
     * hierarchy in the WHERE clause.
     */
    public void testCompoundSlicer() {
        // Reference query.
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where {[Product].[Drink]}",
            "Axis #0:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 24,597\n"
            + "Row #1: 12,202\n"
            + "Row #2: 12,395\n");
        // Reference query.
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where {[Product].[Food]}",
            "Axis #0:\n"
            + "{[Product].[Products].[Food]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 191,940\n"
            + "Row #1: 94,814\n"
            + "Row #2: 97,126\n");

        // Sum members at same level.
        // Note that 216,537 = 24,597 (drink) + 191,940 (food).
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where {[Product].[Drink], [Product].[Food]}",
            "Axis #0:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 216,537\n"
            + "Row #1: 107,016\n"
            + "Row #2: 109,521\n");

        // sum list that contains duplicates
        // duplicates are ignored (checked SSAS 2005)
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where {[Product].[Drink], [Product].[Food], [Product].[Drink]}",
            Bug.BugMondrian555Fixed
                ? "Axis #0:\n"
                  + "{[Product].[Products].[Drink]}\n"
                  + "{[Product].[Products].[Food]}\n"
                  + "{[Product].[Products].[Drink]}\n"
                  + "Axis #1:\n"
                  + "{[Measures].[Unit Sales]}\n"
                  + "Axis #2:\n"
                  + "{[Customer].[Gender].[All Gender]}\n"
                  + "{[Customer].[Gender].[F]}\n"
                  + "{[Customer].[Gender].[M]}\n"
                  + "Row #0: 241,134, 241,134, 241,134\n"
                  + "Row #1: 119,218, 119,218, 119,218\n"
                  + "Row #2: 121,916, 121,916, 121,916\n"
                : "Axis #0:\n"
                  + "{[Product].[Products].[Drink]}\n"
                  + "{[Product].[Products].[Food]}\n"
                  + "{[Product].[Products].[Drink]}\n"
                  + "Axis #1:\n"
                  + "{[Measures].[Unit Sales]}\n"
                  + "Axis #2:\n"
                  + "{[Customer].[Gender].[All Gender]}\n"
                  + "{[Customer].[Gender].[F]}\n"
                  + "{[Customer].[Gender].[M]}\n"
                  + "Row #0: 241,134\n"
                  + "Row #1: 119,218\n"
                  + "Row #2: 121,916\n");

        // sum list that contains a null member -
        // null member is ignored;
        // confirmed behavior with ssas 2005
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where {[Product].[All Products].Parent, [Product].[Food], [Product].[Drink]}",
            "Axis #0:\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Drink]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 216,537\n"
            + "Row #1: 107,016\n"
            + "Row #2: 109,521\n");

        // Reference query.
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where {\n"
            + "  [Product].[Drink],\n"
            + "  [Product].[Food].[Dairy]}",
            "Axis #0:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food].[Dairy]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 37,482\n"
            + "Row #1: 18,715\n"
            + "Row #2: 18,767\n");

        // Sum list that contains a member and one of its children;
        // SSAS 2005 doesn't simply sum them: it behaves behavior as if
        // predicates are pushed down to the fact table. Mondrian double-counts,
        // and that is a bug.
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where {\n"
            + "  [Product].[Drink],\n"
            + "  [Product].[Food].[Dairy],\n"
            + "  [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]}",
            Bug.BugMondrian555Fixed
                ? "Axis #0:\n"
                  + "{[Product].[Products].[Drink]}\n"
                  + "{[Product].[Products].[Food].[Dairy]}\n"
                  + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]}\n"
                  + "Axis #1:\n"
                  + "{[Measures].[Unit Sales]}\n"
                  + "Axis #2:\n"
                  + "{[Customer].[Gender].[All Gender]}\n"
                  + "{[Customer].[Gender].[F]}\n"
                  + "{[Customer].[Gender].[M]}\n"
                  + "Row #0: 37,482\n"
                  + "Row #1: 18,715\n"
                  + "Row #2: 18.767\n"
                : "Axis #0:\n"
                  + "{[Product].[Products].[Drink]}\n"
                  + "{[Product].[Products].[Food].[Dairy]}\n"
                  + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]}\n"
                  + "Axis #1:\n"
                  + "{[Measures].[Unit Sales]}\n"
                  + "Axis #2:\n"
                  + "{[Customer].[Gender].[All Gender]}\n"
                  + "{[Customer].[Gender].[F]}\n"
                  + "{[Customer].[Gender].[M]}\n"
                  + "Row #0: 39,165\n"
                  + "Row #1: 19,532\n"
                  + "Row #2: 19,633\n");

        // The correct behavior of the aggregate function is to double-count.
        // SSAS 2005 and Mondrian give the same behavior.
        assertQueryReturns(
            "with member [Product].[Foo] as\n"
            + "  Aggregate({\n"
            + "    [Product].[Drink],\n"
            + "    [Product].[Food].[Dairy],\n"
            + "    [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]})\n"
            + "select [Measures].[Unit Sales] on 0,\n"
            + "[Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where [Product].[Foo]\n",
            "Axis #0:\n"
            + "{[Product].[Products].[Foo]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 39,165\n"
            + "Row #1: 19,532\n"
            + "Row #2: 19,633\n");
    }

    /**
     * Slicer that is a member expression that evaluates to null.
     * SSAS 2005 allows this, and returns null cells.
     */
    public void testSlicerContainsNullMember() {
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where [Product].Parent",
            "Axis #0:\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n");
    }

    /**
     * Slicer that is literal null.
     * SSAS 2005 allows this, and returns null cells; Mondrian currently gives
     * an error.
     */
    public void testSlicerContainsLiteralNull() {
        final String mdx =
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where null";
        if (Bug.Ssas2005Compatible) {
            // SSAS returns a cell set containing null cells.
            assertQueryReturns(
                mdx,
                "xxx");
        } else {
            // Mondrian gives an error. This is not unreasonable. It is very
            // low priority to make Mondrian consistent with SSAS 2005 in this
            // behavior.
            assertQueryThrows(
                mdx,
                "Function does not support NULL member parameter");
        }
    }

    /**
     * Slicer that is a tuple and one of the members evaluates to null;
     * that makes it a null tuple, and it is eliminated from the list.
     * SSAS 2005 allows this, and returns null cells.
     */
    public void testSlicerContainsPartiallyNullMember() {
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where ([Product].Parent, [Store].[USA].[CA])",
            "Axis #0:\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n");
    }

    /**
     * Compound slicer with distinct-count measure.
     */
    public void testCompoundSlicerWithDistinctCount() {
        // Reference query.
        assertQueryReturns(
            "select [Measures].[Customer Count] on 0,\n"
            + "  {[Store].[USA].[CA], [Store].[USA].[OR].[Portland]}\n"
            + "  * {([Product].[Food], [Time].[1997].[Q1]),\n"
            + "    ([Product].[Drink], [Time].[1997].[Q2].[4])} on 1\n"
            + "from [Sales]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA], [Product].[Products].[Food], [Time].[Time].[1997].[Q1]}\n"
            + "{[Store].[Stores].[USA].[CA], [Product].[Products].[Drink], [Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Store].[Stores].[USA].[OR].[Portland], [Product].[Products].[Food], [Time].[Time].[1997].[Q1]}\n"
            + "{[Store].[Stores].[USA].[OR].[Portland], [Product].[Products].[Drink], [Time].[Time].[1997].[Q2].[4]}\n"
            + "Row #0: 1,069\n"
            + "Row #1: 155\n"
            + "Row #2: 332\n"
            + "Row #3: 48\n");

        // The figures look reasonable, because:
        //  332 + 48 = 380 > 352
        //  1069 + 155 = 1224 > 1175
        assertQueryReturns(
            "select [Measures].[Customer Count] on 0,\n"
            + "{[Store].[USA].[CA], [Store].[USA].[OR].[Portland]} on 1\n"
            + "from [Sales]\n"
            + "where {\n"
            + "  ([Product].[Food], [Time].[1997].[Q1]),\n"
            + "  ([Product].[Drink], [Time].[1997].[Q2].[4])}",
            "Axis #0:\n"
            + "{[Product].[Products].[Food], [Time].[Time].[1997].[Q1]}\n"
            + "{[Product].[Products].[Drink], [Time].[Time].[1997].[Q2].[4]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR].[Portland]}\n"
            + "Row #0: 1,175\n"
            + "Row #1: 352\n");
    }

    /**
     * Tests compound slicer, and other rollups, with AVG function.
     *
     * <p>Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-675">
     * Bug MONDRIAN-675,
     * "Allow rollup of measures based on AVG aggregate function"</a>.
     */
    public void testRollupAvg() {
        final TestContext testContext =
            TestContext.instance().legacy().createSubstitutingCube(
                "Sales",
                null,
                "<Measure name='Avg Unit Sales' aggregator='avg' column='unit_sales'/>\n"
                + "<Measure name='Count Unit Sales' aggregator='count' column='unit_sales'/>\n"
                + "<Measure name='Sum Unit Sales' aggregator='sum' column='unit_sales'/>\n",
                null,
                null);
        // basic query with avg
        testContext.assertQueryReturns(
            "select from [Sales]\n"
            + "where [Measures].[Avg Unit Sales]",
            "Axis #0:\n"
            + "{[Measures].[Avg Unit Sales]}\n"
            + "3.072");

        // roll up using compound slicer
        // (should give a real value, not an error)
        testContext.assertQueryReturns(
            "select from [Sales]\n"
            + "where [Measures].[Avg Unit Sales]\n"
            + "   * {[Customers].[USA].[OR], [Customers].[USA].[CA]}",
            "Axis #0:\n"
            + "{[Measures].[Avg Unit Sales], [Customers].[Customers].[USA].[OR]}\n"
            + "{[Measures].[Avg Unit Sales], [Customers].[Customers].[USA].[CA]}\n"
            + "6.189");

        // roll up using a named set
        testContext.assertQueryReturns(
            "with member [Customers].[OR and CA] as Aggregate(\n"
            + " {[Customers].[USA].[OR], [Customers].[USA].[CA]})\n"
            + "select from [Sales]\n"
            + "where ([Measures].[Avg Unit Sales], [Customers].[OR and CA])",
            "Axis #0:\n"
            + "{[Measures].[Avg Unit Sales], [Customers].[Customers].[OR and CA]}\n"
            + "3.092");
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-899">
     * Bug MONDRIAN-899,
     * "Order() function does not work properly together with WHERE clause"</a>.
     */
    public void testBugMondrian899() {
        final String expected =
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[Time].[1997].[Q4].[11]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Wildon Cameron]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Emily Barela]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Dauna Barton]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Mona Vigil]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Linda Combs]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Eric Winters]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Jack Zucconi]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Luann Crawford]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Suzanne Davis]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Lucy Flowers]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Donna Weisinger]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Stanley Marks]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[James Short]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Curtis Pollard]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Dawn Laner]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Patricia Towns]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Puyallup].[William Wade]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Lorriene Weathers]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Grace McLaughlin]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Edna Woodson]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Harry Torphy]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Anne Allard]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Bonnie Staley]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Olympia].[Patricia Gervasi]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Shirley Gottbehuet]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Puyallup].[Jeremy Styers]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Beth Ohnheiser]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Bremerton].[Harold Powers]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Daniel Thompson]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Fran McEvilly]}\n"
            + "Row #0: 327\n"
            + "Row #1: 323\n"
            + "Row #2: 319\n"
            + "Row #3: 308\n"
            + "Row #4: 305\n"
            + "Row #5: 296\n"
            + "Row #6: 296\n"
            + "Row #7: 295\n"
            + "Row #8: 291\n"
            + "Row #9: 289\n"
            + "Row #10: 285\n"
            + "Row #11: 284\n"
            + "Row #12: 281\n"
            + "Row #13: 279\n"
            + "Row #14: 279\n"
            + "Row #15: 278\n"
            + "Row #16: 277\n"
            + "Row #17: 271\n"
            + "Row #18: 268\n"
            + "Row #19: 266\n"
            + "Row #20: 265\n"
            + "Row #21: 264\n"
            + "Row #22: 260\n"
            + "Row #23: 251\n"
            + "Row #24: 250\n"
            + "Row #25: 249\n"
            + "Row #26: 249\n"
            + "Row #27: 248\n"
            + "Row #28: 247\n"
            + "Row #29: 247\n";
        assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS, \n"
            + "  Subset(Order([Customers].[Name].Members, [Measures].[Unit Sales], BDESC), 10.0, 30.0) ON ROWS \n"
            + "from [Sales] \n"
            + "where ([Time].[1997].[Q1].[2] : [Time].[1997].[Q4].[11])",
            expected);

        // Equivalent query.
        assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS, \n"
            + "  Tail(\n"
            + "    TopCount([Customers].[Name].Members, 40, [Measures].[Unit Sales]),\n"
            + "    30) ON ROWS \n"
            + "from [Sales] \n"
            + "where ([Time].[1997].[Q1].[2] : [Time].[1997].[Q4].[11])",
            expected);
    }

    // similar to MONDRIAN-899 testcase
    public void testTopCount() {
        assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS, \n"
            + "  TopCount([Customers].[USA].[WA].[Spokane].Children, 10, [Measures].[Unit Sales]) ON ROWS \n"
            + "from [Sales] \n"
            + "where ([Time].[1997].[Q1].[2] : [Time].[1997].[Q1].[3])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Grace McLaughlin]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[George Todero]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Matt Bellah]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Mary Francis Benigar]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Lucy Flowers]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[David Hassard]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Dauna Barton]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Dora Sims]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Joann Mramor]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Mike Madrid]}\n"
            + "Row #0: 131\n"
            + "Row #1: 129\n"
            + "Row #2: 113\n"
            + "Row #3: 103\n"
            + "Row #4: 95\n"
            + "Row #5: 94\n"
            + "Row #6: 92\n"
            + "Row #7: 85\n"
            + "Row #8: 79\n"
            + "Row #9: 79\n");
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-900">
     * Bug MONDRIAN-900,
     * "Filter() function works incorrectly together with WHERE clause"</a>.
     */
    public void testBugMondrian900() {
        assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "  Tail(Filter([Customers].[Name].Members, ([Measures].[Unit Sales] IS EMPTY)), 3) ON ROWS \n"
            + "from [Sales]\n"
            + "where ([Time].[1997].[Q1].[2] : [Time].[1997].[Q4].[10])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[Time].[1997].[Q4].[10]}\n"
            + "Axis #1:\n"
            + "Axis #2:\n"
            + "{[Customer].[Customers].[USA].[WA].[Walla Walla].[Melanie Snow]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Walla Walla].[Ramon Williams]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Yakima].[Louis Gomez]}\n");
    }

    public void testSlicerWithCalcMembers() throws Exception {
        final TestContext testContext = TestContext.instance();
        //2 calc mems
        testContext.assertQueryReturns(
            "WITH "
            + "MEMBER [Store].[Stores].[aggCA] AS "
            + "'Aggregate({[Store].[Stores].[USA].[CA].[Los Angeles], "
            + "[Store].[Stores].[USA].[CA].[San Francisco]})'"
            + " MEMBER [Store].[Stores].[aggOR] AS "
            + "'Aggregate({[Store].[Stores].[USA].[OR].[Portland]})' "
            + " SELECT FROM SALES WHERE { [Store].[Stores].[aggCA], [Store].[Stores].[aggOR] } ",
            "Axis #0:\n"
            + "{[Store].[Stores].[aggCA]}\n"
            + "{[Store].[Stores].[aggOR]}\n"
            + "53,859");

        // mix calc and non-calc
        testContext.assertQueryReturns(
            "WITH "
            + "MEMBER [Store].[Stores].[aggCA] AS "
            + "'Aggregate({[Store].[Stores].[USA].[CA].[Los Angeles], "
            + "[Store].[Stores].[USA].[CA].[San Francisco]})'"
            + " SELECT FROM SALES WHERE { [Store].[Stores].[aggCA], [Store].[Stores].[All Stores].[USA].[OR].[Portland] } ",
            "Axis #0:\n"
            + "{[Store].[Stores].[aggCA]}\n"
            + "{[Store].[Stores].[USA].[OR].[Portland]}\n"
            + "53,859");

        // multi-position slicer with mix of calc and non-calc
        testContext.assertQueryReturns(
            "WITH "
            + "MEMBER [Store].[Stores].[aggCA] AS "
            + "'Aggregate({[Store].[Stores].[USA].[CA].[Los Angeles], "
            + "[Store].[Stores].[USA].[CA].[San Francisco]})'"
            + " SELECT FROM SALES WHERE "
            +  "Gender.Gender.members * "
            + "{ [Store].[Stores].[aggCA], [Store].[Stores].[All Stores].[USA].[OR].[Portland] } ",
            "Axis #0:\n"
            + "{[Customer].[Gender].[F], [Store].[Stores].[aggCA]}\n"
            + "{[Customer].[Gender].[F], [Store].[Stores].[USA].[OR].[Portland]}\n"
            + "{[Customer].[Gender].[M], [Store].[Stores].[aggCA]}\n"
            + "{[Customer].[Gender].[M], [Store].[Stores].[USA].[OR].[Portland]}\n"
            + "53,859");

        // named set with calc mem and non-calc
        testContext.assertQueryReturns(
            "with member Time.Time.aggTime as "
            + "'aggregate({ [Time].[1997].[Q1], [Time].[1997].[Q2] })'"
            + "set [timeMembers] as "
            + "'{Time.aggTime, [Time].[1997].[Q3] }'"
            + "select from sales where [timeMembers]",
            "Axis #0:\n"
            + "{[Time].[Time].[aggTime]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "194,749");

        // calculated measure in slicer
        testContext.assertQueryReturns(
            " SELECT FROM SALES WHERE "
            + "[Measures].[Profit] * { [Store].[Stores].[USA].[CA], [Store].[Stores].[USA].[OR]}",
            "Axis #0:\n"
            + "{[Measures].[Profit], [Store].[Stores].[USA].[CA]}\n"
            + "{[Measures].[Profit], [Store].[Stores].[USA].[OR]}\n"
            + "$181,141.98");
    }

}

// End CompoundSlicerTest.java
