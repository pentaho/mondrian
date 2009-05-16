/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2009-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import mondrian.util.Bug;

/**
 * Tests the expressions used for calculated members. Please keep in sync
 * with the actual code used by the wizard.
 *
 * @author jhyde
 * @since 15 May, 2009
 * @version $Id$
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
            + "{[Product].[Top], [Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Price per Unit]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M]}\n"
            + "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}\n"
            + "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}\n"
            + "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}\n"
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
            + "{[Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos], [Time].[1997].[Q3]}\n"
            + "{[Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale], [Time].[1997].[Q3]}\n"
            + "{[Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony], [Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Price per Unit]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M]}\n"
            + "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}\n"
            + "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}\n"
            + "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}\n"
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
            + "{[Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Burnaby]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Cliffside]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Haney]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Ladner]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Langford]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Langley]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Metchosin]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[N. Vancouver]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Newton]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Oak Bay]}\n"
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
            + "{[Product].[All Products].[Food].[Snack Foods].[Snack Foods], [Customers].[All Customers].[USA].[WA].[Spokane], [Time].[1997].[Q3]}\n"
            + "{[Product].[All Products].[Food].[Produce].[Vegetables], [Customers].[All Customers].[USA].[WA].[Spokane], [Time].[1997].[Q3]}\n"
            + "{[Product].[All Products].[Food].[Snack Foods].[Snack Foods], [Customers].[All Customers].[USA].[WA].[Puyallup], [Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Price per Unit]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M]}\n"
            + "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}\n"
            + "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}\n"
            + "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}\n"
            + "Row #0: 483\n"
            + "Row #0: 2.21\n"
            + "Row #1: 419\n"
            + "Row #1: 2.21\n"
            + "Row #2: 422\n"
            + "Row #2: 2.22\n"
            + "Row #3: 332\n"
            + "Row #3: 2.20\n");
    }

    /**
     * Tests that if the slicer contains zero members, all cells are null.
     */
    public void testEmptySetSlicerReturnsNull() {
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Product].Children on 1\n"
            + "from [Sales]\n"
            + "where {}",
            "Axis #0:\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[All Products].[Drink]}\n"
            + "{[Product].[All Products].[Food]}\n"
            + "{[Product].[All Products].[Non-Consumable]}\n"
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
            + "[Product].Children on 1\n"
            + "from [Sales]\n"
            + "where filter([Gender].members * [Marital Status].members, 1 = 0)",
            "Axis #0:\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[All Products].[Drink]}\n"
            + "{[Product].[All Products].[Food]}\n"
            + "{[Product].[All Products].[Non-Consumable]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n");
    }

    public void testCompoundSlicer() {
        // Reference query.
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Gender].Members on 1\n"
            + "from [Sales]\n"
            + "where {[Product].[Drink]}",
            "Axis #0:\n"
                + "{[Product].[All Products].[Drink]}\n"
                + "Axis #1:\n"
                + "{[Measures].[Unit Sales]}\n"
                + "Axis #2:\n"
                + "{[Gender].[All Gender]}\n"
                + "{[Gender].[All Gender].[F]}\n"
                + "{[Gender].[All Gender].[M]}\n"
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
            + "{[Product].[All Products].[Food]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[All Gender].[F]}\n"
            + "{[Gender].[All Gender].[M]}\n"
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
            + "{[Product].[All Products].[Drink]}\n"
            + "{[Product].[All Products].[Food]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[All Gender].[F]}\n"
            + "{[Gender].[All Gender].[M]}\n"
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
                  + "{[Product].[All Products].[Drink]}\n"
                  + "{[Product].[All Products].[Food]}\n"
                  + "{[Product].[All Products].[Drink]}\n"
                  + "Axis #1:\n"
                  + "{[Measures].[Unit Sales]}\n"
                  + "Axis #2:\n"
                  + "{[Gender].[All Gender]}\n"
                  + "{[Gender].[All Gender].[F]}\n"
                  + "{[Gender].[All Gender].[M]}\n"
                  + "Row #0: 241,134, 241,134, 241,134\n"
                  + "Row #1: 119,218, 119,218, 119,218\n"
                  + "Row #2: 121,916, 121,916, 121,916\n"
                : "Axis #0:\n"
                  + "{[Product].[All Products].[Drink]}\n"
                  + "{[Product].[All Products].[Food]}\n"
                  + "{[Product].[All Products].[Drink]}\n"
                  + "Axis #1:\n"
                  + "{[Measures].[Unit Sales]}\n"
                  + "Axis #2:\n"
                  + "{[Gender].[All Gender]}\n"
                  + "{[Gender].[All Gender].[F]}\n"
                  + "{[Gender].[All Gender].[M]}\n"
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
            + "{[Product].[All Products].[Food]}\n"
            + "{[Product].[All Products].[Drink]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[All Gender].[F]}\n"
            + "{[Gender].[All Gender].[M]}\n"
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
            + "{[Product].[All Products].[Drink]}\n"
            + "{[Product].[All Products].[Food].[Dairy]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[All Gender].[F]}\n"
            + "{[Gender].[All Gender].[M]}\n"
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
                  + "{[Product].[All Products].[Drink]}\n"
                  + "{[Product].[All Products].[Food].[Dairy]}\n"
                  + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]}\n"
                  + "Axis #1:\n"
                  + "{[Measures].[Unit Sales]}\n"
                  + "Axis #2:\n"
                  + "{[Gender].[All Gender]}\n"
                  + "{[Gender].[All Gender].[F]}\n"
                  + "{[Gender].[All Gender].[M]}\n"
                  + "Row #0: 37,482\n"
                  + "Row #1: 18,715\n"
                  + "Row #2: 18.767\n"
                : "Axis #0:\n"
                  + "{[Product].[All Products].[Drink]}\n"
                  + "{[Product].[All Products].[Food].[Dairy]}\n"
                  + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]}\n"
                  + "Axis #1:\n"
                  + "{[Measures].[Unit Sales]}\n"
                  + "Axis #2:\n"
                  + "{[Gender].[All Gender]}\n"
                  + "{[Gender].[All Gender].[F]}\n"
                  + "{[Gender].[All Gender].[M]}\n"
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
            + "{[Product].[Foo]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[All Gender].[F]}\n"
            + "{[Gender].[All Gender].[M]}\n"
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
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[All Gender].[F]}\n"
            + "{[Gender].[All Gender].[M]}\n"
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
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[All Gender].[F]}\n"
            + "{[Gender].[All Gender].[M]}\n"
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
            + "{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Food], [Time].[1997].[Q1]}\n"
            + "{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink], [Time].[1997].[Q2].[4]}\n"
            + "{[Store].[All Stores].[USA].[OR].[Portland], [Product].[All Products].[Food], [Time].[1997].[Q1]}\n"
            + "{[Store].[All Stores].[USA].[OR].[Portland], [Product].[All Products].[Drink], [Time].[1997].[Q2].[4]}\n"
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
            + "{[Product].[All Products].[Food], [Time].[1997].[Q1]}\n"
            + "{[Product].[All Products].[Drink], [Time].[1997].[Q2].[4]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores].[USA].[CA]}\n"
            + "{[Store].[All Stores].[USA].[OR].[Portland]}\n"
            + "Row #0: 1,175\n"
            + "Row #1: 352\n");
    }
}

// End CompoundSlicerTest.java
