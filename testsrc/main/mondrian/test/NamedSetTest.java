/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.spi.impl.FilterDynamicSchemaProcessor;

import java.io.InputStream;
import java.util.Collections;

/**
 * Unit-test for named sets, in all their various forms: <code>WITH SET</code>,
 * sets defined against cubes, virtual cubes, and at the schema level.
 *
 * @author jhyde
 * @since April 30, 2005
 */
public class NamedSetTest extends FoodMartTestCase {

    public NamedSetTest() {
        super();
    }

    @SuppressWarnings("UnusedDeclaration")
    public NamedSetTest(String name) {
        super(name);
    }

    /**
     * Set defined in query according measures, hence context-dependent.
     */
    public void testNamedSet() {
        assertQueryReturns(
            "WITH\n"
            + "    SET [Top Sellers]\n"
            + "AS \n"
            + "    'TopCount([Warehouse].[Warehouse Name].MEMBERS, 10, \n"
            + "        [Measures].[Warehouse Sales])'\n"
            + "SELECT \n"
            + "    {[Measures].[Warehouse Sales]} ON COLUMNS,\n"
            + "        {[Top Sellers]} ON ROWS\n"
            + "FROM \n"
            + "    [Warehouse]\n"
            + "WHERE \n"
            + "    [Time].[Year].[1997]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Warehouse Sales]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouses].[USA].[OR].[Salem].[Treehouse Distribution]}\n"
            + "{[Warehouse].[Warehouses].[USA].[WA].[Tacoma].[Jorge Garcia, Inc.]}\n"
            + "{[Warehouse].[Warehouses].[USA].[CA].[Los Angeles].[Artesia Warehousing, Inc.]}\n"
            + "{[Warehouse].[Warehouses].[USA].[CA].[San Diego].[Jorgensen Service Storage]}\n"
            + "{[Warehouse].[Warehouses].[USA].[WA].[Bremerton].[Destination, Inc.]}\n"
            + "{[Warehouse].[Warehouses].[USA].[WA].[Seattle].[Quality Warehousing and Trucking]}\n"
            + "{[Warehouse].[Warehouses].[USA].[WA].[Spokane].[Jones International]}\n"
            + "{[Warehouse].[Warehouses].[USA].[WA].[Yakima].[Maddock Stored Foods]}\n"
            + "{[Warehouse].[Warehouses].[USA].[CA].[Beverly Hills].[Big  Quality Warehouse]}\n"
            + "{[Warehouse].[Warehouses].[USA].[OR].[Portland].[Quality Distribution, Inc.]}\n"
            + "Row #0: 31,116.375\n"
            + "Row #1: 30,743.772\n"
            + "Row #2: 22,907.959\n"
            + "Row #3: 22,869.79\n"
            + "Row #4: 22,187.418\n"
            + "Row #5: 22,046.942\n"
            + "Row #6: 10,879.674\n"
            + "Row #7: 10,212.201\n"
            + "Row #8: 10,156.496\n"
            + "Row #9: 7,718.678\n");
    }

    /**
     * Set defined on top of calc member.
     */
    public void testNamedSetOnMember() {
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case INFOBRIGHT:
            // Mondrian generates 'select ... sum(warehouse_sales) -
            // sum(warehouse_cost) as c ... order by c4', correctly, but
            // Infobright gives error "'c4' isn't in GROUP BY".
            return;
        }
        assertQueryReturns(
            "WITH\n"
            + "    MEMBER [Measures].[Profit]\n"
            + "AS '[Measures].[Warehouse Sales] - [Measures].[Warehouse Cost] '\n"
            + "    SET [Top Performers]\n"
            + "AS \n"
            + "    'TopCount([Warehouse].[Warehouse Name].MEMBERS, 5, \n"
            + "        [Measures].[Profit])'\n"
            + "SELECT \n"
            + "    {[Measures].[Profit]} ON COLUMNS,\n"
            + "        {[Top Performers]} ON ROWS\n"
            + "FROM \n"
            + "    [Warehouse]\n"
            + "WHERE \n"
            + "    [Time].[Year].[1997].[Q2]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Profit]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouses].[USA].[WA].[Bremerton].[Destination, Inc.]}\n"
            + "{[Warehouse].[Warehouses].[USA].[CA].[San Diego].[Jorgensen Service Storage]}\n"
            + "{[Warehouse].[Warehouses].[USA].[OR].[Salem].[Treehouse Distribution]}\n"
            + "{[Warehouse].[Warehouses].[USA].[CA].[Los Angeles].[Artesia Warehousing, Inc.]}\n"
            + "{[Warehouse].[Warehouses].[USA].[WA].[Seattle].[Quality Warehousing and Trucking]}\n"
            + "Row #0: 4,516.756\n"
            + "Row #1: 4,189.36\n"
            + "Row #2: 4,169.318\n"
            + "Row #3: 3,848.647\n"
            + "Row #4: 3,708.717\n");
    }

    /**
     * Set defined by explicit tlist in query.
     */
    public void testNamedSetAsList() {
        assertQueryReturns(
            "WITH SET [ChardonnayChablis] AS\n"
            + "   '{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chardonnay],\n"
            + "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chardonnay],\n"
            + "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chardonnay],\n"
            + "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chardonnay],\n"
            + "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chardonnay],\n"
            + "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chablis Wine],\n"
            + "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chablis Wine],\n"
            + "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chablis Wine],\n"
            + "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chablis Wine],\n"
            + "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chablis Wine]}'\n"
            + "SELECT\n"
            + "   [ChardonnayChablis] ON COLUMNS,\n"
            + "   {Measures.[Unit Sales]} ON ROWS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chardonnay]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chardonnay]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chardonnay]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chardonnay]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chardonnay]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chablis Wine]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chablis Wine]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chablis Wine]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chablis Wine]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chablis Wine]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 192\n"
            + "Row #0: 189\n"
            + "Row #0: 170\n"
            + "Row #0: 164\n"
            + "Row #0: 173\n"
            + "Row #0: 163\n"
            + "Row #0: 209\n"
            + "Row #0: 136\n"
            + "Row #0: 140\n"
            + "Row #0: 185\n");
    }

    /**
     * Set defined using filter expression.
     */
    public void testIntrinsic() {
//        testNamedSet();
//        testNamedSetOnMember();
        testNamedSetAsList();
        assertQueryReturns(
            "WITH SET [ChardonnayChablis] AS\n"
            + "   'Filter([Product].Members, (InStr(1, [Product].CurrentMember.Name, \"chardonnay\") <> 0) OR (InStr(1, [Product].CurrentMember.Name, \"chablis\") <> 0))'\n"
            + "SELECT\n"
            + "   [ChardonnayChablis] ON COLUMNS,\n"
            + "   {Measures.[Unit Sales]} ON ROWS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n");

        assertQueryReturns(
            "WITH SET [BeerMilk] AS\n"
            + "   'Filter([Product].Members, (InStr(1, [Product].CurrentMember.Name, \"Beer\") <> 0) OR (InStr(1, LCase([Product].CurrentMember.Name), \"milk\") <> 0))'\n"
            + "SELECT\n"
            + "   [BeerMilk] ON COLUMNS,\n"
            + "   {Measures.[Unit Sales]} ON ROWS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl].[Pearl Imported Beer]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl].[Pearl Light Beer]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Light Beer]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure].[Top Measure Imported Beer]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure].[Top Measure Light Beer]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus].[Walrus Imported Beer]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus].[Walrus Light Beer]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker 1% Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker 2% Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker Buttermilk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker Chocolate Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker Whole Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson 1% Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson 2% Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson Buttermilk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson Chocolate Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson Whole Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club 1% Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club 2% Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Buttermilk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Chocolate Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Whole Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better 1% Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better 2% Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better Buttermilk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better Chocolate Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better Whole Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla 1% Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla 2% Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla Buttermilk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla Chocolate Milk]}\n"
            + "{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla Whole Milk]}\n"
            + "{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Atomic].[Atomic Malted Milk Balls]}\n"
            + "{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Choice].[Choice Malted Milk Balls]}\n"
            + "{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Gulf Coast].[Gulf Coast Malted Milk Balls]}\n"
            + "{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Musial].[Musial Malted Milk Balls]}\n"
            + "{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Thresher].[Thresher Malted Milk Balls]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 6,838\n"
            + "Row #0: 1,683\n"
            + "Row #0: 154\n"
            + "Row #0: 115\n"
            + "Row #0: 175\n"
            + "Row #0: 210\n"
            + "Row #0: 187\n"
            + "Row #0: 175\n"
            + "Row #0: 145\n"
            + "Row #0: 161\n"
            + "Row #0: 174\n"
            + "Row #0: 187\n"
            + "Row #0: 4,186\n"
            + "Row #0: 189\n"
            + "Row #0: 177\n"
            + "Row #0: 110\n"
            + "Row #0: 133\n"
            + "Row #0: 163\n"
            + "Row #0: 212\n"
            + "Row #0: 131\n"
            + "Row #0: 175\n"
            + "Row #0: 175\n"
            + "Row #0: 234\n"
            + "Row #0: 155\n"
            + "Row #0: 145\n"
            + "Row #0: 140\n"
            + "Row #0: 159\n"
            + "Row #0: 168\n"
            + "Row #0: 190\n"
            + "Row #0: 177\n"
            + "Row #0: 227\n"
            + "Row #0: 197\n"
            + "Row #0: 168\n"
            + "Row #0: 160\n"
            + "Row #0: 133\n"
            + "Row #0: 174\n"
            + "Row #0: 151\n"
            + "Row #0: 143\n"
            + "Row #0: 188\n"
            + "Row #0: 176\n"
            + "Row #0: 192\n"
            + "Row #0: 157\n"
            + "Row #0: 164\n");
    }

    /**
     * Tests a named set defined in a query which consists of tuples.
     */
    public void testNamedSetCrossJoin() {
        assertQueryReturns(
            "WITH\n"
            + "    SET [Store Types by Country]\n"
            + "AS\n"
            + "    'CROSSJOIN({[Store].[Store Country].MEMBERS},\n"
            + "               {[Store Type].[Store Type].MEMBERS})'\n"
            + "SELECT\n"
            + "    {[Measures].[Units Ordered]} ON COLUMNS,\n"
            + "    NON EMPTY {[Store Types by Country]} ON ROWS\n"
            + "FROM\n"
            + "    [Warehouse]\n"
            + "WHERE\n"
            + "    [Time].[1997].[Q2]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Units Ordered]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA], [Store].[Store Type].[Deluxe Supermarket]}\n"
            + "{[Store].[Stores].[USA], [Store].[Store Type].[Mid-Size Grocery]}\n"
            + "{[Store].[Stores].[USA], [Store].[Store Type].[Supermarket]}\n"
            + "Row #0: 16843.0\n"
            + "Row #1: 2295.0\n"
            + "Row #2: 34856.0\n");
    }

    // Disabled because fails with error '<Value> = <String> is not a function'
    // Also, don't know whether [oNormal] will correctly resolve to
    // [Store Type].[oNormal].
    public void _testXxx() {
        assertQueryReturns(
            "WITH MEMBER [Store Type].[All Store Type].[oNormal] AS 'Aggregate(Filter([Customers].[Name].Members, [Customers].CurrentMember.Properties(\"Member Card\") = \"Normal\") * {[Store Type].[All Store Type]})'\n"
            + "MEMBER [Store Type].[All Store Type].[oBronze] AS 'Aggregate(Filter([Customers].[Name].Members, [Customers].CurrentMember.Properties(\"Member Card\") = \"Bronze\") * {[Store Type].[All Store Type]})'\n"
            + "MEMBER [Store Type].[All Store Type].[oGolden] AS 'Aggregate(Filter([Customers].[Name].Members, [Customers].CurrentMember.Properties(\"Member Card\") = \"Golden\") * {[Store Type].[All Store Type]})'\n"
            + "MEMBER [Store Type].[All Store Type].[oSilver] AS 'Aggregate(Filter([Customers].[Name].Members, [Customers].CurrentMember.Properties(\"Member Card\") = \"Silver\") * {[Store Type].[All Store Type]})'\n"
            + "SET CardTypes AS '{[oNormal], [oBronze], [oGolden], [oSilver]}'\n"
            + "SELECT {[Unit Sales]} ON COLUMNS, CardTypes ON ROWS\n"
            + "FROM Sales",
            "xxxx");
    }

    /**
     * Set used inside expression (Crossjoin).
     */
    public void testNamedSetUsedInCrossJoin() {
        assertQueryReturns(
            "WITH\n"
            + "  SET [TopMedia] AS 'TopCount([Promotion].[Media Type].children, 5, [Measures].[Store Sales])' \n"
            + "SELECT {[Time].[1997].[Q1], [Time].[1997].[Q2]} ON COLUMNS,\n"
            + " {CrossJoin([TopMedia], [Product].children)} ON ROWS\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Axis #2:\n"
            + "{[Promotion].[Media Type].[No Media], [Product].[Products].[Drink]}\n"
            + "{[Promotion].[Media Type].[No Media], [Product].[Products].[Food]}\n"
            + "{[Promotion].[Media Type].[No Media], [Product].[Products].[Non-Consumable]}\n"
            + "{[Promotion].[Media Type].[Daily Paper, Radio, TV], [Product].[Products].[Drink]}\n"
            + "{[Promotion].[Media Type].[Daily Paper, Radio, TV], [Product].[Products].[Food]}\n"
            + "{[Promotion].[Media Type].[Daily Paper, Radio, TV], [Product].[Products].[Non-Consumable]}\n"
            + "{[Promotion].[Media Type].[Daily Paper], [Product].[Products].[Drink]}\n"
            + "{[Promotion].[Media Type].[Daily Paper], [Product].[Products].[Food]}\n"
            + "{[Promotion].[Media Type].[Daily Paper], [Product].[Products].[Non-Consumable]}\n"
            + "{[Promotion].[Media Type].[Product Attachment], [Product].[Products].[Drink]}\n"
            + "{[Promotion].[Media Type].[Product Attachment], [Product].[Products].[Food]}\n"
            + "{[Promotion].[Media Type].[Product Attachment], [Product].[Products].[Non-Consumable]}\n"
            + "{[Promotion].[Media Type].[Cash Register Handout], [Product].[Products].[Drink]}\n"
            + "{[Promotion].[Media Type].[Cash Register Handout], [Product].[Products].[Food]}\n"
            + "{[Promotion].[Media Type].[Cash Register Handout], [Product].[Products].[Non-Consumable]}\n"
            + "Row #0: 3,970\n"
            + "Row #0: 4,287\n"
            + "Row #1: 32,939\n"
            + "Row #1: 33,238\n"
            + "Row #2: 8,650\n"
            + "Row #2: 9,057\n"
            + "Row #3: 142\n"
            + "Row #3: 364\n"
            + "Row #4: 975\n"
            + "Row #4: 2,523\n"
            + "Row #5: 250\n"
            + "Row #5: 603\n"
            + "Row #6: 464\n"
            + "Row #6: 66\n"
            + "Row #7: 3,173\n"
            + "Row #7: 464\n"
            + "Row #8: 862\n"
            + "Row #8: 121\n"
            + "Row #9: 171\n"
            + "Row #9: 106\n"
            + "Row #10: 1,344\n"
            + "Row #10: 814\n"
            + "Row #11: 362\n"
            + "Row #11: 165\n"
            + "Row #12: \n"
            + "Row #12: 92\n"
            + "Row #13: \n"
            + "Row #13: 933\n"
            + "Row #14: \n"
            + "Row #14: 229\n");
    }

    public void testAggOnCalcMember() {
        assertQueryReturns(
            "WITH\n"
            + "  SET [TopMedia] AS 'TopCount([Promotion].[Media Type].children, 5, [Measures].[Store Sales])' \n"
            + "  MEMBER [Measures].[California sales for Top Media] AS 'Sum([TopMedia], ([Store].[USA].[CA], [Measures].[Store Sales]))'\n"
            + "SELECT {[Time].[1997].[Q1], [Time].[1997].[Q2]} ON COLUMNS,\n"
            + " {[Product].children} ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE [Measures].[California sales for Top Media]",
            "Axis #0:\n"
            + "{[Measures].[California sales for Top Media]}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n"
            + "Row #0: 2,725.85\n"
            + "Row #0: 2,715.56\n"
            + "Row #1: 21,200.84\n"
            + "Row #1: 23,263.72\n"
            + "Row #2: 5,598.71\n"
            + "Row #2: 6,111.74\n");
    }

    public void testContextSensitiveNamedSet() {
        // For reference.
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "Order([Promotion].[Media Type].Children, [Measures].[Unit Sales], DESC) ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE [Time].[1997]",

            "Axis #0:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Promotion].[Media Type].[No Media]}\n"
            + "{[Promotion].[Media Type].[Daily Paper, Radio, TV]}\n"
            + "{[Promotion].[Media Type].[Daily Paper]}\n"
            + "{[Promotion].[Media Type].[Product Attachment]}\n"
            + "{[Promotion].[Media Type].[Daily Paper, Radio]}\n"
            + "{[Promotion].[Media Type].[Cash Register Handout]}\n"
            + "{[Promotion].[Media Type].[Sunday Paper, Radio]}\n"
            + "{[Promotion].[Media Type].[Street Handout]}\n"
            + "{[Promotion].[Media Type].[Sunday Paper]}\n"
            + "{[Promotion].[Media Type].[Bulk Mail]}\n"
            + "{[Promotion].[Media Type].[In-Store Coupon]}\n"
            + "{[Promotion].[Media Type].[TV]}\n"
            + "{[Promotion].[Media Type].[Sunday Paper, Radio, TV]}\n"
            + "{[Promotion].[Media Type].[Radio]}\n"
            + "Row #0: 195,448\n"
            + "Row #1: 9,513\n"
            + "Row #2: 7,738\n"
            + "Row #3: 7,544\n"
            + "Row #4: 6,891\n"
            + "Row #5: 6,697\n"
            + "Row #6: 5,945\n"
            + "Row #7: 5,753\n"
            + "Row #8: 4,339\n"
            + "Row #9: 4,320\n"
            + "Row #10: 3,798\n"
            + "Row #11: 3,607\n"
            + "Row #12: 2,726\n"
            + "Row #13: 2,454\n");

        // For reference.
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "Order([Promotion].[Media Type].Children, [Measures].[Unit Sales], DESC) ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE [Time].[1997].[Q2]",

            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Promotion].[Media Type].[No Media]}\n"
            + "{[Promotion].[Media Type].[Daily Paper, Radio, TV]}\n"
            + "{[Promotion].[Media Type].[Daily Paper, Radio]}\n"
            + "{[Promotion].[Media Type].[Sunday Paper, Radio]}\n"
            + "{[Promotion].[Media Type].[TV]}\n"
            + "{[Promotion].[Media Type].[Cash Register Handout]}\n"
            + "{[Promotion].[Media Type].[Sunday Paper, Radio, TV]}\n"
            + "{[Promotion].[Media Type].[Product Attachment]}\n"
            + "{[Promotion].[Media Type].[Sunday Paper]}\n"
            + "{[Promotion].[Media Type].[Bulk Mail]}\n"
            + "{[Promotion].[Media Type].[Daily Paper]}\n"
            + "{[Promotion].[Media Type].[Street Handout]}\n"
            + "{[Promotion].[Media Type].[Radio]}\n"
            + "{[Promotion].[Media Type].[In-Store Coupon]}\n"
            + "Row #0: 46,582\n"
            + "Row #1: 3,490\n"
            + "Row #2: 2,704\n"
            + "Row #3: 2,327\n"
            + "Row #4: 1,344\n"
            + "Row #5: 1,254\n"
            + "Row #6: 1,108\n"
            + "Row #7: 1,085\n"
            + "Row #8: 784\n"
            + "Row #9: 733\n"
            + "Row #10: 651\n"
            + "Row #11: 473\n"
            + "Row #12: 40\n"
            + "Row #13: 35\n");

        // The bottom medium in 1997 is Radio, with $2454 in sales.
        // The bottom medium in 1997.Q2 is In-Store Coupon, with $35 in sales,
        //  whereas Radio has $40 in sales in 1997.Q2.

        assertQueryReturns(
            "WITH\n"
            + "  SET [Bottom Media] AS 'BottomCount([Promotion].[Media Type].children, 1, [Measures].[Unit Sales])' \n"
            + "  MEMBER [Measures].[Unit Sales for Bottom Media] AS 'Sum([Bottom Media], [Measures].[Unit Sales])'\n"
            + "SELECT {[Measures].[Unit Sales for Bottom Media]} ON COLUMNS,\n"
            + " {[Time].[1997], [Time].[1997].[Q2]} ON ROWS\n"
            + "FROM [Sales]",
            // Note that Row #1 gives 40. 35 would be wrong.
            // [In-Store Coupon], which was bottom for 1997.Q2 but not for
            // 1997.
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales for Bottom Media]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Row #0: 2,454\n"
            + "Row #1: 40\n");

        assertQueryReturns(
            "WITH\n"
            + "  SET [TopMedia] AS 'TopCount([Promotion].[Media Type].children, 3, [Measures].[Store Sales])' \n"
            + "  MEMBER [Measures].[California sales for Top Media] AS 'Sum([TopMedia], [Measures].[Store Sales])'\n"
            + "SELECT \n"
            + "  CrossJoin({[Stores], [Stores].[USA].[CA]},\n"
            + "    {[Time].[1997].[Q1], [Time].[1997].[Q2]}) ON COLUMNS,\n"
            + " {[Product], [Product].children} ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE [Measures].[California sales for Top Media]",

            "Axis #0:\n"
            + "{[Measures].[California sales for Top Media]}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[All Stores], [Time].[Time].[1997].[Q1]}\n"
            + "{[Store].[Stores].[All Stores], [Time].[Time].[1997].[Q2]}\n"
            + "{[Store].[Stores].[USA].[CA], [Time].[Time].[1997].[Q1]}\n"
            + "{[Store].[Stores].[USA].[CA], [Time].[Time].[1997].[Q2]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[All Products]}\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n"
            + "Row #0: 108,249.52\n"
            + "Row #0: 107,649.93\n"
            + "Row #0: 29,482.53\n"
            + "Row #0: 28,953.02\n"
            + "Row #1: 8,930.95\n"
            + "Row #1: 9,551.93\n"
            + "Row #1: 2,721.23\n"
            + "Row #1: 2,444.78\n"
            + "Row #2: 78,375.66\n"
            + "Row #2: 77,219.13\n"
            + "Row #2: 21,165.50\n"
            + "Row #2: 20,924.43\n"
            + "Row #3: 20,942.91\n"
            + "Row #3: 20,878.87\n"
            + "Row #3: 5,595.80\n"
            + "Row #3: 5,583.81\n");
    }

    public void testOrderedNamedSet() {
        // From http://www.developersdex.com
        assertQueryReturns(
            "WITH SET [SET1] AS\n"
            + "'ORDER ({[Education Level].[Education Level].Members}, [Gender].[All Gender].[F], ASC)'\n"
            + "MEMBER [Gender].[RANK1] AS 'rank([Education Level].currentmember, [SET1])'\n"
            + "select\n"
            + "{[Customer].[Gender].[All Gender].[F], [Gender].[RANK1]} on columns,\n"
            + "{[Education Level].[Education Level].Members} on rows\n"
            + "from Sales\n"
            + "where ([Measures].[Store Sales])",
            // MSAS gives results as below, except ranks are displayed as
            // integers, e.g. '1'.
            "Axis #0:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #1:\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[RANK1]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Education Level].[Bachelors Degree]}\n"
            + "{[Customer].[Education Level].[Graduate Degree]}\n"
            + "{[Customer].[Education Level].[High School Degree]}\n"
            + "{[Customer].[Education Level].[Partial College]}\n"
            + "{[Customer].[Education Level].[Partial High School]}\n"
            + "Row #0: 72,119.26\n"
            + "Row #0: 3\n"
            + "Row #1: 17,641.64\n"
            + "Row #1: 1\n"
            + "Row #2: 81,112.23\n"
            + "Row #2: 4\n"
            + "Row #3: 27,175.97\n"
            + "Row #3: 2\n"
            + "Row #4: 82,177.11\n"
            + "Row #4: 5\n");

        assertQueryReturns(
            "WITH SET [SET1] AS\n"
            + "'ORDER ({[Education Level].[Education Level].Members}, [Gender].[All Gender].[F], ASC)'\n"
            + "MEMBER [Gender].[RANK1] AS 'rank([Education Level].currentmember, [SET1])'\n"
            + "select\n"
            + "{[Customer].[Gender].[All Gender].[F], [Gender].[RANK1]} on columns,\n"
            + "{[Education Level].[Education Level].Members} on rows\n"
            + "from Sales\n"
            + "where ([Measures].[Profit])",
            // MSAS gives results as below. The ranks are (correctly) 0
            // because profit is a calc member.
            "Axis #0:\n"
            + "{[Measures].[Profit]}\n"
            + "Axis #1:\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[RANK1]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Education Level].[Bachelors Degree]}\n"
            + "{[Customer].[Education Level].[Graduate Degree]}\n"
            + "{[Customer].[Education Level].[High School Degree]}\n"
            + "{[Customer].[Education Level].[Partial College]}\n"
            + "{[Customer].[Education Level].[Partial High School]}\n"
            + "Row #0: $43,382.33\n"
            + "Row #0: $0.00\n"
            + "Row #1: $10,599.59\n"
            + "Row #1: $0.00\n"
            + "Row #2: $48,766.50\n"
            + "Row #2: $0.00\n"
            + "Row #3: $16,306.05\n"
            + "Row #3: $0.00\n"
            + "Row #4: $49,394.27\n"
            + "Row #4: $0.00\n");

        // Solve order fixes the problem.
        assertQueryReturns(
            "WITH SET [SET1] AS\n"
            + "'ORDER ({[Education Level].[Education Level].Members}, [Gender].[F], ASC)'\n"
            + "MEMBER [Gender].[RANK1] AS 'rank([Education Level].currentmember, [SET1])', \n"
            + "  SOLVE_ORDER = 10\n"
            + "select\n"
            + "{[Customer].[Gender].[F], [Gender].[RANK1]} on columns,\n"
            + "{[Education Level].[Education Level].Members} on rows\n"
            + "from Sales\n"
            + "where ([Measures].[Profit])",
            // MSAS gives results as below.
            "Axis #0:\n"
            + "{[Measures].[Profit]}\n"
            + "Axis #1:\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[RANK1]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Education Level].[Bachelors Degree]}\n"
            + "{[Customer].[Education Level].[Graduate Degree]}\n"
            + "{[Customer].[Education Level].[High School Degree]}\n"
            + "{[Customer].[Education Level].[Partial College]}\n"
            + "{[Customer].[Education Level].[Partial High School]}\n"
            + "Row #0: $43,382.33\n"
            + "Row #0: 3\n"
            + "Row #1: $10,599.59\n"
            + "Row #1: 1\n"
            + "Row #2: $48,766.50\n"
            + "Row #2: 4\n"
            + "Row #3: $16,306.05\n"
            + "Row #3: 2\n"
            + "Row #4: $49,394.27\n"
            + "Row #4: 5\n");
    }

    public void testGenerate() {
        assertQueryReturns(
            "with \n"
            + "  member [Measures].[DateName] as \n"
            + "    'Generate({[Time].[1997].[Q1], [Time].[1997].[Q2]}, [Time].[Time].CurrentMember.Name) '\n"
            + "select {[Measures].[DateName]} on columns,\n"
            + " {[Time].[1997].[Q1], [Time].[1997].[Q2]} on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[DateName]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Row #0: Q1Q2\n"
            + "Row #1: Q1Q2\n");

        assertQueryReturns(
            "with \n"
            + "  member [Measures].[DateName] as \n"
            + "    'Generate({[Time].[1997].[Q1], [Time].[1997].[Q2]}, [Time].[Time].CurrentMember.Name, \" and \") '\n"
            + "select {[Measures].[DateName]} on columns,\n"
            + " {[Time].[1997].[Q1], [Time].[1997].[Q2]} on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[DateName]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Row #0: Q1 and Q2\n"
            + "Row #1: Q1 and Q2\n");
    }

    public void testNamedSetAgainstCube() {
        final TestContext tc =
            getTestContext().legacy().withSchemaProcessor(
                NamedSetsInCubeProcessor.class);
        // Set defined against cube, using 'formula' attribute.
        tc.assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[CA Cities]} ON ROWS\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[USA].[CA].[Alameda]}\n"
            + "{[Store].[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[Store].[USA].[CA].[San Francisco]}\n"
            + "Row #0: \n"
            + "Row #1: 21,333\n"
            + "Row #2: 25,663\n"
            + "Row #3: 25,635\n"
            + "Row #4: 2,117\n");

        // Set defined against cube, in terms of another set, and using
        // '<Formula>' element.
        tc.assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Top CA Cities]} ON ROWS\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[Store].[USA].[CA].[San Diego]}\n"
            + "Row #0: 25,663\n"
            + "Row #1: 25,635\n");

        // Override named set in query.
        tc.assertQueryReturns(
            "WITH SET [CA Cities] AS '{[Store].[USA].[OR].[Portland]}' "
            + "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[CA Cities]} ON ROWS\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[USA].[OR].[Portland]}\n"
            + "Row #0: 26,079\n");

        // When [CA Cities] is overridden, does the named set [Top CA Cities],
        // which is derived from it, use the new definition? No. It stays
        // bound to the original definition.
        tc.assertQueryReturns(
            "WITH SET [CA Cities] AS '{[Store].[USA].[OR].[Portland]}' "
            + "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Top CA Cities]} ON ROWS\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[Store].[USA].[CA].[San Diego]}\n"
            + "Row #0: 25,663\n"
            + "Row #1: 25,635\n");
    }

    public void testNamedSetAgainstSchema() {
        final TestContext tc =
            getTestContext().legacy().withSchemaProcessor(
                NamedSetsInCubeAndSchemaProcessor.class);
        tc.assertQueryReturns(
            "SELECT {[Measures].[Store Sales]} on columns,\n"
            + " Intersect([Top CA Cities], [Top USA Stores]) on rows\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[USA].[CA].[Los Angeles]}\n"
            + "Row #0: 54,545.28\n");
        // Use non-existent set.
        tc.assertQueryThrows(
            "SELECT {[Measures].[Store Sales]} on columns,\n"
            + " Intersect([Top CA Cities], [Top Ukrainian Cities]) on rows\n"
            + "FROM [Sales]",
            "MDX object '[Top Ukrainian Cities]' not found in cube 'Sales'");
    }

    public void testBadNamedSet() {
        final TestContext tc = TestContext.instance().create(
            null,
            null,
            null,
            "<NamedSet name=\"Bad\" formula=\"{[Store].[USA].[WA].Children}}\"/>",
            null,
            null);
        tc.assertQueryThrows(
            "SELECT {[Measures].[Store Sales]} on columns,\n"
            + " {[Bad]} on rows\n"
            + "FROM [Sales]", "Named set 'Bad' has bad formula");
    }

    public void testNamedSetMustBeSet() {
        Result result;
        String queryString;
        String pattern;

        // Formula for a named set must not be a member.
        queryString =
            "with set [Foo] as ' [Store].CurrentMember  '"
            + "select {[Foo]} on columns from [Sales]";
        pattern = "Set expression '[Foo]' must be a set";
        assertQueryThrows(queryString, pattern);

        // Formula for a named set must not be a dimension.
        queryString =
            "with set [Foo] as ' [Store] '"
            + "select {[Foo]} on columns from [Sales]";
        assertQueryThrows(queryString, pattern);

        // Formula for a named set must not be a level.
        queryString =
            "with set [Foo] as ' [Store].[Store Country] '"
            + "select {[Foo]} on columns from [Sales]";
        assertQueryThrows(queryString, pattern);

        // Formula for a named set must not be a cube name.
        queryString =
            "with set [Foo] as ' [Sales] '"
            + "select {[Foo]} on columns from [Sales]";
        assertQueryThrows(
            queryString,
            "MDX object '[Sales]' not found in cube 'Sales'");

        // Formula for a named set must not be a string.
        queryString =
            "with set [Foo] as ' \"foobar\" '"
            + "select {[Foo]} on columns from [Sales]";
        assertQueryThrows(queryString, pattern);

        // Formula for a named set must not be a number.
        queryString =
            "with set [Foo] as ' -1.45 '"
            + "select {[Foo]} on columns from [Sales]";
        assertQueryThrows(queryString, pattern);

        // Formula for a named set must not be a tuple.
        queryString =
            "with set [Foo] as ' ([Gender].[M], [Marital Status].[S]) '"
            + "select {[Foo]} on columns from [Sales]";
        assertQueryThrows(queryString, pattern);

        // Formula for a named set may be a set of tuples.
        queryString =
            "with set [Foo] as ' CrossJoin([Gender].members, [Marital Status].members) '"
            + "select {[Foo]} on columns from [Sales]";
        result = executeQuery(queryString);
        Util.discard(result);

        // Formula for a named set may be a set of members.
        queryString =
            "with set [Foo] as ' [Gender].members '"
            + "select {[Foo]} on columns from [Sales]";
        result = executeQuery(queryString);
        Util.discard(result);
    }

    public void testNamedSetsMixedWithCalcMembers()
    {
        final TestContext tc =
            getTestContext().legacy().withSchemaProcessor(
                MixedNamedSetSchemaProcessor.class);
        tc.assertQueryReturns(
            "select {\n"
            + "    [Measures].[Unit Sales],\n"
            + "    [Measures].[CA City Sales]} on columns,\n"
            + "  Crossjoin(\n"
            + "    [Time].[1997].Children,\n"
            + "    [Top Products In CA]) on rows\n"
            + "from [Sales]\n"
            + "where [Marital Status].[S]",
            "Axis #0:\n"
            + "{[Marital Status].[Marital Status].[S]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[CA City Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1], [Product].[Product].[Food].[Produce]}\n"
            + "{[Time].[Time].[1997].[Q1], [Product].[Product].[Food].[Snack Foods]}\n"
            + "{[Time].[Time].[1997].[Q1], [Product].[Product].[Non-Consumable].[Household]}\n"
            + "{[Time].[Time].[1997].[Q2], [Product].[Product].[Food].[Produce]}\n"
            + "{[Time].[Time].[1997].[Q2], [Product].[Product].[Food].[Snack Foods]}\n"
            + "{[Time].[Time].[1997].[Q2], [Product].[Product].[Non-Consumable].[Household]}\n"
            + "{[Time].[Time].[1997].[Q3], [Product].[Product].[Food].[Produce]}\n"
            + "{[Time].[Time].[1997].[Q3], [Product].[Product].[Food].[Snack Foods]}\n"
            + "{[Time].[Time].[1997].[Q3], [Product].[Product].[Non-Consumable].[Household]}\n"
            + "{[Time].[Time].[1997].[Q4], [Product].[Product].[Food].[Produce]}\n"
            + "{[Time].[Time].[1997].[Q4], [Product].[Product].[Food].[Snack Foods]}\n"
            + "{[Time].[Time].[1997].[Q4], [Product].[Product].[Non-Consumable].[Household]}\n"
            + "Row #0: 4,872\n"
            + "Row #0: $1,218.0\n"
            + "Row #1: 3,746\n"
            + "Row #1: $840.0\n"
            + "Row #2: 3,425\n"
            + "Row #2: $817.0\n"
            + "Row #3: 4,633\n"
            + "Row #3: $1,320.0\n"
            + "Row #4: 3,588\n"
            + "Row #4: $1,058.0\n"
            + "Row #5: 3,149\n"
            + "Row #5: $938.0\n"
            + "Row #6: 4,651\n"
            + "Row #6: $1,353.0\n"
            + "Row #7: 3,895\n"
            + "Row #7: $1,134.0\n"
            + "Row #8: 3,395\n"
            + "Row #8: $1,029.0\n"
            + "Row #9: 5,160\n"
            + "Row #9: $1,550.0\n"
            + "Row #10: 4,160\n"
            + "Row #10: $1,301.0\n"
            + "Row #11: 3,808\n"
            + "Row #11: $1,166.0\n");
    }

    public void testNamedSetAndUnion() {
        assertQueryReturns(
            "with set [Set Education Level] as\n"
            + "   '{([Education Level].[All Education Levels].[Bachelors Degree]),\n"
            + "     ([Education Level].[All Education Levels].[Graduate Degree])}'\n"
            + "select\n"
            + "   {[Measures].[Unit Sales],\n"
            + "    [Measures].[Store Cost],\n"
            + "    [Measures].[Store Sales]} ON COLUMNS,\n"
            + "   UNION(\n"
            + "      CROSSJOIN(\n"
            + "         {[Time].[1997].[Q1]},\n"
            + "          [Set Education Level]),\n"
            + "      {([Time].[1997].[Q1],\n"
            + "        [Education Level].[All Education Levels].[Graduate Degree])}) ON ROWS\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1], [Customer].[Education Level].[Bachelors Degree]}\n"
            + "{[Time].[Time].[1997].[Q1], [Customer].[Education Level].[Graduate Degree]}\n"
            + "Row #0: 17,066\n"
            + "Row #0: 14,234.10\n"
            + "Row #0: 35,699.43\n"
            + "Row #1: 3,637\n"
            + "Row #1: 3,030.82\n"
            + "Row #1: 7,583.71\n");
    }

    /**
     * Tests that named sets never depend on anything.
     */
    public void testNamedSetDependencies() {
        final TestContext tc =
            getTestContext().legacy().withSchemaProcessor(
                NamedSetsInCubeProcessor.class);
        tc.assertSetExprDependsOn(
            "[Top CA Cities]",
            Collections.<String>emptySet());
    }

    /**
     * Test csae for bug 1971080, "hierarchize(named set) causes attempt to
     * sort immutable list".
     */
    public void testHierarchizeNamedSetImmutable() {
        assertQueryReturns(
            "with set necj as\n"
            + "NonEmptyCrossJoin([Customers].[Name].members,[Store].[Store Name].members)\n"
            + "select\n"
            + "{[Measures].[Unit Sales]} on columns,\n"
            + "Tail(hierarchize(necj),5) on rows\n"
            + "from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Customers].[USA].[WA].[Yakima].[Tracy Meyer], [Store].[Stores].[USA].[WA].[Yakima].[Store 23]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Yakima].[Vanessa Thompson], [Store].[Stores].[USA].[WA].[Yakima].[Store 23]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Yakima].[Velma Lykes], [Store].[Stores].[USA].[WA].[Yakima].[Store 23]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Yakima].[William Battaglia], [Store].[Stores].[USA].[WA].[Yakima].[Store 23]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Yakima].[Wilma Fink], [Store].[Stores].[USA].[WA].[Yakima].[Store 23]}\n"
            + "Row #0: 44\n"
            + "Row #1: 128\n"
            + "Row #2: 55\n"
            + "Row #3: 149\n"
            + "Row #4: 89\n");
    }

    public void testCurrentAndCurrentOrdinal() {
        assertQueryReturns(
            "with set [Gender Marital Status] as\n"
            + " [Gender].members * [Marital Status].members\n"
            + "member [Measures].[GMS Ordinal] as\n"
            + " [Gender Marital Status].CurrentOrdinal\n"
            + "member [Measures].[GMS Name]\n"
            + " as TupleToStr([Gender Marital Status].Current)\n"
            + "select {\n"
            + "  [Measures].[Unit Sales],\n"
            + "  [Measures].[GMS Ordinal],\n"
            + "  [Measures].[GMS Name]} on 0,\n"
            + " {[Gender Marital Status]} on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[GMS Ordinal]}\n"
            + "{[Measures].[GMS Name]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[All Marital Status]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[S]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[All Marital Status]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[All Marital Status]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 0\n"
            + "Row #0: ([Customer].[Gender].[All Gender], [Customer].[Marital Status].[All Marital Status])\n"
            + "Row #1: 131,796\n"
            + "Row #1: 1\n"
            + "Row #1: ([Customer].[Gender].[All Gender], [Customer].[Marital Status].[M])\n"
            + "Row #2: 134,977\n"
            + "Row #2: 2\n"
            + "Row #2: ([Customer].[Gender].[All Gender], [Customer].[Marital Status].[S])\n"
            + "Row #3: 131,558\n"
            + "Row #3: 3\n"
            + "Row #3: ([Customer].[Gender].[F], [Customer].[Marital Status].[All Marital Status])\n"
            + "Row #4: 65,336\n"
            + "Row #4: 4\n"
            + "Row #4: ([Customer].[Gender].[F], [Customer].[Marital Status].[M])\n"
            + "Row #5: 66,222\n"
            + "Row #5: 5\n"
            + "Row #5: ([Customer].[Gender].[F], [Customer].[Marital Status].[S])\n"
            + "Row #6: 135,215\n"
            + "Row #6: 6\n"
            + "Row #6: ([Customer].[Gender].[M], [Customer].[Marital Status].[All Marital Status])\n"
            + "Row #7: 66,460\n"
            + "Row #7: 7\n"
            + "Row #7: ([Customer].[Gender].[M], [Customer].[Marital Status].[M])\n"
            + "Row #8: 68,755\n"
            + "Row #8: 8\n"
            + "Row #8: ([Customer].[Gender].[M], [Customer].[Marital Status].[S])\n");
    }

    /**
     * Test case for issue on developers list which involves a named set and a
     * range in the WHERE clause. Current Mondrian behavior appears to be
     * correct.
     */
    public void testNamedSetRangeInSlicer() {
        String expected =
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
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
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Mary Francis Benigar], [Measures].[Unit Sales]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[James Horvat], [Measures].[Unit Sales]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Matt Bellah], [Measures].[Unit Sales]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Ida Rodriguez], [Measures].[Unit Sales]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane].[Kristin Miller], [Measures].[Unit Sales]}\n"
            + "Row #0: 422\n"
            + "Row #0: 369\n"
            + "Row #0: 363\n"
            + "Row #0: 344\n"
            + "Row #0: 323\n";
        assertQueryReturns(
            "SELECT\n"
            + "NON EMPTY TopCount([Customers].[Name].Members, 5, [Measures].[Unit Sales]) * [Measures].[Unit Sales] on 0\n"
            + "FROM [Sales]\n"
            + "WHERE [Time].[1997].[Q1].[1]:[Time].[1997].[Q4].[10]",
            expected);
        // as above, but remove NON EMPTY
        assertQueryReturns(
            "SELECT\n"
            + "TopCount([Customers].[Name].Members, 5, [Measures].[Unit Sales]) * [Measures].[Unit Sales] on 0\n"
            + "FROM [Sales]\n"
            + "WHERE [Time].[1997].[Q1].[1]:[Time].[1997].[Q4].[10]",
            expected);
        // as above, but with DISTINCT
        assertQueryReturns(
            "SELECT\n"
            + "TopCount(Distinct([Customers].[Name].Members), 5, [Measures].[Unit Sales]) * [Measures].[Unit Sales] on 0\n"
            + "FROM [Sales]\n"
            + "WHERE [Time].[1997].[Q1].[1]:[Time].[1997].[Q4].[10]",
            expected);
        // As above, but convert TopCount expression to a named set. Named
        // sets are evaluated after the slicer but before any axes. I.e. not
        // in the context of any particular position on ROWS or COLUMNS, nor
        // inheriting the NON EMPTY constraint on the axis.
        assertQueryReturns(
            "WITH SET [Top Count] AS\n"
            + "  TopCount([Customers].[Name].Members, 5, [Measures].[Unit Sales])\n"
            + "SELECT [Top Count] * [Measures].[Unit Sales] on 0\n"
            + "FROM [Sales]\n"
            + "WHERE [Time].[1997].[Q1].[1]:[Time].[1997].[Q4].[10]",
            expected);
        // as above, but with DISTINCT
        if (false)
        assertQueryReturns(
            "WITH SET [Top Count] AS\n"
            + "{\n"
            + "  TopCount(\n"
            + "    Distinct([Customers].[Name].Members),\n"
            + "    5,\n"
            + "    [Measures].[Unit Sales])\n"
            + "}\n"
            + "SELECT [Top Count] * [Measures].[Unit Sales] on 0\n"
            + "FROM [Sales]\n"
            + "WHERE [Time].[1997].[Q1].[1]:[Time].[1997].[Q4].[10]",
            expected);
    }

    /**
     * Variant of {@link #testNamedSetRangeInSlicer()} that calls
     * {@link mondrian.test.CompoundSlicerTest#testBugMondrian899()} to
     * prime the cache and therefore fails even when run standalone.
     *
     * <p>Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-1203">
     * MONDRIAN-1203, "Error 'Failed to load all aggregations after 10 passes'
     * while evaluating composite slicer"</a>.</p>
     */
    public void testNamedSetRangeInSlicerPrimed() {
        new CompoundSlicerTest().testBugMondrian899();
        testNamedSetRangeInSlicer();
    }

    /**
     * Dynamic schema processor which adds two named sets to a the first cube
     * in a schema.
     */
    public static class NamedSetsInCubeProcessor
        extends FilterDynamicSchemaProcessor
    {
        public String filter(
            String schemaUrl,
            Util.PropertyList connectInfo,
            InputStream stream) throws Exception
        {
            String s = super.filter(schemaUrl, connectInfo, stream);
            int i = s.indexOf("</Cube>");
            return
                s.substring(0, i) + "\n"
                + "<NamedSet name=\"CA Cities\" formula=\"{[Store].[USA].[CA].Children}\"/>\n"
                + "<NamedSet name=\"Top CA Cities\">\n"
                + "  <Formula>TopCount([CA Cities], 2, [Measures].[Unit Sales])</Formula>\n"
                + "</NamedSet>\n"
                + s.substring(i);
        }
    }

    /**
     * Dynamic schema processor which adds two named sets to a the first cube
     * in a schema.
     */
    public static class NamedSetsInCubeAndSchemaProcessor
        extends FilterDynamicSchemaProcessor
    {
        protected String filter(
            String schemaUrl,
            Util.PropertyList connectInfo,
            InputStream stream) throws Exception
        {
            String s = super.filter(schemaUrl, connectInfo, stream);
            int i = s.indexOf("</Cube>");
            s =
                s.substring(0, i) + "\n"
                + "<NamedSet name=\"CA Cities\" formula=\"{[Store].[USA].[CA].Children}\"/>\n"
                + "<NamedSet name=\"Top CA Cities\">\n"
                + "  <Formula>TopCount([CA Cities], 2, [Measures].[Unit Sales])</Formula>\n"
                + "</NamedSet>\n"
                + s.substring(i);
            // Schema-level named sets occur after <Cube> and <VirtualCube> and
            // before <Role> elements.
            i = s.indexOf("<Role");
            if (i < 0) {
                i = s.indexOf("</Schema>");
            }
            s =
                s.substring(0, i)
                + "\n"
                + "<NamedSet name=\"CA Cities\" formula=\"{[Store].[USA].[WA].Children}\"/>\n"
                + "<NamedSet name=\"Top USA Stores\">\n"
                + "  <Formula>TopCount(Descendants([Store].[USA]), 7)</Formula>\n"
                + "</NamedSet>\n"
                + s.substring(i);
            return s;
        }
    }

    /**
     * Dynamic schema processor which adds a named set which has a syntax
     * error.
     */
    public static class MixedNamedSetSchemaProcessor
        extends FilterDynamicSchemaProcessor
    {
        protected String filter(
            String schemaUrl,
            Util.PropertyList connectInfo,
            InputStream stream) throws Exception
        {
            String s = super.filter(schemaUrl, connectInfo, stream);
            // Declare mutually dependent named sets and calculated members
            // at the end of a cube:
            //   m2 references s1
            //   s1 references s0 and m1 and m0
            int i = s.indexOf("</Cube>");
            s = s.substring(0, i) + "\n"
                // member [CA City Sales] references set [CA Cities]
                + "  <CalculatedMember\n"
                + "      name=\"CA City Sales\"\n"
                + "      dimension=\"Measures\"\n"
                + "      visible=\"false\"\n"
                + "      formula=\"Aggregate([CA Cities], [Measures].[Unit Sales])\">\n"
                + "    <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"$#,##0.0\"/>\n"
                + "  </CalculatedMember>\n"

                // set [Top Products In CA] references member [CA City Sales]
                + "<NamedSet name=\"Top Products In CA\">\n"
                + "  <Formula>TopCount([Product].[Product Department].MEMBERS, 3, ([Time].[1997].[Q3], [Measures].[CA City Sales]))</Formula>\n"
                + "</NamedSet>\n"
                + "<NamedSet name=\"CA Cities\" formula=\"{[Store].[USA].[CA].Children}\"/>\n"
                + s.substring(i);
            return s;
        }
    }
}

// End NamedSetTest.java
