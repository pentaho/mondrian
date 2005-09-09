/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.test;

import mondrian.olap.*;

/**
 * Unit-test for named sets, in all their various forms: <code>WITH SET</code>,
 * sets defined against cubes, virtual cubes, and at the schema level.
 *
 * @author jhyde
 * @since April 30, 2005
 * @version $Id$
 **/
public class NamedSetTest extends FoodMartTestCase {

    /**
     * Set defined in query according measures, hence context-dependent.
     */
    public void testNamedSet() {
        assertQueryReturns(
                "WITH" + nl +
                "    SET [Top Sellers]" + nl +
                "AS " + nl +
                "    'TopCount([Warehouse].[Warehouse Name].MEMBERS, 10, " + nl +
                "        [Measures].[Warehouse Sales])'" + nl +
                "SELECT " + nl +
                "    {[Measures].[Warehouse Sales]} ON COLUMNS," + nl +
                "        {[Top Sellers]} ON ROWS" + nl +
                "FROM " + nl +
                "    [Warehouse]" + nl +
                "WHERE " + nl +
                "    [Time].[Year].[1997]",
                "Axis #0:" + nl +
                "{[Time].[1997]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Warehouse Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Warehouse].[All Warehouses].[USA].[OR].[Salem].[Treehouse Distribution]}" + nl +
                "{[Warehouse].[All Warehouses].[USA].[WA].[Tacoma].[Jorge Garcia, Inc.]}" + nl +
                "{[Warehouse].[All Warehouses].[USA].[CA].[Los Angeles].[Artesia Warehousing, Inc.]}" + nl +
                "{[Warehouse].[All Warehouses].[USA].[CA].[San Diego].[Jorgensen Service Storage]}" + nl +
                "{[Warehouse].[All Warehouses].[USA].[WA].[Bremerton].[Destination, Inc.]}" + nl +
                "{[Warehouse].[All Warehouses].[USA].[WA].[Seattle].[Quality Warehousing and Trucking]}" + nl +
                "{[Warehouse].[All Warehouses].[USA].[WA].[Spokane].[Jones International]}" + nl +
                "{[Warehouse].[All Warehouses].[USA].[WA].[Yakima].[Maddock Stored Foods]}" + nl +
                "{[Warehouse].[All Warehouses].[USA].[CA].[Beverly Hills].[Big  Quality Warehouse]}" + nl +
                "{[Warehouse].[All Warehouses].[USA].[OR].[Portland].[Quality Distribution, Inc.]}" + nl +
                "Row #0: 31116.3749" + nl +
                "Row #1: 30743.7722" + nl +
                "Row #2: 22907.9591" + nl +
                "Row #3: 22869.7904" + nl +
                "Row #4: 22187.4183" + nl +
                "Row #5: 22046.9416" + nl +
                "Row #6: 10879.6737" + nl +
                "Row #7: 10212.2007" + nl +
                "Row #8: 10156.4955" + nl +
                "Row #9: 7718.6783" + nl);
    }

    /**
     * Set defined on top of calc member.
     */
    public void testNamedSetOnMember() {
        assertQueryReturns(
                "WITH" + nl +
                "    MEMBER [Measures].[Profit]" + nl +
                "AS '[Measures].[Warehouse Sales] - [Measures].[Warehouse Cost] '" + nl +
                "    SET [Top Performers]" + nl +
                "AS " + nl +
                "    'TopCount([Warehouse].[Warehouse Name].MEMBERS, 5, " + nl +
                "        [Measures].[Profit])'" + nl +
                "SELECT " + nl +
                "    {[Measures].[Profit]} ON COLUMNS," + nl +
                "        {[Top Performers]} ON ROWS" + nl +
                "FROM " + nl +
                "    [Warehouse]" + nl +
                "WHERE " + nl +
                "    [Time].[Year].[1997].[Q2]",
                "Axis #0:" + nl +
                "{[Time].[1997].[Q2]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Profit]}" + nl +
                "Axis #2:" + nl +
                "{[Warehouse].[All Warehouses].[USA].[WA].[Bremerton].[Destination, Inc.]}" + nl +
                "{[Warehouse].[All Warehouses].[USA].[CA].[San Diego].[Jorgensen Service Storage]}" + nl +
                "{[Warehouse].[All Warehouses].[USA].[OR].[Salem].[Treehouse Distribution]}" + nl +
                "{[Warehouse].[All Warehouses].[USA].[CA].[Los Angeles].[Artesia Warehousing, Inc.]}" + nl +
                "{[Warehouse].[All Warehouses].[USA].[WA].[Seattle].[Quality Warehousing and Trucking]}" + nl +
                "Row #0: 4516.7561" + nl +
                "Row #1: 4189.3604" + nl +
                "Row #2: 4169.318499999999" + nl +
                "Row #3: 3848.6467" + nl +
                "Row #4: 3708.717" + nl);
    }

    /**
     * Set defined by explicit tlist in query.
     */
    public void testNamedSetAsList() {
        assertQueryReturns("WITH SET [ChardonnayChablis] AS" + nl +
                "   '{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chardonnay]," + nl +
                "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chardonnay]," + nl +
                "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chardonnay]," + nl +
                "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chardonnay]," + nl +
                "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chardonnay]," + nl +
                "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chablis Wine]," + nl +
                "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chablis Wine]," + nl +
                "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chablis Wine]," + nl +
                "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chablis Wine]," + nl +
                "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chablis Wine]}'" + nl +
                "SELECT" + nl +
                "   [ChardonnayChablis] ON COLUMNS," + nl +
                "   {Measures.[Unit Sales]} ON ROWS" + nl +
                "FROM Sales",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chardonnay]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chardonnay]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chardonnay]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chardonnay]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chardonnay]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chablis Wine]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chablis Wine]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chablis Wine]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chablis Wine]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chablis Wine]}" + nl +
                "Axis #2:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Row #0: 192" + nl +
                "Row #0: 189" + nl +
                "Row #0: 170" + nl +
                "Row #0: 164" + nl +
                "Row #0: 173" + nl +
                "Row #0: 163" + nl +
                "Row #0: 209" + nl +
                "Row #0: 136" + nl +
                "Row #0: 140" + nl +
                "Row #0: 185" + nl);
    }

    /**
     * Set defined using filter expression.
     *
     * <p>Probably doesn't work because we don't support the InStr VB function.
     */
    public void _testIntrinsic() {
        assertQueryReturns("WITH SET [ChardonnayChablis] AS" + nl +
                "   'Filter([Product].Members, (InStr(1, [Product].CurrentMember.Name, \"chardonnay\") <> 0) OR (InStr(1, [Product].CurrentMember.Name, \"chablis\") <> 0))'" + nl +
                "SELECT" + nl +
                "   [ChardonnayChablis] ON COLUMNS," + nl +
                "   {Measures.[Unit Sales]} ON ROWS" + nl +
                "FROM Sales",
                "xxxx");
    }

    /**
     * Tests a named set defined in a query which consists of tuples.
     */
    public void testNamedSetCrossJoin() {
        assertQueryReturns("WITH" + nl +
                "    SET [Store Types by Country]" + nl +
                "AS" + nl +
                "    'CROSSJOIN({[Store].[Store Country].MEMBERS}," + nl +
                "               {[Store Type].[Store Type].MEMBERS})'" + nl +
                "SELECT" + nl +
                "    {[Measures].[Units Ordered]} ON COLUMNS," + nl +
                "    NON EMPTY {[Store Types by Country]} ON ROWS" + nl +
                "FROM" + nl +
                "    [Warehouse]" + nl +
                "WHERE" + nl +
                "    [Time].[1997].[Q2]",
                "Axis #0:" + nl +
                "{[Time].[1997].[Q2]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Units Ordered]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[USA], [Store Type].[All Store Types].[Deluxe Supermarket]}" + nl +
                "{[Store].[All Stores].[USA], [Store Type].[All Store Types].[Mid-Size Grocery]}" + nl +
                "{[Store].[All Stores].[USA], [Store Type].[All Store Types].[Supermarket]}" + nl +
                "Row #0: 16843.0" + nl +
                "Row #1: 2295.0" + nl +
                "Row #2: 34856.0" + nl);
    }

    // Disabled because fails with error '<Value> = <String> is not a function'
    // Also, don't know whether [oNormal] will correctly resolve to
    // [Store Type].[oNormal].
    public void _testXxx() {
        assertQueryReturns(
                "WITH MEMBER [Store Type].[All Store Type].[oNormal] AS 'Aggregate(Filter( [Customers].[Name].Members, [Customers].CurrentMember.Properties(\"Member Card\") = \"Normal\") * {[Store Type].[All Store Type]} )'" + nl +
                "MEMBER [Store Type].[All Store Type].[oBronze] AS 'Aggregate(Filter( [Customers].[Name].Members, [Customers].CurrentMember.Properties(\"Member Card\") = \"Bronze\") * {[Store Type].[All Store Type]} )'" + nl +
                "MEMBER [Store Type].[All Store Type].[oGolden] AS 'Aggregate(Filter( [Customers].[Name].Members, [Customers].CurrentMember.Properties(\"Member Card\") = \"Golden\") * {[Store Type].[All Store Type]} )'" + nl +
                "MEMBER [Store Type].[All Store Type].[oSilver] AS 'Aggregate(Filter( [Customers].[Name].Members, [Customers].CurrentMember.Properties(\"Member Card\") = \"Silver\") * {[Store Type].[All Store Type]} )'" + nl +
                "SET CardTypes AS '{[oNormal], [oBronze], [oGolden], [oSilver]}'" + nl +
                "SELECT {[Unit Sales]} ON COLUMNS, CardTypes ON ROWS" + nl +
                "FROM Sales",
                "xxxx");
    }

    /**
     * Set used inside expression (Crossjoin).
     */
    public void testNamedSetUsedInCrossJoin() {
        assertQueryReturns(
                "WITH" + nl +
                "  SET [TopMedia] AS 'TopCount([Promotion Media].children, 5, [Measures].[Store Sales])' " + nl +
                "SELECT {[Time].[1997].[Q1], [Time].[1997].[Q2]} ON COLUMNS," + nl +
                " {CrossJoin([TopMedia], [Product].children)} ON ROWS" + nl +
                "FROM [Sales]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Time].[1997].[Q1]}" + nl +
                "{[Time].[1997].[Q2]}" + nl +
                "Axis #2:" + nl +
                "{[Promotion Media].[All Media].[No Media], [Product].[All Products].[Drink]}" + nl +
                "{[Promotion Media].[All Media].[No Media], [Product].[All Products].[Food]}" + nl +
                "{[Promotion Media].[All Media].[No Media], [Product].[All Products].[Non-Consumable]}" + nl +
                "{[Promotion Media].[All Media].[Daily Paper, Radio, TV], [Product].[All Products].[Drink]}" + nl +
                "{[Promotion Media].[All Media].[Daily Paper, Radio, TV], [Product].[All Products].[Food]}" + nl +
                "{[Promotion Media].[All Media].[Daily Paper, Radio, TV], [Product].[All Products].[Non-Consumable]}" + nl +
                "{[Promotion Media].[All Media].[Daily Paper], [Product].[All Products].[Drink]}" + nl +
                "{[Promotion Media].[All Media].[Daily Paper], [Product].[All Products].[Food]}" + nl +
                "{[Promotion Media].[All Media].[Daily Paper], [Product].[All Products].[Non-Consumable]}" + nl +
                "{[Promotion Media].[All Media].[Product Attachment], [Product].[All Products].[Drink]}" + nl +
                "{[Promotion Media].[All Media].[Product Attachment], [Product].[All Products].[Food]}" + nl +
                "{[Promotion Media].[All Media].[Product Attachment], [Product].[All Products].[Non-Consumable]}" + nl +
                "{[Promotion Media].[All Media].[Cash Register Handout], [Product].[All Products].[Drink]}" + nl +
                "{[Promotion Media].[All Media].[Cash Register Handout], [Product].[All Products].[Food]}" + nl +
                "{[Promotion Media].[All Media].[Cash Register Handout], [Product].[All Products].[Non-Consumable]}" + nl +
                "Row #0: 3,970" + nl +
                "Row #0: 4,287" + nl +
                "Row #1: 32,939" + nl +
                "Row #1: 33,238" + nl +
                "Row #2: 8,650" + nl +
                "Row #2: 9,057" + nl +
                "Row #3: 142" + nl +
                "Row #3: 364" + nl +
                "Row #4: 975" + nl +
                "Row #4: 2,523" + nl +
                "Row #5: 250" + nl +
                "Row #5: 603" + nl +
                "Row #6: 464" + nl +
                "Row #6: 66" + nl +
                "Row #7: 3,173" + nl +
                "Row #7: 464" + nl +
                "Row #8: 862" + nl +
                "Row #8: 121" + nl +
                "Row #9: 171" + nl +
                "Row #9: 106" + nl +
                "Row #10: 1,344" + nl +
                "Row #10: 814" + nl +
                "Row #11: 362" + nl +
                "Row #11: 165" + nl +
                "Row #12: (null)" + nl +
                "Row #12: 92" + nl +
                "Row #13: (null)" + nl +
                "Row #13: 933" + nl +
                "Row #14: (null)" + nl +
                "Row #14: 229" + nl);
    }

    public void testAggOnCalcMember() {
        assertQueryReturns(
                "WITH" + nl +
                "  SET [TopMedia] AS 'TopCount([Promotion Media].children, 5, [Measures].[Store Sales])' " + nl +
                "  MEMBER [Measures].[California sales for Top Media] AS 'Sum([TopMedia], ([Store].[USA].[CA], [Measures].[Store Sales]))'" + nl +
                "SELECT {[Time].[1997].[Q1], [Time].[1997].[Q2]} ON COLUMNS," + nl +
                " {[Product].children} ON ROWS" + nl +
                "FROM [Sales]" + nl +
                "WHERE [Measures].[California sales for Top Media]",
                "Axis #0:" + nl +
                "{[Measures].[California sales for Top Media]}" + nl +
                "Axis #1:" + nl +
                "{[Time].[1997].[Q1]}" + nl +
                "{[Time].[1997].[Q2]}" + nl +
                "Axis #2:" + nl +
                "{[Product].[All Products].[Drink]}" + nl +
                "{[Product].[All Products].[Food]}" + nl +
                "{[Product].[All Products].[Non-Consumable]}" + nl +
                "Row #0: 2,725.85" + nl +
                "Row #0: 2,715.56" + nl +
                "Row #1: 21,200.84" + nl +
                "Row #1: 23,263.72" + nl +
                "Row #2: 5,598.71" + nl +
                "Row #2: 6,111.74" + nl);
    }

    public void testContextSensitiveNamedSet() {
        // For reference.
        assertQueryReturns("SELECT {[Measures].[Unit Sales]} ON COLUMNS," + nl +
                "Order([Promotion Media].Children, [Measures].[Unit Sales], DESC) ON ROWS" + nl +
                "FROM [Sales]" + nl +
                "WHERE [Time].[1997]",

                "Axis #0:" + nl +
                "{[Time].[1997]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Promotion Media].[All Media].[No Media]}" + nl +
                "{[Promotion Media].[All Media].[Daily Paper, Radio, TV]}" + nl +
                "{[Promotion Media].[All Media].[Daily Paper]}" + nl +
                "{[Promotion Media].[All Media].[Product Attachment]}" + nl +
                "{[Promotion Media].[All Media].[Daily Paper, Radio]}" + nl +
                "{[Promotion Media].[All Media].[Cash Register Handout]}" + nl +
                "{[Promotion Media].[All Media].[Sunday Paper, Radio]}" + nl +
                "{[Promotion Media].[All Media].[Street Handout]}" + nl +
                "{[Promotion Media].[All Media].[Sunday Paper]}" + nl +
                "{[Promotion Media].[All Media].[Bulk Mail]}" + nl +
                "{[Promotion Media].[All Media].[In-Store Coupon]}" + nl +
                "{[Promotion Media].[All Media].[TV]}" + nl +
                "{[Promotion Media].[All Media].[Sunday Paper, Radio, TV]}" + nl +
                "{[Promotion Media].[All Media].[Radio]}" + nl +
                "Row #0: 195,448" + nl +
                "Row #1: 9,513" + nl +
                "Row #2: 7,738" + nl +
                "Row #3: 7,544" + nl +
                "Row #4: 6,891" + nl +
                "Row #5: 6,697" + nl +
                "Row #6: 5,945" + nl +
                "Row #7: 5,753" + nl +
                "Row #8: 4,339" + nl +
                "Row #9: 4,320" + nl +
                "Row #10: 3,798" + nl +
                "Row #11: 3,607" + nl +
                "Row #12: 2,726" + nl +
                "Row #13: 2,454" + nl);
        // For reference.
        assertQueryReturns("SELECT {[Measures].[Unit Sales]} ON COLUMNS," + nl +
                "Order([Promotion Media].Children, [Measures].[Unit Sales], DESC) ON ROWS" + nl +
                "FROM [Sales]" + nl +
                "WHERE [Time].[1997].[Q2]",

                "Axis #0:" + nl +
                "{[Time].[1997].[Q2]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Promotion Media].[All Media].[No Media]}" + nl +
                "{[Promotion Media].[All Media].[Daily Paper, Radio, TV]}" + nl +
                "{[Promotion Media].[All Media].[Daily Paper, Radio]}" + nl +
                "{[Promotion Media].[All Media].[Sunday Paper, Radio]}" + nl +
                "{[Promotion Media].[All Media].[TV]}" + nl +
                "{[Promotion Media].[All Media].[Cash Register Handout]}" + nl +
                "{[Promotion Media].[All Media].[Sunday Paper, Radio, TV]}" + nl +
                "{[Promotion Media].[All Media].[Product Attachment]}" + nl +
                "{[Promotion Media].[All Media].[Sunday Paper]}" + nl +
                "{[Promotion Media].[All Media].[Bulk Mail]}" + nl +
                "{[Promotion Media].[All Media].[Daily Paper]}" + nl +
                "{[Promotion Media].[All Media].[Street Handout]}" + nl +
                "{[Promotion Media].[All Media].[Radio]}" + nl +
                "{[Promotion Media].[All Media].[In-Store Coupon]}" + nl +
                "Row #0: 46,582" + nl +
                "Row #1: 3,490" + nl +
                "Row #2: 2,704" + nl +
                "Row #3: 2,327" + nl +
                "Row #4: 1,344" + nl +
                "Row #5: 1,254" + nl +
                "Row #6: 1,108" + nl +
                "Row #7: 1,085" + nl +
                "Row #8: 784" + nl +
                "Row #9: 733" + nl +
                "Row #10: 651" + nl +
                "Row #11: 473" + nl +
                "Row #12: 40" + nl +
                "Row #13: 35" + nl);

        // The bottom medium in 1997 is Radio, with $2454 in sales.
        // The bottom medium in 1997.Q2 is In-Store Coupon, with $35 in sales,
        //  whereas Radio has $40 in sales in 1997.Q2.

        assertQueryReturns(
                "WITH" + nl +
                "  SET [Bottom Media] AS 'BottomCount([Promotion Media].children, 1, [Measures].[Unit Sales])' " + nl +
                "  MEMBER [Measures].[Unit Sales for Bottom Media] AS 'Sum([Bottom Media], [Measures].[Unit Sales])'" + nl +
                "SELECT {[Measures].[Unit Sales for Bottom Media]} ON COLUMNS," + nl +
                " {[Time].[1997], [Time].[1997].[Q2]} ON ROWS" + nl +
                "FROM [Sales]",
                // Note that Row #1 gives 40. 35 would be wrong.
                // [In-Store Coupon], which was bottom for 1997.Q2 but not for
                // 1997.
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales for Bottom Media]}" + nl +
                "Axis #2:" + nl +
                "{[Time].[1997]}" + nl +
                "{[Time].[1997].[Q2]}" + nl +
                "Row #0: 2,454" + nl +
                "Row #1: 40" + nl);

        assertQueryReturns(
                "WITH" + nl +
                "  SET [TopMedia] AS 'TopCount([Promotion Media].children, 3, [Measures].[Store Sales])' " + nl +
                "  MEMBER [Measures].[California sales for Top Media] AS 'Sum([TopMedia], [Measures].[Store Sales])'" + nl +
                "SELECT " + nl +
                "  CrossJoin({[Store], [Store].[USA].[CA]}," + nl +
                "    {[Time].[1997].[Q1], [Time].[1997].[Q2]}) ON COLUMNS," + nl +
                " {[Product], [Product].children} ON ROWS" + nl +
                "FROM [Sales]" + nl +
                "WHERE [Measures].[California sales for Top Media]",

                "Axis #0:" + nl +
                "{[Measures].[California sales for Top Media]}" + nl +
                "Axis #1:" + nl +
                "{[Store].[All Stores], [Time].[1997].[Q1]}" + nl +
                "{[Store].[All Stores], [Time].[1997].[Q2]}" + nl +
                "{[Store].[All Stores].[USA].[CA], [Time].[1997].[Q1]}" + nl +
                "{[Store].[All Stores].[USA].[CA], [Time].[1997].[Q2]}" + nl +
                "Axis #2:" + nl +
                "{[Product].[All Products]}" + nl +
                "{[Product].[All Products].[Drink]}" + nl +
                "{[Product].[All Products].[Food]}" + nl +
                "{[Product].[All Products].[Non-Consumable]}" + nl +
                "Row #0: 108,249.52" + nl +
                "Row #0: 107,649.93" + nl +
                "Row #0: 29,482.53" + nl +
                "Row #0: 28,953.02" + nl +
                "Row #1: 8,930.95" + nl +
                "Row #1: 9,551.93" + nl +
                "Row #1: 2,721.23" + nl +
                "Row #1: 2,444.78" + nl +
                "Row #2: 78,375.66" + nl +
                "Row #2: 77,219.13" + nl +
                "Row #2: 21,165.50" + nl +
                "Row #2: 20,924.43" + nl +
                "Row #3: 20,942.91" + nl +
                "Row #3: 20,878.87" + nl +
                "Row #3: 5,595.80" + nl +
                "Row #3: 5,583.81" + nl);
    }

    public void testOrderedNamedSet() {
        // From http://www.developersdex.com
        assertQueryReturns(
                "WITH SET [SET1] AS" + nl +
                "'ORDER ({[Education Level].[Education Level].Members}, [Gender].[All Gender].[F], ASC)'" + nl +
                "MEMBER [Gender].[RANK1] AS 'rank([Education Level].currentmember, [SET1])'" + nl +
                "select" + nl +
                "{[Gender].[All Gender].[F], [Gender].[RANK1]} on columns," + nl +
                "{[Education Level].[Education Level].Members} on rows" + nl +
                "from Sales" + nl +
                "where ([Measures].[Store Sales])",
                // MSAS gives results as below, except ranks are displayed as
                // integers, e.g. '1'.
                "Axis #0:" + nl +
                "{[Measures].[Store Sales]}" + nl +
                "Axis #1:" + nl +
                "{[Gender].[All Gender].[F]}" + nl +
                "{[Gender].[RANK1]}" + nl +
                "Axis #2:" + nl +
                "{[Education Level].[All Education Levels].[Bachelors Degree]}" + nl +
                "{[Education Level].[All Education Levels].[Graduate Degree]}" + nl +
                "{[Education Level].[All Education Levels].[High School Degree]}" + nl +
                "{[Education Level].[All Education Levels].[Partial College]}" + nl +
                "{[Education Level].[All Education Levels].[Partial High School]}" + nl +
                "Row #0: 72,119.26" + nl +
                "Row #0: 3" + nl +
                "Row #1: 17,641.64" + nl +
                "Row #1: 1" + nl +
                "Row #2: 81,112.23" + nl +
                "Row #2: 4" + nl +
                "Row #3: 27,175.97" + nl +
                "Row #3: 2" + nl +
                "Row #4: 82,177.11" + nl +
                "Row #4: 5" + nl);

        assertQueryReturns(
                "WITH SET [SET1] AS" + nl +
                "'ORDER ({[Education Level].[Education Level].Members}, [Gender].[All Gender].[F], ASC)'" + nl +
                "MEMBER [Gender].[RANK1] AS 'rank([Education Level].currentmember, [SET1])'" + nl +
                "select" + nl +
                "{[Gender].[All Gender].[F], [Gender].[RANK1]} on columns," + nl +
                "{[Education Level].[Education Level].Members} on rows" + nl +
                "from Sales" + nl +
                "where ([Measures].[Profit])",
                // MSAS gives results as below. The ranks are (correctly) 0
                // because profit is a calc member.
                "Axis #0:" + nl +
                "{[Measures].[Profit]}" + nl +
                "Axis #1:" + nl +
                "{[Gender].[All Gender].[F]}" + nl +
                "{[Gender].[RANK1]}" + nl +
                "Axis #2:" + nl +
                "{[Education Level].[All Education Levels].[Bachelors Degree]}" + nl +
                "{[Education Level].[All Education Levels].[Graduate Degree]}" + nl +
                "{[Education Level].[All Education Levels].[High School Degree]}" + nl +
                "{[Education Level].[All Education Levels].[Partial College]}" + nl +
                "{[Education Level].[All Education Levels].[Partial High School]}" + nl +
                "Row #0: $43,382.33" + nl +
                "Row #0: $0.00" + nl +
                "Row #1: $10,599.59" + nl +
                "Row #1: $0.00" + nl +
                "Row #2: $48,766.50" + nl +
                "Row #2: $0.00" + nl +
                "Row #3: $16,306.05" + nl +
                "Row #3: $0.00" + nl +
                "Row #4: $49,394.27" + nl +
                "Row #4: $0.00" + nl);

        // Solve order fixes the problem.
        assertQueryReturns(
                "WITH SET [SET1] AS" + nl +
                "'ORDER ({[Education Level].[Education Level].Members}, [Gender].[All Gender].[F], ASC)'" + nl +
                "MEMBER [Gender].[RANK1] AS 'rank([Education Level].currentmember, [SET1])', " + nl +
                "  SOLVE_ORDER = 10" + nl +
                "select" + nl +
                "{[Gender].[All Gender].[F], [Gender].[RANK1]} on columns," + nl +
                "{[Education Level].[Education Level].Members} on rows" + nl +
                "from Sales" + nl +
                "where ([Measures].[Profit])",
                // MSAS gives results as below.
                "Axis #0:" + nl +
                "{[Measures].[Profit]}" + nl +
                "Axis #1:" + nl +
                "{[Gender].[All Gender].[F]}" + nl +
                "{[Gender].[RANK1]}" + nl +
                "Axis #2:" + nl +
                "{[Education Level].[All Education Levels].[Bachelors Degree]}" + nl +
                "{[Education Level].[All Education Levels].[Graduate Degree]}" + nl +
                "{[Education Level].[All Education Levels].[High School Degree]}" + nl +
                "{[Education Level].[All Education Levels].[Partial College]}" + nl +
                "{[Education Level].[All Education Levels].[Partial High School]}" + nl +
                "Row #0: $43,382.33" + nl +
                "Row #0: 3" + nl +
                "Row #1: $10,599.59" + nl +
                "Row #1: 1" + nl +
                "Row #2: $48,766.50" + nl +
                "Row #2: 4" + nl +
                "Row #3: $16,306.05" + nl +
                "Row #3: 2" + nl +
                "Row #4: $49,394.27" + nl +
                "Row #4: 5" + nl);


    }

    // TODO: Implement Generate function.
    public void _testGenerate() {
        assertQueryReturns(
                "with " + nl +
                "  member [Measures].[DateName] as " + nl +
                "    'Generate({[Time].[1997].[Q1], [Time].[1997].[Q2]}, [Time].CurrentMember.Name) '" + nl +
                "select {[Measures].[DateName]} on columns," + nl +
                " {[Time].[1997].[Q1], [Time].[1997].[Q2]} on rows" + nl +
                "from [Sales]",
                "two rows q1, q2; q1q2 for each cell");

        assertQueryReturns(
                "with " + nl +
                "  member [Measures].[DateName] as " + nl +
                "    'Generate({[Time].[1997].[Q1], [Time].[1997].[Q2]}, [Time].CurrentMember.Name, \" and \") '" + nl +
                "select {[Measures].[DateName]} on columns," + nl +
                " {[Time].[1997].[Q1], [Time].[1997].[Q2]} on rows" + nl +
                "from [Sales]",
                "two rows q1, q2; q1q2 for each cell");
    }

    public void testNamedSetAgainstCube() {
        final TestContext tc = new TestContext() {
            public synchronized Connection getFoodMartConnection(boolean fresh) {
                return getFoodMartConnection(NamedSetsInCubeProcessor.class.getName());
            }
        };
        // Set defined against cube, using 'formula' attribute.
        tc.assertQueryReturns(
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS," + nl +
                " {[CA Cities]} ON ROWS" + nl +
                "FROM [Sales]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[USA].[CA].[Alameda]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Beverly Hills]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Diego]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Francisco]}" + nl +
                "Row #0: (null)" + nl +
                "Row #1: 21,333" + nl +
                "Row #2: 25,663" + nl +
                "Row #3: 25,635" + nl +
                "Row #4: 2,117" + nl);
        // Set defined against cube, in terms of another set, and using
        // '<Formula>' element.
        tc.assertQueryReturns(
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS," + nl +
                " {[Top CA Cities]} ON ROWS" + nl +
                "FROM [Sales]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Diego]}" + nl +
                "Row #0: 25,663" + nl +
                "Row #1: 25,635" + nl);
        // Override named set in query.
        tc.assertQueryReturns(
                "WITH SET [CA Cities] AS '{[Store].[USA].[OR].[Portland]}' " +
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS," + nl +
                " {[CA Cities]} ON ROWS" + nl +
                "FROM [Sales]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[USA].[OR].[Portland]}" + nl +
                "Row #0: 26,079" + nl);
        // When [CA Cities] is overridden, does the named set [Top CA Cities],
        // which is derived from it, use the new definition? No. It stays
        // bound to the original definition.
        tc.assertQueryReturns(
                "WITH SET [CA Cities] AS '{[Store].[USA].[OR].[Portland]}' " +
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS," + nl +
                " {[Top CA Cities]} ON ROWS" + nl +
                "FROM [Sales]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Diego]}" + nl +
                "Row #0: 25,663" + nl +
                "Row #1: 25,635" + nl);
    }

    public void testNamedSetAgainstSchema() {
        final TestContext tc = new TestContext() {
            public synchronized Connection getFoodMartConnection(boolean fresh) {
                return getFoodMartConnection(NamedSetsInCubeAndSchemaProcessor.class.getName());
            }
        };
        tc.assertQueryReturns(
                "SELECT {[Measures].[Store Sales]} on columns," + nl +
                " Intersect([Top CA Cities], [Top USA Stores]) on rows" + nl +
                "FROM [Sales]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Store Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles]}" + nl +
                "Row #0: 54,545.28" + nl);
        // Use non-existent set.
        tc.assertThrows(
                "SELECT {[Measures].[Store Sales]} on columns," + nl +
                " Intersect([Top CA Cities], [Top Ukrainian Cities]) on rows" + nl +
                "FROM [Sales]",
                "MDX object '[Top Ukrainian Cities]' not found in cube 'Sales'");
    }

    public void testBadNamedSet() {
        final TestContext tc = new TestContext() {
            public synchronized Connection getFoodMartConnection(boolean fresh) {
                return getFoodMartConnection(BadNamedSetSchemaProcessor.class.getName());
            }
        };
        tc.assertThrows(
                "SELECT {[Measures].[Store Sales]} on columns," + nl +
                " {[Bad]} on rows" + nl +
                "FROM [Sales]",
                "Named set 'Bad' has bad formula");
    }

    public void testNamedSetMustBeSet() {
        Result result;
        String queryString;
        String pattern;

        // Formula for a named set must not be a member.
        queryString = "with set [Foo] as ' [Store].CurrentMember  '" +
                        "select {[Foo]} on columns from [Sales]";
        pattern = "Set expression '[Foo]' must be a set";
        assertThrows(queryString, pattern);

        // Formula for a named set must not be a dimension.
        queryString =
                "with set [Foo] as ' [Store] '" +
                "select {[Foo]} on columns from [Sales]";
        assertThrows(queryString, pattern);

        // Formula for a named set must not be a level.
        queryString =
                "with set [Foo] as ' [Store].[Store Country] '" +
                "select {[Foo]} on columns from [Sales]";
        assertThrows(queryString, pattern);

        // Formula for a named set must not be a cube name.
        queryString =
                "with set [Foo] as ' [Sales] '" +
                "select {[Foo]} on columns from [Sales]";
        assertThrows(queryString,
                "MDX object '[Sales]' not found in cube 'Sales'");

        // Formula for a named set must not be a string.
        queryString =
                "with set [Foo] as ' \"foobar\" '" +
                "select {[Foo]} on columns from [Sales]";
        assertThrows(queryString, pattern);

        // Formula for a named set must not be a number.
        queryString =
                "with set [Foo] as ' -1.45 '" +
                "select {[Foo]} on columns from [Sales]";
        assertThrows(queryString, pattern);

        // Formula for a named set must not be a tuple.
        queryString =
                "with set [Foo] as ' ([Gender].[M], [Marital Status].[S]) '" +
                "select {[Foo]} on columns from [Sales]";
        assertThrows(queryString, pattern);

        // Formula for a named set may be a set of tuples.
        queryString =
                "with set [Foo] as ' CrossJoin([Gender].members, [Marital Status].members) '" +
                "select {[Foo]} on columns from [Sales]";
        result = executeQuery(queryString);

        // Formula for a named set may be a set of members.
        queryString =
                "with set [Foo] as ' [Gender].members '" +
                "select {[Foo]} on columns from [Sales]";
        result = executeQuery(queryString);
        Util.discard(result);
    }

    public void testNamedSetsMixedWithCalcMembers()
    {
        final TestContext tc = new TestContext() {
            public synchronized Connection getFoodMartConnection(boolean fresh) {
                return getFoodMartConnection(MixedNamedSetSchemaProcessor.class.getName());
            }
        };
        tc.assertQueryReturns(
                fold(new String[] {
                    "select {",
                    "    [Measures].[Unit Sales],",
                    "    [Measures].[CA City Sales]} on columns,",
                    "  Crossjoin(",
                    "    [Time].[1997].Children,",
                    "    [Top Products In CA]) on rows",
                    "from [Sales]",
                    "where [Marital Status].[S]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Marital Status].[All Marital Status].[S]}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "{[Measures].[CA City Sales]}",
                    "Axis #2:",
                    "{[Time].[1997].[Q1], [Product].[All Products].[Food].[Produce]}",
                    "{[Time].[1997].[Q1], [Product].[All Products].[Food].[Snack Foods]}",
                    "{[Time].[1997].[Q1], [Product].[All Products].[Non-Consumable].[Household]}",
                    "{[Time].[1997].[Q2], [Product].[All Products].[Food].[Produce]}",
                    "{[Time].[1997].[Q2], [Product].[All Products].[Food].[Snack Foods]}",
                    "{[Time].[1997].[Q2], [Product].[All Products].[Non-Consumable].[Household]}",
                    "{[Time].[1997].[Q3], [Product].[All Products].[Food].[Produce]}",
                    "{[Time].[1997].[Q3], [Product].[All Products].[Food].[Snack Foods]}",
                    "{[Time].[1997].[Q3], [Product].[All Products].[Non-Consumable].[Household]}",
                    "{[Time].[1997].[Q4], [Product].[All Products].[Food].[Produce]}",
                    "{[Time].[1997].[Q4], [Product].[All Products].[Food].[Snack Foods]}",
                    "{[Time].[1997].[Q4], [Product].[All Products].[Non-Consumable].[Household]}",
                    "Row #0: 4,872",
                    "Row #0: $1,218.0",
                    "Row #1: 3,746",
                    "Row #1: $840.0",
                    "Row #2: 3,425",
                    "Row #2: $817.0",
                    "Row #3: 4,633",
                    "Row #3: $1,320.0",
                    "Row #4: 3,588",
                    "Row #4: $1,058.0",
                    "Row #5: 3,149",
                    "Row #5: $938.0",
                    "Row #6: 4,651",
                    "Row #6: $1,353.0",
                    "Row #7: 3,895",
                    "Row #7: $1,134.0",
                    "Row #8: 3,395",
                    "Row #8: $1,029.0",
                    "Row #9: 5,160",
                    "Row #9: $1,550.0",
                    "Row #10: 4,160",
                    "Row #10: $1,301.0",
                    "Row #11: 3,808",
                    "Row #11: $1,166.0",
                    ""}));
    }

    /**
     * Dynamic schema processor which adds two named sets to a the first cube
     * in a schema.
     */
    public static class NamedSetsInCubeProcessor
            extends DecoratingSchemaProcessor {

        protected String filterSchema(String s) {
            int i = s.indexOf("</Cube>");
            return fold(new String[] {
                s.substring(0, i),
                "<NamedSet name=\"CA Cities\" formula=\"{[Store].[USA].[CA].Children}\"/>",
                "<NamedSet name=\"Top CA Cities\">",
                "  <Formula>TopCount([CA Cities], 2, [Measures].[Unit Sales])</Formula>",
                "</NamedSet>",
                s.substring(i)});
        }
    }

    /**
     * Dynamic schema processor which adds two named sets to a the first cube
     * in a schema.
     */
    public static class NamedSetsInCubeAndSchemaProcessor
            extends DecoratingSchemaProcessor {

        protected String filterSchema(String s) {
            int i = s.indexOf("</Cube>");
            s = fold(new String[] {
                s.substring(0, i),
                "<NamedSet name=\"CA Cities\" formula=\"{[Store].[USA].[CA].Children}\"/>",
                "<NamedSet name=\"Top CA Cities\">",
                "  <Formula>TopCount([CA Cities], 2, [Measures].[Unit Sales])</Formula>",
                "</NamedSet>",
                s.substring(i)});
            // Schema-level named sets occur after <Cube> and <VirtualCube> and
            // before <Role> elements.
            i = s.indexOf("<Role");
            if (i < 0) {
                i = s.indexOf("</Schema>");
            }
            s = fold(new String[] {
                s.substring(0, i),
                "<NamedSet name=\"CA Cities\" formula=\"{[Store].[USA].[WA].Children}\"/>",
                "<NamedSet name=\"Top USA Stores\">",
                "  <Formula>TopCount(Descendants([Store].[USA]), 7)</Formula>",
                "</NamedSet>",
                s.substring(i)});
            return s;
        }
    }

    /**
     * Dynamic schema processor which adds a named set which has a syntax
     * error.
     */
    public static class BadNamedSetSchemaProcessor
            extends DecoratingSchemaProcessor {

        protected String filterSchema(String s) {
            // Schema-level named sets occur after <Cube> and <VirtualCube> and
            // before <Role> elements.
            int i = s.indexOf("<Role");
            if (i < 0) {
                i = s.indexOf("</Schema>");
            }
            s = fold(new String[] {
                s.substring(0, i),
                "<NamedSet name=\"Bad\" formula=\"{[Store].[USA].[WA].Children}}\"/>",
                s.substring(i)});
            return s;
        }
    }

    /**
     * Dynamic schema processor which adds a named set which has a syntax
     * error.
     */
    public static class MixedNamedSetSchemaProcessor
            extends DecoratingSchemaProcessor {

        protected String filterSchema(String s) {
            // Declare mutually dependent named sets and calculated members
            // at the end of a cube:
            //   m2 references s1
            //   s1 references s0 and m1 and m0
            int i = s.indexOf("</Cube>");
            s = fold(new String[] {
                s.substring(0, i),
                // member [CA City Sales] references set [CA Cities]
                "  <CalculatedMember",
                "      name=\"CA City Sales\"",
                "      dimension=\"Measures\"",
                "      visible=\"false\"",
                "      formula=\"Aggregate([CA Cities], [Measures].[Unit Sales])\">",
                "    <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"$#,##0.0\"/>",
                "  </CalculatedMember>",
                // set [Top Products In CA] references member [CA City Sales]
                "<NamedSet name=\"Top Products In CA\">",
                "  <Formula>TopCount([Product].[Product Department].MEMBERS, 3, ([Time].[1997].[Q3], [Measures].[CA City Sales]))</Formula>",
                "</NamedSet>",
                "<NamedSet name=\"CA Cities\" formula=\"{[Store].[USA].[CA].Children}\"/>",
                s.substring(i)});
            return s;
        }
    }
}

// End NamedSetTest.java
