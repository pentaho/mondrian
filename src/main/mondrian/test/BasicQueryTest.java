/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2003 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.test;

import mondrian.olap.*;

/**
 * <code>BasicQueryTest</code> is a test case which tests simple queries against
 * the FoodMart database.
 *
 * @author jhyde
 * @since Feb 14, 2003
 * @version $Id$
 **/
public class BasicQueryTest extends FoodMartTestCase {
	public BasicQueryTest(String name) {
		super(name);
	}

	private static final QueryAndResult[] sampleQueries = new QueryAndResult[] {
		// 0
		new QueryAndResult(
				"select {[Measures].[Unit Sales]} on columns" + nl +
				" from Sales",

				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales]}" + nl +
				"Row #0: 266,773" + nl),

		// 1
		new QueryAndResult(
				"select" + nl +
				"	 {[Measures].[Unit Sales]} on columns," + nl +
				"	 order(except([Promotion Media].[Media Type].members,{[Promotion Media].[Media Type].[No Media]}),[Measures].[Unit Sales],DESC) on rows" + nl +
				"from Sales ",

				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales]}" + nl +
				"Axis #2:" + nl +
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
				"Row #0: 9,513" + nl +
				"Row #1: 7,738" + nl +
				"Row #2: 7,544" + nl +
				"Row #3: 6,891" + nl +
				"Row #4: 6,697" + nl +
				"Row #5: 5,945" + nl +
				"Row #6: 5,753" + nl +
				"Row #7: 4,339" + nl +
				"Row #8: 4,320" + nl +
				"Row #9: 3,798" + nl +
				"Row #10: 3,607" + nl +
				"Row #11: 2,726" + nl +
				"Row #12: 2,454" + nl),

		// 2
		new QueryAndResult(
				"select" + nl +
				"	 { [Measures].[Units Shipped], [Measures].[Units Ordered] } on columns," + nl +
				"	 NON EMPTY [Store].[Store Name].members on rows" + nl +
				"from Warehouse",

				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Units Shipped]}" + nl +
				"{[Measures].[Units Ordered]}" + nl +
				"Axis #2:" + nl +
				"{[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6]}" + nl +
				"{[Store].[All Stores].[USA].[CA].[Los Angeles].[Store 7]}" + nl +
				"{[Store].[All Stores].[USA].[CA].[San Diego].[Store 24]}" + nl +
				"{[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]}" + nl +
				"{[Store].[All Stores].[USA].[OR].[Portland].[Store 11]}" + nl +
				"{[Store].[All Stores].[USA].[OR].[Salem].[Store 13]}" + nl +
				"{[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]}" + nl +
				"{[Store].[All Stores].[USA].[WA].[Bremerton].[Store 3]}" + nl +
				"{[Store].[All Stores].[USA].[WA].[Seattle].[Store 15]}" + nl +
				"{[Store].[All Stores].[USA].[WA].[Spokane].[Store 16]}" + nl +
				"{[Store].[All Stores].[USA].[WA].[Tacoma].[Store 17]}" + nl +
				"{[Store].[All Stores].[USA].[WA].[Walla Walla].[Store 22]}" + nl +
				"{[Store].[All Stores].[USA].[WA].[Yakima].[Store 23]}" + nl +
				"Row #0: 10759.0" + nl +
				"Row #0: 11699.0" + nl +
				"Row #1: 24587.0" + nl +
				"Row #1: 26463.0" + nl +
				"Row #2: 23835.0" + nl +
				"Row #2: 26270.0" + nl +
				"Row #3: 1696.0" + nl +
				"Row #3: 1875.0" + nl +
				"Row #4: 8515.0" + nl +
				"Row #4: 9109.0" + nl +
				"Row #5: 32393.0" + nl +
				"Row #5: 35797.0" + nl +
				"Row #6: 2348.0" + nl +
				"Row #6: 2454.0" + nl +
				"Row #7: 22734.0" + nl +
				"Row #7: 24610.0" + nl +
				"Row #8: 24110.0" + nl +
				"Row #8: 26703.0" + nl +
				"Row #9: 11889.0" + nl +
				"Row #9: 12828.0" + nl +
				"Row #10: 32411.0" + nl +
				"Row #10: 35930.0" + nl +
				"Row #11: 1860.0" + nl +
				"Row #11: 2074.0" + nl +
				"Row #12: 10589.0" + nl +
				"Row #12: 11426.0" + nl),

		// 3
		new QueryAndResult(
				"with member [Measures].[Store Sales Last Period] as " +
				"    '([Measures].[Store Sales], Time.PrevMember)'," + nl +
				"    format='#,###.00'" + nl +
				"select" + nl +
				"	 {[Measures].[Store Sales Last Period]} on columns," + nl +
				"	 {TopCount([Product].[Product Department].members,5, [Measures].[Store Sales Last Period])} on rows" + nl +
				"from Sales" + nl +
				"where ([Time].[1998])",

				"Axis #0:" + nl +
				"{[Time].[1998]}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Store Sales Last Period]}" + nl +
				"Axis #2:" + nl +
				"{[Product].[All Products].[Food].[Produce]}" + nl +
				"{[Product].[All Products].[Food].[Snack Foods]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Household]}" + nl +
				"{[Product].[All Products].[Food].[Frozen Foods]}" + nl +
				"{[Product].[All Products].[Food].[Canned Foods]}" + nl +
				"Row #0: 82,248.42" + nl +
				"Row #1: 67,609.82" + nl +
				"Row #2: 60,469.89" + nl +
				"Row #3: 55,207.50" + nl +
				"Row #4: 39,774.34" + nl),

		// 4
		new QueryAndResult(
				"with member [Measures].[Total Store Sales] as 'Sum(YTD(),[Measures].[Store Sales])', format_string='#.00'" + nl +
				"select" + nl +
				"	 {[Measures].[Total Store Sales]} on columns," + nl +
				"	 {TopCount([Product].[Product Department].members,5, [Measures].[Total Store Sales])} on rows" + nl +
				"from Sales" + nl +
				"where ([Time].[1997].[Q2].[4])",

				"Axis #0:" + nl +
				"{[Time].[1997].[Q2].[4]}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Total Store Sales]}" + nl +
				"Axis #2:" + nl +
				"{[Product].[All Products].[Food].[Produce]}" + nl +
				"{[Product].[All Products].[Food].[Snack Foods]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Household]}" + nl +
				"{[Product].[All Products].[Food].[Frozen Foods]}" + nl +
				"{[Product].[All Products].[Food].[Canned Foods]}" + nl +
				"Row #0: 26526.67" + nl +
				"Row #1: 21897.10" + nl +
				"Row #2: 19980.90" + nl +
				"Row #3: 17882.63" + nl +
				"Row #4: 12963.23" + nl),

		// 5
		new QueryAndResult(
				"with member [Measures].[Store Profit Rate] as '([Measures].[Store Sales]-[Measures].[Store Cost])/[Measures].[Store Cost]', format = '#.00%'" + nl +
				"select" + nl +
				"	 {[Measures].[Store Cost],[Measures].[Store Sales],[Measures].[Store Profit Rate]} on columns," + nl +
				"	 Order([Product].[Product Department].members, [Measures].[Store Profit Rate], BDESC) on rows" + nl +
				"from Sales" + nl +
				"where ([Time].[1997])",

				"Axis #0:" + nl +
				"{[Time].[1997]}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Store Cost]}" + nl +
				"{[Measures].[Store Sales]}" + nl +
				"{[Measures].[Store Profit Rate]}" + nl +
				"Axis #2:" + nl +
				"{[Product].[All Products].[Food].[Breakfast Foods]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Carousel]}" + nl +
				"{[Product].[All Products].[Food].[Canned Products]}" + nl +
				"{[Product].[All Products].[Food].[Baking Goods]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Health and Hygiene]}" + nl +
				"{[Product].[All Products].[Food].[Snack Foods]}" + nl +
				"{[Product].[All Products].[Food].[Baked Goods]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Product].[All Products].[Food].[Frozen Foods]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Periodicals]}" + nl +
				"{[Product].[All Products].[Food].[Produce]}" + nl +
				"{[Product].[All Products].[Food].[Seafood]}" + nl +
				"{[Product].[All Products].[Food].[Deli]}" + nl +
				"{[Product].[All Products].[Food].[Meat]}" + nl +
				"{[Product].[All Products].[Food].[Canned Foods]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Household]}" + nl +
				"{[Product].[All Products].[Food].[Starchy Foods]}" + nl +
				"{[Product].[All Products].[Food].[Eggs]}" + nl +
				"{[Product].[All Products].[Food].[Snacks]}" + nl +
				"{[Product].[All Products].[Food].[Dairy]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Checkout]}" + nl +
				"Row #0: 2,756.80" + nl +
				"Row #0: 6,941.46" + nl +
				"Row #0: 151.79%" + nl +
				"Row #1: 595.97" + nl +
				"Row #1: 1,500.11" + nl +
				"Row #1: 151.71%" + nl +
				"Row #2: 1,317.13" + nl +
				"Row #2: 3,314.52" + nl +
				"Row #2: 151.65%" + nl +
				"Row #3: 15,370.61" + nl +
				"Row #3: 38,670.41" + nl +
				"Row #3: 151.59%" + nl +
				"Row #4: 5,576.79" + nl +
				"Row #4: 14,029.08" + nl +
				"Row #4: 151.56%" + nl +
				"Row #5: 12,972.99" + nl +
				"Row #5: 32,571.86" + nl +
				"Row #5: 151.07%" + nl +
				"Row #6: 26,963.34" + nl +
				"Row #6: 67,609.82" + nl +
				"Row #6: 150.75%" + nl +
				"Row #7: 6,564.09" + nl +
				"Row #7: 16,455.43" + nl +
				"Row #7: 150.69%" + nl +
				"Row #8: 11,069.53" + nl +
				"Row #8: 27,748.53" + nl +
				"Row #8: 150.67%" + nl +
				"Row #9: 22,030.66" + nl +
				"Row #9: 55,207.50" + nl +
				"Row #9: 150.59%" + nl +
				"Row #10: 3,614.55" + nl +
				"Row #10: 9,056.76" + nl +
				"Row #10: 150.56%" + nl +
				"Row #11: 32,831.33" + nl +
				"Row #11: 82,248.42" + nl +
				"Row #11: 150.52%" + nl +
				"Row #12: 1,520.70" + nl +
				"Row #12: 3,809.14" + nl +
				"Row #12: 150.49%" + nl +
				"Row #13: 10,108.87" + nl +
				"Row #13: 25,318.93" + nl +
				"Row #13: 150.46%" + nl +
				"Row #14: 1,465.42" + nl +
				"Row #14: 3,669.89" + nl +
				"Row #14: 150.43%" + nl +
				"Row #15: 15,894.53" + nl +
				"Row #15: 39,774.34" + nl +
				"Row #15: 150.24%" + nl +
				"Row #16: 24,170.73" + nl +
				"Row #16: 60,469.89" + nl +
				"Row #16: 150.18%" + nl +
				"Row #17: 4,705.91" + nl +
				"Row #17: 11,756.07" + nl +
				"Row #17: 149.82%" + nl +
				"Row #18: 3,684.90" + nl +
				"Row #18: 9,200.76" + nl +
				"Row #18: 149.69%" + nl +
				"Row #19: 5,827.58" + nl +
				"Row #19: 14,550.05" + nl +
				"Row #19: 149.68%" + nl +
				"Row #20: 12,228.85" + nl +
				"Row #20: 30,508.85" + nl +
				"Row #20: 149.48%" + nl +
				"Row #21: 2,830.92" + nl +
				"Row #21: 7,058.60" + nl +
				"Row #21: 149.34%" + nl +
				"Row #22: 1,525.04" + nl +
				"Row #22: 3,767.71" + nl +
				"Row #22: 147.06%" + nl),

		// 6
		new QueryAndResult(
				"with" + nl +
				"	member [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] as '[Product].[All Products].[Drink].[Alcoholic Beverages]/[Product].[All Products].[Drink]'," + nl +
				"		format_string = '#.00%'" + nl +
				"select" + nl +
				"	{ [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] } on columns," + nl +
				"	order([Customers].[All Customers].[USA].[WA].Children, [Product].[All Products].[Drink].[Percent of Alcoholic Drinks],BDESC ) on rows" + nl +
				"from Sales" + nl +
				"where ( [Measures].[Unit Sales] )",

				"Axis #0:" + nl +
				"{[Measures].[Unit Sales]}" + nl +
				"Axis #1:" + nl +
				"{[Product].[All Products].[Drink].[Percent of Alcoholic Drinks]}" + nl +
				"Axis #2:" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Seattle]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Kirkland]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Marysville]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Anacortes]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Olympia]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Ballard]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Bremerton]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Puyallup]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Yakima]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Tacoma]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Everett]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Renton]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Issaquah]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Bellingham]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Port Orchard]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Redmond]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Spokane]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Burien]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Lynnwood]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Walla Walla]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Edmonds]}" + nl +
				"{[Customers].[All Customers].[USA].[WA].[Sedro Woolley]}" + nl +
				"Row #0: 44.05%" + nl +
				"Row #1: 34.41%" + nl +
				"Row #2: 34.20%" + nl +
				"Row #3: 32.93%" + nl +
				"Row #4: 31.05%" + nl +
				"Row #5: 30.84%" + nl +
				"Row #6: 30.69%" + nl +
				"Row #7: 29.81%" + nl +
				"Row #8: 28.82%" + nl +
				"Row #9: 28.70%" + nl +
				"Row #10: 28.37%" + nl +
				"Row #11: 26.67%" + nl +
				"Row #12: 26.60%" + nl +
				"Row #13: 26.47%" + nl +
				"Row #14: 26.42%" + nl +
				"Row #15: 26.28%" + nl +
				"Row #16: 25.96%" + nl +
				"Row #17: 24.70%" + nl +
				"Row #18: 21.89%" + nl +
				"Row #19: 21.47%" + nl +
				"Row #20: 17.47%" + nl +
				"Row #21: 13.79%" + nl),

		// 7
		new QueryAndResult(
				"with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
				"select" + nl +
				"	 {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns," + nl +
				"	 {Descendants([Time].[1997],[Time].[Month])} on rows" + nl +
				"from Sales",

			"Axis #0:" + nl +
			"{}" + nl +
			"Axis #1:" + nl +
			"{[Measures].[Store Sales]}" + nl +
			"{[Measures].[Accumulated Sales]}" + nl +
			"Axis #2:" + nl +
			"{[Time].[1997].[Q1].[1]}" + nl +
			"{[Time].[1997].[Q1].[2]}" + nl +
			"{[Time].[1997].[Q1].[3]}" + nl +
			"{[Time].[1997].[Q2].[4]}" + nl +
			"{[Time].[1997].[Q2].[5]}" + nl +
			"{[Time].[1997].[Q2].[6]}" + nl +
			"{[Time].[1997].[Q3].[7]}" + nl +
			"{[Time].[1997].[Q3].[8]}" + nl +
			"{[Time].[1997].[Q3].[9]}" + nl +
			"{[Time].[1997].[Q4].[10]}" + nl +
			"{[Time].[1997].[Q4].[11]}" + nl +
			"{[Time].[1997].[Q4].[12]}" + nl +
			"Row #0: 45,539.69" + nl +
			"Row #0: 45539.69" + nl +
			"Row #1: 44,058.79" + nl +
			"Row #1: 89598.48000000001" + nl +
			"Row #2: 50,029.87" + nl +
			"Row #2: 139628.35" + nl +
			"Row #3: 42,878.25" + nl +
			"Row #3: 182506.6" + nl +
			"Row #4: 44,456.29" + nl +
			"Row #4: 226962.89" + nl +
			"Row #5: 45,331.73" + nl +
			"Row #5: 272294.62" + nl +
			"Row #6: 50,246.88" + nl +
			"Row #6: 322541.5" + nl +
			"Row #7: 46,199.04" + nl +
			"Row #7: 368740.54" + nl +
			"Row #8: 43,825.97" + nl +
			"Row #8: 412566.51" + nl +
			"Row #9: 42,342.27" + nl +
			"Row #9: 454908.78" + nl +
			"Row #10: 53,363.71" + nl +
			"Row #10: 508272.49000000005" + nl +
			"Row #11: 56,965.64" + nl +
			"Row #11: 565238.13" + nl),
	};

	public void testSample0() {
		runQueryCheckResult(sampleQueries[0]);
	}

	public void testSample1() {
		runQueryCheckResult(sampleQueries[1]);
	}

	public void testSample2() {
		runQueryCheckResult(sampleQueries[2]);
	}

	public void testSample3() {
		runQueryCheckResult(sampleQueries[3]);
	}

	public void testSample4() {
		runQueryCheckResult(sampleQueries[4]);
	}

	public void testSample5() {
		runQueryCheckResult(sampleQueries[5]);
	}

	public void testSample6() {
		runQueryCheckResult(sampleQueries[6]);
	}

	public void testSample7() {
		runQueryCheckResult(sampleQueries[7]);
	}

	/** Requires the use of a sparse segment, because the product dimension
	 * has 6 atttributes, the product of whose cardinalities is ~8M. If we
	 * use a dense segment, we run out of memory trying to allocate a huge
	 * array. */
	public void testBigQuery() {
		Result result = runQuery(
				"SELECT {[Measures].[Unit Sales]} on columns," + nl +
				" {[Product].members} on rows" + nl +
				"from Sales");
		final int rowCount = result.getAxes()[1].positions.length;
		assertEquals(2256, rowCount);
		assertEquals("152", result.getCell(new int[] {0, rowCount - 1}).getFormattedValue());
	}

	public void testNonEmpty1() {
		assertSize(
				"select" + nl +
				"  NON EMPTY CrossJoin({[Product].[All Products].[Drink].Children}," + nl +
				"    {[Customers].[All Customers].[USA].[WA].[Bellingham]}) on rows," + nl +
				"  CrossJoin(" + nl +
				"    {[Measures].[Unit Sales], [Measures].[Store Sales]}," + nl +
				"    { [Promotion Media].[All Media].[Radio]," + nl +
				"      [Promotion Media].[All Media].[TV]," + nl +
				"      [Promotion Media].[All Media].[Sunday Paper]," + nl +
				"      [Promotion Media].[All Media].[Street Handout] }" + nl +
				"    ) on columns" + nl +
				"from Sales" + nl +
				"where ([Time].[1997])", 8, 2);
	}

	public void testNonEmpty2() {
		assertSize(
				"select" + nl +
				"  NON EMPTY CrossJoin(" + nl +
				"    {[Product].[All Products].Children}," + nl +
				"    {[Customers].[All Customers].[USA].[WA].[Bellingham]}) on rows," + nl +
				"  NON EMPTY CrossJoin(" + nl +
				"    {[Measures].[Unit Sales]}," + nl +
				"    { [Promotion Media].[All Media].[Cash Register Handout]," + nl +
				"      [Promotion Media].[All Media].[Sunday Paper]," + nl +
				"      [Promotion Media].[All Media].[Street Handout] }" + nl +
				"    ) on columns" + nl +
				"from Sales" + nl +
				"where ([Time].[1997])", 2, 2);
	}

	public void testOneDimensionalQueryWithCompoundSlicer() {
		Result result = runQuery(
				"select" + nl +
				"  [Product].[All Products].[Drink].children on columns" + nl +
				"from Sales" + nl +
				"where ([Measures].[Unit Sales], [Promotion Media].[All Media].[Street Handout], [Time].[1997])");
		assertTrue(result.getAxes().length == 1);
		assertTrue(result.getAxes()[0].positions.length == 3);
		assertTrue(result.getSlicerAxis().positions.length == 1);
		assertTrue(result.getSlicerAxis().positions[0].members.length == 3);
	}

	public void _testEver() {
		runQueryCheckResult(
				"select" + nl +
				" {[Measures].[Unit Sales], [Measures].[Ever]} on columns," + nl +
				" [Gender].members on rows" + nl +
				"from Sales", null);
	}

	public void _testDairy() {
		runQueryCheckResult(
				"with" + nl +
				"  member [Product].[Non dairy] as '[Product].[All Products] - [Product].[Food].[Dairy]'" + nl +
				"  member [Measures].[Dairy ever] as 'sum([Time].members, ([Measures].[Unit Sales],[Product].[Food].[Dairy]))'" + nl +
				"  set [Customers who never bought dairy] as 'filter([Customers].members, [Measures].[Dairy ever] = 0)'" + nl +
				"select" + nl +
				" {[Measures].[Unit Sales], [Measures].[Dairy ever]}  on columns," + nl +
				"  [Customers who never bought dairy] on rows" + nl +
				"from Sales\r\n", null);
	}

	public void testHasBoughtDairy() {
		Util.discard(runQuery(
				"select {[Has bought dairy].members} on columns," + nl +
				" {[Customers].[USA]} on rows" + nl +
				"from Sales" + nl +
				"where ([Measures].[Unit Sales])"));
	}

	public void _testSolveOrder() {
		runQueryCheckResult(
				"WITH" + nl +
				"   MEMBER [Measures].[StoreType] AS " + nl +
				"   '[Store].CurrentMember.Properties(\"Store Type\")'," + nl +
				"   SOLVE_ORDER = 2" + nl +
				"   MEMBER [Measures].[ProfitPct] AS " + nl +
				"   'Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])'," + nl +
				"   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'" + nl +
				"SELECT" + nl +
				"   { [Store].[Store Name].Members} ON COLUMNS," + nl +
				"   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[StoreType]," + nl +
				"   [Measures].[ProfitPct] } ON ROWS" + nl +
				"FROM Sales", null);
	}

	public void testCalculatedMemberWhichIsNotAMeasure() {
		String query = "WITH" + nl +
				"MEMBER [Product].[BigSeller] AS" + nl +
				"  'IIf([Product].[Drink].[Alcoholic Beverages].[Beer and Wine] > 100, \"Yes\",\"No\")'" + nl +
				"SELECT {[Product].[BigSeller],[Product].children} ON COLUMNS," + nl +
				"   {[Store].[All Stores].[USA].[CA].children} ON ROWS" + nl +
				"FROM Sales";
		String desiredResult = "Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Product].[BigSeller]}" + nl +
				"{[Product].[All Products].[Drink]}" + nl +
				"{[Product].[All Products].[Food]}" + nl +
				"{[Product].[All Products].[Non-Consumable]}" + nl +
				"Axis #2:" + nl +
				"{[Store].[All Stores].[USA].[CA].[Beverly Hills]}" + nl +
				"{[Store].[All Stores].[USA].[CA].[Los Angeles]}" + nl +
				"{[Store].[All Stores].[USA].[CA].[San Diego]}" + nl +
				"{[Store].[All Stores].[USA].[CA].[San Francisco]}" + nl +
				"Row #0: Yes" + nl +
				"Row #0: 1,945" + nl +
				"Row #0: 15,438" + nl +
				"Row #0: 3,950" + nl +
				"Row #1: Yes" + nl +
				"Row #1: 2,422" + nl +
				"Row #1: 18,294" + nl +
				"Row #1: 4,947" + nl +
				"Row #2: Yes" + nl +
				"Row #2: 2,560" + nl +
				"Row #2: 18,369" + nl +
				"Row #2: 4,706" + nl +
				"Row #3: No" + nl +
				"Row #3: 175" + nl +
				"Row #3: 1,555" + nl +
				"Row #3: 387" + nl;
		runQueryCheckResult(query, desiredResult);
	}

	public void _testVal() {
		Util.discard(runQuery(
				"WITH" + nl +
				"   MEMBER [Measures].[ProfitPct] AS " + nl +
				"   'Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])'," + nl +
				"   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'" + nl +
				"   MEMBER [Measures].[ProfitValue] AS " + nl +
				"   '[Measures].[Store Sales] * [Measures].[ProfitPct]'," + nl +
				"   SOLVE_ORDER = 2, FORMAT_STRING = 'Currency'" + nl +
				"SELECT" + nl +
				"   { [Store].[Store Name].Members} ON COLUMNS," + nl +
				"   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[ProfitValue]," + nl +
				"   [Measures].[ProfitPct] } ON ROWS" + nl +
				"FROM Sales"));
	}

	public void testConstantString() {
		String s = executeExpr(" \"a string\" ");
		assertEquals("a string", s);
	}

	public void testConstantNumber() {
		String s = executeExpr(" 12 ");
		assertEquals("12.0", s);
	}

	public void testCyclicalCalculatedMembers() {
		Util.discard(runQuery(
				"WITH" + nl +
				"   MEMBER [Product].[X] AS '[Product].[Y]'" + nl +
				"   MEMBER [Product].[Y] AS '[Product].[X]'" + nl +
				"SELECT" + nl +
				"   {[Product].[X]} ON COLUMNS," + nl +
				"   {Store.[Store Name].Members} ON ROWS" + nl +
				"FROM Sales"));
	}

	/**
	 * Disabled test. It used throw an 'infinite loop' error (which is what
	 * Plato does). But now we revert to the context of the default member when
	 * calculating calculated members (we used to stay in the context of the
	 * calculated member), and we get a result.
	 **/
	public void testCycle() {
		if (false) {
			assertExprThrows("[Time].[1997].[Q4]", "infinite loop");
		} else {
			String s = executeExpr("[Time].[1997].[Q4]");
			assertEquals("72024.0", s);
		}
	}

	public void testHalfYears() {
		Util.discard(runQuery(
				"WITH MEMBER [Measures].[ProfitPercent] AS" + nl +
				"     '([Measures].[Store Sales]-[Measures].[Store Cost])/([Measures].[Store Cost])'," + nl +
				" FORMAT_STRING = '#.00%', SOLVE_ORDER = 1" + nl +
				" MEMBER [Time].[First Half 97] AS  '[Time].[1997].[Q1] + [Time].[1997].[Q2]'" + nl +
				" MEMBER [Time].[Second Half 97] AS '[Time].[1997].[Q3] + [Time].[1997].[Q4]'" + nl +
				" SELECT {[Time].[First Half 97]," + nl +
				"     [Time].[Second Half 97]," + nl +
				"     [Time].[1997].CHILDREN} ON COLUMNS," + nl +
				" {[Store].[Store Country].[USA].CHILDREN} ON ROWS" + nl +
				" FROM [Sales]" + nl +
				" WHERE ([Measures].[ProfitPercent])"));
	}

	public void _testHalfYearsTrickyCase() {
		Util.discard(runQuery(
				"WITH MEMBER MEASURES.ProfitPercent AS" + nl +
				"     '([Measures].[Store Sales]-[Measures].[Store Cost])/([Measures].[Store Cost])'," + nl +
				" FORMAT_STRING = '#.00%', SOLVE_ORDER = 1" + nl +
				" MEMBER [Time].[First Half 97] AS  '[Time].[1997].[Q1] + [Time].[1997].[Q2]'" + nl +
				" MEMBER [Time].[Second Half 97] AS '[Time].[1997].[Q3] + [Time].[1997].[Q4]'" + nl +
				" SELECT {[Time].[First Half 97]," + nl +
				"     [Time].[Second Half 97]," + nl +
				"     [Time].[1997].CHILDREN} ON COLUMNS," + nl +
				" {[Store].[Store Country].[USA].CHILDREN} ON ROWS" + nl +
				" FROM [Sales]" + nl +
				" WHERE (MEASURES.ProfitPercent)"));
	}

	public void testAsSample7ButUsingVirtualCube() {
		Util.discard(runQuery(
				"with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
				"select" + nl +
				"	 {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns," + nl +
				"	 {Descendants([Time].[1997],[Time].[Month])} on rows" + nl +
				"from [Warehouse and Sales]"));
	}

	public void _testVirtualCube() {
		Util.discard(runQuery(
				// Note that Unit Sales is independent of Warehouse.
				"select CrossJoin(" + nl +
				"  {[Warehouse].DefaultMember, [Warehouse].[USA].children}," + nl +
				"  {[Measures].[Unit Sales], [Measures].[Units Shipped]}) on columns," + nl +
				" [Time].children on rows" + nl +
				"from [Warehouse and Sales]"));
	}

	public void testUseDimensionAsShorthandForMember() {
		Util.discard(runQuery(
				"select {[Measures].[Unit Sales]} on columns," + nl +
				" {[Store], [Store].children} on rows" + nl +
				"from [Sales]"));
	}

	public void _testMembersFunction() {
		Util.discard(runQuery(
				// the simple-minded implementation of members(<number>) is
				// inefficient
				"select {[Measures].[Unit Sales]} on columns," + nl +
				" {[Customers].members(0)} on rows" + nl +
				"from [Sales]"));
	}

	public void _testProduct2() {
		final Axis axis = executeAxis2("{[Product2].members}");
		System.out.println(toString(axis.positions));
	}

	public static final QueryAndResult[] taglibQueries = {
		// 0
		new QueryAndResult(
				"select" + nl +
				"  {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns," + nl +
				"  CrossJoin(" + nl +
				"    { [Promotion Media].[All Media].[Radio]," + nl +
				"      [Promotion Media].[All Media].[TV]," + nl +
				"      [Promotion Media].[All Media].[Sunday Paper]," + nl +
				"      [Promotion Media].[All Media].[Street Handout] }," + nl +
				"    [Product].[All Products].[Drink].children) on rows" + nl +
				"from Sales" + nl +
				"where ([Time].[1997])",

				"Axis #0:" + nl +
				"{[Time].[1997]}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales]}" + nl +
				"{[Measures].[Store Cost]}" + nl +
				"{[Measures].[Store Sales]}" + nl +
				"Axis #2:" + nl +
				"{[Promotion Media].[All Media].[Radio], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Promotion Media].[All Media].[Radio], [Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Promotion Media].[All Media].[Radio], [Product].[All Products].[Drink].[Dairy]}" + nl +
				"{[Promotion Media].[All Media].[TV], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Promotion Media].[All Media].[TV], [Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Promotion Media].[All Media].[TV], [Product].[All Products].[Drink].[Dairy]}" + nl +
				"{[Promotion Media].[All Media].[Sunday Paper], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Promotion Media].[All Media].[Sunday Paper], [Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Promotion Media].[All Media].[Sunday Paper], [Product].[All Products].[Drink].[Dairy]}" + nl +
				"{[Promotion Media].[All Media].[Street Handout], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Promotion Media].[All Media].[Street Handout], [Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Promotion Media].[All Media].[Street Handout], [Product].[All Products].[Drink].[Dairy]}" + nl +
				"Row #0: 75" + nl +
				"Row #0: 70.40" + nl +
				"Row #0: 168.62" + nl +
				"Row #1: 97" + nl +
				"Row #1: 75.70" + nl +
				"Row #1: 186.03" + nl +
				"Row #2: 54" + nl +
				"Row #2: 36.75" + nl +
				"Row #2: 89.03" + nl +
				"Row #3: 76" + nl +
				"Row #3: 70.99" + nl +
				"Row #3: 182.38" + nl +
				"Row #4: 188" + nl +
				"Row #4: 167.00" + nl +
				"Row #4: 419.14" + nl +
				"Row #5: 68" + nl +
				"Row #5: 45.19" + nl +
				"Row #5: 119.55" + nl +
				"Row #6: 148" + nl +
				"Row #6: 128.97" + nl +
				"Row #6: 316.88" + nl +
				"Row #7: 197" + nl +
				"Row #7: 161.81" + nl +
				"Row #7: 399.58" + nl +
				"Row #8: 85" + nl +
				"Row #8: 54.75" + nl +
				"Row #8: 140.27" + nl +
				"Row #9: 158" + nl +
				"Row #9: 121.14" + nl +
				"Row #9: 294.55" + nl +
				"Row #10: 270" + nl +
				"Row #10: 201.28" + nl +
				"Row #10: 520.55" + nl +
				"Row #11: 84" + nl +
				"Row #11: 50.26" + nl +
				"Row #11: 128.32" + nl),

		// 1
		new QueryAndResult(
				"select" + nl +
				"  [Product].[All Products].[Drink].children on rows," + nl +
				"  CrossJoin(" + nl +
				"    {[Measures].[Unit Sales], [Measures].[Store Sales]}," + nl +
				"    { [Promotion Media].[All Media].[Radio]," + nl +
				"      [Promotion Media].[All Media].[TV]," + nl +
				"      [Promotion Media].[All Media].[Sunday Paper]," + nl +
				"      [Promotion Media].[All Media].[Street Handout] }" + nl +
				"    ) on columns" + nl +
				"from Sales" + nl +
				"where ([Time].[1997])",

				"Axis #0:" + nl +
				"{[Time].[1997]}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Media].[Radio]}" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Media].[TV]}" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Media].[Sunday Paper]}" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Media].[Street Handout]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Media].[Radio]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Media].[TV]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Media].[Sunday Paper]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Media].[Street Handout]}" + nl +
				"Axis #2:" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy]}" + nl +
				"Row #0: 75" + nl +
				"Row #0: 76" + nl +
				"Row #0: 148" + nl +
				"Row #0: 158" + nl +
				"Row #0: 168.62" + nl +
				"Row #0: 182.38" + nl +
				"Row #0: 316.88" + nl +
				"Row #0: 294.55" + nl +
				"Row #1: 97" + nl +
				"Row #1: 188" + nl +
				"Row #1: 197" + nl +
				"Row #1: 270" + nl +
				"Row #1: 186.03" + nl +
				"Row #1: 419.14" + nl +
				"Row #1: 399.58" + nl +
				"Row #1: 520.55" + nl +
				"Row #2: 54" + nl +
				"Row #2: 68" + nl +
				"Row #2: 85" + nl +
				"Row #2: 84" + nl +
				"Row #2: 89.03" + nl +
				"Row #2: 119.55" + nl +
				"Row #2: 140.27" + nl +
				"Row #2: 128.32" + nl),

		// 2
		new QueryAndResult(
				"select" + nl +
				"  {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns," + nl +
				"  Order([Product].[Product Department].members, [Measures].[Store Sales], DESC) on rows" + nl +
				"from Sales",

				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales]}" + nl +
				"{[Measures].[Store Sales]}" + nl +
				"Axis #2:" + nl +
				"{[Product].[All Products].[Food].[Produce]}" + nl +
				"{[Product].[All Products].[Food].[Snack Foods]}" + nl +
				"{[Product].[All Products].[Food].[Frozen Foods]}" + nl +
				"{[Product].[All Products].[Food].[Canned Foods]}" + nl +
				"{[Product].[All Products].[Food].[Baking Goods]}" + nl +
				"{[Product].[All Products].[Food].[Dairy]}" + nl +
				"{[Product].[All Products].[Food].[Deli]}" + nl +
				"{[Product].[All Products].[Food].[Baked Goods]}" + nl +
				"{[Product].[All Products].[Food].[Snacks]}" + nl +
				"{[Product].[All Products].[Food].[Starchy Foods]}" + nl +
				"{[Product].[All Products].[Food].[Eggs]}" + nl +
				"{[Product].[All Products].[Food].[Breakfast Foods]}" + nl +
				"{[Product].[All Products].[Food].[Seafood]}" + nl +
				"{[Product].[All Products].[Food].[Meat]}" + nl +
				"{[Product].[All Products].[Food].[Canned Products]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Household]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Health and Hygiene]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Periodicals]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Checkout]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Carousel]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy]}" + nl +
				"Row #0: 37,792" + nl +
				"Row #0: 82,248.42" + nl +
				"Row #1: 30,545" + nl +
				"Row #1: 67,609.82" + nl +
				"Row #2: 26,655" + nl +
				"Row #2: 55,207.50" + nl +
				"Row #3: 19,026" + nl +
				"Row #3: 39,774.34" + nl +
				"Row #4: 20,245" + nl +
				"Row #4: 38,670.41" + nl +
				"Row #5: 12,885" + nl +
				"Row #5: 30,508.85" + nl +
				"Row #6: 12,037" + nl +
				"Row #6: 25,318.93" + nl +
				"Row #7: 7,870" + nl +
				"Row #7: 16,455.43" + nl +
				"Row #8: 6,884" + nl +
				"Row #8: 14,550.05" + nl +
				"Row #9: 5,262" + nl +
				"Row #9: 11,756.07" + nl +
				"Row #10: 4,132" + nl +
				"Row #10: 9,200.76" + nl +
				"Row #11: 3,317" + nl +
				"Row #11: 6,941.46" + nl +
				"Row #12: 1,764" + nl +
				"Row #12: 3,809.14" + nl +
				"Row #13: 1,714" + nl +
				"Row #13: 3,669.89" + nl +
				"Row #14: 1,812" + nl +
				"Row #14: 3,314.52" + nl +
				"Row #15: 27,038" + nl +
				"Row #15: 60,469.89" + nl +
				"Row #16: 16,284" + nl +
				"Row #16: 32,571.86" + nl +
				"Row #17: 4,294" + nl +
				"Row #17: 9,056.76" + nl +
				"Row #18: 1,779" + nl +
				"Row #18: 3,767.71" + nl +
				"Row #19: 841" + nl +
				"Row #19: 1,500.11" + nl +
				"Row #20: 13,573" + nl +
				"Row #20: 27,748.53" + nl +
				"Row #21: 6,838" + nl +
				"Row #21: 14,029.08" + nl +
				"Row #22: 4,186" + nl +
				"Row #22: 7,058.60" + nl),

		// 3
		new QueryAndResult(
				"select" + nl +
				"  [Product].[All Products].[Drink].children on columns" + nl +
				"from Sales" + nl +
				"where ([Measures].[Unit Sales], [Promotion Media].[All Media].[Street Handout], [Time].[1997])",

				"Axis #0:" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Media].[Street Handout], [Time].[1997]}" + nl +
				"Axis #1:" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy]}" + nl +
				"Row #0: 158" + nl +
				"Row #0: 270" + nl +
				"Row #0: 84" + nl),

		// 4
		new QueryAndResult(
				"select" + nl +
				"  NON EMPTY CrossJoin([Product].[All Products].[Drink].children, [Customers].[All Customers].[USA].[WA].Children) on rows," + nl +
				"  CrossJoin(" + nl +
				"    {[Measures].[Unit Sales], [Measures].[Store Sales]}," + nl +
				"    { [Promotion Media].[All Media].[Radio]," + nl +
				"      [Promotion Media].[All Media].[TV]," + nl +
				"      [Promotion Media].[All Media].[Sunday Paper]," + nl +
				"      [Promotion Media].[All Media].[Street Handout] }" + nl +
				"    ) on columns" + nl +
				"from Sales" + nl +
				"where ([Time].[1997])",

				("Axis #0:" + nl +
				"{[Time].[1997]}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Media].[Radio]}" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Media].[TV]}" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Media].[Sunday Paper]}" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Media].[Street Handout]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Media].[Radio]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Media].[TV]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Media].[Sunday Paper]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Media].[Street Handout]}" + nl +
				"Axis #2:" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Anacortes]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Ballard]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Bellingham]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Bremerton]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Burien]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Everett]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Issaquah]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Kirkland]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Lynnwood]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Marysville]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Olympia]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Port Orchard]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Puyallup]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Redmond]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Renton]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Seattle]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Spokane]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Tacoma]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Customers].[All Customers].[USA].[WA].[Yakima]}" + nl) + (
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Anacortes]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Ballard]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Bremerton]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Burien]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Edmonds]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Everett]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Issaquah]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Kirkland]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Lynnwood]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Marysville]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Olympia]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Port Orchard]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Puyallup]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Redmond]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Seattle]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Sedro Woolley]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Spokane]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Tacoma]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Walla Walla]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages], [Customers].[All Customers].[USA].[WA].[Yakima]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Ballard]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Bellingham]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Bremerton]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Burien]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Everett]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Issaquah]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Kirkland]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Lynnwood]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Marysville]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Olympia]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Port Orchard]}" + nl) + (
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Puyallup]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Redmond]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Renton]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Seattle]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Spokane]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Tacoma]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy], [Customers].[All Customers].[USA].[WA].[Yakima]}" + nl +
				"Row #0: (null)" + nl +
				"Row #0: 2" + nl +
				"Row #0: (null)" + nl +
				"Row #0: (null)" + nl +
				"Row #0: (null)" + nl +
				"Row #0: 1.14" + nl +
				"Row #0: (null)" + nl +
				"Row #0: (null)" + nl +
				"Row #1: 4" + nl +
				"Row #1: (null)" + nl +
				"Row #1: (null)" + nl +
				"Row #1: 4" + nl +
				"Row #1: 10.40" + nl +
				"Row #1: (null)" + nl +
				"Row #1: (null)" + nl +
				"Row #1: 2.16" + nl +
				"Row #2: (null)" + nl +
				"Row #2: 1" + nl +
				"Row #2: (null)" + nl +
				"Row #2: (null)" + nl +
				"Row #2: (null)" + nl +
				"Row #2: 2.37" + nl +
				"Row #2: (null)" + nl +
				"Row #2: (null)" + nl) + (
				"Row #3: (null)" + nl +
				"Row #3: (null)" + nl +
				"Row #3: 24" + nl +
				"Row #3: (null)" + nl +
				"Row #3: (null)" + nl +
				"Row #3: (null)" + nl +
				"Row #3: 46.09" + nl +
				"Row #3: (null)" + nl +
				"Row #4: 3" + nl +
				"Row #4: (null)" + nl +
				"Row #4: (null)" + nl +
				"Row #4: 8" + nl +
				"Row #4: 2.10" + nl +
				"Row #4: (null)" + nl +
				"Row #4: (null)" + nl +
				"Row #4: 9.63" + nl +
				"Row #5: 6" + nl +
				"Row #5: (null)" + nl +
				"Row #5: (null)" + nl +
				"Row #5: 5" + nl +
				"Row #5: 8.06" + nl +
				"Row #5: (null)" + nl +
				"Row #5: (null)" + nl +
				"Row #5: 6.21" + nl +
				"Row #6: 3" + nl +
				"Row #6: (null)" + nl +
				"Row #6: (null)" + nl +
				"Row #6: 7" + nl +
				"Row #6: 7.80" + nl +
				"Row #6: (null)" + nl +
				"Row #6: (null)" + nl) + (
				"Row #6: 15.00" + nl +
				"Row #7: 14" + nl +
				"Row #7: (null)" + nl +
				"Row #7: (null)" + nl +
				"Row #7: (null)" + nl +
				"Row #7: 36.10" + nl +
				"Row #7: (null)" + nl +
				"Row #7: (null)" + nl +
				"Row #7: (null)" + nl +
				"Row #8: 3" + nl +
				"Row #8: (null)" + nl +
				"Row #8: (null)" + nl +
				"Row #8: 16" + nl +
				"Row #8: 10.29" + nl +
				"Row #8: (null)" + nl +
				"Row #8: (null)" + nl +
				"Row #8: 32.20" + nl +
				"Row #9: 3" + nl +
				"Row #9: (null)" + nl +
				"Row #9: (null)" + nl +
				"Row #9: (null)" + nl +
				"Row #9: 10.56" + nl +
				"Row #9: (null)" + nl +
				"Row #9: (null)" + nl +
				"Row #9: (null)" + nl +
				"Row #10: (null)" + nl +
				"Row #10: (null)" + nl +
				"Row #10: 15" + nl +
				"Row #10: 11" + nl +
				"Row #10: (null)" + nl +
				"Row #10: (null)" + nl) + (
				"Row #10: 34.79" + nl +
				"Row #10: 15.67" + nl +
				"Row #11: (null)" + nl +
				"Row #11: (null)" + nl +
				"Row #11: 7" + nl +
				"Row #11: (null)" + nl +
				"Row #11: (null)" + nl +
				"Row #11: (null)" + nl +
				"Row #11: 17.44" + nl +
				"Row #11: (null)" + nl +
				"Row #12: (null)" + nl +
				"Row #12: (null)" + nl +
				"Row #12: 22" + nl +
				"Row #12: 9" + nl +
				"Row #12: (null)" + nl +
				"Row #12: (null)" + nl +
				"Row #12: 32.35" + nl +
				"Row #12: 17.43" + nl +
				"Row #13: 7" + nl +
				"Row #13: (null)" + nl +
				"Row #13: (null)" + nl +
				"Row #13: 4" + nl +
				"Row #13: 4.77" + nl +
				"Row #13: (null)" + nl +
				"Row #13: (null)" + nl +
				"Row #13: 15.16" + nl +
				"Row #14: 4" + nl +
				"Row #14: (null)" + nl +
				"Row #14: (null)" + nl +
				"Row #14: 4" + nl +
				"Row #14: 3.64" + nl) + (
				"Row #14: (null)" + nl +
				"Row #14: (null)" + nl +
				"Row #14: 9.64" + nl +
				"Row #15: 2" + nl +
				"Row #15: (null)" + nl +
				"Row #15: (null)" + nl +
				"Row #15: 7" + nl +
				"Row #15: 6.86" + nl +
				"Row #15: (null)" + nl +
				"Row #15: (null)" + nl +
				"Row #15: 8.38" + nl +
				"Row #16: (null)" + nl +
				"Row #16: (null)" + nl +
				"Row #16: (null)" + nl +
				"Row #16: 28" + nl +
				"Row #16: (null)" + nl +
				"Row #16: (null)" + nl +
				"Row #16: (null)" + nl +
				"Row #16: 61.98" + nl +
				"Row #17: (null)" + nl +
				"Row #17: (null)" + nl +
				"Row #17: 3" + nl +
				"Row #17: 4" + nl +
				"Row #17: (null)" + nl +
				"Row #17: (null)" + nl +
				"Row #17: 10.56" + nl +
				"Row #17: 8.96" + nl +
				"Row #18: 6" + nl +
				"Row #18: (null)" + nl +
				"Row #18: (null)" + nl +
				"Row #18: 3" + nl) + (
				"Row #18: 7.16" + nl +
				"Row #18: (null)" + nl +
				"Row #18: (null)" + nl +
				"Row #18: 8.10" + nl +
				"Row #19: 7" + nl +
				"Row #19: (null)" + nl +
				"Row #19: (null)" + nl +
				"Row #19: (null)" + nl +
				"Row #19: 15.63" + nl +
				"Row #19: (null)" + nl +
				"Row #19: (null)" + nl +
				"Row #19: (null)" + nl +
				"Row #20: 3" + nl +
				"Row #20: (null)" + nl +
				"Row #20: (null)" + nl +
				"Row #20: 13" + nl +
				"Row #20: 6.96" + nl +
				"Row #20: (null)" + nl +
				"Row #20: (null)" + nl +
				"Row #20: 12.22" + nl +
				"Row #21: (null)" + nl +
				"Row #21: (null)" + nl +
				"Row #21: 16" + nl +
				"Row #21: (null)" + nl +
				"Row #21: (null)" + nl +
				"Row #21: (null)" + nl +
				"Row #21: 45.08" + nl +
				"Row #21: (null)" + nl +
				"Row #22: 3" + nl +
				"Row #22: (null)" + nl +
				"Row #22: (null)") + nl + (
				"Row #22: 18" + nl +
				"Row #22: 6.39" + nl +
				"Row #22: (null)" + nl +
				"Row #22: (null)" + nl +
				"Row #22: 21.08" + nl +
				"Row #23: (null)" + nl +
				"Row #23: (null)" + nl +
				"Row #23: (null)" + nl +
				"Row #23: 21" + nl +
				"Row #23: (null)" + nl +
				"Row #23: (null)" + nl +
				"Row #23: (null)" + nl +
				"Row #23: 33.22" + nl +
				"Row #24: (null)" + nl +
				"Row #24: (null)" + nl +
				"Row #24: (null)" + nl +
				"Row #24: 9" + nl +
				"Row #24: (null)" + nl +
				"Row #24: (null)" + nl +
				"Row #24: (null)" + nl +
				"Row #24: 22.65" + nl +
				"Row #25: 2" + nl +
				"Row #25: (null)" + nl +
				"Row #25: (null)" + nl +
				"Row #25: 9" + nl +
				"Row #25: 6.80" + nl +
				"Row #25: (null)" + nl +
				"Row #25: (null)" + nl +
				"Row #25: 18.90" + nl +
				"Row #26: 3" + nl +
				"Row #26: (null)" + nl) + (
				"Row #26: (null)" + nl +
				"Row #26: 9" + nl +
				"Row #26: 1.50" + nl +
				"Row #26: (null)" + nl +
				"Row #26: (null)" + nl +
				"Row #26: 23.01" + nl +
				"Row #27: (null)" + nl +
				"Row #27: (null)" + nl +
				"Row #27: (null)" + nl +
				"Row #27: 22" + nl +
				"Row #27: (null)" + nl +
				"Row #27: (null)" + nl +
				"Row #27: (null)" + nl +
				"Row #27: 50.71" + nl +
				"Row #28: 4" + nl +
				"Row #28: (null)" + nl +
				"Row #28: (null)" + nl +
				"Row #28: (null)" + nl +
				"Row #28: 5.16" + nl +
				"Row #28: (null)" + nl +
				"Row #28: (null)" + nl +
				"Row #28: (null)" + nl +
				"Row #29: (null)" + nl +
				"Row #29: (null)" + nl +
				"Row #29: 20" + nl +
				"Row #29: 14" + nl +
				"Row #29: (null)" + nl +
				"Row #29: (null)" + nl +
				"Row #29: 48.02" + nl +
				"Row #29: 28.80" + nl +
				"Row #30: (null)" + nl) + (
				"Row #30: (null)" + nl +
				"Row #30: 14" + nl +
				"Row #30: (null)" + nl +
				"Row #30: (null)" + nl +
				"Row #30: (null)" + nl +
				"Row #30: 19.96" + nl +
				"Row #30: (null)" + nl +
				"Row #31: (null)" + nl +
				"Row #31: (null)" + nl +
				"Row #31: 10" + nl +
				"Row #31: 40" + nl +
				"Row #31: (null)" + nl +
				"Row #31: (null)" + nl +
				"Row #31: 26.36" + nl +
				"Row #31: 74.49" + nl +
				"Row #32: 6" + nl +
				"Row #32: (null)" + nl +
				"Row #32: (null)" + nl +
				"Row #32: (null)" + nl +
				"Row #32: 17.01" + nl +
				"Row #32: (null)" + nl +
				"Row #32: (null)" + nl +
				"Row #32: (null)" + nl +
				"Row #33: 4" + nl +
				"Row #33: (null)" + nl +
				"Row #33: (null)" + nl +
				"Row #33: (null)" + nl +
				"Row #33: 2.80" + nl +
				"Row #33: (null)" + nl +
				"Row #33: (null)" + nl +
				"Row #33: (null)" + nl) + (
				"Row #34: 4" + nl +
				"Row #34: (null)" + nl +
				"Row #34: (null)" + nl +
				"Row #34: (null)" + nl +
				"Row #34: 7.98" + nl +
				"Row #34: (null)" + nl +
				"Row #34: (null)" + nl +
				"Row #34: (null)" + nl +
				"Row #35: (null)" + nl +
				"Row #35: (null)" + nl +
				"Row #35: (null)" + nl +
				"Row #35: 46" + nl +
				"Row #35: (null)" + nl +
				"Row #35: (null)" + nl +
				"Row #35: (null)" + nl +
				"Row #35: 81.71" + nl +
				"Row #36: (null)" + nl +
				"Row #36: (null)" + nl +
				"Row #36: 21" + nl +
				"Row #36: 6" + nl +
				"Row #36: (null)" + nl +
				"Row #36: (null)" + nl +
				"Row #36: 37.93" + nl +
				"Row #36: 14.73" + nl +
				"Row #37: (null)" + nl +
				"Row #37: (null)" + nl +
				"Row #37: 3" + nl +
				"Row #37: (null)" + nl +
				"Row #37: (null)" + nl +
				"Row #37: (null)" + nl +
				"Row #37: 7.92" + nl) + (
				"Row #37: (null)" + nl +
				"Row #38: 25" + nl +
				"Row #38: (null)" + nl +
				"Row #38: (null)" + nl +
				"Row #38: 3" + nl +
				"Row #38: 51.65" + nl +
				"Row #38: (null)" + nl +
				"Row #38: (null)" + nl +
				"Row #38: 2.34" + nl +
				"Row #39: 3" + nl +
				"Row #39: (null)" + nl +
				"Row #39: (null)" + nl +
				"Row #39: 4" + nl +
				"Row #39: 4.47" + nl +
				"Row #39: (null)" + nl +
				"Row #39: (null)" + nl +
				"Row #39: 9.20" + nl +
				"Row #40: (null)" + nl +
				"Row #40: 1" + nl +
				"Row #40: (null)" + nl +
				"Row #40: (null)" + nl +
				"Row #40: (null)" + nl +
				"Row #40: 1.47" + nl +
				"Row #40: (null)" + nl +
				"Row #40: (null)" + nl +
				"Row #41: (null)" + nl +
				"Row #41: (null)" + nl +
				"Row #41: 15" + nl +
				"Row #41: (null)" + nl +
				"Row #41: (null)" + nl +
				"Row #41: (null)" + nl) + (
				"Row #41: 18.88" + nl +
				"Row #41: (null)" + nl +
				"Row #42: (null)" + nl +
				"Row #42: (null)" + nl +
				"Row #42: (null)" + nl +
				"Row #42: 3" + nl +
				"Row #42: (null)" + nl +
				"Row #42: (null)" + nl +
				"Row #42: (null)" + nl +
				"Row #42: 3.75" + nl +
				"Row #43: 9" + nl +
				"Row #43: (null)" + nl +
				"Row #43: (null)" + nl +
				"Row #43: 10" + nl +
				"Row #43: 31.41" + nl +
				"Row #43: (null)" + nl +
				"Row #43: (null)" + nl +
				"Row #43: 15.12" + nl +
				"Row #44: 3" + nl +
				"Row #44: (null)" + nl +
				"Row #44: (null)" + nl +
				"Row #44: 3" + nl +
				"Row #44: 7.41" + nl +
				"Row #44: (null)" + nl +
				"Row #44: (null)" + nl +
				"Row #44: 2.55" + nl +
				"Row #45: 3" + nl +
				"Row #45: (null)" + nl +
				"Row #45: (null)" + nl +
				"Row #45: (null)" + nl +
				"Row #45: 1.71" + nl) + (
				"Row #45: (null)" + nl +
				"Row #45: (null)" + nl +
				"Row #45: (null)" + nl +
				"Row #46: (null)" + nl +
				"Row #46: (null)" + nl +
				"Row #46: (null)" + nl +
				"Row #46: 7" + nl +
				"Row #46: (null)" + nl +
				"Row #46: (null)" + nl +
				"Row #46: (null)" + nl +
				"Row #46: 11.86" + nl +
				"Row #47: (null)" + nl +
				"Row #47: (null)" + nl +
				"Row #47: (null)" + nl +
				"Row #47: 3" + nl +
				"Row #47: (null)" + nl +
				"Row #47: (null)" + nl +
				"Row #47: (null)" + nl +
				"Row #47: 2.76" + nl +
				"Row #48: (null)" + nl +
				"Row #48: (null)" + nl +
				"Row #48: 4" + nl +
				"Row #48: 5" + nl +
				"Row #48: (null)" + nl +
				"Row #48: (null)" + nl +
				"Row #48: 4.50" + nl +
				"Row #48: 7.27" + nl +
				"Row #49: (null)" + nl +
				"Row #49: (null)" + nl +
				"Row #49: 7" + nl +
				"Row #49: (null)" + nl) + (
				"Row #49: (null)" + nl +
				"Row #49: (null)" + nl +
				"Row #49: 10.01" + nl +
				"Row #49: (null)" + nl +
				"Row #50: (null)" + nl +
				"Row #50: (null)" + nl +
				"Row #50: 5" + nl +
				"Row #50: 4" + nl +
				"Row #50: (null)" + nl +
				"Row #50: (null)" + nl +
				"Row #50: 12.88" + nl +
				"Row #50: 5.28" + nl +
				"Row #51: 2" + nl +
				"Row #51: (null)" + nl +
				"Row #51: (null)" + nl +
				"Row #51: (null)" + nl +
				"Row #51: 2.64" + nl +
				"Row #51: (null)" + nl +
				"Row #51: (null)" + nl +
				"Row #51: (null)" + nl +
				"Row #52: (null)" + nl +
				"Row #52: (null)" + nl +
				"Row #52: (null)" + nl +
				"Row #52: 5" + nl +
				"Row #52: (null)" + nl +
				"Row #52: (null)" + nl +
				"Row #52: (null)" + nl +
				"Row #52: 12.34" + nl +
				"Row #53: (null)" + nl +
				"Row #53: (null)" + nl +
				"Row #53: (null)" + nl) + (
				"Row #53: 5" + nl +
				"Row #53: (null)" + nl +
				"Row #53: (null)" + nl +
				"Row #53: (null)" + nl +
				"Row #53: 3.41" + nl +
				"Row #54: (null)" + nl +
				"Row #54: (null)" + nl +
				"Row #54: (null)" + nl +
				"Row #54: 4" + nl +
				"Row #54: (null)" + nl +
				"Row #54: (null)" + nl +
				"Row #54: (null)" + nl +
				"Row #54: 2.44" + nl +
				"Row #55: (null)" + nl +
				"Row #55: (null)" + nl +
				"Row #55: 2" + nl +
				"Row #55: (null)" + nl +
				"Row #55: (null)" + nl +
				"Row #55: (null)" + nl +
				"Row #55: 6.92" + nl +
				"Row #55: (null)" + nl +
				"Row #56: 13" + nl +
				"Row #56: (null)" + nl +
				"Row #56: (null)" + nl +
				"Row #56: 7" + nl +
				"Row #56: 23.69" + nl +
				"Row #56: (null)" + nl +
				"Row #56: (null)" + nl +
				"Row #56: 7.07" + nl)),

		// 5
		new QueryAndResult(
				"select from Sales" + nl +
					"where ([Measures].[Store Sales], [Time].[1997], [Promotion Media].[All Media].[TV])",

				"Axis #0:" + nl +
				"{[Measures].[Store Sales], [Time].[1997], [Promotion Media].[All Media].[TV]}" + nl +
				"7,786.21"),
	};

	public void testTaglib0() {
		runQueryCheckResult(taglibQueries[0]);
	}

	public void testTaglib1() {
		runQueryCheckResult(taglibQueries[1]);
	}

	public void testTaglib2() {
		runQueryCheckResult(taglibQueries[2]);
	}

	public void testTaglib3() {
		runQueryCheckResult(taglibQueries[3]);
	}

	public void testTaglib4() {
		runQueryCheckResult(taglibQueries[4]);
	}

	public void testTaglib5() {
		runQueryCheckResult(taglibQueries[5]);
	}

	public void testCellValue() {
		Result result = runQuery(
				"select {[Measures].[Unit Sales],[Measures].[Store Sales]} on columns," + nl +
				" {[Gender].[M]} on rows" + nl +
				"from Sales");
		Cell cell = result.getCell(new int[] {0,0});
		Object value = cell.getValue();
		assertTrue(value instanceof Number);
		assertEquals(135215, ((Number) value).intValue());
		cell = result.getCell(new int[] {1,0});
		value = cell.getValue();
		assertTrue(value instanceof Number);
		// Plato give 285011.12, Oracle gives 285011, MySQL gives 285964 (bug!)
		assertEquals(285011, ((Number) value).intValue());
	}

	public void testDynamicFormat() {
		runQueryCheckResult(
				"with member [Measures].[USales] as [Measures].[Unit Sales]," + nl +
				"  format_string = iif([Measures].[Unit Sales] > 50000, \"\\<b\\>#.00\\<\\/b\\>\", \"\\<i\\>#.00\\<\\/i\\>\")" + nl +
				"select " + nl +
				"  {[Measures].[USales]} on columns," + nl +
				"  {[Store Type].members} on rows" + nl +
				"from Sales",

				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[USales]}" + nl +
				"Axis #2:" + nl +
				"{[Store Type].[All Store Types]}" + nl +
				"{[Store Type].[All Store Types].[Deluxe Supermarket]}" + nl +
				"{[Store Type].[All Store Types].[Gourmet Supermarket]}" + nl +
				"{[Store Type].[All Store Types].[Mid-Size Grocery]}" + nl +
				"{[Store Type].[All Store Types].[Small Grocery]}" + nl +
				"{[Store Type].[All Store Types].[Supermarket]}" + nl +
				"Row #0: <b>266773.00</b>" + nl +
				"Row #1: <b>76837.00</b>" + nl +
				"Row #2: <i>21333.00</i>" + nl +
				"Row #3: <i>11491.00</i>" + nl +
				"Row #4: <i>6557.00</i>" + nl +
				"Row #5: <b>150555.00</b>" + nl);
	}
	/**
	 * If a measure (in this case, <code>[Measures].[Sales Count]</code>)
	 * occurs only within a format expression, bug 684593 causes an internal
	 * error ("value not found") when the cell's formatted value is retrieved.
	 */
	public void testBug684593(FoodMartTestCase test) {
		test.runQueryCheckResult(
				"with member [Measures].[USales] as '[Measures].[Unit Sales]'," + nl +
				" format_string = iif([Measures].[Sales Count] > 30, \"#.00 good\",\"#.00 bad\")" + nl +
				"select {[Measures].[USales], [Measures].[Store Cost], [Measures].[Store Sales]} ON columns," + nl +
				" Crossjoin({[Promotion Media].[All Media].[Radio], [Promotion Media].[All Media].[TV], [Promotion Media]. [All Media].[Sunday Paper], [Promotion Media].[All Media].[Street Handout]}, [Product].[All Products].[Drink].Children) ON rows" + nl +
				"from [Sales] where ([Time].[1997])",

				"Axis #0:" + nl +
				"{[Time].[1997]}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[USales]}" + nl +
				"{[Measures].[Store Cost]}" + nl +
				"{[Measures].[Store Sales]}" + nl +
				"Axis #2:" + nl +
				"{[Promotion Media].[All Media].[Radio], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Promotion Media].[All Media].[Radio], [Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Promotion Media].[All Media].[Radio], [Product].[All Products].[Drink].[Dairy]}" + nl +
				"{[Promotion Media].[All Media].[TV], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Promotion Media].[All Media].[TV], [Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Promotion Media].[All Media].[TV], [Product].[All Products].[Drink].[Dairy]}" + nl +
				"{[Promotion Media].[All Media].[Sunday Paper], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Promotion Media].[All Media].[Sunday Paper], [Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Promotion Media].[All Media].[Sunday Paper], [Product].[All Products].[Drink].[Dairy]}" + nl +
				"{[Promotion Media].[All Media].[Street Handout], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Promotion Media].[All Media].[Street Handout], [Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Promotion Media].[All Media].[Street Handout], [Product].[All Products].[Drink].[Dairy]}" + nl +
				"Row #0: 75.00 bad" + nl +
				"Row #0: 70.40" + nl +
				"Row #0: 168.62" + nl +
				"Row #1: 97.00 good" + nl +
				"Row #1: 75.70" + nl +
				"Row #1: 186.03" + nl +
				"Row #2: 54.00 bad" + nl +
				"Row #2: 36.75" + nl +
				"Row #2: 89.03" + nl +
				"Row #3: 76.00 bad" + nl +
				"Row #3: 70.99" + nl +
				"Row #3: 182.38" + nl +
				"Row #4: 188.00 good" + nl +
				"Row #4: 167.00" + nl +
				"Row #4: 419.14" + nl +
				"Row #5: 68.00 bad" + nl +
				"Row #5: 45.19" + nl +
				"Row #5: 119.55" + nl +
				"Row #6: 148.00 good" + nl +
				"Row #6: 128.97" + nl +
				"Row #6: 316.88" + nl +
				"Row #7: 197.00 good" + nl +
				"Row #7: 161.81" + nl +
				"Row #7: 399.58" + nl +
				"Row #8: 85.00 bad" + nl +
				"Row #8: 54.75" + nl +
				"Row #8: 140.27" + nl +
				"Row #9: 158.00 good" + nl +
				"Row #9: 121.14" + nl +
				"Row #9: 294.55" + nl +
				"Row #10: 270.00 good" + nl +
				"Row #10: 201.28" + nl +
				"Row #10: 520.55" + nl +
				"Row #11: 84.00 bad" + nl +
				"Row #11: 50.26" + nl +
				"Row #11: 128.32" + nl);
	}
	/** Make sure that the "Store" cube is working. **/
	public void testStoreCube() {
		runQueryCheckResult(
				"select {[Measures].members} on columns," + nl +
				" {[Store Type].members} on rows" + nl +
				"from [Store]" +
				"where [Store].[USA].[CA]",

				"Axis #0:" + nl +
				"{[Store].[All Stores].[USA].[CA]}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Store Sqft]}" + nl +
				"{[Measures].[Grocery Sqft]}" + nl +
				"Axis #2:" + nl +
				"{[Store Type].[All Store Types]}" + nl +
				"{[Store Type].[All Store Types].[Deluxe Supermarket]}" + nl +
				"{[Store Type].[All Store Types].[Gourmet Supermarket]}" + nl +
				"{[Store Type].[All Store Types].[Mid-Size Grocery]}" + nl +
				"{[Store Type].[All Store Types].[Small Grocery]}" + nl +
				"{[Store Type].[All Store Types].[Supermarket]}" + nl +
				"Row #0: 69,764" + nl +
				"Row #0: 44,868" + nl +
				"Row #1: (null)" + nl +
				"Row #1: (null)" + nl +
				"Row #2: 23,688" + nl +
				"Row #2: 15,337" + nl +
				"Row #3: (null)" + nl +
				"Row #3: (null)" + nl +
				"Row #4: 22,478" + nl +
				"Row #4: 15,321" + nl +
				"Row #5: 23,598" + nl +
				"Row #5: 14,210" + nl);
	}

	public void testSchemaLevelTableIsBad() {
		// todo: <Level table="nonexistentTable">
	}

	public void testSchemaLevelTableInAnotherHierarchy() {
		// todo:
		// <Cube>
		// <Hierarchy name="h1"><Table name="t1"/></Hierarchy>
		// <Hierarchy name="h2"><Table name="t2"/><Level tableName="t1"/></Hierarchy>
		// </Cube>
	}

	public void testSchemaLevelWithViewSpecifiesTable() {
		// todo:
		// <Hierarchy>
		//  <View><SQL dialect="generic">select * from emp</SQL></View>
		//  <Level tableName="emp"/>
		// </hierarchy>
		// Should get error that tablename is not allowed
	}

	public void testSchemaLevelOrdinalInOtherTable() {
		// todo:
		// Hierarchy is based upon a join.
		// Level's name expression is in a different table than its ordinal.
	}

	public void testSchemaTopLevelNotUnique() {
		// todo:
		// Should get error if the top level of a hierarchy does not have
		// uniqueNames="true"
	}

	/**
	 * Bug 645744 happens when getting the children of a member crosses a table
	 * boundary. The symptom
	 */
	public void testBug645744() {
		// minimal test case
		runQueryCheckResult(
				"select {[Measures].[Unit Sales]} ON columns," + nl +
 				"{[Product].[All Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].children} ON rows" + nl +
				"from [Sales]",

				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales]}" + nl +
				"Axis #2:" + nl +
				"{[Product].[All Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Excellent]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Fabulous]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Skinner]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Token]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Washington]}" + nl +
				"Row #0: 468" + nl +
				"Row #1: 469" + nl +
				"Row #2: 506" + nl +
				"Row #3: 466" + nl +
				"Row #4: 560" + nl);

		// shorter test case
		runQuery(
				"select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON columns,"+
				"ToggleDrillState({"+
				"([Promotion Media].[All Media].[Radio], [Product].[All Products].[Drink].[Beverages].[Drinks].[Flavored Drinks])"+
				"}, {[Product].[All Products].[Drink].[Beverages].[Drinks].[Flavored Drinks]}) ON rows "+
				"from [Sales] where ([Time].[1997])");
	}

	/**
	 * The bug happened when a cell which was in cache was compared with a cell
	 * which was not in cache. The compare method could not deal with the
	 * {@link RuntimeException} which indicates that the cell is not in cache.
	 */
	public void testBug636687() {
		runQuery(
				"select {[Measures].[Unit Sales], [Measures].[Store Cost],[Measures].[Store Sales]} ON columns, " +
				"Order(" +
				"{([Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Alcoholic Beverages]), " +
				"([Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Beverages]), " +
				"Crossjoin({[Store].[All Stores].[USA].[CA].Children}, {[Product].[All Products].[Drink].[Beverages]}), " +
				"([Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Dairy]), " +
				"([Store].[All Stores].[USA].[OR], [Product].[All Products].[Drink].[Alcoholic Beverages]), " +
				"([Store].[All Stores].[USA].[OR], [Product].[All Products].[Drink].[Beverages]), " +
				"([Store].[All Stores].[USA].[OR], [Product].[All Products].[Drink].[Dairy]), " +
				"([Store].[All Stores].[USA].[WA], [Product].[All Products].[Drink].[Alcoholic Beverages]), " +
				"([Store].[All Stores].[USA].[WA], [Product].[All Products].[Drink].[Beverages]), " +
				"([Store].[All Stores].[USA].[WA], [Product].[All Products].[Drink].[Dairy])}, " +
				"[Measures].[Store Cost], BDESC) ON rows " +
				"from [Sales] " +
				"where ([Time].[1997])");
		runQuery(
				"select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON columns, " +
				"Order(" +
				"{([Store].[All Stores].[USA].[WA], [Product].[All Products].[Drink].[Beverages]), " +
				"([Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Beverages]), " +
				"([Store].[All Stores].[USA].[OR], [Product].[All Products].[Drink].[Beverages]), " +
				"([Store].[All Stores].[USA].[WA], [Product].[All Products].[Drink].[Alcoholic Beverages]), " +
				"([Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Alcoholic Beverages]), " +
				"([Store].[All Stores].[USA].[OR], [Product].[All Products].[Drink].[Alcoholic Beverages]), " +
				"([Store].[All Stores].[USA].[WA], [Product].[All Products].[Drink].[Dairy]), " +
				"([Store].[All Stores].[USA].[CA].[San Diego], [Product].[All Products].[Drink].[Beverages]), " +
				"([Store].[All Stores].[USA].[CA].[Los Angeles], [Product].[All Products].[Drink].[Beverages]), " +
				"Crossjoin({[Store].[All Stores].[USA].[CA].[Los Angeles]}, {[Product].[All Products].[Drink].[Beverages].Children}), " +
				"([Store].[All Stores].[USA].[CA].[Beverly Hills], [Product].[All Products].[Drink].[Beverages]), " +
				"([Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Dairy]), " +
				"([Store].[All Stores].[USA].[OR], [Product].[All Products].[Drink].[Dairy]), " +
				"([Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Drink].[Beverages])}, " +
				"[Measures].[Store Cost], BDESC) ON rows " +
				"from [Sales] " +
				"where ([Time].[1997])");
	}

	public void testCatalogHierarchyBasedOnView() {
		Schema schema = getConnection().getSchema();
		final Cube salesCube = schema.lookupCube("Sales", true);
		schema.createDimension(
				salesCube,
				"<Dimension name=\"Gender2\" foreignKey=\"customer_id\">" + nl +
				"  <Hierarchy hasAll=\"true\" allMemberName=\"All Gender\" primaryKey=\"customer_id\">" + nl +
				"    <View alias=\"gender2\">" + nl +
				"      <SQL dialect=\"generic\">" + nl +
				"        <![CDATA[SELECT * FROM customer]]>" + nl +
				"      </SQL>" + nl +
				"      <SQL dialect=\"oracle\">" + nl +
				"        <![CDATA[SELECT * FROM \"customer\"]]>" + nl +
				"      </SQL>" + nl +
				"    </View>" + nl +
				"    <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>" + nl +
				"  </Hierarchy>" + nl +
				"</Dimension>");
		final Axis axis = executeAxis2("[Gender2].members");
		assertEquals("[Gender2].[All Gender]" + nl +
				"[Gender2].[All Gender].[F]" + nl +
				"[Gender2].[All Gender].[M]",
				toString(axis.positions));
	}

	/**
	 * Run a query against a large hierarchy, to make sure that we can generate
	 * joins correctly. This probably won't work in MySQL.
	 */
	public void testCatalogHierarchyBasedOnView2() {
		Schema schema = getConnection().getSchema();
		final Cube salesCube = schema.lookupCube("Sales", true);
		schema.createDimension(
				salesCube,
				"<Dimension name=\"ProductView\" foreignKey=\"product_id\">" + nl +
				"	<Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"productView\">" + nl +
				"		<View alias=\"productView\">" + nl +
				"			<SQL dialect=\"generic\"><![CDATA[" + nl +
				"SELECT *" + nl +
				"FROM \"product\", \"product_class\"" + nl +
				"WHERE \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"" + nl +
				"]]>" + nl +
				"			</SQL>" + nl +
				"		</View>" + nl +
				"		<Level name=\"Product Family\" column=\"product_family\" uniqueMembers=\"true\"/>" + nl +
				"		<Level name=\"Product Department\" column=\"product_department\" uniqueMembers=\"false\"/>" + nl +
				"		<Level name=\"Product Category\" column=\"product_category\" uniqueMembers=\"false\"/>" + nl +
				"		<Level name=\"Product Subcategory\" column=\"product_subcategory\" uniqueMembers=\"false\"/>" + nl +
				"		<Level name=\"Brand Name\" column=\"brand_name\" uniqueMembers=\"false\"/>" + nl +
				"		<Level name=\"Product Name\" column=\"product_name\" uniqueMembers=\"true\"/>" + nl +
				"	</Hierarchy>" + nl +
				"</Dimension>");
		runQueryCheckResult(
				"select {[Measures].[Unit Sales]} on columns," + nl +
				" {[ProductView].[Drink].[Beverages].children} on rows" + nl +
				"from Sales",

				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales]}" + nl +
				"Axis #2:" + nl +
				"{[ProductView].[All ProductViews].[Drink].[Beverages].[Carbonated Beverages]}" + nl +
				"{[ProductView].[All ProductViews].[Drink].[Beverages].[Drinks]}" + nl +
				"{[ProductView].[All ProductViews].[Drink].[Beverages].[Hot Beverages]}" + nl +
				"{[ProductView].[All ProductViews].[Drink].[Beverages].[Pure Juice Beverages]}" + nl +
				"Row #0: 3,407" + nl +
				"Row #1: 2,469" + nl +
				"Row #2: 4,301" + nl +
				"Row #3: 3,396" + nl);
	}

	public void testMemberWithNullKey() {
		runQueryCheckResult(
				"select {[Measures].[Unit Sales]} on columns," + nl +
				"{[Store Size in SQFT].members} on rows" + nl +
				"from Sales",

				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales]}" + nl +
				"Axis #2:" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[null]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[20319.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[21215.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[22478.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[23112.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[23593.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[23598.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[23688.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[23759.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[24597.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[27694.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[28206.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[30268.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[30584.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[30797.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[33858.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[34452.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[34791.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[36509.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[38382.0]}" + nl +
				"{[Store Size in SQFT].[All Store Size in SQFTs].[39696.0]}" + nl +
				"Row #0: 266,773" + nl +
				"Row #1: (null)" + nl +
				"Row #2: 26,079" + nl +
				"Row #3: 25,011" + nl +
				"Row #4: 2,117" + nl +
				"Row #5: (null)" + nl +
				"Row #6: (null)" + nl +
				"Row #7: 25,663" + nl +
				"Row #8: 21,333" + nl +
				"Row #9: (null)" + nl +
				"Row #10: (null)" + nl +
				"Row #11: 41,580" + nl +
				"Row #12: 2,237" + nl +
				"Row #13: 23,591" + nl +
				"Row #14: (null)" + nl +
				"Row #15: (null)" + nl +
				"Row #16: 35,257" + nl +
				"Row #17: (null)" + nl +
				"Row #18: (null)" + nl +
				"Row #19: (null)" + nl +
				"Row #20: (null)" + nl +
				"Row #21: 24,576" + nl);
	}

	public void testMembersOfLargeDimensionTheHardWay() {
		final MondrianProperties properties = MondrianProperties.instance();
		int old = properties.getLargeDimensionThreshold();
		try {
			// prevent a CacheMemberReader from kicking in
			properties.setProperty(MondrianProperties.LargeDimensionThreshold, "1");
			final Connection connection =
					TestContext.instance().getFoodMartConnection(true);
			String queryString = "select {[Measures].[Unit Sales]} on columns," + nl +
					"{[Customers].members} on rows" + nl +
					"from Sales";
			Query query = connection.parseQuery(queryString);
			Result result = connection.execute(query);
			assertEquals(10407, result.getAxes()[1].positions.length);
		} finally {
			properties.setProperty(MondrianProperties.LargeDimensionThreshold, old + "");
		}
	}

	public void testParallelNot() {
		runParallelQueries(1, 1);
	}

	public void _testParallelSomewhat() {
		runParallelQueries(3, 2);
	}

	public void _testParallelVery() {
		runParallelQueries(6, 10);
	}

	private void runParallelQueries(final int threadCount, final int iterationCount) {
		int timeoutMs = threadCount * iterationCount * 10 * 1000; // 1 minute per query
		final int[] executeCount = new int[] {0};
		final QueryAndResult[] queries = new QueryAndResult[sampleQueries.length + taglibQueries.length];
		System.arraycopy(sampleQueries, 0, queries, 0, sampleQueries.length);
		System.arraycopy(taglibQueries, 0, queries, sampleQueries.length, taglibQueries.length);
		TestCaseForker threaded = new TestCaseForker(
				this, timeoutMs, threadCount, new ChooseRunnable() {
					public void run(int i) {
						for (int j = 0; j < iterationCount; j++) {
							int queryIndex = (i * 2 + j) % queries.length;
							try {
								runQueryCheckResult(queries[queryIndex]);
								executeCount[0]++;
							} catch (Throwable e) {
								e.printStackTrace();
								throw Util.newInternal(
										e,
										"Thread #" + i +
										" failed while executing query #" +
										queryIndex);
							}
						}
					}
				});
		threaded.run();
		assertEquals(threadCount * iterationCount, executeCount[0]);
	}

}

// End BasicQueryTest.java