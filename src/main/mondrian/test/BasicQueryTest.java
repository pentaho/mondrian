/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2003 Julian Hyde <jhyde@users.sf.net>
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.rolap.CachePool;

import java.util.regex.Pattern;

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
			"Row #0: 45,539.69" + nl +
			"Row #1: 44,058.79" + nl +
			"Row #1: 89,598.48" + nl +
			"Row #2: 50,029.87" + nl +
			"Row #2: 139,628.35" + nl +
			"Row #3: 42,878.25" + nl +
			"Row #3: 182,506.60" + nl +
			"Row #4: 44,456.29" + nl +
			"Row #4: 226,962.89" + nl +
			"Row #5: 45,331.73" + nl +
			"Row #5: 272,294.62" + nl +
			"Row #6: 50,246.88" + nl +
			"Row #6: 322,541.50" + nl +
			"Row #7: 46,199.04" + nl +
			"Row #7: 368,740.54" + nl +
			"Row #8: 43,825.97" + nl +
			"Row #8: 412,566.51" + nl +
			"Row #9: 42,342.27" + nl +
			"Row #9: 454,908.78" + nl +
			"Row #10: 53,363.71" + nl +
			"Row #10: 508,272.49" + nl +
			"Row #11: 56,965.64" + nl +
			"Row #11: 565,238.13" + nl),
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

	public void testDrillThrough() {
		Result result = runQuery(
				"WITH MEMBER [Measures].[Price] AS '[Measures].[Store Sales] / [Measures].[Unit Sales]'" + nl +
				"SELECT {[Measures].[Unit Sales], [Measures].[Price]} on columns," + nl +
				" {[Product].Children} on rows" + nl +
				"from Sales");
		String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL();
		assertEquals("select `time_by_day`.`the_year` as `c0`, `product_class`.`product_family` as `c1`, sum(`sales_fact_1997`.`unit_sales`) as `c2` from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, `product_class` as `product_class`, `product` as `product` where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and `time_by_day`.`the_year` in (1997) and `sales_fact_1997`.`product_id` = `product`.`product_id` and `product`.`product_class_id` = `product_class`.`product_class_id` and `product_class`.`product_family` in ('Drink') group by `time_by_day`.`the_year`, `product_class`.`product_family`", sql);
		sql = result.getCell(new int[] {1, 1}).getDrillThroughSQL();
		assertNull(sql); // because it is a calculated member
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

	public void testSlicerIsEvaluatedBeforeAxes() {
		// about 10 products exceeded 20000 units in 1997, only 2 for Q1
		assertSize(
				"SELECT {[Measures].[Unit Sales]} on columns," + nl +
				" filter({[Product].members}, [Measures].[Unit Sales] > 20000) on rows" + nl +
				"FROM Sales" + nl +
				"WHERE [Time].[1997].[Q1]", 1, 2);
	}

	public void testSlicerWithCalculatedMembers() {
		assertSize(
				"WITH MEMBER [Time].[1997].[H1] as ' Aggregate({[Time].[1997].[Q1], [Time].[1997].[Q2]})' " + nl +
				"  MEMBER [Measures].[Store Margin] as '[Measures].[Store Sales] - [Measures].[Store Cost]'" + nl +
				"SELECT {[Gender].children} on columns," + nl +
				" filter({[Product].members}, [Gender].[F] > 10000) on rows" + nl +
				"FROM Sales" + nl +
				"WHERE ([Time].[1997].[H1], [Measures].[Store Margin])",
				2, 6);
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

	public void testSolveOrder() {
		runQueryCheckResult(
				"WITH" + nl +
				"   MEMBER [Measures].[StoreType] AS " + nl +
				"   '[Store].CurrentMember.Properties(\"Store Type\")'," + nl +
				"   SOLVE_ORDER = 2" + nl +
				"   MEMBER [Measures].[ProfitPct] AS " + nl +
				"   '(Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales]'," + nl +
				"   SOLVE_ORDER = 1, FORMAT_STRING = '##.00%'" + nl +
				"SELECT" + nl +
				"   { Descendants([Store].[USA], [Store].[Store Name])} ON COLUMNS," + nl +
				"   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[StoreType]," + nl +
				"   [Measures].[ProfitPct] } ON ROWS" + nl +
				"FROM Sales",

				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Store].[All Stores].[USA].[CA].[Alameda].[HQ]}" + nl +
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
				"Axis #2:" + nl +
				"{[Measures].[Store Sales]}" + nl +
				"{[Measures].[Store Cost]}" + nl +
				"{[Measures].[StoreType]}" + nl +
				"{[Measures].[ProfitPct]}" + nl +
				"Row #0: (null)" + nl +
				"Row #0: 45,750.24" + nl +
				"Row #0: 54,545.28" + nl +
				"Row #0: 54,431.14" + nl +
				"Row #0: 4,441.18" + nl +
				"Row #0: 55,058.79" + nl +
				"Row #0: 87,218.28" + nl +
				"Row #0: 4,739.23" + nl +
				"Row #0: 52,896.30" + nl +
				"Row #0: 52,644.07" + nl +
				"Row #0: 49,634.46" + nl +
				"Row #0: 74,843.96" + nl +
				"Row #0: 4,705.97" + nl +
				"Row #0: 24,329.23" + nl +
				"Row #1: (null)" + nl +
				"Row #1: 18,266.44" + nl +
				"Row #1: 21,771.54" + nl +
				"Row #1: 21,713.53" + nl +
				"Row #1: 1,778.92" + nl +
				"Row #1: 21,948.94" + nl +
				"Row #1: 34,823.56" + nl +
				"Row #1: 1,896.62" + nl +
				"Row #1: 21,121.96" + nl +
				"Row #1: 20,956.80" + nl +
				"Row #1: 19,795.49" + nl +
				"Row #1: 29,959.28" + nl +
				"Row #1: 1,880.34" + nl +
				"Row #1: 9,713.81" + nl +
				"Row #2: HeadQuarters" + nl +
				"Row #2: Gourmet Supermarket" + nl +
				"Row #2: Supermarket" + nl +
				"Row #2: Supermarket" + nl +
				"Row #2: Small Grocery" + nl +
				"Row #2: Supermarket" + nl +
				"Row #2: Deluxe Supermarket" + nl +
				"Row #2: Small Grocery" + nl +
				"Row #2: Supermarket" + nl +
				"Row #2: Supermarket" + nl +
				"Row #2: Supermarket" + nl +
				"Row #2: Deluxe Supermarket" + nl +
				"Row #2: Small Grocery" + nl +
				"Row #2: Mid-Size Grocery" + nl +
				"Row #3: NaN%" + nl +
				"Row #3: 60.07%" + nl +
				"Row #3: 60.09%" + nl +
				"Row #3: 60.11%" + nl +
				"Row #3: 59.94%" + nl +
				"Row #3: 60.14%" + nl +
				"Row #3: 60.07%" + nl +
				"Row #3: 59.98%" + nl +
				"Row #3: 60.07%" + nl +
				"Row #3: 60.19%" + nl +
				"Row #3: 60.12%" + nl +
				"Row #3: 59.97%" + nl +
				"Row #3: 60.04%" + nl +
				"Row #3: 60.07%" + nl);
	}

	public void testCalculatedMemberWhichIsNotAMeasure() {
		String query = "WITH MEMBER [Product].[BigSeller] AS" + nl +
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
				"{[Store].[All Stores].[USA].[CA].[Alameda]}" + nl +
				"{[Store].[All Stores].[USA].[CA].[Beverly Hills]}" + nl +
				"{[Store].[All Stores].[USA].[CA].[Los Angeles]}" + nl +
				"{[Store].[All Stores].[USA].[CA].[San Diego]}" + nl +
				"{[Store].[All Stores].[USA].[CA].[San Francisco]}" + nl +
				"Row #0: No" + nl +
				"Row #0: (null)" + nl +
				"Row #0: (null)" + nl +
				"Row #0: (null)" + nl +
				"Row #1: Yes" + nl +
				"Row #1: 1,945" + nl +
				"Row #1: 15,438" + nl +
				"Row #1: 3,950" + nl +
				"Row #2: Yes" + nl +
				"Row #2: 2,422" + nl +
				"Row #2: 18,294" + nl +
				"Row #2: 4,947" + nl +
				"Row #3: Yes" + nl +
				"Row #3: 2,560" + nl +
				"Row #3: 18,369" + nl +
				"Row #3: 4,706" + nl +
				"Row #4: No" + nl +
				"Row #4: 175" + nl +
				"Row #4: 1,555" + nl +
				"Row #4: 387" + nl;
		runQueryCheckResult(query, desiredResult);
	}

	public void testConstantString() {
		String s = executeExpr(" \"a string\" ");
		assertEquals("a string", s);
	}

	public void testConstantNumber() {
		String s = executeExpr(" 1234 ");
		assertEquals("1,234", s);
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
			assertEquals("72,024", s);
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
		final Axis axis = executeAxis2("Sales", "{[Product2].members}");
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
				"{[Store Type].[All Store Types].[HeadQuarters]}" + nl +
				"{[Store Type].[All Store Types].[Mid-Size Grocery]}" + nl +
				"{[Store Type].[All Store Types].[Small Grocery]}" + nl +
				"{[Store Type].[All Store Types].[Supermarket]}" + nl +
				"Row #0: <b>266773.00</b>" + nl +
				"Row #1: <b>76837.00</b>" + nl +
				"Row #2: <i>21333.00</i>" + nl +
				"Row #3: (null)" + nl +
				"Row #4: <i>11491.00</i>" + nl +
				"Row #5: <i>6557.00</i>" + nl +
				"Row #6: <b>150555.00</b>" + nl);
	}

	public void testFormatOfNulls() {
		runQueryCheckResult(
				"with member [Measures].[Foo] as '([Measures].[Store Sales])'," + nl +
				" format_string = '$#,##0.00;($#,##0.00);ZERO;NULL;Nil'" + nl +
				"select" + nl +
				" {[Measures].[Foo]} on columns," + nl +
				" {[Customers].[Country].members} on rows" + nl +
				"from Sales",
				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Foo]}" + nl +
				"Axis #2:" + nl +
				"{[Customers].[All Customers].[Canada]}" + nl +
				"{[Customers].[All Customers].[Mexico]}" + nl +
				"{[Customers].[All Customers].[USA]}" + nl +
				"Row #0: NULL" + nl +
				"Row #1: NULL" + nl +
				"Row #2: $565,238.13" + nl);
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
    /** This bug causes all of the format strings to be the same, because the
     * required expression [Measures].[Unit Sales] is not in the cache. */
    public void testBug761196() {
        runQueryCheckResult(
                "with member [Measures].[xxx] as '[Measures].[Store Sales]'," + nl +
                " format_string = IIf([Measures].[Unit Sales] > 100000, \"AAA######.00\",\"BBB###.00\")" + nl +
                "select {[Measures].[xxx]} ON columns," + nl +
                " {[Product].[All Products].children} ON rows" + nl +
                "from [Sales] where [Time].[1997]",
                
                "Axis #0:" + nl +
                "{[Time].[1997]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[xxx]}" + nl +
                "Axis #2:" + nl +
                "{[Product].[All Products].[Drink]}" + nl +
                "{[Product].[All Products].[Food]}" + nl +
                "{[Product].[All Products].[Non-Consumable]}" + nl +
                "Row #0: BBB48836.21" + nl +
                "Row #1: AAA409035.59" + nl +
                "Row #2: BBB107366.33" + nl);
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
				"{[Store Type].[All Store Types].[HeadQuarters]}" + nl +
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
				"Row #4: (null)" + nl +
				"Row #4: (null)" + nl +
				"Row #5: 22,478" + nl +
				"Row #5: 15,321" + nl +
				"Row #6: 23,598" + nl +
				"Row #6: 14,210" + nl);
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
		final Axis axis = executeAxis2("Sales", "[Gender2].members");
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

    public void testCountDistinct() {
        runQueryCheckResult(
                "select {[Measures].[Unit Sales], [Measures].[Customer Count]} on columns," + nl +
                " {[Gender].members} on rows" + nl +
                "from Sales",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "{[Measures].[Customer Count]}" + nl +
                "Axis #2:" + nl +
                "{[Gender].[All Gender]}" + nl +
                "{[Gender].[All Gender].[F]}" + nl +
                "{[Gender].[All Gender].[M]}" + nl +
                "Row #0: 266,773" + nl +
                "Row #0: 5,581" + nl +
                "Row #1: 131,558" + nl +
                "Row #1: 2,755" + nl +
                "Row #2: 135,215" + nl +
                "Row #2: 2,826" + nl);
    }

	public void testMemberWithNullKey() {
		Result result = runQuery(
				"select {[Measures].[Unit Sales]} on columns," + nl +
				"{[Store Size in SQFT].members} on rows" + nl +
				"from Sales");
		String resultString = toString(result);
		resultString = Pattern.compile("\\.0\\]").matcher(resultString).replaceAll("]");
		final String expected = "Axis #0:" + nl +
						"{}" + nl +
						"Axis #1:" + nl +
						"{[Measures].[Unit Sales]}" + nl +
						"Axis #2:" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[null]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[20319]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[21215]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[22478]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[23112]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[23593]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[23598]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[23688]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[23759]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[24597]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[27694]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[28206]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[30268]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[30584]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[30797]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[33858]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[34452]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[34791]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[36509]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[38382]}" + nl +
						"{[Store Size in SQFT].[All Store Size in SQFTs].[39696]}" + nl +
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
						"Row #21: 24,576" + nl;
		assertEquals(expected, resultString);
	}

	/**
	 * Slicer contains <code>[Promotion Media].[Daily Paper]</code>, but
	 * filter expression is in terms of <code>[Promotion Media].[Radio]</code>.
	 */
	public void testSlicerOverride() {
		runQueryCheckResult(
				"with member [Measures].[Radio Unit Sales] as " + nl +
				" '([Measures].[Unit Sales], [Promotion Media].[Radio])'" + nl +
				"select {[Measures].[Unit Sales], [Measures].[Radio Unit Sales]} on columns," + nl +
				" filter([Product].[Product Department].members, [Promotion Media].[Radio] > 50) on rows" + nl +
				"from Sales" + nl +
				"where ([Promotion Media].[Daily Paper], [Time].[1997].[Q1])",
				"Axis #0:" + nl +
				"{[Promotion Media].[All Media].[Daily Paper], [Time].[1997].[Q1]}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales]}" + nl +
				"{[Measures].[Radio Unit Sales]}" + nl +
				"Axis #2:" + nl +
				"{[Product].[All Products].[Food].[Produce]}" + nl +
				"{[Product].[All Products].[Food].[Snack Foods]}" + nl +
				"Row #0: 692" + nl +
				"Row #0: 87" + nl +
				"Row #1: 447" + nl +
				"Row #1: 63" + nl);
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

	public void testUnparse() {
		Connection connection = getConnection();
		Query query = connection.parseQuery(
				"with member [Measures].[Rendite] as " + nl +
				" '(([Measures].[Store Sales] - [Measures].[Store Cost])) / [Measures].[Store Cost]'," + nl +
				" format_string = iif(([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost] * 100 > " + nl +
				"     Parameter (\"UpperLimit\", NUMERIC, 151, \"Obere Grenze\"), " + nl +
				"   \"|#.00%|arrow='up'\"," + nl +
				"   iif(([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost] * 100 < " + nl +
				"       Parameter(\"LowerLimit\", NUMERIC, 150, \"Untere Grenze\")," + nl +
				"     \"|#.00%|arrow='down'\"," + nl +
				"     \"|#.00%|arrow='right'\"))" + nl +
				"select {[Measures].members} on columns" + nl +
				"from Sales");
		final String s = query.toString();
		// Parentheses are added to reflect operator precedence, but that's ok.
		// Note that the doubled parentheses in line #2 of the query have been
		// reduced to a single level.
		assertEquals("with member [Measures].[Rendite] as '(([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost])', " +
				"format_string = IIf((((([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost]) * 100.0) > Parameter(\"UpperLimit\", NUMERIC, 151.0, \"Obere Grenze\")), " +
				"\"|#.00%|arrow='up'\", " +
				"IIf((((([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost]) * 100.0) < Parameter(\"LowerLimit\", NUMERIC, 150.0, \"Untere Grenze\")), " +
				"\"|#.00%|arrow='down'\", \"|#.00%|arrow='right'\"))" + nl +
				"select {[Measures].Members} ON columns" + nl +
				"from [Sales]" + nl, s);
	}

	public void testUnparse2() {
		Connection connection = getConnection();
		Query query = connection.parseQuery(
				"with member [Measures].[Foo] as '1', " +
				"format_string='##0.00', " +
				"funny=IIf(1=1,\"x\"\"y\",\"foo\") " +
				"select {[Measures].[Foo]} on columns from Sales");
		final String s = query.toString();
		// The "format_string" property, a string literal, is now delimited by
		// double-quotes. This won't work in MSOLAP, but for Mondrian it's
		// consistent with the fact that property values are expressions,
		// not enclosed in single-quotes.
		assertEquals("with member [Measures].[Foo] as '1.0', " +
				"format_string = \"##0.00\", " +
				"funny = IIf((1.0 = 1.0), \"x\"\"y\", \"foo\")" + nl +
				"select {[Measures].[Foo]} ON columns" + nl +
				"from [Sales]" + nl,
				s);
	}

    /**
     * Basically, the LookupCube function can evaluate a single MDX statement
     * against a cube other than the cube currently indicated by query context
     * to retrieve a single string or numeric result.
     *
     * <p>For example, the Budget cube in the FoodMart 2000 database contains
     * budget information that can be displayed by store. The Sales cube in the
     * FoodMart 2000 database contains sales information that can be displayed
     * by store. Since no virtual cube exists in the FoodMart 2000 database that
     * joins the Sales and Budget cubes together, comparing the two sets of
     * figures would be difficult at best.
     *
     * <p><b>Note<b> In many situations a virtual cube can be used to integrate
     * data from multiple cubes, which will often provide a simpler and more
     * efficient solution than the LookupCube function. This example uses the
     * LookupCube function for purposes of illustration.
     *
     * <p>The following MDX query, however, uses the LookupCube function to
     * retrieve unit sales information for each store from the Sales cube,
     * presenting it side by side with the budget information from the Budget
     * cube.
     **/
    public void _testLookupCube() {
        runQueryCheckResult(
                "WITH MEMBER Measures.[Store Unit Sales] AS " + nl +
                " 'LookupCube(\"Sales\", \"(\" + MemberToStr(Store.CurrentMember) + \", Measures.[Unit Sales])\")'" + nl +
                "SELECT" + nl +
                " {Measures.Amount, Measures.[Store Unit Sales]} ON COLUMNS," + nl +
                " Store.CA.CHILDREN ON ROWS" + nl +
                "FROM Budget", "");
    }

    /**
     * <p>Basket analysis is a topic better suited to data mining discussions, but
     * some basic forms of basket analysis can be handled through the use of MDX
     * queries.
     *
     * <p>For example, one method of basket analysis groups customers based on
     * qualification. In the following example, a qualified customer is one who
     * has more than $10,000 in store sales or more than 10 unit sales. The
     * following table illustrates such a report, run against the Sales cube in
     * FoodMart 2000 with qualified customers grouped by the Country and State
     * Province levels of the Customers dimension. The count and store sales
     * total of qualified customers is represented by the Qualified Count and
     * Qualified Sales columns, respectively.
     *
     * <p>To accomplish this basic form of basket analysis, the following MDX
     * query constructs two calculated members. The first calculated member uses
     * the MDX Count, Filter, and Descendants functions to create the Qualified
     * Count column, while the second calculated member uses the MDX Sum,
     * Filter, and Descendants functions to create the Qualified Sales column.
     *
     * <p>The key to this MDX query is the use of Filter and Descendants together
     * to screen out non-qualified customers. Once screened out, the Sum and
     * Count MDX functions can then be used to provide aggregation data only on
     * qualified customers.
     */
    public void testBasketAnalysis() {
        runQueryCheckResult(
                "WITH MEMBER [Measures].[Qualified Count] AS" + nl +
                " 'COUNT(FILTER(DESCENDANTS(Customers.CURRENTMEMBER, [Customers].[Name])," + nl +
                "               ([Measures].[Store Sales]) > 10000 OR ([Measures].[Unit Sales]) > 10))'" + nl +
                "MEMBER [Measures].[Qualified Sales] AS" + nl +
                " 'SUM(FILTER(DESCENDANTS(Customers.CURRENTMEMBER, [Customers].[Name])," + nl +
                "             ([Measures].[Store Sales]) > 10000 OR ([Measures].[Unit Sales]) > 10)," + nl +
                "      ([Measures].[Store Sales]))'" + nl +
                "SELECT {[Measures].[Qualified Count], [Measures].[Qualified Sales]} ON COLUMNS," + nl +
                "  DESCENDANTS([Customers].[All Customers], [State Province], SELF_AND_BEFORE) ON ROWS" + nl +
                "FROM Sales",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Qualified Count]}" + nl +
                "{[Measures].[Qualified Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Customers].[All Customers]}" + nl +
                "{[Customers].[All Customers].[Canada]}" + nl +
                "{[Customers].[All Customers].[Mexico]}" + nl +
                "{[Customers].[All Customers].[USA]}" + nl +
                "{[Customers].[All Customers].[Canada].[BC]}" + nl +
                "{[Customers].[All Customers].[Mexico].[DF]}" + nl +
                "{[Customers].[All Customers].[Mexico].[Guerrero]}" + nl +
                "{[Customers].[All Customers].[Mexico].[Jalisco]}" + nl +
                "{[Customers].[All Customers].[Mexico].[Mexico]}" + nl +
                "{[Customers].[All Customers].[Mexico].[Oaxaca]}" + nl +
                "{[Customers].[All Customers].[Mexico].[Sinaloa]}" + nl +
                "{[Customers].[All Customers].[Mexico].[Veracruz]}" + nl +
                "{[Customers].[All Customers].[Mexico].[Yucatan]}" + nl +
                "{[Customers].[All Customers].[Mexico].[Zacatecas]}" + nl +
                "{[Customers].[All Customers].[USA].[CA]}" + nl +
                "{[Customers].[All Customers].[USA].[OR]}" + nl +
                "{[Customers].[All Customers].[USA].[WA]}" + nl +
                "Row #0: 4,719.00" + nl +
                "Row #0: 553,587.77" + nl +
                "Row #1: .00" + nl +
                "Row #1: (null)" + nl +
                "Row #2: .00" + nl +
                "Row #2: (null)" + nl +
                "Row #3: 4,719.00" + nl +
                "Row #3: 553,587.77" + nl +
                "Row #4: .00" + nl +
                "Row #4: (null)" + nl +
                "Row #5: .00" + nl +
                "Row #5: (null)" + nl +
                "Row #6: .00" + nl +
                "Row #6: (null)" + nl +
                "Row #7: .00" + nl +
                "Row #7: (null)" + nl +
                "Row #8: .00" + nl +
                "Row #8: (null)" + nl +
                "Row #9: .00" + nl +
                "Row #9: (null)" + nl +
                "Row #10: .00" + nl +
                "Row #10: (null)" + nl +
                "Row #11: .00" + nl +
                "Row #11: (null)" + nl +
                "Row #12: .00" + nl +
                "Row #12: (null)" + nl +
                "Row #13: .00" + nl +
                "Row #13: (null)" + nl +
                "Row #14: 2,149.00" + nl +
                "Row #14: 151,509.69" + nl +
                "Row #15: 1,008.00" + nl +
                "Row #15: 141,899.84" + nl +
                "Row #16: 1,562.00" + nl +
                "Row #16: 260,178.24" + nl);
    }

    /**
     * <b>How Can I Perform Complex String Comparisons?</b>
     *
     * <p>MDX can handle basic string comparisons, but does not include complex
     * string comparison and manipulation functions, for example, for finding
     * substrings in strings or for supporting case-insensitive string
     * comparisons. However, since MDX can take advantage of external function
     * libraries, this question is easily resolved using string manipulation and
     * comparison functions from the Microsoft Visual Basic® for Applications
     * (VBA) external function library.
     *
     * <p>For example, you want to report the unit sales of all fruit-based
     * products—not only the sales of fruit, but canned fruit, fruit snacks,
     * fruit juices, and so on. By using the LCase and InStr VBA functions, the
     * following results are easily accomplished in a single MDX query, without
     * complex set construction or explicit member names within the query.
     *
     * <p>The following MDX query demonstrates how to achieve the results
     * displayed in the previous table. For each member in the Product
     * dimension, the name of the member is converted to lowercase using the
     * LCase VBA function. Then, the InStr VBA function is used to discover
     * whether or not the name contains the word "fruit". This information is
     * used to then construct a set, using the Filter MDX function, from only
     * those members from the Product dimension that contain the substring
     * "fruit" in their names.
     */
    public void _testStringComparisons() {
        runQueryCheckResult(
                "SELECT {Measures.[Unit Sales]} ON COLUMNS," + nl +
                "  FILTER([Product].[Product Name].MEMBERS," + nl +
                "         INSTR(LCASE([Product].CURRENTMEMBER.NAME), \"fruit\") <> 0) ON ROWS " + nl +
                "FROM Sales", "");
    }

    /**
     * <b>How Can I Show Percentages as Measures?</b>
     *
     * <p>Another common business question easily answered through MDX is the
     * display of percent values created as available measures.
     *
     * <p>For example, the Sales cube in the FoodMart 2000 database contains
     * unit sales for each store in a given city, state, and country, organized
     * along the Sales dimension. A report is requested to show, for
     * California, the percentage of total unit sales attained by each city
     * with a store. The results are illustrated in the following table.
     *
     * <p>Because the parent of a member is typically another, aggregated
     * member in a regular dimension, this is easily achieved by the
     * construction of a calculated member, as demonstrated in the following MDX
     * query, using the CurrentMember and Parent MDX functions.
     */
    public void testPercentagesAsMeasures() {
        runQueryCheckResult( // todo: "Store.[USA].[CA]" should be "Store.CA"
                "WITH MEMBER Measures.[Unit Sales Percent] AS" + nl +
                "  '((Store.CURRENTMEMBER, Measures.[Unit Sales]) /" + nl +
                "    (Store.CURRENTMEMBER.PARENT, Measures.[Unit Sales])) '," + nl +
                "  FORMAT_STRING = 'Percent'" + nl +
                "SELECT {Measures.[Unit Sales], Measures.[Unit Sales Percent]} ON COLUMNS," + nl +
                "  ORDER(DESCENDANTS(Store.[USA].[CA], Store.[Store City], SELF), " + nl +
                "        [Measures].[Unit Sales], ASC) ON ROWS" + nl +
                "FROM Sales",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "{[Measures].[Unit Sales Percent]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[USA].[CA].[Alameda]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Francisco]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Beverly Hills]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Diego]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles]}" + nl +
                "Row #0: (null)" + nl +
                "Row #0: 0.00%" + nl +
                "Row #1: 2,117" + nl +
                "Row #1: 2.83%" + nl +
                "Row #2: 21,333" + nl +
                "Row #2: 28.54%" + nl +
                "Row #3: 25,635" + nl +
                "Row #3: 34.30%" + nl +
                "Row #4: 25,663" + nl +
                "Row #4: 34.33%" + nl);
    }

    /**
     * <b>How Can I Show Cumulative Sums as Measures?</b>
     *
     * <p>Another common business request, cumulative sums, is useful for
     * business reporting purposes. However, since aggregations are handled in a
     * hierarchical fashion, cumulative sums present some unique challenges in
     * Analysis Services.
     *
     * <p>The best way to create a cumulative sum is as a calculated measure in
     * MDX, using the Rank, Head, Order, and Sum MDX functions together.
     *
     * <p>For example, the following table illustrates a report that shows two
     * views of employee count in all stores and cities in California, sorted
     * by employee count. The first column shows the aggregated counts for each
     * store and city, while the second column shows aggregated counts for each
     * store, but cumulative counts for each city.
     *
     * <p>The cumulative number of employees for San Diego represents the value
     * of both Los Angeles and San Diego, the value for Beverly Hills represents
     * the cumulative total of Los Angeles, San Diego, and Beverly Hills, and so
     * on.
     *
     * <p>Since the members within the state of California have been ordered
     * from highest to lowest number of employees, this form of cumulative sum
     * measure provides a form of pareto analysis within each state.
     *
     * <p>To support this, the Order function is first used to reorder members
     * accordingly for both the Rank and Head functions. Once reordered, the
     * Rank function is used to supply the ranking of each tuple within the
     * reordered set of members, progressing as each member in the Store
     * dimension is examined. The value is then used to determine the number of
     * tuples to retrieve from the set of reordered members using the Head
     * function. Finally, the retrieved members are then added together using
     * the Sum function to obtain a cumulative sum. The following MDX query
     * demonstrates how all of this works in concert to provide cumulative sums.
     *
     * <p>As an aside, a named set cannot be used in this situation to replace
     * the duplicate Order function calls. Named sets are evaluated once, when a
     * query is parsed—since the set can change based on the fact that the set
     * can be different for each store member because the set is evaluated for
     * the children of multiple parents, the set does not change with respect to
     * its use in the Sum function. Since the named set is only evaluated once,
     * it would not satisfy the needs of this query.
     */
    public void _testCumlativeSums() {
        runQueryCheckResult( // todo: "[Store].[USA].[CA]" should be "Store.CA"; implement "AS"
                "WITH MEMBER Measures.[Cumulative No of Employees] AS" + nl +
                "  'SUM(HEAD(ORDER({[Store].Siblings}, [Measures].[Number of Employees], BDESC) AS OrderedSiblings," + nl +
                "            RANK([Store], OrderedSiblings))," + nl +
                "       [Measures].[Number of Employees])'" + nl +
                "SELECT {[Measures].[Number of Employees], [Measures].[Cumulative No of Employees]} ON COLUMNS," + nl +
                "  ORDER(DESCENDANTS([Store].[USA].[CA], [Store State], AFTER), " + nl +
                "        [Measures].[Number of Employees], BDESC) ON ROWS" + nl +
                "FROM HR", "");
    }

    /**
     * <b>How Can I Implement a Logical AND or OR Condition in a WHERE
     * Clause?</b>
     *
     * <p>For SQL users, the use of AND and OR logical operators in the WHERE
     * clause of a SQL statement is an essential tool for constructing business
     * queries. However, the WHERE clause of an MDX statement serves a
     * slightly different purpose, and understanding how the WHERE clause is
     * used in MDX can assist in constructing such business queries.
     *
     * <p>The WHERE clause in MDX is used to further restrict the results of
     * an MDX query, in effect providing another dimension on which the results
     * of the query are further sliced. As such, only expressions that resolve
     * to a single tuple are allowed. The WHERE clause implicitly supports a
     * logical AND operation involving members across different dimensions, by
     * including the members as part of a tuple. To support logical AND
     * operations involving members within a single dimensions, as well as
     * logical OR operations, a calculated member needs to be defined in
     * addition to the use of the WHERE clause.
     *
     * <p>For example, the following MDX query illustrates the use of a
     * calculated member to support a logical OR. The query returns unit sales
     * by quarter and year for all food and drink related products sold in 1997,
     * run against the Sales cube in the FoodMart 2000 database.
     *
     * <p>The calculated member simply adds the values of the Unit Sales
     * measure for the Food and the Drink levels of the Product dimension
     * together. The WHERE clause is then used to restrict return of
     * information only to the calculated member, effectively implementing a
     * logical OR to return information for all time periods that contain unit
     * sales values for either food, drink, or both types of products.
     *
     * <p>You can use the Aggregate function in similar situations where all
     * measures are not aggregated by summing. To return the same results in the
     * above example using the Aggregate function, replace the definition for
     * the calculated member with this definition:
     *
     * <blockquote><pre>'Aggregate({[Product].[Food], [Product].[Drink]})'</pre></blockquote>
     */
    public void testLogicalOps() {
        runQueryCheckResult(
                "WITH MEMBER [Product].[Food OR Drink] AS" + nl +
                "  '([Product].[Food], Measures.[Unit Sales]) + ([Product].[Drink], Measures.[Unit Sales])'" + nl +
                "SELECT {Measures.[Unit Sales]} ON COLUMNS," + nl +
                "  DESCENDANTS(Time.[1997], [Quarter], SELF_AND_BEFORE) ON ROWS" + nl +
                "FROM Sales" + nl +
                "WHERE [Product].[Food OR Drink]",

                "Axis #0:" + nl +
                "{[Product].[Food OR Drink]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Time].[1997]}" + nl +
                "{[Time].[1997].[Q1]}" + nl +
                "{[Time].[1997].[Q2]}" + nl +
                "{[Time].[1997].[Q3]}" + nl +
                "{[Time].[1997].[Q4]}" + nl +
                "Row #0: 216,537" + nl +
                "Row #1: 53,785" + nl +
                "Row #2: 50,720" + nl +
                "Row #3: 53,505" + nl +
                "Row #4: 58,527" + nl);
    }

    /**
     * <p>A logical AND, by contrast, can be supported by using two different
     * techniques. If the members used to construct the logical AND reside on
     * different dimensions, all that is required is a WHERE clause that uses
     * a tuple representing all involved members. The following MDX query uses a
     * WHERE clause that effectively restricts the query to retrieve unit
     * sales for drink products in the USA, shown by quarter and year for 1997.
     */
    public void testLogicalAnd() {
        runQueryCheckResult( // todo: "[Store].USA" should be "[Store].[USA]"
                "SELECT {Measures.[Unit Sales]} ON COLUMNS," + nl +
                "  DESCENDANTS([Time].[1997], [Quarter], SELF_AND_BEFORE) ON ROWS" + nl +
                "FROM Sales" + nl +
                "WHERE ([Product].[Drink], [Store].[USA])",

                "Axis #0:" + nl +
                "{[Product].[All Products].[Drink], [Store].[All Stores].[USA]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Time].[1997]}" + nl +
                "{[Time].[1997].[Q1]}" + nl +
                "{[Time].[1997].[Q2]}" + nl +
                "{[Time].[1997].[Q3]}" + nl +
                "{[Time].[1997].[Q4]}" + nl +
                "Row #0: 24,597" + nl +
                "Row #1: 5,976" + nl +
                "Row #2: 5,895" + nl +
                "Row #3: 6,065" + nl +
                "Row #4: 6,661" + nl);
    }

    /**
     * <p>The WHERE clause in the previous MDX query effectively provides a
     * logical AND operator, in which all unit sales for 1997 are returned only
     * for drink products and only for those sold in stores in the USA.
     *
     * <p>If the members used to construct the logical AND condition reside on
     * the same dimension, you can use a calculated member or a named set to
     * filter out the unwanted members, as demonstrated in the following MDX
     * query.
     *
     * <p>The named set, [Good AND Pearl Stores], restricts the displayed unit
     * sales totals only to those stores that have sold both Good products and
     * Pearl products.
     */
    public void _testSet() {
        runQueryCheckResult(
                "WITH SET [Good AND Pearl Stores] AS" + nl +
                "  'FILTER(Store.Members," + nl +
                "          ([Product].[Good], Measures.[Unit Sales]) > 0 AND " + nl +
                "          ([Product].[Pearl], Measures.[Unit Sales]) > 0)'" + nl +
                "SELECT DESCENDANTS([Time].[1997], [Quarter], SELF_AND_BEFORE) ON COLUMNS," + nl +
                "  [Good AND Pearl Stores] ON ROWS" + nl +
                "FROM Sales", "");
    }

    /**
     * <b>How Can I Use Custom Member Properties in MDX?</b>
     *
     * <p>Member properties are a good way of adding secondary business
     * information to members in a dimension. However, getting that information
     * out can be confusing—member properties are not readily apparent in a
     * typical MDX query.
     *
     * <p>Member properties can be retrieved in one of two ways. The easiest
     * and most used method of retrieving member properties is to use the
     * DIMENSION PROPERTIES MDX statement when constructing an axis in an MDX
     * query.
     *
     * <p>For example, a member property in the Store dimension in the FoodMart
     * 2000 database details the total square feet for each store. The following
     * MDX query can retrieve this member property as part of the returned
     * cellset.
     */
    public void _testCustomMemberProperties() {
        runQueryCheckResult( // todo: implement "DIMENSION PROPERTIES" syntax
                "SELECT {[Measures].[Units Shipped], [Measures].[Units Ordered]} ON COLUMNS," + nl +
                "  NON EMPTY [Store].[Store Name].MEMBERS" + nl +
                "    DIMENSION PROPERTIES [Store].[Store Name].[Store Sqft] ON ROWS" + nl +
                "FROM Warehouse", "");
    }

    /**
     * <p>The drawback to using the DIMENSION PROPERTIES statement is that,
     * for most client applications, the member property is not readily
     * apparent. If the previous MDX query is executed in the MDX sample
     * application shipped with SQL Server 2000 Analysis Services, for example,
     * you must double-click the name of the member in the grid to open the
     * Member Properties dialog box, which displays all of the member properties
     * shipped as part of the cellset, including the [Store].[Store Name].[Store
     * Sqft] member property.
     *
     * <p>The other method of retrieving member properties involves the creation
     * of a calculated member based on the member property. The following MDX
     * query brings back the total square feet for each store as a measure,
     * included in the COLUMNS axis.
     *
     * <p>The [Store SqFt] measure is constructed with the Properties MDX
     * function to retrieve the [Store SQFT] member property for each member in
     * the Store dimension. The benefit to this technique is that the calculated
     * member is readily apparent and easily accessible in client applications
     * that do not support member properties.
     */
    public void _testMemberPropertyAsCalcMember() {
        runQueryCheckResult( // todo: implement <member>.PROPERTIES
                "WITH MEMBER Measures.[Store SqFt] AS '[Store].CURRENTMEMBER.PROPERTIES(\"Store SQFT\")'" + nl +
                "SELECT { [Measures].[Store SQFT], [Measures].[Units Shipped], [Measures].[Units Ordered] }  ON COLUMNS," + nl +
                "  [Store].[Store Name].MEMBERS ON ROWS" + nl +
                "FROM Warehouse", "");
    }

    /**
     * <b>How Can I Drill Down More Than One Level Deep, or Skip Levels When
     * Drilling Down?</b>
     *
     * <p>Drilling down is an essential ability for most OLAP products, and
     * Analysis Services is no exception. Several functions exist that support
     * drilling up and down the hierarchy of dimensions within a cube.
     * Typically, drilling up and down the hierarchy is done one level at a
     * time; think of this functionality as a zoom feature for OLAP data.
     *
     * <p>There are times, though, when the need to drill down more than one
     * level at the same time, or even skip levels when displaying information
     * about multiple levels, exists for a business scenario.
     *
     * <p>For example, you would like to show report results from a query of
     * the Sales cube in the FoodMart 2000 sample database showing sales totals
     * for individual cities and the subtotals for each country, as shown in the
     * following table.
     *
     * <p>The Customers dimension, however, has Country, State Province, and
     * City levels. In order to show the above report, you would have to show
     * the Country level and then drill down two levels to show the City
     * level, skipping the State Province level entirely.
     *
     * <p>However, the MDX ToggleDrillState and DrillDownMember functions
     * provide drill down functionality only one level below a specified set. To
     * drill down more than one level below a specified set, you need to use a
     * combination of MDX functions, including Descendants, Generate, and
     * Except. This technique essentially constructs a large set that includes
     * all levels between both upper and lower desired levels, then uses a
     * smaller set representing the undesired level or levels to remove the
     * appropriate members from the larger set.
     *
     * <p>The MDX Descendants function is used to construct a set consisting of
     * the descendants of each member in the Customers dimension. The
     * descendants are determined using the MDX Descendants function, with the
     * descendants of the City level and the level above, the State Province
     * level, for each member of the Customers dimension being added to the
     * set.
     *
     * <p>The MDX Generate function now creates a set consisting of all members
     * at the Country level as well as the members of the set generated by the
     * MDX Descendants function. Then, the MDX Except function is used to
     * exclude all members at the State Province level, so the returned set
     * contains members at the Country and City levels.
     *
     * <p>Note, however, that the previous MDX query will still order the
     * members according to their hierarchy. Although the returned set contains
     * members at the Country and City levels, the Country, State Province,
     * and City levels determine the order of the members.
     */
    public void _testDrillingDownMoreThanOneLevel() {
        runQueryCheckResult( // todo: implement "GENERATE"
                "SELECT  {[Measures].[Unit Sales]} ON COLUMNS," + nl +
                "  EXCEPT(GENERATE([Customers].[Country].MEMBERS," + nl +
                "                  {DESCENDANTS([Customers].CURRENTMEMBER, [Customers].[City], SELF_AND_BEFORE)})," + nl +
                "         {[Customers].[State Province].MEMBERS}) ON ROWS" + nl +
                "FROM Sales", "");
    }

    /**
     * <b>How Do I Get the Topmost Members of a Level Broken Out by an Ancestor
     * Level?</b>
     *
     * <p>This type of MDX query is common when only the facts for the lowest
     * level of a dimension within a cube are needed, but information about
     * other levels within the same dimension may also be required to satisfy a
     * specific business scenario.
     *
     * <p>For example, a report that shows the unit sales for the store with
     * the highest unit sales from each country is needed for marketing
     * purposes. The following table provides an example of this report, run
     * against the Sales cube in the FoodMart 2000 sample database.
     *
     * <p>This looks simple enough, but the Country Name column provides
     * unexpected difficulty. The values for the Store Country column are taken
     * from the Store Country level of the Store dimension, so the Store
     * Country column is constructed as a calculated member as part of the MDX
     * query, using the MDX Ancestor and Name functions to return the country
     * names for each store.
     *
     * <p>A combination of the MDX Generate, TopCount, and Descendants
     * functions are used to create a set containing the top stores in unit
     * sales for each country.
     */
    public void _testTopmost() {
        runQueryCheckResult( // todo: implement "GENERATE"
                "WITH MEMBER Measures.[Country Name] AS " + nl +
                "  'Ancestor(Store.CurrentMember, [Store Country]).Name'" + nl +
                "SELECT {Measures.[Country Name], Measures.[Unit Sales]} ON COLUMNS," + nl +
                "  GENERATE([Store Country].MEMBERS, " + nl +
                "    TOPCOUNT(DESCENDANTS([Store].CURRENTMEMBER, [Store].[Store Name])," + nl +
                "      1, [Measures].[Unit Sales])) ON ROWS" + nl +
                "FROM Sales", "");
    }

    /**
     * <p>The MDX Descendants function is used to construct a set consisting of
     * only those members at the Store Name level in the Store dimension. Then,
     * the MDX TopCount function is used to return only the topmost store based
     * on the Unit Sales measure. The MDX Generate function then constructs a
     * set based on the topmost stores, following the hierarchy of the Store
     * dimension.
     *
     * <p>Alternate techniques, such as using the MDX Crossjoin function, may
     * not provide the desired results because non-related joins can occur.
     * Since the Store Country and Store Name levels are within the same
     * dimension, they cannot be cross-joined. Another dimension that provides
     * the same regional hierarchy structure, such as the Customers dimension,
     * can be employed with the Crossjoin function. But, using this technique
     * can cause non-related joins and return unexpected results.
     *
     * <p>For example, the following MDX query uses the Crossjoin function to
     * attempt to return the same desired results.
     *
     * <p>However, some unexpected surprises occur because the topmost member
     * in the Store dimension is cross-joined with all of the children of the
     * Customers dimension, as shown in the following table.
     *
     * <p>In this instance, the use of a calculated member to provide store
     * country names is easier to understand and debug than attempting to
     * cross-join across unrelated members
     */
    public void testTopmost2() {
        runQueryCheckResult(
                "SELECT {Measures.[Unit Sales]} ON COLUMNS," + nl +
                "  CROSSJOIN(Customers.CHILDREN," + nl +
                "    TOPCOUNT(DESCENDANTS([Store].CURRENTMEMBER, [Store].[Store Name])," + nl +
                "             1, [Measures].[Unit Sales])) ON ROWS" + nl +
                "FROM Sales",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Customers].[All Customers].[Canada], [Store].[All Stores].[USA].[OR].[Salem].[Store 13]}" + nl +
                "{[Customers].[All Customers].[Mexico], [Store].[All Stores].[USA].[OR].[Salem].[Store 13]}" + nl +
                "{[Customers].[All Customers].[USA], [Store].[All Stores].[USA].[OR].[Salem].[Store 13]}" + nl +
                "Row #0: (null)" + nl +
                "Row #1: (null)" + nl +
                "Row #2: 41,580" + nl);
    }

    /**
     * <b>How Can I Rank or Reorder Members?</b>
     *
     * <p>One of the issues commonly encountered in business scenarios is the
     * need to rank the members of a dimension according to their corresponding
     * measure values. The Order MDX function allows you to order a set based on
     * a string or numeric expression evaluated against the members of a set.
     * Combined with other MDX functions, the Order function can support
     * several different types of ranking.
     *
     * <p>For example, the Sales cube in the FoodMart 2000 database can be used
     * to show unit sales for each store. However, the business scenario
     * requires a report that ranks the stores from highest to lowest unit
     * sales, individually, of nonconsumable products.
     *
     * <p>Because of the requirement that stores be sorted individually, the
     * hierarchy must be broken (in other words, ignored) for the purpose of
     * ranking the stores. The Order function is capable of sorting within the
     * hierarchy, based on the topmost level represented in the set to be
     * sorted, or, by breaking the hierarchy, sorting all of the members of the
     * set as if they existed on the same level, with the same parent.
     *
     * <p>The following MDX query illustrates the use of the Order function to
     * rank the members according to unit sales.
     */
    public void testRank() {
        runQueryCheckResult(
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS, " + nl +
                "  ORDER([Store].[Store Name].MEMBERS, (Measures.[Unit Sales]), BDESC) ON ROWS" + nl +
                "FROM Sales" + nl +
                "WHERE [Product].[Non-Consumable]",
                "Axis #0:" + nl +
                "{[Product].[All Products].[Non-Consumable]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[USA].[OR].[Salem].[Store 13]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Tacoma].[Store 17]}" + nl +
                "{[Store].[All Stores].[USA].[OR].[Portland].[Store 11]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles].[Store 7]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Diego].[Store 24]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Seattle].[Store 15]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bremerton].[Store 3]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Spokane].[Store 16]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Yakima].[Store 23]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Walla Walla].[Store 22]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]}" + nl +
                "{[Store].[All Stores].[Canada].[BC].[Vancouver].[Store 19]}" + nl +
                "{[Store].[All Stores].[Canada].[BC].[Victoria].[Store 20]}" + nl +
                "{[Store].[All Stores].[Mexico].[DF].[Mexico City].[Store 9]}" + nl +
                "{[Store].[All Stores].[Mexico].[DF].[San Andres].[Store 21]}" + nl +
                "{[Store].[All Stores].[Mexico].[Guerrero].[Acapulco].[Store 1]}" + nl +
                "{[Store].[All Stores].[Mexico].[Jalisco].[Guadalajara].[Store 5]}" + nl +
                "{[Store].[All Stores].[Mexico].[Veracruz].[Orizaba].[Store 10]}" + nl +
                "{[Store].[All Stores].[Mexico].[Yucatan].[Merida].[Store 8]}" + nl +
                "{[Store].[All Stores].[Mexico].[Zacatecas].[Camacho].[Store 4]}" + nl +
                "{[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 12]}" + nl +
                "{[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 18]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Alameda].[HQ]}" + nl +
                "Row #0: 7,940" + nl +
                "Row #1: 6,712" + nl +
                "Row #2: 5,076" + nl +
                "Row #3: 4,947" + nl +
                "Row #4: 4,706" + nl +
                "Row #5: 4,639" + nl +
                "Row #6: 4,479" + nl +
                "Row #7: 4,428" + nl +
                "Row #8: 3,950" + nl +
                "Row #9: 2,140" + nl +
                "Row #10: 442" + nl +
                "Row #11: 390" + nl +
                "Row #12: 387" + nl +
                "Row #13: (null)" + nl +
                "Row #14: (null)" + nl +
                "Row #15: (null)" + nl +
                "Row #16: (null)" + nl +
                "Row #17: (null)" + nl +
                "Row #18: (null)" + nl +
                "Row #19: (null)" + nl +
                "Row #20: (null)" + nl +
                "Row #21: (null)" + nl +
                "Row #22: (null)" + nl +
                "Row #23: (null)" + nl +
                "Row #24: (null)" + nl);
    }

    /**
     * <b>How Can I Use Different Calculations for Different Levels in a
     * Dimension?</b>
     *
     * <p>This type of MDX query frequently occurs when different aggregations
     * are needed at different levels in a dimension. One easy way to support
     * such functionality is through the use of a calculated measure, created as
     * part of the query, which uses the MDX Descendants function in conjunction
     * with one of the MDX aggregation functions to provide results.
     *
     * <p>For example, the Warehouse cube in the FoodMart 2000 database
     * supplies the [Units Ordered] measure, aggregated through the Sum
     * function. But, you would also like to see the average number of units
     * ordered per store. The following table demonstrates the desired results.
     *
     * <p>By using the following MDX query, the desired results can be
     * achieved. The calculated measure, [Average Units Ordered], supplies the
     * average number of ordered units per store by using the Avg,
     * CurrentMember, and Descendants MDX functions.
     */
    public void testDifferentCalculationsForDifferentLevels() {
        runQueryCheckResult(
                "WITH MEMBER Measures.[Average Units Ordered] AS" + nl +
                "  'AVG(DESCENDANTS([Store].CURRENTMEMBER, [Store].[Store Name]), [Measures].[Units Ordered])'" + nl +
                "SELECT {[Measures].[Units ordered], Measures.[Average Units Ordered]} ON COLUMNS," + nl +
                "  [Store].[Store State].MEMBERS ON ROWS" + nl +
                "FROM Warehouse",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Units Ordered]}" + nl +
                "{[Measures].[Average Units Ordered]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[Canada].[BC]}" + nl +
                "{[Store].[All Stores].[Mexico].[DF]}" + nl +
                "{[Store].[All Stores].[Mexico].[Guerrero]}" + nl +
                "{[Store].[All Stores].[Mexico].[Jalisco]}" + nl +
                "{[Store].[All Stores].[Mexico].[Veracruz]}" + nl +
                "{[Store].[All Stores].[Mexico].[Yucatan]}" + nl +
                "{[Store].[All Stores].[Mexico].[Zacatecas]}" + nl +
                "{[Store].[All Stores].[USA].[CA]}" + nl +
                "{[Store].[All Stores].[USA].[OR]}" + nl +
                "{[Store].[All Stores].[USA].[WA]}" + nl +
                "Row #0: (null)" + nl +
                "Row #0: (null)" + nl +
                "Row #1: (null)" + nl +
                "Row #1: (null)" + nl +
                "Row #2: (null)" + nl +
                "Row #2: (null)" + nl +
                "Row #3: (null)" + nl +
                "Row #3: (null)" + nl +
                "Row #4: (null)" + nl +
                "Row #4: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #7: 66307.0" + nl +
                "Row #7: 16576.75" + nl +
                "Row #8: 44906.0" + nl +
                "Row #8: 22453.0" + nl +
                "Row #9: 116025.0" + nl +
                "Row #9: 16575.0" + nl);
    }

    /**
     * <p>This calculated measure is more powerful than it seems; if, for
     * example, you then want to see the average number of units ordered for
     * beer products in all of the stores in the California area, the following
     * MDX query can be executed with the same calculated measure.
     */
    public void testDifferentCalculations2() {
        runQueryCheckResult( // todo: "[Store].[USA].[CA]" should be "[Store].CA", "[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]" should be "[Product].[Beer]"
                "WITH MEMBER Measures.[Average Units Ordered] AS" + nl +
                "  'AVG(DESCENDANTS([Store].CURRENTMEMBER, [Store].[Store Name]), [Measures].[Units Ordered])'" + nl +
                "SELECT {[Measures].[Units ordered], Measures.[Average Units Ordered]} ON COLUMNS," + nl +
                "  [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].CHILDREN ON ROWS" + nl +
                "FROM Warehouse" + nl +
                "WHERE [Store].[USA].[CA]",

                "Axis #0:" + nl +
                "{[Store].[All Stores].[USA].[CA]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Units Ordered]}" + nl +
                "{[Measures].[Average Units Ordered]}" + nl +
                "Axis #2:" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}" + nl +
                "Row #0: (null)" + nl +
                "Row #0: (null)" + nl +
                "Row #1: 151.0" + nl +
                "Row #1: 75.5" + nl +
                "Row #2: 95.0" + nl +
                "Row #2: 95.0" + nl +
                "Row #3: (null)" + nl +
                "Row #3: (null)" + nl +
                "Row #4: 211.0" + nl +
                "Row #4: 105.5" + nl);
    }

    /**
     * <b>How Can I Use Different Calculations for Different Dimensions?</b>
     *
     * <p>Each measure in a cube uses the same aggregation function across all
     * dimensions. However, there are times where a different aggregation
     * function may be needed to represent a measure for reporting purposes. Two
     * basic cases involve aggregating a single dimension using a different
     * aggregation function than the one used for other dimensions.<ul>
     *
     * <li>Aggregating minimums, maximums, or averages along a time dimension</li>
     *
     * <li>Aggregating opening and closing period values along a time
     * dimension</li></ul>
     *
     * <p>The first case involves some knowledge of the behavior of the time
     * dimension specified in the cube. For instance, to create a calculated
     * measure that contains the average, along a time dimension, of measures
     * aggregated as sums along other dimensions, the average of the aggregated
     * measures must be taken over the set of averaging time periods,
     * constructed through the use of the Descendants MDX function. Minimum and
     * maximum values are more easily calculated through the use of the Min and
     * Max MDX functions, also combined with the Descendants function.
     *
     * <p>For example, the Warehouse cube in the FoodMart 2000 database
     * contains information on ordered and shipped inventory; from it, a report
     * is requested to show the average number of units shipped, by product, to
     * each store. Information on units shipped is added on a monthly basis, so
     * the aggregated measure [Units Shipped] is divided by the count of
     * descendants, at the Month level, of the current member in the Time
     * dimension. This calculation provides a measure representing the average
     * number of units shipped per month, as demonstrated in the following MDX
     * query.
     */
    public void _testDifferentCalculationsForDifferentDimensions() {
        runQueryCheckResult( // todo: implement "NONEMPTYCROSSJOIN"
                "WITH MEMBER [Measures].[Avg Units Shipped] AS" + nl +
                "  '[Measures].[Units Shipped] / " + nl +
                "    COUNT(DESCENDANTS([Time].CURRENTMEMBER, [Time].[Month], SELF))'" + nl +
                "SELECT {Measures.[Units Shipped], Measures.[Avg Units Shipped]} ON COLUMNS," + nl +
                "NONEMPTYCROSSJOIN(Store.CA.Children, Product.MEMBERS) ON ROWS" + nl +
                "FROM Warehouse", "");
    }

    /**
     * <p>The second case is easier to resolve, because MDX provides the
     * OpeningPeriod and ClosingPeriod MDX functions specifically to support
     * opening and closing period values.
     *
     * <p>For example, the Warehouse cube in the FoodMart 2000 database
     * contains information on ordered and shipped inventory; from it, a report
     * is requested to show on-hand inventory at the end of every month. Because
     * the inventory on hand should equal ordered inventory minus shipped
     * inventory, the ClosingPeriod MDX function can be used to create a
     * calculated measure to supply the value of inventory on hand, as
     * demonstrated in the following MDX query.
     */
    public void _testDifferentCalculationsForDifferentDimensions2() {
        runQueryCheckResult(
                "WITH MEMBER Measures.[Closing Balance] AS" + nl +
                "  '([Measures].[Units Ordered], " + nl +
                "    CLOSINGPERIOD([Time].[Month], [Time].CURRENTMEMBER)) -" + nl +
                "   ([Measures].[Units Shipped], " + nl +
                "    CLOSINGPERIOD([Time].[Month], [Time].CURRENTMEMBER))'" + nl +
                "SELECT {[Measures].[Closing Balance]} ON COLUMNS," + nl +
                "  Product.MEMBERS ON ROWS" + nl +
                "FROM Warehouse", "");
    }

    /**
     * <b>How Can I Use Date Ranges in MDX?</b>
     *
     * <p>Date ranges are a frequently encountered problem. Business questions
     * use ranges of dates, but OLAP objects provide aggregated information in
     * date levels.
     *
     * <p>Using the technique described here, you can establish date ranges in
     * MDX queries at the level of granularity provided by a time dimension.
     * Date ranges cannot be established below the granularity of the dimension
     * without additional information. For example, if the lowest level of a
     * time dimension represents months, you will not be able to establish a
     * two-week date range without other information. Member properties can be
     * added to supply specific dates for members; using such member properties,
     * you can take advantage of the date and time functions provided by VBA and
     * Excel external function libraries to establish date ranges.
     *
     * <p>The easiest way to specify a static date range is by using the colon
     * (:) operator. This operator creates a naturally ordered set, using the
     * members specified on either side of the operator as the endpoints for the
     * ordered set. For example, to specify the first six months of 1998 from
     * the Time dimension in FoodMart 2000, the MDX syntax would resemble:
     *
     * <blockquote><pre>[Time].[1998].[1]:[Time].[1998].[6]</pre></blockquote>
     *
     * <p>For example, the Sales cube uses a time dimension that supports Year,
     * Quarter, and Month levels. To add a six-month and nine-month total, two
     * calculated members are created in the following MDX query.
     */
    public void _testDateRange() {
        runQueryCheckResult( // todo: implement "AddCalculatedMembers"
                "WITH MEMBER [Time].[1997].[Six Month] AS" + nl +
                "  'SUM([Time].[1]:[Time].[6])'" + nl +
                "MEMBER [Time].[1997].[Nine Month] AS" + nl +
                "  'SUM([Time].[1]:[Time].[9])'" + nl +
                "SELECT AddCalculatedMembers([Time].[1997].Children) ON COLUMNS," + nl +
                "  [Product].Children ON ROWS" + nl +
                "FROM Sales", "");
    }

    /**
     * <b>How Can I Use Rolling Date Ranges in MDX?</b>
     *
     * <p>There are several techniques that can be used in MDX to support
     * rolling date ranges. All of these techniques tend to fall into two
     * groups. The first group involves the use of relative hierarchical
     * functions to construct named sets or calculated members, and the second
     * group involves the use of absolute date functions from external function
     * libraries to construct named sets or calculated members. Both groups are
     * applicable in different business scenarios.
     *
     * <p>In the first group of techniques, typically a named set is
     * constructed which contains a number of periods from a time dimension. For
     * example, the following table illustrates a 12-month rolling period, in
     * which the figures for unit sales of the previous 12 months are shown.
     *
     * <p>The following MDX query accomplishes this by using a number of MDX
     * functions, including LastPeriods, Tail, Filter, Members, and Item, to
     * construct a named set containing only those members across all other
     * dimensions that share data with the time dimension at the Month level.
     * The example assumes that there is at least one measure, such as [Unit
     * Sales], with a value greater than zero in the current period. The Filter
     * function creates a set of months with unit sales greater than zero, while
     * the Tail function returns the last month in this set, the current month.
     * The LastPeriods function, finally, is then used to retrieve the last 12
     * periods at this level, including the current period.
     */
    public void _testRolling() {
        runQueryCheckResult(
                "WITH SET Rolling12 AS" + nl +
                "  'LASTPERIODS(12, TAIL(FILTER([Time].[Month].MEMBERS, " + nl +
                "    ([Customers].[All Customers], " + nl +
                "    [Education Level].[All Education Level]," + nl +
                "    [Gender].[All Gender]," + nl +
                "    [Marital Status].[All Marital Status]," + nl +
                "    [Product].[All Products], " + nl +
                "    [Promotion Media].[All Media]," + nl +
                "    [Promotions].[All Promotions]," + nl +
                "    [Store].[All Stores]," + nl +
                "    [Store Size in SQFT].[All Store Size in SQFT]," + nl +
                "    [Store Type].[All Store Type]," + nl +
                "    [Yearly Income].[All Yearly Income]," + nl +
                "    Measures.[Unit Sales]) >0)," + nl +
                "  1).ITEM(0).ITEM(0))'" + nl +
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS, " + nl +
                "  Rolling12 ON ROWS" + nl +
                "FROM Sales", "");
    }

    /**
     * <b>How Can I Use Different Calculations for Different Time Periods?</b>
     *
     * <p>A few techniques can be used, depending on the structure of the cube
     * being queried, to support different calculations for members depending
     * on the time period. The following example includes the MDX IIf function,
     * and is easy to use but difficult to maintain. This example works well for
     * ad hoc queries, but is not the ideal technique for client applications in
     * a production environment.
     *
     * <p>For example, the following table illustrates a standard and dynamic
     * forecast of warehouse sales, from the Warehouse cube in the FoodMart 2000
     * database, for drink products. The standard forecast is double the
     * warehouse sales of the previous year, while the dynamic forecast varies
     * from month to month—the forecast for January is 120 percent of previous
     * sales, while the forecast for July is 260 percent of previous sales.
     *
     * <p>The most flexible way of handling this type of report is the use of
     * nested MDX IIf functions to return a multiplier to be used on the members
     * of the Products dimension, at the Drinks level. The following MDX query
     * demonstrates this technique.
     */
    public void testDifferentCalcsForDifferentTimePeriods() {
        runQueryCheckResult( // note: "[Product].[Drink Forecast - Standard]" was "[Drink Forecast - Standard]"
                "WITH MEMBER [Product].[Drink Forecast - Standard] AS" + nl +
                "  '[Product].[All Products].[Drink] * 2'" + nl +
                "MEMBER [Product].[Drink Forecast - Dynamic] AS " + nl +
                "  '[Product].[All Products].[Drink] * " + nl +
                "   IIF([Time].CurrentMember.Name = \"1\", 1.2," + nl +
                "     IIF([Time].CurrentMember.Name = \"2\", 1.3," + nl +
                "       IIF([Time].CurrentMember.Name = \"3\", 1.4," + nl +
                "         IIF([Time].CurrentMember.Name = \"4\", 1.6," + nl +
                "           IIF([Time].CurrentMember.Name = \"5\", 2.1," + nl +
                "             IIF([Time].CurrentMember.Name = \"6\", 2.4," + nl +
                "               IIF([Time].CurrentMember.Name = \"7\", 2.6," + nl +
                "                 IIF([Time].CurrentMember.Name = \"8\", 2.3," + nl +
                "                   IIF([Time].CurrentMember.Name = \"9\", 1.9," + nl +
                "                     IIF([Time].CurrentMember.Name = \"10\", 1.5," + nl +
                "                       IIF([Time].CurrentMember.Name = \"11\", 1.4," + nl +
                "                         IIF([Time].CurrentMember.Name = \"12\", 1.2, 1.0))))))))))))'" + nl +
                "SELECT DESCENDANTS(Time.[1997], [Month], SELF) ON COLUMNS, " + nl +
                "  {[Product].CHILDREN, [Product].[Drink Forecast - Standard], [Product].[Drink Forecast - Dynamic]} ON ROWS" + nl +
                "FROM Warehouse",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
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
                "Axis #2:" + nl +
                "{[Product].[All Products].[Drink]}" + nl +
                "{[Product].[All Products].[Food]}" + nl +
                "{[Product].[All Products].[Non-Consumable]}" + nl +
                "{[Product].[Drink Forecast - Standard]}" + nl +
                "{[Product].[Drink Forecast - Dynamic]}" + nl +
                "Row #0: 881.8467" + nl +
                "Row #0: 579.051" + nl +
                "Row #0: 476.2922" + nl +
                "Row #0: 618.722" + nl +
                "Row #0: 778.8864" + nl +
                "Row #0: 636.9348" + nl +
                "Row #0: 937.8423" + nl +
                "Row #0: 767.3325" + nl +
                "Row #0: 920.7068" + nl +
                "Row #0: 1007.764" + nl +
                "Row #0: 820.8075" + nl +
                "Row #0: 792.1672" + nl +
                "Row #1: 8383.4455" + nl +
                "Row #1: 4851.4058" + nl +
                "Row #1: 5353.188" + nl +
                "Row #1: 6061.829" + nl +
                "Row #1: 6039.282" + nl +
                "Row #1: 5259.2425" + nl +
                "Row #1: 6902.0103" + nl +
                "Row #1: 5790.7723" + nl +
                "Row #1: 8167.0532" + nl +
                "Row #1: 6188.7316" + nl +
                "Row #1: 5344.8452" + nl +
                "Row #1: 5025.7443" + nl +
                "Row #2: 2040.3959" + nl +
                "Row #2: 1269.8157" + nl +
                "Row #2: 1460.6858" + nl +
                "Row #2: 1696.7566" + nl +
                "Row #2: 1397.0354" + nl +
                "Row #2: 1578.1365" + nl +
                "Row #2: 1671.0463" + nl +
                "Row #2: 1609.4467" + nl +
                "Row #2: 2059.6172" + nl +
                "Row #2: 1617.4927" + nl +
                "Row #2: 1909.7132" + nl +
                "Row #2: 1382.3638" + nl +
                "Row #3: 1763.6934" + nl +
                "Row #3: 1158.102" + nl +
                "Row #3: 952.5844" + nl +
                "Row #3: 1237.444" + nl +
                "Row #3: 1557.7728" + nl +
                "Row #3: 1273.8696" + nl +
                "Row #3: 1875.6846" + nl +
                "Row #3: 1534.665" + nl +
                "Row #3: 1841.4136" + nl +
                "Row #3: 2015.528" + nl +
                "Row #3: 1641.615" + nl +
                "Row #3: 1584.3344" + nl +
                "Row #4: 1058.21604" + nl +
                "Row #4: 752.7663000000001" + nl +
                "Row #4: 666.8090799999999" + nl +
                "Row #4: 989.9552" + nl +
                "Row #4: 1635.66144" + nl +
                "Row #4: 1528.6435199999999" + nl +
                "Row #4: 2438.38998" + nl +
                "Row #4: 1764.8647499999997" + nl +
                "Row #4: 1749.34292" + nl +
                "Row #4: 1511.646" + nl +
                "Row #4: 1149.1305" + nl +
                "Row #4: 950.6006399999999" + nl);
    }

    /**
     * <p>Other techniques, such as the addition of member properties to the
     * Time or Product dimensions to support such calculations, are not as
     * flexible but are much more efficient. The primary drawback to using such
     * techniques is that the calculations are not easily altered for
     * speculative analysis purposes. For client applications, however, where
     * the calculations are static or slowly changing, using a member property
     * is an excellent way of supplying such functionality to clients while
     * keeping maintenance of calculation variables at the server level. The
     * same MDX query, for example, could be rewritten to use a member property
     * named [Dynamic Forecast Multiplier] as shown in the following MDX query.
     */
    public void _testDc4dtp2() {
        runQueryCheckResult(
                "WITH MEMBER [Product].[Drink Forecast - Standard] AS" + nl +
                "  '[Product].[All Products].[Drink] * 2'" + nl +
                "MEMBER [Product].[Drink Forecast - Dynamic] AS " + nl +
                "  '[Product].[All Products].[Drink] * " + nl +
                "   [Time].CURRENTMEMBER.PROPERTIES(\"Dynamic Forecast Multiplier\")'" + nl +
                "SELECT DESCENDANTS(Time.[1997], [Month], SELF) ON COLUMNS, " + nl +
                "  {[Product].CHILDREN, [Drink Forecast - Standard], [Drink Forecast - Dynamic]} ON ROWS" + nl +
                "FROM Warehouse", "");
    }

    /**
     * <b>How Can I Compare Time Periods in MDX?</b>
     *
     * <p>To answer such a common business question, MDX provides a number of
     * functions specifically designed to navigate and aggregate information
     * across time periods. For example, year-to-date (YTD) totals are directly
     * supported through the YTD function in MDX. In combination with the MDX
     * ParallelPeriod function, you can create calculated members to support
     * direct comparison of totals across time periods.
     *
     * <p>For example, the following table represents a comparison of YTD unit
     * sales between 1997 and 1998, run against the Sales cube in the FoodMart
     * 2000 database.
     *
     * <p>The following MDX query uses three calculated members to illustrate
     * how to use the YTD and ParallelPeriod functions in combination to compare
     * time periods.
     */
    public void _testYtdGrowth() {
        runQueryCheckResult( // todo: implement "ParallelPeriod"
                "WITH MEMBER [Measures].[YTD Unit Sales] AS" + nl +
                "  'COALESCEEMPTY(SUM(YTD(), [Measures].[Unit Sales]), 0)'" + nl +
                "MEMBER [Measures].[Previous YTD Unit Sales] AS" + nl +
                "  '(Measures.[YTD Unit Sales], PARALLELPERIOD([Time].[Year]))'" + nl +
                "MEMBER [Measures].[YTD Growth] AS" + nl +
                "  '[Measures].[YTD Unit Sales] - ([Measures].[Previous YTD Unit Sales])'" + nl +
                "SELECT {[Time].[1998]} ON COLUMNS," + nl +
                "  {[Measures].[YTD Unit Sales], [Measures].[Previous YTD Unit Sales], [Measures].[YTD Growth]} ON ROWS" + nl +
                "FROM Sales ", "");
    }

	public void testParallelNot() {
		runParallelQueries(1, 1, false);
	}

	public void testParallelSomewhat() {
		runParallelQueries(3, 2, false);
	}

    public void testParallelFlushCache() {
        runParallelQueries(4, 6, true);
    }

	public void testParallelVery() {
		runParallelQueries(6, 10, false);
	}

	private void runParallelQueries(final int threadCount,
            final int iterationCount, final boolean flush) {
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
                                if (flush && i == 0) {
                                    CachePool.instance().flush();
                                }
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
