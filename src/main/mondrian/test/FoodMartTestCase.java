/*
// $Id$
// (C) Copyright 2002 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 29 March, 2002
*/
package mondrian.test;

import junit.framework.TestCase;
import mondrian.olap.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;

/**
 * <code>FoodMartTestCase</code> is a unit test which runs against the FoodMart
 * database.
 *
 * @author jhyde
 * @since 29 March, 2002
 * @version $Id$
 **/
public class FoodMartTestCase extends TestCase {
	protected static final String nl = System.getProperty("line.separator");

	public FoodMartTestCase(String name) {
		super(name);
	}

	public Result runQuery(String queryString) {
		Connection connection = getConnection();
		Query query = connection.parseQuery(queryString);
		return connection.execute(query);
	}

	protected Connection getConnection() {
		return TestContext.instance().getFoodMartConnection();
	}

	/**
	 * Runs a query, and asserts that the result has a given number of columns
	 * and rows.
	 */
	protected void assertSize(String queryString, int columnCount, int rowCount) {
		Result result = runQuery(queryString);
		Axis[] axes = result.getAxes();
		assertTrue(axes.length == 2);
		assertTrue(axes[0].positions.length == columnCount);
		assertTrue(axes[1].positions.length == rowCount);
	}

	/**
	 * Runs a query, and asserts that it throws an exception which contains
	 * the given pattern.
	 */
	public void assertThrows(String queryString, String pattern) {
		Throwable throwable = TestContext.instance().executeFoodMartCatch(
				queryString);
		assertNotNull(throwable);
		String s = throwable.toString();
		assertTrue(s, s.indexOf(pattern) >= 0);
	}

	/**
	 * Runs a query with a given expression on an axis, and returns the whole
	 * axis.
	 */
	public Axis executeAxis2(String expression) {
		Result result = TestContext.instance().executeFoodMart(
				"select {" + expression + "} on columns from Sales");
		return result.getAxes()[0];
	}

	/**
	 * Runs a query with a given expression on an axis, and returns the single
	 * member.
	 */
	public Member executeAxis(String expression) {
		Result result = TestContext.instance().executeFoodMart(
				"select {" + expression + "} on columns from Sales");
		Axis axis = result.getAxes()[0];
		switch (axis.positions.length) {
		case 0:
			// The mdx "{...}" operator eliminates null members (that is,
			// members for which member.isNull() is true). So if "expression"
			// yielded just the null member, the array will be empty.
			return null;
		case 1:
			// Java nulls should never happen during expression evaluation.
			Position position = axis.positions[0];
			Util.assertTrue(position.members.length == 1);
			Member member = position.members[0];
			Util.assertTrue(member != null);
			return member;
		default:
			throw Util.newInternal(
					"expression " + expression + " yielded " +
					axis.positions.length + " positions");
		}
	}

	/**
	 * Runs a query and checks that the result is a given string.
	 */
	public void runQueryCheckResult(QueryAndResult queryAndResult) {
		runQueryCheckResult(queryAndResult.query, queryAndResult.result);
	}

	/**
	 * Runs a query and checks that the result is a given string.
	 */
	public void runQueryCheckResult(String query, String desiredResult) {
		Result result = runQuery(query);
		String resultString = toString(result);
		if (desiredResult != null) {
			assertEquals(desiredResult, resultString);
		}
	}

	/**
	 * Runs a query.
	 */
	public Result execute(String queryString) {
		return TestContext.instance().executeFoodMart(queryString);
	}

	/**
	 * Runs a query with a given expression on an axis, and asserts that it
	 * throws an error which matches a particular pattern.
	 */
	public void assertAxisThrows(String expression, String pattern) {
		try {
			Result result = TestContext.instance().executeFoodMart(
					"select {" + expression + "} on columns from Sales");
		} catch (Throwable e) {
			String stackTrace = getStackTrace(e);
			assertTrue(stackTrace, stackTrace.indexOf(pattern) >= 0);
			return;
		}
		fail("axis '" + expression + "' did not yield an exception");
	}

	private static String getStackTrace(Throwable e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		pw.flush();
		return sw.toString();
	}

	/** Executes an expression against the FoodMart database to form a single
	 * cell result set, then returns that cell's formatted value. **/
	public String executeExpr(String expression) {
		Result result = TestContext.instance().executeFoodMart(
				"with member [Measures].[Foo] as '" +
				expression +
				"' select {[Measures].[Foo]} on columns from Sales");
		Cell cell = result.getCell(new int[]{0});
		return cell.getFormattedValue();
	}

	/** Executes an expression which returns a boolean result. **/
	public String executeBooleanExpr(String expression) {
		return executeExpr("Iif(" + expression + ",\"true\",\"false\")");
	}

	/**
	 * Runs an expression, and asserts that it is the error cell which contains
	 * a particular pattern.
	 */
	public void assertExprThrows(String expression, String pattern) {
		Result result = TestContext.instance().executeFoodMart(
				"with member [Measures].[Foo] as '" +
				expression +
				"' select {[Measures].[Foo]} on columns from Sales");
		Cell cell = result.getCell(new int[]{0});
		assertTrue(cell.isError());
		String value = cell.getFormattedValue();
		assertTrue(value, value.indexOf(pattern) >= 0);
	}

	/**
	 * Converts a set of positions into a string. Useful if you want to check
	 * that an axis has the results you expected.
	 */
	public String toString(Position[] positions) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < positions.length; i++) {
			Position position = positions[i];
			if (i > 0) {
				sb.append(nl);
			}
			if (position.members.length != 1) {
				sb.append("{");
			}
			for (int j = 0; j < position.members.length; j++) {
				Member member = position.members[j];
				if (j > 0) {
					sb.append(", ");
				}
				sb.append(member.getUniqueName());
			}
			if (position.members.length != 1) {
				sb.append("}");
			}
		}
		return sb.toString();
	}

	/** Formats {@link Result}. **/
	public String toString(Result result) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		result.print(pw);
		pw.flush();
		return sw.toString();
	}

	// --------------------------------------------------------------
	// tests follow

	private static final QueryAndResult[] sampleQueries = new QueryAndResult[] {
		// 0
		new QueryAndResult(
				"select {[Measures].[Unit Sales]} on columns" + nl +
				" from Sales",

				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales]}" + nl +
				"Row #0: 266,773.00" + nl),

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
				"{[Promotion Media].[All Promotion Media].[Daily Paper, Radio, TV]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Daily Paper]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Product Attachment]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Daily Paper, Radio]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Cash Register Handout]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Sunday Paper, Radio]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Street Handout]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Sunday Paper]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Bulk Mail]}" + nl +
				"{[Promotion Media].[All Promotion Media].[In-Store Coupon]}" + nl +
				"{[Promotion Media].[All Promotion Media].[TV]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Sunday Paper, Radio, TV]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Radio]}" + nl +
				"Row #0: 9,513.00" + nl +
				"Row #1: 7,738.00" + nl +
				"Row #2: 7,544.00" + nl +
				"Row #3: 6,891.00" + nl +
				"Row #4: 6,697.00" + nl +
				"Row #5: 5,945.00" + nl +
				"Row #6: 5,753.00" + nl +
				"Row #7: 4,339.00" + nl +
				"Row #8: 4,320.00" + nl +
				"Row #9: 3,798.00" + nl +
				"Row #10: 3,607.00" + nl +
				"Row #11: 2,726.00" + nl +
				"Row #12: 2,454.00" + nl),

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
				"with member [Measures].[Store Sales Last Period] as '([Measures].[Store Sales], Time.PrevMember)'" + nl +
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
				"{[Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy]}" + nl +
				"{[Product].[All Products].[Food].[Baked Goods]}" + nl +
				"{[Product].[All Products].[Food].[Baking Goods]}" + nl +
				"Row #0: (null)" + nl +
				"Row #1: (null)" + nl +
				"Row #2: (null)" + nl +
				"Row #3: (null)" + nl +
				"Row #4: (null)" + nl),

		// 4
		new QueryAndResult(
				"with member [Measures].[Total Store Sales] as 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
				"select" + nl +
				"	 {[Measures].[Total Store Sales]} on columns," + nl +
				"	 {TopCount([Product].[Product Department].members,5, [Measures].[Total Store Sales])} on rows" + nl +
				"from Sales" + nl +
				"where ([Time].[1997].[Q2].[4])",
				null),

		// 5
		new QueryAndResult(
				"with member [Measures].[Store Profit Rate] as '([Measures].[Store Sales]-[Measures].[Store Cost])/[Measures].[Store Cost]', format = '#.00%'" + nl +
				"select" + nl +
				"	 {[Measures].[Store Cost],[Measures].[Store Sales],[Measures].[Store Profit Rate]} on columns," + nl +
				"	 Order([Product].[Product Department].members, [Measures].[Store Profit Rate], BDESC) on rows" + nl +
				"from Sales" + nl +
				"where ([Time].[1997])",
				null),

		// 6
		new QueryAndResult(
				"with" + nl +
				"	member [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] as '[Product].[All Products].[Drink].[Alcoholic Beverages]/[Product].[All Products].[Drink]', format = '#.00%'" + nl +
				"select" + nl +
				"	{ [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] } on columns," + nl +
				"	order([Customers].[All Customers].[USA].[WA].Children, [Product].[All Products].[Drink].[Percent of Alcoholic Drinks],BDESC ) on rows" + nl +
				"from Sales" + nl +
				"where ( [Measures].[Unit Sales] )",
				null),

		// 7
		new QueryAndResult(
				"with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
				"select" + nl +
				"	 {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns," + nl +
				"	 {Descendants([Time].[1997],[Time].[Month])} on rows" + nl +
				"from Sales",
				null),
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

	public void testNonEmpty1() {
		assertSize(
				"select" + nl +
				"  NON EMPTY CrossJoin({[Product].[All Products].[Drink].Children}," + nl +
				"    {[Customers].[All Customers].[USA].[WA].[Bellingham]}) on rows," + nl +
				"  CrossJoin(" + nl +
				"    {[Measures].[Unit Sales], [Measures].[Store Sales]}," + nl +
				"    { [Promotion Media].[All Promotion Media].[Radio]," + nl +
				"      [Promotion Media].[All Promotion Media].[TV]," + nl +
				"      [Promotion Media].[All Promotion Media].[Sunday Paper]," + nl +
				"      [Promotion Media].[All Promotion Media].[Street Handout] }" + nl +
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
				"    { [Promotion Media].[All Promotion Media].[Cash Register Handout]," + nl +
				"      [Promotion Media].[All Promotion Media].[Sunday Paper]," + nl +
				"      [Promotion Media].[All Promotion Media].[Street Handout] }" + nl +
				"    ) on columns" + nl +
				"from Sales" + nl +
				"where ([Time].[1997])", 2, 2);
	}

	public void testOneDimensionalQueryWithCompoundSlicer() {
		Result result = runQuery(
				"select" + nl +
				"  [Product].[All Products].[Drink].children on columns" + nl +
				"from Sales" + nl +
				"where ([Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[Street Handout], [Time].[1997])");
		assertTrue(result.getAxes().length == 1);
		assertTrue(result.getAxes()[0].positions.length == 3);
		assertTrue(result.getSlicerAxis().positions.length == 1);
		assertTrue(result.getSlicerAxis().positions[0].members.length == 3);
	}

	public void _testEver() {
		Result result = runQuery(
				"select" + nl +
				" {[Measures].[Unit Sales], [Measures].[Ever]} on columns," + nl +
				" [Gender].members on rows" + nl +
				"from Sales");
	}

	public void _testDairy() {
		Result result = runQuery(
				"with" + nl +
				"  member [Product].[Non dairy] as '[Product].[All Products] - [Product].[Food].[Dairy]'" + nl +
				"  member [Measures].[Dairy ever] as 'sum([Time].members, ([Measures].[Unit Sales],[Product].[Food].[Dairy]))'" + nl +
				"  set [Customers who never bought dairy] as 'filter([Customers].members, [Measures].[Dairy ever] = 0)'" + nl +
				"select" + nl +
				" {[Measures].[Unit Sales], [Measures].[Dairy ever]}  on columns," + nl +
				"  [Customers who never bought dairy] on rows" + nl +
				"from Sales\r\n");
	}

	public void testHasBoughtDairy() {
		Result result = runQuery(
				"select {[Has bought dairy].members} on columns," + nl +
				" {[Customers].[USA]} on rows" + nl +
				"from Sales" + nl +
				"where ([Measures].[Unit Sales])");
	}

	public void _testSolveOrder() {
		Result result = runQuery(
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
				"FROM Sales");
	}

	public void testIIf() {
		Result result = runQuery(
				"WITH" + nl +
				"   MEMBER [Product].[BigSeller] AS" + nl +
				"  'IIf([Product].[Drink].[Alcoholic Beverages].[Beer and Wine] > 100, \"Yes\",\"No\")'" + nl +
				"SELECT" + nl +
				"   {[Product].[BigSeller]} ON COLUMNS," + nl +
				"   {Store.[Store Name].Members} ON ROWS" + nl +
				"FROM Sales");
	}

	public void _testVal() {
		Result result = runQuery(
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
				"FROM Sales");
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
		Result result = runQuery(
				"WITH" + nl +
				"   MEMBER [Product].[X] AS '[Product].[Y]'" + nl +
				"   MEMBER [Product].[Y] AS '[Product].[X]'" + nl +
				"SELECT" + nl +
				"   {[Product].[X]} ON COLUMNS," + nl +
				"   {Store.[Store Name].Members} ON ROWS" + nl +
				"FROM Sales");
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
		Result result = runQuery(
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
				" WHERE ([Measures].[ProfitPercent])");
	}

	public void _testHalfYearsTrickyCase() {
		Result result = runQuery(
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
				" WHERE (MEASURES.ProfitPercent)");
	}

	public void testAsSample7ButUsingVirtualCube() {
		Result result = runQuery(
				"with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
				"select" + nl +
				"	 {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns," + nl +
				"	 {Descendants([Time].[1997],[Time].[Month])} on rows" + nl +
				"from [Warehouse and Sales]");
	}

	public void _testVirtualCube() {
		Result result = runQuery(
				// Note that Unit Sales is independent of Warehouse.
				"select CrossJoin(" + nl +
				"  {[Warehouse].DefaultMember, [Warehouse].[USA].children}," + nl +
				"  {[Measures].[Unit Sales], [Measures].[Units Shipped]}) on columns," + nl +
				" [Time].children on rows" + nl +
				"from [Warehouse and Sales]");
	}

	public void testUseDimensionAsShorthandForMember() {
		Result result = runQuery(
				"select {[Measures].[Unit Sales]} on columns," + nl +
				" {[Store], [Store].children} on rows" + nl +
				"from [Sales]");
	}

	public void _testMembersFunction() {
		Result result = runQuery(
				// the simple-minded implementation of members(<number>) is
				// inefficient
				"select {[Measures].[Unit Sales]} on columns," + nl +
				" {[Customers].members(0)} on rows" + nl +
				"from [Sales]");
	}

	public static final QueryAndResult[] taglibQueries = {
		// 0
		new QueryAndResult(
				"select" + nl +
				"  {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns," + nl +
				"  CrossJoin(" + nl +
				"    { [Promotion Media].[All Promotion Media].[Radio]," + nl +
				"      [Promotion Media].[All Promotion Media].[TV]," + nl +
				"      [Promotion Media].[All Promotion Media].[Sunday Paper]," + nl +
				"      [Promotion Media].[All Promotion Media].[Street Handout] }," + nl +
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
				"{[Promotion Media].[All Promotion Media].[Radio], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Radio], [Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Radio], [Product].[All Products].[Drink].[Dairy]}" + nl +
				"{[Promotion Media].[All Promotion Media].[TV], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Promotion Media].[All Promotion Media].[TV], [Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Promotion Media].[All Promotion Media].[TV], [Product].[All Products].[Drink].[Dairy]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Sunday Paper], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Sunday Paper], [Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Sunday Paper], [Product].[All Products].[Drink].[Dairy]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Street Handout], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Street Handout], [Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Promotion Media].[All Promotion Media].[Street Handout], [Product].[All Products].[Drink].[Dairy]}" + nl +
				"Row #0: 75" + nl +
				"Row #0: 70.4" + nl +
				"Row #0: 168.62" + nl +
				"Row #1: 97" + nl +
				"Row #1: 75.7" + nl +
				"Row #1: 186.03" + nl +
				"Row #2: 54" + nl +
				"Row #2: 36.75" + nl +
				"Row #2: 89.03" + nl +
				"Row #3: 76" + nl +
				"Row #3: 70.99" + nl +
				"Row #3: 182.38" + nl +
				"Row #4: 188" + nl +
				"Row #4: 167" + nl +
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
				"    { [Promotion Media].[All Promotion Media].[Radio]," + nl +
				"      [Promotion Media].[All Promotion Media].[TV]," + nl +
				"      [Promotion Media].[All Promotion Media].[Sunday Paper]," + nl +
				"      [Promotion Media].[All Promotion Media].[Street Handout] }" + nl +
				"    ) on columns" + nl +
				"from Sales" + nl +
				"where ([Time].[1997])",

				"Axis #0:" + nl +
				"{[Time].[1997]}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[Radio]}" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[TV]}" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[Sunday Paper]}" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[Street Handout]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Promotion Media].[Radio]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Promotion Media].[TV]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Promotion Media].[Sunday Paper]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Promotion Media].[Street Handout]}" + nl +
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
				"{[Product].[All Products].[Non-Consumable].[Household]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Health and Hygiene]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Periodicals]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Checkout]}" + nl +
				"{[Product].[All Products].[Non-Consumable].[Carousel]}" + nl +
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
				"{[Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy]}" + nl +
				"Row #0: 27,038" + nl +
				"Row #0: 60,469.89" + nl +
				"Row #1: 16,284" + nl +
				"Row #1: 32,571.86" + nl +
				"Row #2: 4,294" + nl +
				"Row #2: 9,056.76" + nl +
				"Row #3: 1,779" + nl +
				"Row #3: 3,767.71" + nl +
				"Row #4: 841" + nl +
				"Row #4: 1,500.11" + nl +
				"Row #5: 37,792" + nl +
				"Row #5: 82,248.42" + nl +
				"Row #6: 30,545" + nl +
				"Row #6: 67,609.82" + nl +
				"Row #7: 26,655" + nl +
				"Row #7: 55,207.5" + nl +
				"Row #8: 19,026" + nl +
				"Row #8: 39,774.34" + nl +
				"Row #9: 20,245" + nl +
				"Row #9: 38,670.41" + nl +
				"Row #10: 12,885" + nl +
				"Row #10: 30,508.85" + nl +
				"Row #11: 12,037" + nl +
				"Row #11: 25,318.93" + nl +
				"Row #12: 7,870" + nl +
				"Row #12: 16,455.43" + nl +
				"Row #13: 6,884" + nl +
				"Row #13: 14,550.05" + nl +
				"Row #14: 5,262" + nl +
				"Row #14: 11,756.07" + nl +
				"Row #15: 4,132" + nl +
				"Row #15: 9,200.76" + nl +
				"Row #16: 3,317" + nl +
				"Row #16: 6,941.46" + nl +
				"Row #17: 1,764" + nl +
				"Row #17: 3,809.14" + nl +
				"Row #18: 1,714" + nl +
				"Row #18: 3,669.89" + nl +
				"Row #19: 1,812" + nl +
				"Row #19: 3,314.52" + nl +
				"Row #20: 13,573" + nl +
				"Row #20: 27,748.53" + nl +
				"Row #21: 6,838" + nl +
				"Row #21: 14,029.08" + nl +
				"Row #22: 4,186" + nl +
				"Row #22: 7,058.6" + nl),

		// 3
		new QueryAndResult(
				"select" + nl +
				"  [Product].[All Products].[Drink].children on columns" + nl +
				"from Sales" + nl +
				"where ([Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[Street Handout], [Time].[1997])",

				"Axis #0:" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[Street Handout], [Time].[1997]}" + nl +
				"Axis #1:" + nl +
				"{[Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
				"{[Product].[All Products].[Drink].[Beverages]}" + nl +
				"{[Product].[All Products].[Drink].[Dairy]}" + nl +
				"Row #0: 158.00" + nl +
				"Row #0: 270.00" + nl +
				"Row #0: 84.00" + nl),

		// 4
		new QueryAndResult(
				"select" + nl +
				"  NON EMPTY CrossJoin([Product].[All Products].[Drink].children, [Customers].[All Customers].[USA].[WA].Children) on rows," + nl +
				"  CrossJoin(" + nl +
				"    {[Measures].[Unit Sales], [Measures].[Store Sales]}," + nl +
				"    { [Promotion Media].[All Promotion Media].[Radio]," + nl +
				"      [Promotion Media].[All Promotion Media].[TV]," + nl +
				"      [Promotion Media].[All Promotion Media].[Sunday Paper]," + nl +
				"      [Promotion Media].[All Promotion Media].[Street Handout] }" + nl +
				"    ) on columns" + nl +
				"from Sales" + nl +
				"where ([Time].[1997])",

				("Axis #0:" + nl +
				"{[Time].[1997]}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[Radio]}" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[TV]}" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[Sunday Paper]}" + nl +
				"{[Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[Street Handout]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Promotion Media].[Radio]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Promotion Media].[TV]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Promotion Media].[Sunday Paper]}" + nl +
				"{[Measures].[Store Sales], [Promotion Media].[All Promotion Media].[Street Handout]}" + nl +
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
				"Row #1: 10.4" + nl +
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
				"Row #4: 2.1" + nl +
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
				"Row #6: 7.8" + nl +
				"Row #6: (null)" + nl +
				"Row #6: (null)" + nl) + (
				"Row #6: 15" + nl +
				"Row #7: 14" + nl +
				"Row #7: (null)" + nl +
				"Row #7: (null)" + nl +
				"Row #7: (null)" + nl +
				"Row #7: 36.1" + nl +
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
				"Row #8: 32.2" + nl +
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
				"Row #18: 8.1" + nl +
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
				"Row #25: 6.8" + nl +
				"Row #25: (null)" + nl +
				"Row #25: (null)" + nl +
				"Row #25: 18.9" + nl +
				"Row #26: 3" + nl +
				"Row #26: (null)" + nl) + (
				"Row #26: (null)" + nl +
				"Row #26: 9" + nl +
				"Row #26: 1.5" + nl +
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
				"Row #29: 28.8" + nl +
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
				"Row #33: 2.8" + nl +
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
				"Row #39: 9.2" + nl +
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
				"Row #48: 4.5" + nl +
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
					"where ([Measures].[Store Sales], [Time].[1997], [Promotion Media].[All Promotion Media].[TV])",

				"Axis #0:" + nl +
				"{[Measures].[Store Sales], [Time].[1997], [Promotion Media].[All Promotion Media].[TV]}" + nl +
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

	public void testParallelButSingle() {
		runParallelQueries(1, 1);
	}

	public void testParallel() {
		runParallelQueries(5, 3);
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

/**
 * Similar to {@link Runnable}, except classes which implement
 * <code>ChooseRunnable</code> choose what to do based upon an integer
 * parameter.
 */
interface ChooseRunnable {
	void run(int i);
}

/**
 * Runs a test case in several parallel threads, catching exceptions from
 * each one, and succeeding only if they all succeed.
 */
class TestCaseForker {
	TestCase testCase;
	int timeoutMs;
	Thread[] threads;
	ArrayList failures = new ArrayList();
	ChooseRunnable chooseRunnable;

	public TestCaseForker(
			TestCase testCase, int timeoutMs, int threadCount,
			ChooseRunnable chooseRunnable) {
		this.testCase = testCase;
		this.timeoutMs = timeoutMs;
		this.threads = new Thread[threadCount];
		this.chooseRunnable = chooseRunnable;
	}

	public void run() {
		ThreadGroup threadGroup = null;//new ThreadGroup("TestCaseForker thread group");
		final TestCaseForker forker = this;
		for (int i = 0; i < threads.length; i++) {
			final int threadIndex = i;
			this.threads[i] = new Thread(threadGroup, "thread #" + threadIndex) {
				public void run() {
					try {
						chooseRunnable.run(threadIndex);
					} catch (Throwable e) {
						e.printStackTrace();
						failures.add(e);
					} finally {
						synchronized (forker) {
							forker.notify();
						}
					}
				}
			};
		}
		for (int i = 0; i < threads.length; i++) {
			threads[i].start();
		}
		for (int i = 0; i < threads.length; i++) {
			try {
				synchronized (this) {
					this.wait(timeoutMs);
				}
			} catch (InterruptedException e) {
				failures.add(
						Util.newInternal(
								e, "Interrupted after " + timeoutMs + "ms"));
			}
		}
		if (failures.size() > 0) {
			for (int i = 0; i < failures.size(); i++) {
				Throwable throwable = (Throwable) failures.get(i);
				throwable.printStackTrace();
			}
			testCase.fail(failures.size() + " threads failed");
		}
	}
}

class QueryAndResult {
	String query;
	String result;
	QueryAndResult(String query, String result) {
		this.query = query;
		this.result = result;
	}
}

// End FoodMartTestCase.java
