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

/**
 * <code>FoodMartTestCase</code> is a unit test which runs against the FoodMart
 * database.
 *
 * @author jhyde
 * @since 29 March, 2002
 * @version $Id$
 **/
public class FoodMartTestCase extends TestCase {
	private static final String nl = System.getProperty("line.separator");

	public FoodMartTestCase(String name) {
		super(name);
	}

	Result runQuery(String queryString) {
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
			String message = e.toString();
			assertTrue(message, message.indexOf(pattern) >= 0);
			return;
		}
		fail("axis '" + expression + "' did not yield an exception");
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

	// --------------------------------------------------------------
	// tests follow

	public void testSample0() {
		Result result = runQuery(
				"select {[Measures].[Unit Sales]} on columns" + nl +
				" from Sales");
	}

	public void testSample1() {
		Result result = runQuery(
				"select" + nl +
				"	 {[Measures].[Unit Sales]} on columns," + nl +
				"	 order(except([Promotion Media].[Media Type].members,{[Promotion Media].[Media Type].[No Media]}),[Measures].[Unit Sales],DESC) on rows" + nl +
				"from Sales ");
	}

	public void testSample2() {
		Result result = runQuery(
				"select" + nl +
				"	 { [Measures].[Units Shipped], [Measures].[Units Ordered] } on columns," + nl +
				"	 NON EMPTY [Store].[Store Name].members on rows" + nl +
				"from Warehouse");
	}

	public void testSample3() {
		Result result = runQuery(
				"with member [Measures].[Store Sales Last Period] as '([Measures].[Store Sales], Time.PrevMember)'" + nl +
				"select" + nl +
				"	 {[Measures].[Store Sales Last Period]} on columns," + nl +
				"	 {TopCount([Product].[Product Department].members,5, [Measures].[Store Sales Last Period])} on rows" + nl +
				"from Sales" + nl +
				"where ([Time].[1998])");
	}

	public void testSample4() {
		Result result = runQuery(
				"with member [Measures].[Total Store Sales] as 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
				"select" + nl +
				"	 {[Measures].[Total Store Sales]} on columns," + nl +
				"	 {TopCount([Product].[Product Department].members,5, [Measures].[Total Store Sales])} on rows" + nl +
				"from Sales" + nl +
				"where ([Time].[1997].[Q2].[4])");
	}

	public void testSample5() {
		Result result = runQuery(
				"with member [Measures].[Store Profit Rate] as '([Measures].[Store Sales]-[Measures].[Store Cost])/[Measures].[Store Cost]', format = '#.00%'" + nl +
				"select" + nl +
				"	 {[Measures].[Store Cost],[Measures].[Store Sales],[Measures].[Store Profit Rate]} on columns," + nl +
				"	 Order([Product].[Product Department].members, [Measures].[Store Profit Rate], BDESC) on rows" + nl +
				"from Sales" + nl +
				"where ([Time].[1997])");
	}

	public void testSample6() {
		Result result = runQuery(
				"with" + nl +
				"	member [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] as '[Product].[All Products].[Drink].[Alcoholic Beverages]/[Product].[All Products].[Drink]', format = '#.00%'" + nl +
				"select" + nl +
				"	{ [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] } on columns," + nl +
				"	order([Customers].[All Customers].[USA].[WA].Children, [Product].[All Products].[Drink].[Percent of Alcoholic Drinks],BDESC ) on rows" + nl +
				"from Sales" + nl +
				"where ( [Measures].[Unit Sales] )");
	}

	public void testSample7() {
		Result result = runQuery(
				"with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
				"select" + nl +
				"	 {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns," + nl +
				"	 {Descendants([Time].[1997],[Time].[Month])} on rows" + nl +
				"from Sales");
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

	public void testCycle() {
		assertExprThrows("[Time].[1997].[Q4]", "infinite loop");
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

	public static final String[] taglibQueries = {
		// 0
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

		// 1
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

		// 2
		"select" + nl +
			"  {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns," + nl +
			"  Order([Product].[Product Department].members, [Measures].[Store Sales], DESC) on rows" + nl +
			"from Sales",

		// 3
		"select" + nl +
			"  [Product].[All Products].[Drink].children on columns" + nl +
			"from Sales" + nl +
			"where ([Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[Street Handout], [Time].[1997])",

		// 4
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

		// 5
		"select from Sales" + nl +
			"where ([Measures].[Store Sales], [Time].[1997], [Promotion Media].[All Promotion Media].[TV])",
	};

	public void testTaglib0() {
		Result result = runQuery(taglibQueries[0]);
	}

	public void testTaglib1() {
		Result result = runQuery(taglibQueries[1]);
	}

	public void testTaglib2() {
		Result result = runQuery(taglibQueries[2]);
	}

	public void testTaglib3() {
		Result result = runQuery(taglibQueries[3]);
	}

	public void testTaglib4() {
		Result result = runQuery(taglibQueries[4]);
	}

	public void testTaglib5() {
		Result result = runQuery(taglibQueries[5]);

	}
}

// End FoodMartTestCase.java
