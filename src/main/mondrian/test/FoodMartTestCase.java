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

import mondrian.olap.*;
import junit.framework.TestCase;

/**
 * <code>FoodMartTestCase</code> is a unit test which runs against the FoodMart
 * database.
 *
 * @author jhyde
 * @since 29 March, 2002
 * @version $Id$
 **/
public class FoodMartTestCase extends TestCase {
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

	// --------------------------------------------------------------
	// tests follow

	public void testSample0() {
		Result result = runQuery(
			"select {[Measures].[Unit Sales]} on columns\r\n" +
			" from Sales");
	}
	public void testSample1() {
		Result result = runQuery(
			"select\r\n" +
			"	 {[Measures].[Unit Sales]} on columns,\r\n" +
			"	 order(except([Promotion Media].[Media Type].members,{[Promotion Media].[Media Type].[No Media]}),[Measures].[Unit Sales],DESC) on rows\r\n" +
			"from Sales ");
	}
	public void testSample2() {
		Result result = runQuery(
			"select\r\n" +
			"	 { [Measures].[Units Shipped], [Measures].[Units Ordered] } on columns,\r\n" +
			"	 NON EMPTY [Store].[Store Name].members on rows\r\n" +
			"from Warehouse");
	}
	public void testSample3() {
		Result result = runQuery(
			"with member [Measures].[Store Sales Last Period] as '([Measures].[Store Sales], Time.PrevMember)'\r\n" +
			"select\r\n" +
			"	 {[Measures].[Store Sales Last Period]} on columns,\r\n" +
			"	 {TopCount([Product].[Product Department].members,5, [Measures].[Store Sales Last Period])} on rows\r\n" +
			"from Sales\r\n" +
			"where ([Time].[1998])");
	}
	public void testSample4() {
		Result result = runQuery(
			"with member [Measures].[Total Store Sales] as 'Sum(YTD(),[Measures].[Store Sales])'\r\n" +
			"select\r\n" +
			"	 {[Measures].[Total Store Sales]} on columns,\r\n" +
			"	 {TopCount([Product].[Product Department].members,5, [Measures].[Total Store Sales])} on rows\r\n" +
			"from Sales\r\n" +
			"where ([Time].[1997].[Q2].[4])");
	}
	public void testSample5() {
		Result result = runQuery(
			"with member [Measures].[Store Profit Rate] as '([Measures].[Store Sales]-[Measures].[Store Cost])/[Measures].[Store Cost]', format = '#.00%'\r\n" +
			"select\r\n" +
			"	 {[Measures].[Store Cost],[Measures].[Store Sales],[Measures].[Store Profit Rate]} on columns,\r\n" +
			"	 Order([Product].[Product Department].members, [Measures].[Store Profit Rate], BDESC) on rows\r\n" +
			"from Sales\r\n" +
			"where ([Time].[1997])");
	}
	public void testSample6() {
		Result result = runQuery(
			"with\r\n" +
			"	member [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] as '[Product].[All Products].[Drink].[Alcoholic Beverages]/[Product].[All Products].[Drink]', format = '#.00%'\r\n" +
			"select\r\n" +
			"	{ [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] } on columns,\r\n" +
			"	order([Customers].[All Customers].[USA].[WA].Children, [Product].[All Products].[Drink].[Percent of Alcoholic Drinks],BDESC ) on rows\r\n" +
			"from Sales\r\n" +
			"where ( [Measures].[Unit Sales] )");
	}
	public void testSample7() {
		Result result = runQuery(
			"with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'\r\n" +
			"select\r\n" +
			"	 {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns,\r\n" +
			"	 {Descendants([Time].[1997],[Time].[Month])} on rows\r\n" +
			"from Sales");
	}
	public void testNonEmpty1() {
		assertSize(
			"select\r\n" +
			"  NON EMPTY CrossJoin({[Product].[All Products].[Drink].Children},\r\n" +
			"    {[Customers].[All Customers].[USA].[WA].[Bellingham]}) on rows,\r\n" +
			"  CrossJoin(\r\n" +
			"    {[Measures].[Unit Sales], [Measures].[Store Sales]},\r\n" +
			"    { [Promotion Media].[All Promotion Media].[Radio],\r\n" +
			"      [Promotion Media].[All Promotion Media].[TV],\r\n" +
			"      [Promotion Media].[All Promotion Media].[Sunday Paper],\r\n" +
			"      [Promotion Media].[All Promotion Media].[Street Handout] }\r\n" +
			"    ) on columns\r\n" +
			"from Sales\r\n" +
			"where ([Time].[1997])", 8, 2);
	}
	public void testNonEmpty2() {
		assertSize(
			"select\r\n" +
			"  NON EMPTY CrossJoin(\r\n" +
			"    {[Product].[All Products].Children},\r\n" +
			"    {[Customers].[All Customers].[USA].[WA].[Bellingham]}) on rows,\r\n" +
			"  NON EMPTY CrossJoin(\r\n" +
			"    {[Measures].[Unit Sales]},\r\n" +
			"    { [Promotion Media].[All Promotion Media].[Cash Register Handout],\r\n" +
			"      [Promotion Media].[All Promotion Media].[Sunday Paper],\r\n" +
			"      [Promotion Media].[All Promotion Media].[Street Handout] }\r\n" +
			"    ) on columns\r\n" +
			"from Sales\r\n" +
			"where ([Time].[1997])", 2, 2);
	}
	public void testOneDimensionalQueryWithCompoundSlicer() {
		Result result = runQuery(
			"select\r\n" +
			"  [Product].[All Products].[Drink].children on columns\r\n" +
			"from Sales\r\n" +
			"where ([Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[Street Handout], [Time].[1997])");
		assertTrue(result.getAxes().length == 1);
		assertTrue(result.getAxes()[0].positions.length == 3);
		assertTrue(result.getSlicerAxis().positions.length == 1);
		assertTrue(result.getSlicerAxis().positions[0].members.length == 3);
	}
	public void _testEver() {
		Result result = runQuery(
			"select\r\n" +
			" {[Measures].[Unit Sales], [Measures].[Ever]} on columns,\r\n" +
			" [Gender].members on rows\r\n" +
			"from Sales");
	}
	public void _testDairy() {
		Result result = runQuery(
			"with\r\n" +
			"  member [Product].[Non dairy] as '[Product].[All Products] - [Product].[Food].[Dairy]'\r\n" +
			"  member [Measures].[Dairy ever] as 'sum([Time].members, ([Measures].[Unit Sales],[Product].[Food].[Dairy]))'\r\n" +
			"  set [Customers who never bought dairy] as 'filter([Customers].members, [Measures].[Dairy ever] = 0)'\r\n" +
			"select\r\n" +
			" {[Measures].[Unit Sales], [Measures].[Dairy ever]}  on columns,\r\n" +
			"  [Customers who never bought dairy] on rows\r\n" +
			"from Sales\r\n");
	}
	public void testHasBoughtDairy() {
		Result result = runQuery(
			"select {[Has bought dairy].members} on columns,\r\n" +
			" {[Customers].[USA]} on rows\r\n" +
			"from Sales\r\n" +
			"where ([Measures].[Unit Sales])");
	}
	public void _testSolveOrder() {
		Result result = runQuery(
			"WITH\r\n" +
			"   MEMBER [Measures].[StoreType] AS \r\n" +
			"   '[Store].CurrentMember.Properties(\"Store Type\")',\r\n" +
			"   SOLVE_ORDER = 2\r\n" +
			"   MEMBER [Measures].[ProfitPct] AS \r\n" +
			"   'Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',\r\n" +
			"   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'\r\n" +
			"SELECT\r\n" +
			"   { [Store].[Store Name].Members} ON COLUMNS,\r\n" +
			"   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[StoreType],\r\n" +
			"   [Measures].[ProfitPct] } ON ROWS\r\n" +
			"FROM Sales");
	}
	public void _testIIf() {
		Result result = runQuery(
			"WITH\r\n" +
			"   MEMBER [Product].[Beer and Wine].[BigSeller] AS\r\n" +
			"  'IIf([Product].[Beer and Wine] > 100, \"Yes\",\"No\")'\r\n" +
			"SELECT\r\n" +
			"   {[Product].[BigSeller]} ON COLUMNS,\r\n" +
			"   {Store.[Store Name].Members} ON ROWS\r\n" +
			"FROM Sales");
	}
	public void _testVal() {
		Result result = runQuery(
			"WITH\r\n" +
			"   MEMBER [Measures].[ProfitPct] AS \r\n" +
			"   'Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',\r\n" +
			"   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'\r\n" +
			"   MEMBER [Measures].[ProfitValue] AS \r\n" +
			"   '[Measures].[Store Sales] * [Measures].[ProfitPct]',\r\n" +
			"   SOLVE_ORDER = 2, FORMAT_STRING = 'Currency'\r\n" +
			"SELECT\r\n" +
			"   { [Store].[Store Name].Members} ON COLUMNS,\r\n" +
			"   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[ProfitValue],\r\n" +
			"   [Measures].[ProfitPct] } ON ROWS\r\n" +
			"FROM Sales");
	}
	public void _testCyclicalCalculatedMembers() {
		Result result = runQuery(
			"WITH\r\n" +
			"   MEMBER [Product].[X] AS '[Product].[Y]'\r\n" +
			"   MEMBER [Product].[Y] '[Product].[X]'\r\n" +
			"SELECT\r\n" +
			"   {[Product].[X]} ON COLUMNS,\r\n" +
			"   {Store.[Store Name].Members} ON ROWS\r\n" +
			"FROM Sales");
	}

    	public void testLevels() {
          Result result = runQuery(
			"with member [Measures].[Foo] as" +
				" '[Time].Levels(2)' \r\n" +
             "select {[Measures].[Foo]} on columns,\r\n" +
                " {[Gender].[M]} on rows \r\n" +
                "from Sales");
          Cell cell = result.getCell(new int[]{0,0});
          assertEquals("[Time].[Month]", cell.getFormattedValue());
          }




    public void testStringLevels() {
       Result result = runQuery(
          "with member [Measures].[Foo] as" +
				" 'Levels(\"Year\")' \r\n" +

          "select {[Measures].[Foo]} on columns,\r\n" +
                " {[Gender].[M]} on rows \r\n" +
                "from Sales");

         Cell cell = result.getCell(new int[]{0,0});
         assertEquals("[Time].[Year]", cell.getFormattedValue());
      }


     public void testDimensionHierarchy() {
       Result result = runQuery(
          "with member [Measures].[Foo] as" +
				" '[Time].Dimension' \r\n" +
          "select {[Measures].[Foo]} on columns,\r\n" +
                " {[Gender].[M]} on rows \r\n" +
                "from Sales");

         Cell cell = result.getCell(new int[]{0,0});
         assertEquals("[Time]", cell.getFormattedValue());
      }
 public void testDimensionLevel() {
       Result result = runQuery(
          "with member [Measures].[Foo] as" +
				" '[Time].[Year].Dimension' \r\n" +
          "select {[Measures].[Foo]} on columns,\r\n" +
                " {[Gender].[M]} on rows \r\n" +
                "from Sales");

         Cell cell = result.getCell(new int[]{0,0});
         assertEquals("[Time]", cell.getFormattedValue());
      }

    public void testDimensionMember() {
       Result result = runQuery(
          "with member [Measures].[Foo] as" +
				" '[Time].[1997].[Q2].Dimension' \r\n" +
          "select {[Measures].[Foo]} on columns,\r\n" +
                " {[Gender].[M]} on rows \r\n" +
                "from Sales");

         Cell cell = result.getCell(new int[]{0,0});
         assertEquals("[Time]", cell.getFormattedValue());
      }

    public void testDimensionsNumeric() {
       Result result = runQuery(
          "with member [Measures].[Foo] as" +
				" 'Dimensions(2)' \r\n" +  //1 = gives Measures
          "select {[Measures].[Foo]} on columns,\r\n" +
                " {[Gender].[M]} on rows \r\n" +
                "from Sales");

         Cell cell = result.getCell(new int[]{0,0});
         assertEquals("[Store]", cell.getFormattedValue());
      }

    public void testDimensionsString() {
       Result result = runQuery(
          "with member [Measures].[Foo] as" +
				" 'Dimensions(\"Store\")' \r\n" +

          "select {[Measures].[Foo]} on columns,\r\n" +
                " {[Gender].[M]} on rows \r\n" +
                "from Sales");

         Cell cell = result.getCell(new int[]{0,0});
         assertEquals("[Store]", cell.getFormattedValue());
      }

     public void testLevelHierarchy() {
       Result result = runQuery(
          "with member [Measures].[Foo] as" +
				" '[Time].[1997].[Q1].[1].Hierarchy' \r\n" +

          "select {[Measures].[Foo]} on columns,\r\n" +
                " {[Gender].[M]} on rows \r\n" +
                "from Sales");

         Cell cell = result.getCell(new int[]{0,0});
         assertEquals("[Time]", cell.getFormattedValue());
      }

    public void testLevelMember() {
       Result result = runQuery(
          "with member [Measures].[Foo] as" +
				" '[Time].[1997].[Q1].[1].Level' \r\n" +

          "select {[Measures].[Foo]} on columns,\r\n" +
                " {[Gender].[M]} on rows \r\n" +
                "from Sales");

         Cell cell = result.getCell(new int[]{0,0});
         assertEquals("[Time].[Month]", cell.getFormattedValue());
      }


    public void testAncestorLevel() {
       Result result = runQuery(
       //   "with member [Measures].[Foo] as" +
		//	" 'Ancestor([Store].[USA].[CA].[Los Angeles],[Store Country])' \r\n" +
          "select {Ancestor([Store].[USA].[CA].[Los Angeles],[Store Country])} on columns,\r\n" +
               //
                     //  "select {Ancestor([Store].[USA], [Store].[City])} on columns,\r\n" +

              // "select {Ancestor([Store].[Canada], [Store].[Store Country])} on columns,\r\n" + //all stores is returned
       //        "select {Ancestor([Gender].[M], [Customer].[State])} on columns,\r\n" +  //customer not fount in cube sales
                " {[Gender].[M]} on rows \r\n" +
                "from [Sales]");
         assertEquals("USA", result.getAxes()[0].positions[0].members[0].getName());
      }

    public void _testClosingPeriodNoArgs() {
       Result result = runQuery(
               "select { ClosingPeriod() } on columns, \r\n" +
                " { Gender.[M] } on rows \r\n" +
                "from [Sales]");
         assertEquals("12", result.getAxes()[0].positions[0].members[0].getName());
         //Cell cell = result.getCell(new int[]{0,0});
         //assertEquals("[1997].[12]", cell.getFormattedValue());
      }
    public void testClosingPeriod() {
       Result result = runQuery(
               "select { ClosingPeriod( [Month],[1997]) } on columns, \r\n" +
                " { Gender.[M] } on rows \r\n" +
                "from [Sales]");
         assertEquals("12", result.getAxes()[0].positions[0].members[0].getName());
         //Cell cell = result.getCell(new int[]{0,0});
         //assertEquals("[1997].[12]", cell.getFormattedValue());
      }


    public void testCousin1() {
       Result result = runQuery(
               "select { Cousin( [1997].[Q4],[1998]) } on columns, \r\n" +
                " { Gender.[M] } on rows \r\n" +
                "from [Sales]");
         assertEquals("Q4", result.getAxes()[0].positions[0].members[0].getName());
         //Cell cell = result.getCell(new int[]{0,0});
         //assertEquals("[1997].[12]", cell.getFormattedValue());
      }
    public void testCousin2() {
       Result result = runQuery(
               "select { Cousin( [1997].[Q4].[12],[1998]) } on columns, \r\n" +
                " { Gender.[M] } on rows \r\n" +
                "from [Sales]");
         assertEquals("12", result.getAxes()[0].positions[0].members[0].getName());
         //Cell cell = result.getCell(new int[]{0,0});
         //assertEquals("[1997].[12]", cell.getFormattedValue());
      }
    public void testClosingPeriodMember() {
       Result result = runQuery(
               "select { ClosingPeriod( [USA]) } on columns, \r\n" +
                " { Gender.[M] } on rows \r\n" +
                "from [Sales]");
         assertEquals("WA", result.getAxes()[0].positions[0].members[0].getName());
         //Cell cell = result.getCell(new int[]{0,0});
         //assertEquals("[1997].[12]", cell.getFormattedValue());
      }

    public void testClosingPeriodLevel() {
       Result result = runQuery(
               "select { ClosingPeriod( [Month]) } on columns, \r\n" +
                " { Gender.[M] } on rows \r\n" +
                "from [Sales]");
         assertEquals("12", result.getAxes()[0].positions[0].members[0].getName());
         //Cell cell = result.getCell(new int[]{0,0});
         //assertEquals("[1997].[12]", cell.getFormattedValue());
      }
    public void testAncestorLevel_SameLevel() {
       Result result = runQuery(
               "select {Ancestor([Store].[Canada], [Store].[Store Country])} on columns,\r\n" +
                " {[Gender].[M]} on rows \r\n" +
                "from [Sales]");
         assertEquals("Canada", result.getAxes()[0].positions[0].members[0].getName());
      }

    public void _testAncestorLevel_0thLevel() {
       Result result = runQuery(
               "select {Ancestor([Store].[USA].[CA], [Store].levels(0))} on columns,\r\n" + //fails with class cast exceptio
                " {[Gender].[M]} on rows \r\n" +
                "from [Sales]");
         assertEquals("Store", result.getAxes()[0].positions[0].members[0].getName());
      }
	public void _testHalfYears() {
		Result result = runQuery(
			"WITH MEMBER MEASURES.ProfitPercent AS\r\n" +
			"     '([Measures].[Store Sales]-[Measures].[Store Cost])/([Measures].[Store Cost])',\r\n" +
			" FORMAT_STRING = '#.00%', SOLVE_ORDER = 1\r\n" +
			" MEMBER [Time].[First Half 97] AS  '[Time].[1997].[Q1] + [Time].[1997].[Q2]'\r\n" +
			" MEMBER [Time].[Second Half 97] AS '[Time].[1997].[Q3] + [Time].[1997].[Q4]'\r\n" +
			" SELECT {[Time].[First Half 97],\r\n" +
			"     [Time].[Second Half 97],\r\n" +
			"     [Time].[1997].CHILDREN} ON COLUMNS,\r\n" +
			" {[Store].[Store Country].[USA].CHILDREN} ON ROWS\r\n" +
			" FROM [Sales]\r\n" +
			" WHERE (MEASURES.ProfitPercent)");
	}
	public void testAsSample7ButUsingVirtualCube() {
		Result result = runQuery(
			"with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'\r\n" +
			"select\r\n" +
			"	 {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns,\r\n" +
			"	 {Descendants([Time].[1997],[Time].[Month])} on rows\r\n" +
			"from [Warehouse and Sales]");
	}
	public void _testVirtualCube() {
		Result result = runQuery(
			// Note that Unit Sales is independent of Warehouse.
			"select CrossJoin(\r\n"+
			"  {[Warehouse].DefaultMember, [Warehouse].[USA].children},\r\n" +
			"  {[Measures].[Unit Sales], [Measures].[Units Shipped]}) on columns,\r\n" +
			" [Time].children on rows\r\n" +
			"from [Warehouse and Sales]");
	}
	public void _testUseDimensionAsShorthandForMember() {
		Result result = runQuery(
			"select {[Measures].[Unit Sales]} on columns,\r\n" +
			" {[Store], [Store].children} on rows\r\n" +
			"from [Sales]");
	}
	public void _testMembersFunction() {
		Result result = runQuery(
			// the simple-minded implementation of members(<number>) is
			// inefficient
			"select {[Measures].[Unit Sales]} on columns,\r\n" +
			" {[Customers].members(0)} on rows\r\n" +
			"from [Sales]");
	}
	public void testTaglib1() {
		Result result = runQuery(
			"select\r\n" +
			"  {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns,\r\n" +
			"  CrossJoin(\r\n" +
			"    { [Promotion Media].[All Promotion Media].[Radio],\r\n" +
			"      [Promotion Media].[All Promotion Media].[TV],\r\n" +
			"      [Promotion Media].[All Promotion Media].[Sunday Paper],\r\n" +
			"      [Promotion Media].[All Promotion Media].[Street Handout] },\r\n" +
			"    [Product].[All Products].[Drink].children) on rows\r\n" +
			"from Sales\r\n" +
			"where ([Time].[1997])");
	}
	public void testTaglib2() {
		Result result = runQuery(
			"select\r\n" +
			"  [Product].[All Products].[Drink].children on rows,\r\n" +
			"  CrossJoin(\r\n" +
			"    {[Measures].[Unit Sales], [Measures].[Store Sales]},\r\n" +
			"    { [Promotion Media].[All Promotion Media].[Radio],\r\n" +
			"      [Promotion Media].[All Promotion Media].[TV],\r\n" +
			"      [Promotion Media].[All Promotion Media].[Sunday Paper],\r\n" +
			"      [Promotion Media].[All Promotion Media].[Street Handout] }\r\n" +
			"    ) on columns\r\n" +
			"from Sales\r\n" +
			"where ([Time].[1997])");
	}
	public void testTaglib3() {
		Result result = runQuery(
			"select\r\n" +
			"  {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns,\r\n" +
			"  Order([Product].[Product Department].members, [Measures].[Store Sales], DESC) on rows\r\n" +
			"from Sales");
	}
	public void testTaglib4() {
		Result result = runQuery(
			"select\r\n" +
			"  [Product].[All Products].[Drink].children on columns\r\n" +
			"from Sales\r\n" +
			"where ([Measures].[Unit Sales], [Promotion Media].[All Promotion Media].[Street Handout], [Time].[1997])");
	}
	public void testTaglib5() {
		Result result = runQuery(
			"select\r\n" +
			"  NON EMPTY CrossJoin([Product].[All Products].[Drink].children, [Customers].[All Customers].[USA].[WA].Children) on rows,\r\n" +
			"  CrossJoin(\r\n" +
			"    {[Measures].[Unit Sales], [Measures].[Store Sales]},\r\n" +
			"    { [Promotion Media].[All Promotion Media].[Radio],\r\n" +
			"      [Promotion Media].[All Promotion Media].[TV],\r\n" +
			"      [Promotion Media].[All Promotion Media].[Sunday Paper],\r\n" +
			"      [Promotion Media].[All Promotion Media].[Street Handout] }\r\n" +
			"    ) on columns\r\n" +
			"from Sales\r\n" +
			"where ([Time].[1997])");
	}
	public void testTaglib6() {
		Result result = runQuery(
			"select from Sales\r\n" +
			"where ([Measures].[Store Sales], [Time].[1997], [Promotion Media].[All Promotion Media].[TV])");

	}
	public void _testSlicerAndFilter() {
		// testcase: make sure that slicer is in force when expression is applied
		// on axis, E.g. select filter([Customers].members, [Unit Sales] > 100)
		// from sales where ([Time].[1998])
	}
}

// End FoodMartTestCase.java
