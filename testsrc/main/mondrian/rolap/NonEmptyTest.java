/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import junit.framework.Assert;
import mondrian.olap.Axis;
import mondrian.olap.Cell;
import mondrian.olap.Connection;
import mondrian.olap.Evaluator;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.rolap.RolapConnection.NonEmptyResult;
import mondrian.rolap.RolapNative.Listener;
import mondrian.rolap.RolapNative.NativeEvent;
import mondrian.rolap.RolapNative.TupleEvent;
import mondrian.rolap.cache.CachePool;
import mondrian.rolap.cache.HardSmartCache;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.util.Bug;

import org.apache.log4j.Logger;
import org.eigenbase.util.property.IntegerProperty;

/**
 * Tests for NON EMPTY Optimization, includes SqlConstraint type hierarchy and
 * RolapNative classes.
 *
 * @author av
 * @since Nov 21, 2005
 * @version $Id$
 */
public class NonEmptyTest extends FoodMartTestCase {
    private static Logger logger = Logger.getLogger(NonEmptyTest.class);
    SqlConstraintFactory scf = SqlConstraintFactory.instance();

    
    public NonEmptyTest() {
        super();
    }

    public NonEmptyTest(String name) {
        super(name);
    }

    public void testStrMeasure() {
      TestContext ctx = TestContext.create(
              null,
              "<Cube name=\"StrMeasure\"> \n" +
              "  <Table name=\"promotion\"/> \n" + 
              "  <Dimension name=\"Promotions\"> \n" + 
              "    <Hierarchy hasAll=\"true\" > \n" + 
              "      <Level name=\"Promotion Name\" column=\"promotion_name\" uniqueMembers=\"true\"/> \n" + 
              "    </Hierarchy> \n" + 
              "  </Dimension> \n" + 
              "  <Measure name=\"Media\" column=\"media_type\" aggregator=\"max\" datatype=\"String\"/> \n" + 
              "</Cube> \n",
              null,null,null);
      
      ctx.assertQueryReturns(
              "select {[Measures].[Media]} on columns " +
              "from [StrMeasure]",
              "Axis #0:" + nl +
              "{}" + nl +
              "Axis #1:" + nl +
              "{[Measures].[Media]}" + nl +
              "Row #0: TV" + nl

      );
  }
    
    public void testBug1515302() {
        TestContext ctx = TestContext.create(
                null,
                "<Cube name=\"Bug1515302\"> \n" +
                "  <Table name=\"sales_fact_1997\"/> \n" + 
                "  <Dimension name=\"Promotions\" foreignKey=\"promotion_id\"> \n" + 
                "    <Hierarchy hasAll=\"false\" primaryKey=\"promotion_id\"> \n" + 
                "      <Table name=\"promotion\"/> \n" + 
                "      <Level name=\"Promotion Name\" column=\"promotion_name\" uniqueMembers=\"true\"/> \n" + 
                "    </Hierarchy> \n" + 
                "  </Dimension> \n" + 
                "  <Dimension name=\"Customers\" foreignKey=\"customer_id\"> \n" + 
                "    <Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKey=\"customer_id\"> \n" + 
                "      <Table name=\"customer\"/> \n" + 
                "      <Level name=\"Country\" column=\"country\" uniqueMembers=\"true\"/> \n" + 
                "      <Level name=\"State Province\" column=\"state_province\" uniqueMembers=\"true\"/> \n" + 
                "      <Level name=\"City\" column=\"city\" uniqueMembers=\"false\"/> \n" + 
                "      <Level name=\"Name\" column=\"customer_id\" type=\"Numeric\" uniqueMembers=\"true\"/> \n" + 
                "    </Hierarchy> \n" + 
                "  </Dimension> \n" + 
                "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"/> \n" + 
                "</Cube> \n",
                null,null,null);
        
        ctx.assertQueryReturns(
                "select {[Measures].[Unit Sales]} on columns, " +
                "non empty crossjoin({[Promotions].[Big Promo]}, " +
                "Descendants([Customers].[USA], [City], " +
                "SELF_AND_BEFORE)) on rows " +
                "from [Bug1515302]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Anacortes]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Ballard]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Bellingham]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Burien]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Everett]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Issaquah]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Kirkland]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Lynnwood]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Marysville]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Olympia]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Puyallup]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Redmond]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Renton]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Seattle]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Sedro Woolley]}" + nl +
                "{[Promotions].[Big Promo], [Customers].[All Customers].[USA].[WA].[Tacoma]}" + nl +
                "Row #0: 1,789" + nl +
                "Row #1: 1,789" + nl +
                "Row #2: 20" + nl +
                "Row #3: 35" + nl +
                "Row #4: 15" + nl +
                "Row #5: 18" + nl +
                "Row #6: 60" + nl +
                "Row #7: 42" + nl +
                "Row #8: 36" + nl +
                "Row #9: 79" + nl +
                "Row #10: 58" + nl +
                "Row #11: 520" + nl +
                "Row #12: 438" + nl +
                "Row #13: 14" + nl +
                "Row #14: 20" + nl +
                "Row #15: 65" + nl +
                "Row #16: 3" + nl +
                "Row #17: 366" + nl

        );
    }

    /**
     * must not use native sql optimization because it chooses the wrong RolapStar
     * in SqlContextConstraint/SqlConstraintUtils.
     * Test ensures that no exception is thrown.
     */
    public void testVirtualCube() throws Exception {
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            return;
        }
        TestCase c = new TestCase(99, 3,
                "select NON EMPTY {[Measures].[Unit Sales], [Measures].[Warehouse Sales]} ON COLUMNS, " +
                "NON EMPTY [Product].[All Products].Children ON ROWS " +
                "from [Warehouse and Sales]");
        c.run();
    }
    
    public void testVirtualCubeMembers() throws Exception {
        // ok to use native sql optimization for members on a virtual cube
        TestCase c = new TestCase(6, 3,
                "select NON EMPTY {[Measures].[Unit Sales], [Measures].[Warehouse Sales]} ON COLUMNS, " +
                "NON EMPTY {[Product].[Product Family].Members} ON ROWS " +
                "from [Warehouse and Sales]");
        c.run();
    }
    
    public void testVirtualCubeMembersNonConformingDim() throws Exception {
        // native sql optimization should not be used when you have a
        // non-conforming dimensions because it will result in a cartesian
        // product join
        checkNotNative(
            1,
            "select non empty {[Customers].[Country].members} on columns, " +
            "{[Measures].[Units Ordered]} on rows from " +
            "[Warehouse and Sales]");
    }

    public void testNativeFilter() {
        if (!MondrianProperties.instance().EnableNativeFilter.get()) {
            return;
        }
        checkNative(
                32,
                18,
                "select {[Measures].[Store Sales]} ON COLUMNS, "
                        + "Order(Filter(Descendants([Customers].[All Customers].[USA].[CA], [Customers].[Name]), ([Measures].[Store Sales] > 200.0)), [Measures].[Store Sales], DESC) ON ROWS "
                        + "from [Sales] "
                        + "where ([Time].[1997])");
    }

    /**
     * Executes a Filter() whose condition contains a calculated member.
     */
    public void testCmNativeFilter() {
        String mdx = "with member [Measures].[Rendite] as '([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost]' "
          + "select NON EMPTY {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Rendite], [Measures].[Store Sales]} ON COLUMNS, "
          + "NON EMPTY Order(Filter([Product].[Product Name].Members, ([Measures].[Rendite] > 1.8)), [Measures].[Rendite], BDESC) ON ROWS "
          + "from [Sales] "
          + "where ([Store].[All Stores].[USA].[CA], [Time].[1997])";

        if (!MondrianProperties.instance().EnableNativeFilter.get()) {
          assertQueryReturns(
              mdx,
              "Axis #0:" + nl +
              "{[Store].[All Stores].[USA].[CA], [Time].[1997]}" + nl +
              "Axis #1:" + nl +
              "{[Measures].[Unit Sales]}" + nl +
              "{[Measures].[Store Cost]}" + nl +
              "{[Measures].[Store Sales]}" + nl +
              "{[Measures].[Rendite]}" + nl +
              "Axis #2:" + nl +
              "{[Product].[All Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Plato].[Plato Extra Chunky Peanut Butter]}" + nl +
              "{[Product].[All Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Horatio].[Horatio Buttered Popcorn]}" + nl +
              "{[Product].[All Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Better].[Better Canned Tuna in Oil]}" + nl +
              "{[Product].[All Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Cantelope]}" + nl +
              "{[Product].[All Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Denny].[Denny 75 Watt Lightbulb]}" + nl +
              "{[Product].[All Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Johnson].[Johnson Oatmeal]}" + nl +
              "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Light Wine]}" + nl +
              "{[Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Squash]}" + nl +
              "Row #0: 42" + nl +
              "Row #0: 24.06" + nl +
              "Row #0: 70.56" + nl +
              "Row #0: 1.93" + nl +
              "Row #1: 36" + nl +
              "Row #1: 29.02" + nl +
              "Row #1: 84.60" + nl +
              "Row #1: 1.91" + nl +
              "Row #2: 39" + nl +
              "Row #2: 20.55" + nl +
              "Row #2: 58.50" + nl +
              "Row #2: 1.85" + nl +
              "Row #3: 25" + nl +
              "Row #3: 21.76" + nl +
              "Row #3: 61.75" + nl +
              "Row #3: 1.84" + nl +
              "Row #4: 43" + nl +
              "Row #4: 59.62" + nl +
              "Row #4: 168.99" + nl +
              "Row #4: 1.83" + nl +
              "Row #5: 34" + nl +
              "Row #5: 7.20" + nl +
              "Row #5: 20.40" + nl +
              "Row #5: 1.83" + nl +
              "Row #6: 36" + nl +
              "Row #6: 33.10" + nl +
              "Row #6: 93.60" + nl +
              "Row #6: 1.83" + nl +
              "Row #7: 46" + nl +
              "Row #7: 28.34" + nl +
              "Row #7: 79.58" + nl +
              "Row #7: 1.81" + nl
          );
        } else {
            checkNative(32, 8, mdx);
        }
    }

    /**
     * getMembersInLevel where Level = (All)
     */
    public void testAllLevelMembers() {
        checkNative(
                14,
                14,
                "select {[Measures].[Store Sales]} ON COLUMNS, "
                        + "NON EMPTY Crossjoin([Product].[(All)].Members, [Promotion Media].[All Media].Children) ON ROWS "
                        + "from [Sales]");

    }

    /**
     * enum sets {} containing ALL
     */
    public void testCjDescendantsEnumAllOnly() {
        checkNative(9, 9, "select {[Measures].[Unit Sales]} ON COLUMNS, " + "NON EMPTY Crossjoin("
                + "  Descendants([Customers].[All Customers].[USA], [Customers].[City]), "
                + "  {[Product].[All Products]}) ON ROWS " + "from [Sales] "
                + "where ([Promotions].[All Promotions].[Bag Stuffers])");
    }

    /**
     * checks that crossjoin returns a modifiable copy from cache
     * because its modified during sort
     */
    public void testResultIsModifyableCopy() {
        checkNative(
                3,
                3,
                "select {[Measures].[Store Sales]} on columns,"
                        + "  NON EMPTY Order("
                        + "        CrossJoin([Customers].[All Customers].[USA].children, [Promotions].[Promotion Name].Members), "
                        + "        [Measures].[Store Sales]) ON ROWS" + " from [Sales] where ("
                        + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
                        + "  [Time].[1997].[Q1].[1])");
    }

    /** check that top count is executed native unless disabled */
    public void testNativeTopCount() {
        if (!MondrianProperties.instance().EnableNativeTopCount.get()) {
            return;
        }
        checkNative(
                3,
                3,
                "select {[Measures].[Store Sales]} on columns,"
                        + "  NON EMPTY TopCount("
                        + "        CrossJoin([Customers].[All Customers].[USA].children, [Promotions].[Promotion Name].Members), "
                        + "        3, (3 * [Measures].[Store Sales]) - 100) ON ROWS"
                        + " from [Sales] where ("
                        + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
                        + "  [Time].[1997].[Q1].[1])");
    }

    /** check that top count is executed native with calculated member */
    public void testCmNativeTopCount() {
        if (!MondrianProperties.instance().EnableNativeTopCount.get()) {
            return;
        }
        checkNative(
                3,
                3,
                "with member [Measures].[Store Profit Rate] as '([Measures].[Store Sales]-[Measures].[Store Cost])/[Measures].[Store Cost]', format = '#.00%' "
                        + "select {[Measures].[Store Sales]} on columns,"
                        + "  NON EMPTY TopCount("
                        + "        [Customers].[All Customers].[USA].children, "
                        + "        3, [Measures].[Store Profit Rate] / 2) ON ROWS"
                        + " from [Sales]");
    }

    public void testMeasureAndAggregateInSlicer() {
        String result = "Axis #0:"
                + nl
                + "{[Store Type].[All Store Types].[All Types], [Measures].[Unit Sales], [Customers].[All Customers].[USA], [Product].[All Products].[Drink]}"
                + nl + "Axis #1:" + nl + "{[Time].[1997]}" + nl + "Axis #2:" + nl
                + "{[Store].[All Stores].[USA].[CA].[Beverly Hills]}" + nl
                + "{[Store].[All Stores].[USA].[CA].[Los Angeles]}" + nl
                + "{[Store].[All Stores].[USA].[CA].[San Diego]}" + nl
                + "{[Store].[All Stores].[USA].[CA].[San Francisco]}" + nl + "Row #0: 1,945" + nl
                + "Row #1: 2,422" + nl + "Row #2: 2,560" + nl + "Row #3: 175" + nl;
        assertQueryReturns(
                "with member [Store Type].[All Store Types].[All Types] as 'Aggregate({[Store Type].[All Store Types].[Deluxe Supermarket],  "
                        + "[Store Type].[All Store Types].[Gourmet Supermarket],  "
                        + "[Store Type].[All Store Types].[HeadQuarters],  "
                        + "[Store Type].[All Store Types].[Mid-Size Grocery],  "
                        + "[Store Type].[All Store Types].[Small Grocery],  "
                        + "[Store Type].[All Store Types].[Supermarket]})'  "
                        + "select NON EMPTY {[Time].[1997]} ON COLUMNS,   "
                        + "NON EMPTY [Store].[All Stores].[USA].[CA].Children ON ROWS   "
                        + "from [Sales] "
                        + "where ([Store Type].[All Store Types].[All Types], [Measures].[Unit Sales], [Customers].[All Customers].[USA], [Product].[All Products].[Drink])  ",
                result);

    }

    public void testMeasureInSlicer() {
        String result = "Axis #0:"
                + nl
                + "{[Measures].[Unit Sales], [Customers].[All Customers].[USA], [Product].[All Products].[Drink]}"
                + nl + "Axis #1:" + nl + "{[Time].[1997]}" + nl + "Axis #2:" + nl
                + "{[Store].[All Stores].[USA].[CA].[Beverly Hills]}" + nl
                + "{[Store].[All Stores].[USA].[CA].[Los Angeles]}" + nl
                + "{[Store].[All Stores].[USA].[CA].[San Diego]}" + nl
                + "{[Store].[All Stores].[USA].[CA].[San Francisco]}" + nl + "Row #0: 1,945" + nl
                + "Row #1: 2,422" + nl + "Row #2: 2,560" + nl + "Row #3: 175" + nl;
        assertQueryReturns(
                "select NON EMPTY {[Time].[1997]} ON COLUMNS,   "
                        + "NON EMPTY [Store].[All Stores].[USA].[CA].Children ON ROWS  "
                        + "from [Sales]  "
                        + "where ([Measures].[Unit Sales], [Customers].[All Customers].[USA], [Product].[All Products].[Drink])",
                result);

    }

    /**
     * Calc Member in TopCount: this topcount can not be calculated native because
     * its set contains calculated members.
     */
    public void testCmInTopCount() {
        checkNotNative(1, "with member [Time].[Jan] as  "
                + "'Aggregate({[Time].[1998].[Q1].[1], [Time].[1997].[Q1].[1]})'  "
                + "select NON EMPTY {[Measures].[Unit Sales]} ON columns,  "
                + "NON EMPTY TopCount({[Time].[Jan]}, 2) ON rows from [Sales] ");
    }

    /** calc member in slicer can not be executed natively */
    public void testCmInSlicer() {
        checkNotNative(3, "with member [Time].[Jan] as  "
                + "'Aggregate({[Time].[1998].[Q1].[1], [Time].[1997].[Q1].[1]})'  "
                + "select NON EMPTY {[Measures].[Unit Sales]} ON columns,  "
                + "NON EMPTY [Product].[All Products].Children ON rows from [Sales] "
                + "where ([Time].[Jan]) ");
    }

    public void testCjMembersMembersMembers() {
        checkNative(67, 67, "select {[Measures].[Store Sales]} on columns,"
                + "  NON EMPTY Crossjoin("
                + "    Crossjoin("
                + "        [Customers].[Name].Members,"
                + "        [Product].[Product Name].Members), "
                + "    [Promotions].[Promotion Name].Members) ON rows "
                + " from [Sales] where ("
                + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
                + "  [Time].[1997].[Q1].[1])");
    }

    /** SQL does not make sense because alle members are known */
    public void testCjEnumEnum() {
        checkNotNative(
                4,
                "select {[Measures].[Unit Sales]} ON COLUMNS, "
                        + "Crossjoin({[Product].[All Products].[Drink].[Beverages], [Product].[All Products].[Drink].[Dairy]}, {[Customers].[All Customers].[USA].[OR].[Portland], [Customers].[All Customers].[USA].[OR].[Salem]}) ON ROWS "
                        + "from [Sales] ");
    }

    /**
     * enum sets {} containing members from different levels can not be computed
     * natively currently.
     */
    public void testCjDescendantsEnumAll() {
        checkNotNative(
                13,
                "select {[Measures].[Unit Sales]} ON COLUMNS, "
                        + "NON EMPTY Crossjoin("
                        + "  Descendants([Customers].[All Customers].[USA], [Customers].[City]), "
                        + "  {[Product].[All Products], [Product].[All Products].[Drink].[Dairy]}) ON ROWS "
                        + "from [Sales] "
                        + "where ([Promotions].[All Promotions].[Bag Stuffers])");
    }

    public void testCjDescendantsEnum() {
        checkNative(
                11,
                11,
                "select {[Measures].[Unit Sales]} ON COLUMNS, "
                        + "NON EMPTY Crossjoin("
                        + "  Descendants([Customers].[All Customers].[USA], [Customers].[City]), "
                        + "  {[Product].[All Products].[Drink].[Beverages], [Product].[All Products].[Drink].[Dairy]}) ON ROWS "
                        + "from [Sales] "
                        + "where ([Promotions].[All Promotions].[Bag Stuffers])");
    }

    public void testCjEnumChildren() {
        checkNative(
                3,
                3,
                "select {[Measures].[Unit Sales]} ON COLUMNS, "
                        + "NON EMPTY Crossjoin("
                        + "  {[Product].[All Products].[Drink].[Beverages], [Product].[All Products].[Drink].[Dairy]}, "
                        + "  [Customers].[All Customers].[USA].[WA].Children) ON ROWS "
                        + "from [Sales] " + "where ([Promotions].[All Promotions].[Bag Stuffers])");
    }

    /** {} contains members from different levels, this can not be handled by the current native crossjoin */
    public void testCjEnumDifferentLevelsChildren() {
        // Don't run the test if we're testing expression dependencies.
        // Expression dependencies cause spurious interval calls to
        // 'level.getMembers()' which create false negatives in this test.
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            return;
        }

        TestCase c = new TestCase(8, 5, "select {[Measures].[Unit Sales]} ON COLUMNS, "
                + "NON EMPTY Crossjoin("
                + "  {[Product].[All Products].[Food], [Product].[All Products].[Drink].[Dairy]}, "
                + "  [Customers].[All Customers].[USA].[WA].Children) ON ROWS " + "from [Sales] "
                + "where ([Promotions].[All Promotions].[Bag Stuffers])");
        c.run();
    }

    public void testCjDescendantsMembers() {
        checkNative(67, 67, "select {[Measures].[Store Sales]} on columns,"
                + " NON EMPTY Crossjoin("
                + "   Descendants([Customers].[All Customers].[USA].[CA], [Customers].[Name]),"
                + "     [Product].[Product Name].Members) ON rows " + " from [Sales] where ("
                + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
                + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjMembersDescendants() {
        checkNative(
                67,
                67,
                "select {[Measures].[Store Sales]} on columns,"
                        + " NON EMPTY Crossjoin("
                        + "  [Product].[Product Name].Members,"
                        + "  Descendants([Customers].[All Customers].[USA].[CA], [Customers].[Name])) ON rows "
                        + " from [Sales] where ("
                        + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
                        + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjChildrenMembers() {
        checkNative(67, 67, "select {[Measures].[Store Sales]} on columns,"
                + "  NON EMPTY Crossjoin([Customers].[All Customers].[USA].[CA].children,"
                + "    [Product].[Product Name].Members) ON rows " + " from [Sales] where ("
                + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
                + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjMembersChildren() {
        checkNative(67, 67, "select {[Measures].[Store Sales]} on columns,"
                + "  NON EMPTY Crossjoin([Product].[Product Name].Members,"
                + "    [Customers].[All Customers].[USA].[CA].children) ON rows "
                + " from [Sales] where ("
                + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
                + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjMembersMembers() {
        checkNative(67, 67, "select {[Measures].[Store Sales]} on columns,"
                + "  NON EMPTY Crossjoin([Customers].[Name].Members,"
                + "    [Product].[Product Name].Members) ON rows " + " from [Sales] where ("
                + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
                + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjChildrenChildren() {
        checkNative(
                3,
                3,
                "select {[Measures].[Store Sales]} on columns, "
                        + "  NON EMPTY Crossjoin( "
                        + "    [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].children, "
                        + "    [Customers].[All Customers].[USA].[CA].CHILDREN) ON rows"
                        + " from [Sales] where ("
                        + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
                        + "  [Time].[1997].[Q1].[1])");
    }

    public void testNonEmptyUnionQuery() {
        Result result = executeQuery(
                "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns,\n" +
                " NON EMPTY Hierarchize( \n" +
                "   Union(\n" +
                "     Crossjoin(\n" +
                "       Crossjoin([Gender].[All Gender].children,\n" +
                "                 [Marital Status].[All Marital Status].children ),\n" +
                "       Crossjoin([Customers].[All Customers].children,\n" +
                "                 [Product].[All Products].children ) ),\n" +
                "     Crossjoin( {([Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M] )},\n" +
                "       Crossjoin(\n" + "         [Customers].[All Customers].[USA].children,\n" +
                "         [Product].[All Products].children ) ) )) on rows\n" +
                "from Sales where ([Time].[1997])");
        final Axis rowsAxis = result.getAxes()[1];
        Assert.assertEquals(21, rowsAxis.positions.length);
    }

    /**
     * when Mondrian parses a string like "[Store].[All Stores].[USA].[CA].[San Francisco]"
     * it shall not lookup additional members.
     */
    public void testLookupMemberCache() {
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            // Dependency testing causes extra SQL reads, and screws up this
            // test.
            return;
        }
        SmartMemberReader smr = getSmartMemberReader("Store");
        smr.mapLevelToMembers.setCache(
            new HardSmartCache<
                SmartMemberListCache.Key2<RolapLevel, Object>,
                List<RolapMember>>());
        smr.mapMemberToChildren.setCache(
            new HardSmartCache<
                SmartMemberListCache.Key2<RolapMember, Object>,
                List<RolapMember>>());
        // smr.mapKeyToMember = new HardSmartCache();
        smr.mapKeyToMember.clear();
        RolapResult result = (RolapResult) executeQuery(
                "select {[Store].[All Stores].[USA].[CA].[San Francisco]} on columns from [Sales]");
        assertTrue("no additional members should be read:" + smr.mapKeyToMember.size(),
                smr.mapKeyToMember.size() <= 5);
        RolapMember sf = (RolapMember) result.getAxes()[0].positions[0].members[0];
        RolapMember ca = (RolapMember) sf.getParentMember();

        List list = smr.mapMemberToChildren.get(ca, scf.getMemberChildrenConstraint(null));
        assertNull("children of [CA] are not in cache", list);
        list = smr.mapMemberToChildren.get(ca, scf.getChildByNameConstraint(ca, "San Francisco"));
        assertNotNull("child [San Francisco] of [CA] is in cache", list);
        assertEquals("[San Francisco] expected", sf, list.get(0));
    }

    /**
     * When looking for [Month] Mondrian generates SQL that tries to find 'Month'
     * as a member of the time dimension. This resulted in an SQLException because
     * the year level is numeric and the constant  'Month' in the WHERE condition is not.
     * Its probably a bug that Mondrian does not take into account [Time].[1997]
     * when looking up [Month].
     */
    public void testLookupMember() {
        // ok if no exception occurs
        executeQuery("SELECT DESCENDANTS([Time].[1997], [Month]) ON COLUMNS FROM [Sales]");
    }


    /**
     * Non Empty CrossJoin (A,B) gets turned into CrossJoin (Non Empty(A), Non Empty(B))
     * Verify that there is no crash when the length of B could be non-zero length before the non emptyy
     * and 0 after the non empty.
     *
     */
    public void testNonEmptyCrossJoinList() {
    	boolean oldEnableNativeCJ = MondrianProperties.instance().EnableNativeCrossJoin.get();
    	MondrianProperties.instance().EnableNativeCrossJoin.set(false);
    	boolean oldEnableNativeNonEmpty = MondrianProperties.instance().EnableNativeNonEmpty.get();
    	MondrianProperties.instance().EnableNativeNonEmpty.set(false);

    	executeQuery("select non empty CrossJoin([Customers].[Name].Members, {[Promotions].[All Promotions].[Fantastic Discounts]}) ON COLUMNS FROM [Sales]");

    	MondrianProperties.instance().EnableNativeCrossJoin.set(oldEnableNativeCJ);
    	MondrianProperties.instance().EnableNativeNonEmpty.set(oldEnableNativeNonEmpty);
    }

    /**
     * SQL Optimization must be turned off in ragged hierarchies.
     */
    public void testLookupMember2() {
        // ok if no exception occurs
        executeQuery("select {[Store].[USA].[Washington]} on columns from [Sales Ragged]");
    }

    /**
     * Make sure that the Crossjoin in [Measures].[CustomerCount]
     * is not evaluated in NON EMPTY context.
     */
    public void testCalcMemberWithNonEmptyCrossJoin() {
        CachePool.instance().flush();
        Result result = executeQuery("with member [Measures].[CustomerCount] as \n"
                + "'Count(CrossJoin({[Product].[All Products]}, [Customers].[Name].Members))'\n"
                + "select \n"
                + "NON EMPTY{[Measures].[CustomerCount]} ON columns,\n"
                + "NON EMPTY{[Product].[All Products]} ON rows\n"
                + "from [Sales]\n"
                + "where ([Store].[All Stores].[USA].[CA].[San Francisco].[Store 14], [Time].[1997].[Q1].[1] )");
        Cell c = result.getCell(new int[] { 0, 0});
        // we expect 10281 customers, although there are only 20 non-empty ones
        // @see #testLevelMembers
        assertEquals("10,281", c.getFormattedValue());
    }

    /**
     * runs a MDX query with a predefined resultLimit and checks the number of positions
     * of the row axis. The reduces resultLimit ensures that the optimization is present.
     */
    class TestCase {
        int resultLimit;
        String query;
        int rowCount;
        Connection con;

        public TestCase(int resultLimit, int rowCount, String query) {
            this.con = getConnection();
            this.resultLimit = resultLimit;
            this.rowCount = rowCount;
            this.query = query;
        }

        public TestCase(Connection con, int resultLimit, int rowCount, String query) {
            this.con = con;
            this.resultLimit = resultLimit;
            this.rowCount = rowCount;
            this.query = query;
        }

        private Result run() {
            CachePool.instance().flush();
            IntegerProperty monLimit = MondrianProperties.instance().ResultLimit;
            int oldLimit = monLimit.get();
            try {
                monLimit.set(this.resultLimit);
                Result result = executeQuery(query, con);
                Axis a = result.getAxes()[1];
                assertEquals(rowCount, a.positions.length);
                return result;
            } finally {
                monLimit.set(oldLimit);
            }
        }
    }

    public void testLevelMembers() {
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            // Dependency testing causes extra SQL reads, and screws up this
            // test.
            return;
        }
        SmartMemberReader smr = getSmartMemberReader("Customers");
        smr.mapLevelToMembers.setCache(
            new HardSmartCache<
                SmartMemberListCache.Key2<RolapLevel, Object>,
                List<RolapMember>>());
        smr.mapMemberToChildren.setCache(
            new HardSmartCache<
                SmartMemberListCache.Key2<RolapMember, Object>,
                List<RolapMember>>());
        TestCase c = new TestCase(
                50,
                21,
                "select \n"
                        + "{[Measures].[Unit Sales]} ON columns,\n"
                        + "NON EMPTY {[Customers].[All Customers], [Customers].[Name].Members} ON rows\n"
                        + "from [Sales]\n"
                        + "where ([Store].[All Stores].[USA].[CA].[San Francisco].[Store 14], [Time].[1997].[Q1].[1] )");
        Result r = c.run();
        Level[] levels = smr.getHierarchy().getLevels();
        Level nameLevel = levels[levels.length - 1];

        // evaluator for [All Customers], [Store 14], [1/1/1997]
        Evaluator context = getEvaluator(r, new int[] { 0, 0});

        // make sure that [Customers].[Name].Members is NOT in cache
        TupleConstraint lmc = scf.getLevelMembersConstraint(null);
        assertNull(smr.mapLevelToMembers.get((RolapLevel) nameLevel, lmc));
        // make sure that NON EMPTY [Customers].[Name].Members IS in cache
        lmc = scf.getLevelMembersConstraint(context);
        List list = smr.mapLevelToMembers.get((RolapLevel) nameLevel, lmc);
        assertNotNull(list);
        assertEquals(20, list.size());

        // make sure that the parent/child for the context are cached

        // [Customers].[All Customers].[USA].[CA].[Burlingame].[Peggy Justice]
        Member member = r.getAxes()[1].positions[1].members[0];
        Member parent = member.getParentMember();

        // lookup all children of [Burlingame] -> not in cache
        MemberChildrenConstraint mcc = scf.getMemberChildrenConstraint(null);
        assertNull(smr.mapMemberToChildren.get((RolapMember) parent, mcc));

        // lookup NON EMPTY children of [Burlingame] -> yes these are in cache
        mcc = scf.getMemberChildrenConstraint(context);
        list = smr.mapMemberToChildren.get((RolapMember) parent, mcc);
        assertNotNull(list);
        assertTrue(list.contains(member));
    }

    public void testLevelMembersWithoutNonEmpty() {
        SmartMemberReader smr = getSmartMemberReader("Customers");
        smr.mapLevelToMembers.setCache(
            new HardSmartCache<
                SmartMemberListCache.Key2<RolapLevel, Object>,
                List<RolapMember>>());
        smr.mapMemberToChildren.setCache(
            new HardSmartCache<
                SmartMemberListCache.Key2<RolapMember, Object>,
                List<RolapMember>>());
        Result r = executeQuery("select \n"
                + "{[Measures].[Unit Sales]} ON columns,\n"
                + "{[Customers].[All Customers], [Customers].[Name].Members} ON rows\n"
                + "from [Sales]\n"
                + "where ([Store].[All Stores].[USA].[CA].[San Francisco].[Store 14], [Time].[1997].[Q1].[1] )");
        Level[] levels = smr.getHierarchy().getLevels();
        Level nameLevel = levels[levels.length - 1];

        // evaluator for [All Customers], [Store 14], [1/1/1997]
        Evaluator context = getEvaluator(r, new int[] { 0, 0});

        // make sure that [Customers].[Name].Members IS in cache
        TupleConstraint lmc = scf.getLevelMembersConstraint(null);
        List list = smr.mapLevelToMembers.get((RolapLevel) nameLevel, lmc);
        assertNotNull(list);
        assertEquals(10281, list.size());
        // make sure that NON EMPTY [Customers].[Name].Members is NOT in cache
        lmc = scf.getLevelMembersConstraint(context);
        assertNull(smr.mapLevelToMembers.get((RolapLevel) nameLevel, lmc));

        // make sure that the parent/child for the context are cached

        // [Customers].[All Customers].[Canada].[BC].[Burnaby]
        Member member = r.getAxes()[1].positions[1].members[0];
        Member parent = member.getParentMember();

        // lookup all children of [Burlingame] -> yes, found in cache
        MemberChildrenConstraint mcc = scf.getMemberChildrenConstraint(null);
        list = smr.mapMemberToChildren.get((RolapMember) parent, mcc);
        assertNotNull(list);
        assertTrue(list.contains(member));

        // lookup NON EMPTY children of [Burlingame] -> not in cache
        mcc = scf.getMemberChildrenConstraint(context);
        list = smr.mapMemberToChildren.get((RolapMember) parent, mcc);
        assertNull(list);
    }

    /**
     * Tests non empty children of rolap member
     */
    public void testMemberChildrenOfRolapMember() {
        TestCase c = new TestCase(
                50,
                4,
                "select \n"
                        + "{[Measures].[Unit Sales]} ON columns,\n"
                        + "NON EMPTY [Customers].[All Customers].[USA].[CA].[Palo Alto].Children ON rows\n"
                        + "from [Sales]\n"
                        + "where ([Store].[All Stores].[USA].[CA].[San Francisco].[Store 14], [Time].[1997].[Q1].[1] )");
        c.run();
    }

    /**
     * Tests non empty children of All member
     */
    public void testMemberChildrenOfAllMember() {
        TestCase c = new TestCase(50, 14, "select {[Measures].[Unit Sales]} ON columns,\n"
                + "NON EMPTY [Promotions].[All Promotions].Children ON rows from [Sales]\n"
                + "where ([Time].[1997].[Q1].[1] )");
        c.run();
    }

    /**
     * Tests non empty children of All member w/o WHERE clause
     */
    public void testMemberChildrenNoWhere() {

        // the time dimension is joined because there is no (All) level in the Time
        // hierarchy:
        //
        //      select
        //        `promotion`.`promotion_name` as `c0`
        //      from
        //        `time_by_day` as `time_by_day`,
        //        `sales_fact_1997` as `sales_fact_1997`,
        //        `promotion` as `promotion`
        //      where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`
        //        and `time_by_day`.`the_year` = 1997
        //        and `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id`
        //      group by
        //        `promotion`.`promotion_name`
        //      order by
        //        `promotion`.`promotion_name`

        TestCase c = new TestCase(50, 48, "select {[Measures].[Unit Sales]} ON columns,\n"
                + "NON EMPTY [Promotions].[All Promotions].Children ON rows from [Sales]\n");
        c.run();
    }

    public void testMemberChildrenNameCol() {
        // Bug 1379068 causes no children of [Time].[1997].[Q2] to be
        // found, because it incorrectly constrains on the level's key column
        // rather than name column.
        TestCase c = new TestCase(3, 1,
                "select " +
                " {[Measures].[Count]} ON columns," +
                " {[Time].[1997].[Q2].[April]} on rows " +
                "from [HR]");
        c.run();
    }

    /**
     * When a member is expanded in JPivot with mulitple hierarchies visible it
     * generates a
     *   <code>CrossJoin({[member from left hierarchy]}, [member to expand].Children)</code>
     * This should behave the same as if <code>[member from left hierarchy]</code> was
     * put into the slicer.
     */
    public void testCrossjoin() {
        TestCase c = new TestCase(
                45,
                4,
                "select \n"
                        + "{[Measures].[Unit Sales]} ON columns,\n"
                        + "NON EMPTY Crossjoin({[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]}, [Customers].[All Customers].[USA].[CA].[Palo Alto].Children) ON rows\n"
                        + "from [Sales] where ([Time].[1997].[Q1].[1] )");
        c.run();
    }

    /**
     * Ensures that NON EMPTY Descendants is optimized.
     * Ensures that Descendants as a side effect collects MemberChildren that
     * may be looked up in the cache.
     */
    public void testNonEmptyDescendants() {
        // Don't run the test if we're testing expression dependencies.
        // Expression dependencies cause spurious interval calls to
        // 'level.getMembers()' which create false negatives in this test.
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            return;
        }

        Connection con = getConnection(true);
        SmartMemberReader smr = getSmartMemberReader(con, "Customers");
        smr.mapLevelToMembers.setCache(
            new HardSmartCache<
                SmartMemberListCache.Key2<RolapLevel, Object>,
                List<RolapMember>>());
        smr.mapMemberToChildren.setCache(
            new HardSmartCache<
                SmartMemberListCache.Key2<RolapMember, Object>,
                List<RolapMember>>());
        TestCase c = new TestCase(
                con,
                45,
                21,
                "select \n"
                        + "{[Measures].[Unit Sales]} ON columns, "
                        + "NON EMPTY {[Customers].[All Customers], Descendants([Customers].[All Customers].[USA].[CA], [Customers].[Name])} on rows "
                        + "from [Sales] "
                        + "where ([Store].[All Stores].[USA].[CA].[San Francisco].[Store 14], [Time].[1997].[Q1].[1] )");
        Result result = c.run();
        // [Customers].[All Customers].[USA].[CA].[Burlingame].[Peggy Justice]
        RolapMember peggy = (RolapMember) result.getAxes()[1].positions[1].members[0];
        RolapMember burlingame = (RolapMember) peggy.getParentMember();
        // all children of burlingame are not in cache
        MemberChildrenConstraint mcc = scf.getMemberChildrenConstraint(null);
        assertNull(smr.mapMemberToChildren.get(burlingame, mcc));
        // but non empty children is
        Evaluator evaluator = getEvaluator(result, new int[] { 0, 0});
        mcc = scf.getMemberChildrenConstraint(evaluator);
        List list = smr.mapMemberToChildren.get(burlingame, mcc);
        assertNotNull(list);
        assertTrue(list.contains(peggy));

        // now we run the same query again, this time everything must come out of the cache
        RolapNativeRegistry reg = getRegistry(con);
        reg.setListener(new Listener() {
            public void foundEvaluator(NativeEvent e) {
            }

            public void foundInCache(TupleEvent e) {
            }

            public void excutingSql(TupleEvent e) {
                fail("expected caching");
            }
        });
        try {
            c.run();
        } finally {
            reg.setListener(null);
        }
    }

    public void testBug1412384() {
        // Bug 1412384 causes a NPE in SqlConstraintUtils.
        assertQueryReturns("select NON EMPTY {[Time].[1997]} ON COLUMNS,\n" +
                "NON EMPTY Hierarchize(Union({[Customers].[All Customers]},\n" +
                "[Customers].[All Customers].Children)) ON ROWS\n" +
                "from [Sales]\n" +
                "where [Measures].[Profit]",
                fold(
                    "Axis #0:\n" +
                    "{[Measures].[Profit]}\n" +
                    "Axis #1:\n" +
                    "{[Time].[1997]}\n" +
                    "Axis #2:\n" +
                    "{[Customers].[All Customers]}\n" +
                    "{[Customers].[All Customers].[USA]}\n" +
                    "Row #0: $339,610.90\n" +
                    "Row #1: $339,610.90\n"));
    }

    public void testVirtualCubeCrossJoin()
    {
        checkNative(18, 3,
            "select " +
            "{[Measures].[Units Ordered], [Measures].[Store Sales]} on columns, " +
            "non empty crossjoin([Product].[All Products].children, " +
            "[Store].[All Stores].children) on rows " +
            "from [Warehouse and Sales]");
    }
    
    public void testVirtualCubeNonEmptyCrossJoin()
    {
        checkNative(18, 3,
            "select " +
            "{[Measures].[Units Ordered], [Measures].[Store Sales]} on columns, " +
            "nonEmptyCrossJoin([Product].[All Products].children, " +
            "[Store].[All Stores].children) on rows " +
            "from [Warehouse and Sales]");
    }
    
    public void testVirtualCubeNonEmptyCrossJoin3Args()
    {
        checkNative(3, 3,
            "select " +
            "{[Measures].[Store Sales]} on columns, " +
            "nonEmptyCrossJoin([Product].[All Products].children, " +
            "nonEmptyCrossJoin([Customers].[All Customers].children," +
            "[Store].[All Stores].children)) on rows " +
            "from [Warehouse and Sales]");
    }

    public void testVirtualCubeCrossJoinNonConformingDim()
    {
        // cross join involves non-conforming dimensions should not use
        // native cross joins because it will result in a cartesian
        // product join
        checkNotNative(0,
            "select " +
            "{[Measures].[Units Ordered], [Measures].[Store Sales]} on columns, " +
            "non empty crossjoin([Customers].[All Customers].children, " +
            "[Warehouse].[All Warehouses].children) on rows " +
            "from [Warehouse and Sales]");
    }

    public void testNotNativeVirtualCubeCrossJoin1()
    {
        // native cross join cannot be used due to AllMembers
        checkNotNative(3,
            "select " +
            "{[Measures].AllMembers} on columns, " +
            "non empty crossjoin([Product].[All Products].children, " +
            "[Store].[All Stores].children) on rows " +
            "from [Warehouse and Sales]");
    }

    public void testNotNativeVirtualCubeCrossJoin2()
    {
        // native cross join cannot be used due to the range operator
        checkNotNative(3,
            "select " +
            "{[Measures].[Sales Count] : [Measures].[Unit Sales]} on columns, " +
            "non empty crossjoin([Product].[All Products].children, " +
            "[Store].[All Stores].children) on rows " +
            "from [Warehouse and Sales]");
    }

    public void testVirtualCubeCrossJoinCalculatedMember1()
    {
        // calculated member appears in query
        checkNative(18, 3,
            "WITH MEMBER [Measures].[Total Cost] as " +
            "'[Measures].[Store Cost] + [Measures].[Warehouse Cost]' " +
            "select " +
            "{[Measures].[Total Cost]} on columns, " +
            "non empty crossjoin([Product].[All Products].children, " +
            "[Store].[All Stores].children) on rows " +
            "from [Warehouse and Sales]");
    }

    public void testVirtualCubeCrossJoinCalculatedMember2()
    {
        // calculated member defined in schema
        checkNative(18, 3,
            "select " +
            "{[Measures].[Profit Per Unit Shipped]} on columns, " +
            "non empty crossjoin([Product].[All Products].children, " +
            "[Store].[All Stores].children) on rows " +
            "from [Warehouse and Sales]");
    }

    public void testNotNativeVirtualCubeCrossJoinCalculatedMember()
    {
        // native cross join cannot be used due to CurrentMember in the
        // calculated member
        checkNotNative(3,
            "WITH MEMBER [Measures].[CurrMember] as " +
            "'[Measures].CurrentMember' " +
            "select " +
            "{[Measures].[CurrMember]} on columns, " +
            "non empty crossjoin([Product].[All Products].children, " +
            "[Store].[All Stores].children) on rows " +
            "from [Warehouse and Sales]");
    }

    public void testCjEnumCalcMembers()
    {
        // 3 cross joins -- 2 of the 4 arguments to the cross joins are
        // enumerated sets with calculated members
        checkNative(
            30,
            30,
            "with " +
            "member [Product].[All Products].[Drink].[*SUBTOTAL_MEMBER_SEL~SUM] as " +
            "    'sum({[Product].[All Products].[Drink]})' " +
            "member [Product].[All Products].[Non-Consumable].[*SUBTOTAL_MEMBER_SEL~SUM] as " +
            "    'sum({[Product].[All Products].[Non-Consumable]})' " +
            "member [Customers].[All Customers].[USA].[CA].[*SUBTOTAL_MEMBER_SEL~SUM] as " +
            "    'sum({[Customers].[All Customers].[USA].[CA]})' " +
            "member [Customers].[All Customers].[USA].[OR].[*SUBTOTAL_MEMBER_SEL~SUM] as " +
            "    'sum({[Customers].[All Customers].[USA].[OR]})' " +
            "member [Customers].[All Customers].[USA].[WA].[*SUBTOTAL_MEMBER_SEL~SUM] as " +
            "    'sum({[Customers].[All Customers].[USA].[WA]})' " +
            "select " +
            "{[Measures].[Unit Sales]} on columns, " +
            "non empty " +
            "    crossjoin( " +
            "    crossjoin( " +
            "    crossjoin( " +
            "        {[Product].[All Products].[Drink].[*SUBTOTAL_MEMBER_SEL~SUM], " +
            "            [Product].[All Products].[Non-Consumable].[*SUBTOTAL_MEMBER_SEL~SUM]}, " +
            "        [Education Level].[Education Level].Members), " +
            "        {[Customers].[All Customers].[USA].[CA].[*SUBTOTAL_MEMBER_SEL~SUM], " +
            "            [Customers].[All Customers].[USA].[OR].[*SUBTOTAL_MEMBER_SEL~SUM], " +
            "            [Customers].[All Customers].[USA].[WA].[*SUBTOTAL_MEMBER_SEL~SUM]}), " +
            "        [Time].[Year].members)" +
            "    on rows " +
            "from [Sales]");
    }

    public void testCjEnumEmptyCalcMembers()
    {
        // enumerated list of calculated members results in some empty cells
        checkNative(
            15,
            5,
            "with " +
            "member [Customers].[All Customers].[USA].[*SUBTOTAL_MEMBER_SEL~SUM] as " +
            "    'sum({[Customers].[All Customers].[USA]})' " +
            "member [Customers].[All Customers].[Mexico].[*SUBTOTAL_MEMBER_SEL~SUM] as " +
            "    'sum({[Customers].[All Customers].[Mexico]})' " +
            "member [Customers].[All Customers].[Canada].[*SUBTOTAL_MEMBER_SEL~SUM] as " +
            "    'sum({[Customers].[All Customers].[Canada]})' " +
            "select " +
            "{[Measures].[Unit Sales]} on columns, " +
            "non empty " +
            "    crossjoin( " +
            "        {[Customers].[All Customers].[Mexico].[*SUBTOTAL_MEMBER_SEL~SUM], " +
            "            [Customers].[All Customers].[Canada].[*SUBTOTAL_MEMBER_SEL~SUM], " +
            "            [Customers].[All Customers].[USA].[*SUBTOTAL_MEMBER_SEL~SUM]}, " +
            "        [Education Level].[Education Level].Members) " +
            "    on rows " +
            "from [Sales]");
    }

    public void testCjUnionEnumCalcMembers()
    {
        // native sql should be used to retrieve Product Department members
        // and the second cross join should use the cached results from the
        // first, since the sql select excludes the calculated members
        checkNative(
            46,
            46,
            "with " +
            "member [Education Level].[*SUBTOTAL_MEMBER_SEL~SUM] as " +
            "    'sum({[Education Level].[All Education Levels]})' " +
            "member [Education Level].[*SUBTOTAL_MEMBER_SEL~AVG] as " +
             "   'avg([Education Level].[Education Level].Members)' select " +
            "{[Measures].[Unit Sales]} on columns, " +
            "non empty union (Crossjoin( " +
            "    [Product].[Product Department].Members, " +
            "    {[Education Level].[*SUBTOTAL_MEMBER_SEL~AVG]}), " +
            "crossjoin( " +
            "    [Product].[Product Department].Members, " +
            "    {[Education Level].[*SUBTOTAL_MEMBER_SEL~SUM]})) on rows " +
            "from [Sales]");
    }

    /**
     * Tests the behavior if you have NON EMPTY on both axes, and the default
     * member of a hierarchy is not 'all' or the first child.
     */
    public void testNonEmptyWithWeirdDefaultMember() {
        if (!Bug.Bug1574942Fixed) return;
        TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Time\" type=\"TimeDimension\" foreignKey=\"time_id\">\n" +
                "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\" defaultMember=\"[Time].[1997].[Q1].[1]\" >\n" +
                "      <Table name=\"time_by_day\"/>\n" +
                "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n" +
                "          levelType=\"TimeYears\"/>\n" +
                "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n" +
                "          levelType=\"TimeQuarters\"/>\n" +
                "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n" +
                "          levelType=\"TimeMonths\"/>\n" +
                "    </Hierarchy>\n" +
                "  </Dimension>");

        // Check that the grand total is different than when [Time].[1997] is
        // the default member.
        testContext.assertQueryReturns("select from [Sales]",
            fold("Axis #0:\n" +
                    "{}\n" +
                    "21,628"));

        // Results of this query agree with MSAS 2000 SP1.
        // The query gives the same results if the default member of [Time]
        // is [Time].[1997] or [Time].[1997].[Q1].[1].
        testContext.assertQueryReturns("select\n" +
            "NON EMPTY Crossjoin({[Time].[1997].[Q2].[4]}, [Customers].[Country].members) on columns,\n" +
            "NON EMPTY [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].children on rows\n" +
            "from sales",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Time].[1997].[Q2].[4], [Customers].[All Customers].[USA]}\n" +
                "Axis #2:\n" +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer]}\n" +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Light Beer]}\n" +
                "Row #0: 3\n" +
                "Row #1: 21\n"));
    }
    
    public void testCrossJoinNamedSets1()
    {
        checkNative(
            3,
            3,
            "with " +
            "SET [ProductChildren] as '[Product].[All Products].children' " +
            "SET [StoreMembers] as '[Store].[Store Country].members' " +
            "select {[Measures].[Store Sales]} on columns, " +
            "non empty crossjoin([ProductChildren], [StoreMembers]) " +
            "on rows from [Sales]");
    }
    
    public void testCrossJoinNamedSets2()
    {
        checkNative(
            3,
            3,
            "with " +
            "SET [ProductChildren] as '{[Product].[All Products].[Drink], " +
            "[Product].[All Products].[Food], " +
            "[Product].[All Products].[Non-Consumable]}' " +
            "SET [StoreChildren] as '[Store].[All Stores].children' " +
            "select {[Measures].[Store Sales]} on columns, " +
            "non empty crossjoin([ProductChildren], [StoreChildren]) on rows from " +
            "[Sales]");

    }
    
    public void testCrossJoinSetWithDifferentParents()
    {
        // Verify that only the members explicitly referenced in the set
        // are returned.  Note that different members are referenced in
        // each level in the time dimension.
        checkNative(
            5,
            5,
            "select " +
            "{[Measures].[Unit Sales]} on columns, " +
            "NonEmptyCrossJoin([Education Level].[Education Level].Members, " +
            "{[Time].[1997].[Q1], [Time].[1998].[Q2]}) on rows from Sales");
    }
    
    public void testCrossJoinSetWithCrossProdMembers()
    {
        // members in set are a cross product of (1997, 1998) and (Q1, Q2, Q3)
        checkNative(
            15,
            15,
            "select " +
            "{[Measures].[Unit Sales]} on columns, " +
            "NonEmptyCrossJoin([Education Level].[Education Level].Members, " +
            "{[Time].[1997].[Q1], [Time].[1997].[Q2], [Time].[1997].[Q3], " +
            "[Time].[1998].[Q1], [Time].[1998].[Q2], [Time].[1998].[Q3]})" +
            "on rows from Sales");
    }
    
    public void testCrossJoinSetWithSameParent()
    {
        // members in set have the same parent
        checkNative(
            10,
            10,
            "select " +
            "{[Measures].[Unit Sales]} on columns, " +
            "NonEmptyCrossJoin([Education Level].[Education Level].Members, " +
            "{[Store].[All Stores].[USA].[CA].[Beverly Hills], " +
            "[Store].[All Stores].[USA].[CA].[San Francisco]}) " +
            "on rows from Sales");
    }
    
    public void testCrossJoinSetWithUniqueLevel()
    {
        // members in set have different parents but there is a unique level
        checkNative(
            10,
            10,
            "select " +
            "{[Measures].[Unit Sales]} on columns, " +
            "NonEmptyCrossJoin([Education Level].[Education Level].Members, " +
            "{[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6], "+
            "[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]}) " +
            "on rows from Sales");
    }

    public void testCrossJoinEvaluatorContext()
    {
        // This test ensures that the proper measure members context is
        // set when evaluating a non-empty cross join.  The context should
        // not include the calculated measure [*TOP_BOTTOM_SET].  If it
        // does, the query will result in an infinite loop because the cross
        // join will try evaluating the calculated member (when it shouldn't)
        // and the calculated member references the cross join, resulting
        // in the loop
        assertQueryReturns(
            "With " +
            "Set [*NATIVE_CJ_SET] as " +
            "'NonEmptyCrossJoin([*BASE_MEMBERS_Store], [*BASE_MEMBERS_Products])' " +
            "Set [*TOP_BOTTOM_SET] as " +
            "'Order([*GENERATED_MEMBERS_Store], ([Measures].[Unit Sales], " +
            "[Product].[All Products].[*TOP_BOTTOM_MEMBER]), BDESC)' " +
            "Set [*BASE_MEMBERS_Store] as '[Store].members' " +
            "Set [*GENERATED_MEMBERS_Store] as 'Generate([*NATIVE_CJ_SET], {[Store].CurrentMember})' " +
            "Set [*BASE_MEMBERS_Products] as " +
            "'{[Product].[All Products].[Food], [Product].[All Products].[Drink], " +
            "[Product].[All Products].[Non-Consumable]}' " +
            "Set [*GENERATED_MEMBERS_Products] as " +
            "'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})' " +
            "Member [Product].[All Products].[*TOP_BOTTOM_MEMBER] as " +
            "'Aggregate([*GENERATED_MEMBERS_Products])'" +
            "Member [Measures].[*TOP_BOTTOM_MEMBER] as 'Rank([Store].CurrentMember,[*TOP_BOTTOM_SET])' " +
            "Member [Store].[All Stores].[*SUBTOTAL_MEMBER_SEL~SUM] as " +
            "'sum(Filter([*GENERATED_MEMBERS_Store], [Measures].[*TOP_BOTTOM_MEMBER] <= 10))'" +
            "Select {[Measures].[Store Cost]} on columns, " +
            "Non Empty Filter(Generate([*NATIVE_CJ_SET], {([Store].CurrentMember)}), " +
            "[Measures].[*TOP_BOTTOM_MEMBER] <= 10) on rows From [Sales]",
            
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Store Cost]}\n" +
                "Axis #2:\n" +
                "{[Store].[All Stores]}\n" +
                "{[Store].[All Stores].[USA]}\n" +
                "{[Store].[All Stores].[USA].[CA]}\n" +
                "{[Store].[All Stores].[USA].[OR]}\n" +
                "{[Store].[All Stores].[USA].[OR].[Portland]}\n" +
                "{[Store].[All Stores].[USA].[OR].[Salem]}\n" +
                "{[Store].[All Stores].[USA].[OR].[Salem].[Store 13]}\n" +
                "{[Store].[All Stores].[USA].[WA]}\n" +
                "{[Store].[All Stores].[USA].[WA].[Tacoma]}\n" +
                "{[Store].[All Stores].[USA].[WA].[Tacoma].[Store 17]}\n" +
                "Row #0: 225,627.23\n" +               
                "Row #1: 225,627.23\n" +
                "Row #2: 63,530.43\n" +
                "Row #3: 56,772.50\n" +
                "Row #4: 21,948.94\n" +
                "Row #5: 34,823.56\n" +
                "Row #6: 34,823.56\n" +
                "Row #7: 105,324.31\n" +
                "Row #8: 29,959.28\n" +
                "Row #9: 29,959.28\n"));
    }

    /**
     * make sure the following is not run natively
     */
    void checkNotNative(int rowCount, String mdx) {
        CachePool.instance().flush();
        Connection con = getConnection(true);
        RolapNativeRegistry reg = getRegistry(con);
        reg.setListener(new Listener() {
            public void foundEvaluator(NativeEvent e) {
                fail("should not be executed native");
            }

            public void foundInCache(TupleEvent e) {
            }

            public void excutingSql(TupleEvent e) {
            }
        });
        TestCase c = new TestCase(con, 0, rowCount, mdx);
        c.run();
    }

    RolapNativeRegistry getRegistry(Connection connection) {
        RolapCube cube = (RolapCube) connection.getSchema().lookupCube("Sales", true);
        RolapSchemaReader schemaReader = (RolapSchemaReader) cube.getSchemaReader();
        return schemaReader.getSchema().getNativeRegistry();
    }

    /**
     * runs a query twice, with native crossjoin optimization enabled and
     * disabled. If both results are equal, its considered correct.
     */
    private void checkNative(
        int resultLimit, int rowCount, String mdx) {
        // Don't run the test if we're testing expression dependencies.
        // Expression dependencies cause spurious interval calls to
        // 'level.getMembers()' which create false negatives in this test.
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            return;
        }

        CachePool.instance().flush();
        try {
            logger.debug("*** Native: " + mdx);
            Connection con = getConnection(true);
            RolapNativeRegistry reg = getRegistry(con);
            reg.useHardCache(true);
            TestListener listener = new TestListener();
            reg.setListener(listener);
            reg.setEnabled(true);
            TestCase c = new TestCase(con, resultLimit, rowCount, mdx);
            Result r1 = c.run();
            String s1 = toString(r1);
            if (!listener.isFoundEvaluator()) {
                fail("expected native execution of " + mdx);
            }
            if (!listener.isExcecuteSql()) {
                fail("cache is empty: expected SQL query to be executed");
            }
            // run once more to make sure that the result comes from cache now
            listener.setExcecuteSql(false);
            c.run();
            if (listener.isExcecuteSql()) {
                fail("expected result from cache when query runs twice");
            }
            con.close();

            logger.debug("*** Interpreter: " + mdx);
            CachePool.instance().flush();
            con = getConnection(true);
            reg = getRegistry(con);
            listener.setFoundEvaluator(false);
            reg.setListener(listener);
            // disable RolapNativeSet
            reg.setEnabled(false);
            Result r2 = executeQuery(mdx, con);
            String s2 = toString(r2);
            if (listener.isFoundEvaluator()) {
                fail("did not expect native executions of " + mdx);
            }

            if (!s1.equals(s2)) {
                StringBuilder buf = new StringBuilder();
                buf.append("Result differs");
                buf.append("\n\nMDX:\n").append(mdx);
                buf.append("\n\nNative Implementation returned:\n");
                buf.append(s1);
                buf.append("\n\nInterpreter returned:\n");
                buf.append(s2);
                fail(buf.toString());
            }

        } finally {
            Connection con = getConnection();
            RolapNativeRegistry reg = getRegistry(con);
            reg.setEnabled(true);
            reg.useHardCache(false);
        }
    }

    Result executeQuery(String mdx, Connection connection) {
        Query query = connection.parseQuery(mdx);
        return connection.execute(query);
    }

    private String toString(Result r) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        r.print(pw);
        pw.close();
        return sw.toString();
    }

    SmartMemberReader getSmartMemberReader(String hierName) {
        Connection con = super.getConnection(false);
        return getSmartMemberReader(con, hierName);
    }

    SmartMemberReader getSmartMemberReader(Connection con, String hierName) {
        RolapCube cube = (RolapCube) con.getSchema().lookupCube("Sales", true);
        RolapSchemaReader schemaReader = (RolapSchemaReader) cube.getSchemaReader();
        RolapHierarchy hierarchy = (RolapHierarchy) cube.lookupHierarchy(hierName, false);
        assertNotNull(hierarchy);
        return (SmartMemberReader) hierarchy.getMemberReader(schemaReader.getRole());
    }

    RolapEvaluator getEvaluator(Result res, int[] pos) {
        while (res instanceof NonEmptyResult)
            res = ((NonEmptyResult) res).underlying;
        return (RolapEvaluator) ((RolapResult) res).getEvaluator(pos);
    }

    /**
     * gets notified
     * <ul>
     *   <li>when a matching native evaluator was found
     *   <li>when SQL is executed
     *   <li>when result is found in the cache
     * </ul>
     * @author av
     * @since Nov 22, 2005
     */
    class TestListener implements Listener {
        boolean foundEvaluator;
        boolean foundInCache;
        boolean excecuteSql;

        boolean isExcecuteSql() {
            return excecuteSql;
        }

        void setExcecuteSql(boolean excecuteSql) {
            this.excecuteSql = excecuteSql;
        }

        boolean isFoundEvaluator() {
            return foundEvaluator;
        }

        void setFoundEvaluator(boolean foundEvaluator) {
            this.foundEvaluator = foundEvaluator;
        }

        boolean isFoundInCache() {
            return foundInCache;
        }

        void setFoundInCache(boolean foundInCache) {
            this.foundInCache = foundInCache;
        }

        public void foundEvaluator(NativeEvent e) {
            this.foundEvaluator = true;
        }

        public void foundInCache(TupleEvent e) {
            this.foundInCache = true;
        }

        public void excutingSql(TupleEvent e) {
            this.excecuteSql = true;
        }

    }
}

// End NonEmptyTest.java
