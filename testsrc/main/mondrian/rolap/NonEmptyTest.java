/*
 //This software is subject to the terms of the Common Public License
 //Agreement, available at the following URL:
 //http://www.opensource.org/licenses/cpl.html.
 //Copyright (C) 2004-2005 TONBELLER AG
 //All Rights Reserved.
 //You must accept the terms of that agreement to use this software.
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

import org.apache.log4j.Logger;
import org.eigenbase.util.property.IntegerProperty;

/**
 * tests for NON EMPTY Optimization, includes SqlConstraint type hierarchy and RolapNative 
 * classes
 * 
 * @author av
 * @since Nov 21, 2005
 */
public class NonEmptyTest extends FoodMartTestCase {
    private static Logger logger = Logger.getLogger(NonEmptyTest.class);
    SqlConstraintFactory scf = SqlConstraintFactory.instance();

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
                + "  NON EMPTY Crossjoin(" + "    Crossjoin("
                + "        [Customers].[Name].Members,"
                + "        [Product].[Product Name].Members), "
                + "    [Promotions].[Promotion Name].Members) ON rows " + " from [Sales] where ("
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

    /** Enumerated sets containing ALL will not be computed natively */
    public void testCjDescendantsEnumAllOnly() {
        checkNotNative(9, "select {[Measures].[Unit Sales]} ON COLUMNS, " + "NON EMPTY Crossjoin("
                + "  Descendants([Customers].[All Customers].[USA], [Customers].[City]), "
                + "  {[Product].[All Products]}) ON ROWS " + "from [Sales] "
                + "where ([Promotions].[All Promotions].[Bag Stuffers])");
    }

    /** 
     * Enumerated sets containing ALL will not be computed natively 
     */
    public void testCjDescendantsEnumAll() {
        checkNotNative(
                13,
                "select {[Measures].[Unit Sales]} ON COLUMNS, "
                        + "NON EMPTY Crossjoin("
                        + "  Descendants([Customers].[All Customers].[USA], [Customers].[City]), "
                        + "  {[Product].[All Products], [Product].[All Products].[Drink].[Dairy]}) ON ROWS "
                        + "from [Sales] " + "where ([Promotions].[All Promotions].[Bag Stuffers])");
    }

    public void testCjDescendantsEnum() {
        checkNative(
                11,
                11,
                "select {[Measures].[Unit Sales]} ON COLUMNS, "
                        + "NON EMPTY Crossjoin("
                        + "  Descendants([Customers].[All Customers].[USA], [Customers].[City]), "
                        + "  {[Product].[All Products].[Drink].[Beverages], [Product].[All Products].[Drink].[Dairy]}) ON ROWS "
                        + "from [Sales] " + "where ([Promotions].[All Promotions].[Bag Stuffers])");
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
        Result result = executeQuery(fold(new String[] {
                "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns,",
                " NON EMPTY Hierarchize( ",
                "   Union(",
                "     Crossjoin(",
                "       Crossjoin([Gender].[All Gender].children,",
                "                 [Marital Status].[All Marital Status].children ),",
                "       Crossjoin([Customers].[All Customers].children,",
                "                 [Product].[All Products].children ) ),",
                "     Crossjoin( {([Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M] )},",
                "       Crossjoin(", "         [Customers].[All Customers].[USA].children,",
                "         [Product].[All Products].children ) ) )) on rows",
                "from Sales where ([Time].[1997])"}));
        final Axis rowsAxis = result.getAxes()[1];
        Assert.assertEquals(21, rowsAxis.positions.length);
    }

    /**
     * when Mondrian parses a string like "[Store].[All Stores].[USA].[CA].[San Francisco]"
     * it shall not lookup additional members.
     */
    public void testLookupMemberCache() {
        SmartMemberReader smr = getSmartMemberReader("Store");
        smr.mapLevelToMembers.setCache(new HardSmartCache());
        smr.mapMemberToChildren.setCache(new HardSmartCache());
        // smr.mapKeyToMember = new HardSmartCache();
        smr.mapKeyToMember.clear();
        RolapResult result = (RolapResult) executeQuery("select {[Store].[All Stores].[USA].[CA].[San Francisco]} on columns from [Sales]");
        assertTrue("no additional members should be read", smr.mapKeyToMember.size() <= 5);
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
        assertEquals(new Double(10281), c.getValue());
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
                Result result = (Result) executeQuery(query, con);
                Axis a = result.getAxes()[1];
                assertEquals(rowCount, a.positions.length);
                return result;
            } finally {
                monLimit.set(oldLimit);
            }
        }
    }

    public void testLevelMembers() {
        SmartMemberReader smr = getSmartMemberReader("Customers");
        smr.mapLevelToMembers.setCache(new HardSmartCache());
        smr.mapMemberToChildren.setCache(new HardSmartCache());
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
        assertNull(smr.mapLevelToMembers.get(nameLevel, lmc));
        // make sure that NON EMPTY [Customers].[Name].Members IS in cache
        lmc = scf.getLevelMembersConstraint(context);
        List list = smr.mapLevelToMembers.get(nameLevel, lmc);
        assertNotNull(list);
        assertEquals(20, list.size());

        // make sure that the parent/child for the context are cached

        // [Customers].[All Customers].[USA].[CA].[Burlingame].[Peggy Justice]
        Member member = r.getAxes()[1].positions[1].members[0];
        Member parent = member.getParentMember();

        // lookup all children of [Burlingame] -> not in cache
        MemberChildrenConstraint mcc = scf.getMemberChildrenConstraint(null);
        assertNull(smr.mapMemberToChildren.get(parent, mcc));

        // lookup NON EMPTY children of [Burlingame] -> yes these are in cache
        mcc = scf.getMemberChildrenConstraint(context);
        list = smr.mapMemberToChildren.get(parent, mcc);
        assertNotNull(list);
        assertTrue(list.contains(member));
    }

    public void testLevelMembersWithoutNonEmpty() {
        SmartMemberReader smr = getSmartMemberReader("Customers");
        smr.mapLevelToMembers.setCache(new HardSmartCache());
        smr.mapMemberToChildren.setCache(new HardSmartCache());
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
        List list = smr.mapLevelToMembers.get(nameLevel, lmc);
        assertNotNull(list);
        assertEquals(10281, list.size());
        // make sure that NON EMPTY [Customers].[Name].Members is NOT in cache
        lmc = scf.getLevelMembersConstraint(context);
        assertNull(smr.mapLevelToMembers.get(nameLevel, lmc));

        // make sure that the parent/child for the context are cached

        // [Customers].[All Customers].[Canada].[BC].[Burnaby]
        Member member = r.getAxes()[1].positions[1].members[0];
        Member parent = member.getParentMember();

        // lookup all children of [Burlingame] -> yes, found in cache
        MemberChildrenConstraint mcc = scf.getMemberChildrenConstraint(null);
        list = smr.mapMemberToChildren.get(parent, mcc);
        assertNotNull(list);
        assertTrue(list.contains(member));

        // lookup NON EMPTY children of [Burlingame] -> not in cache
        mcc = scf.getMemberChildrenConstraint(context);
        list = smr.mapMemberToChildren.get(parent, mcc);
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
     * ensures that NON EMPTY Descendants is optimized.
     * ensures that Descendants as a side effect collects MemberChildren that 
     * may be looked up in the cache.
     */
    public void testNonEmptyDescendants() {
        Connection con = getConnection(true);
        SmartMemberReader smr = getSmartMemberReader(con, "Customers");
        smr.mapLevelToMembers.setCache(new HardSmartCache());
        smr.mapMemberToChildren.setCache(new HardSmartCache());
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
        Result r = c.run();
    }

    RolapNativeRegistry getRegistry(Connection connection) {
        RolapCube cube = (RolapCube) connection.getSchema().lookupCube("Sales", true);
        RolapSchemaReader schemaReader = (RolapSchemaReader) cube.getSchemaReader();
        return schemaReader.getSchema().getNativeRegistry();
    }

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

    /**
     * runs a query twice, with native crossjoin optimization enabled and
     * disabled. If both results are equal, its considered correct.
     */
    private Result checkNative(int resultLimit, int rowCount, String mdx) {
        CachePool.instance().flush();
        try {
            logger.info("****************************************************************");
            logger.info("checkNative: " + mdx);
            logger.info("*** Native:");
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

            logger.info("*** Interpreter:");
            CachePool.instance().flush();
            con = getConnection(true);
            reg = getRegistry(con);
            listener.setFoundEvaluator(false);
            reg.setListener(listener);
            reg.setEnabled(false);
            Result r2 = executeQuery(mdx, con);
            String s2 = toString(r2);
            if (listener.isFoundEvaluator()) {
                fail("did not expect native executions of " + mdx);
            }

            if (!s1.equals(s2)) {
                StringBuffer sb = new StringBuffer();
                sb.append("Result differs");
                sb.append("\n\nMDX:\n").append(mdx);
                sb.append("\n\nNative Implementation returned:\n");
                sb.append(s1);
                sb.append("\n\nInterpreter returned:\n");
                sb.append(s2);
                fail(sb.toString());
            }

            return r1;
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
}
