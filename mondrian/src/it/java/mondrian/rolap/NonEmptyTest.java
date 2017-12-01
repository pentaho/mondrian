/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2018 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import junit.framework.Assert;
import mondrian.olap.Axis;
import mondrian.olap.Cell;
import mondrian.olap.Connection;
import mondrian.olap.Evaluator;
import mondrian.olap.Id;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluationUnsupportedException;
import mondrian.olap.Result;
import mondrian.rolap.RolapConnection.NonEmptyResult;
import mondrian.rolap.RolapNative.Listener;
import mondrian.rolap.RolapNative.NativeEvent;
import mondrian.rolap.RolapNative.TupleEvent;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.spi.Dialect;
import mondrian.spi.Dialect.DatabaseProduct;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;
import mondrian.util.Bug;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.eigenbase.util.property.BooleanProperty;
import org.eigenbase.util.property.StringProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Tests for NON EMPTY Optimization, includes SqlConstraint type hierarchy and
 * RolapNative classes.
 *
 * @author av
 * @since Nov 21, 2005
 */
public class NonEmptyTest extends BatchTestCase {
    private static Logger logger = Logger.getLogger(NonEmptyTest.class);
    SqlConstraintFactory scf = SqlConstraintFactory.instance();
    TestContext localTestContext;

    private static final String STORE_TYPE_LEVEL =
        TestContext.levelName("Store Type", "Store Type", "Store Type");

    private static final String EDUCATION_LEVEL_LEVEL =
        TestContext.levelName(
            "Education Level", "Education Level", "Education Level");

    public NonEmptyTest() {
        super();
    }

    public NonEmptyTest(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, true);
        propSaver.set(
            MondrianProperties.instance().EnableNativeNonEmpty, true);
        propSaver.set(
            MondrianProperties.instance().LevelPreCacheThreshold, 0);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        localTestContext = null; // allow gc
    }

    @Override
    public TestContext getTestContext() {
        return localTestContext != null
            ? localTestContext : super.getTestContext();
    }

    public void setTestContext(TestContext testContext) {
        localTestContext = testContext;
    }

    public void testBugMondrian584EnumOrder() {
        // The interpreter results include males before females, which is
        // correct because it is consistent with the explicit order present
        // in the query. Native evaluation returns the females before males,
        // which is probably a reflection of the database ordering.
        //
        if (Bug.BugMondrian584Fixed) {
            checkNative(
                4,
                4,
                "SELECT non empty { CrossJoin( "
                + "  {Gender.M, Gender.F}, "
                + "  { [Marital Status].[Marital Status].members } "
                + ") } on 0 from sales");
        }
    }

    public void testBugCantRestrictSlicerToCalcMember() throws Exception {
        TestContext ctx = getTestContext();
        ctx.assertQueryReturns(
            "WITH Member [Time].[Time].[Aggr] AS 'Aggregate({[Time].[1998].[Q1], [Time].[1998].[Q2]})' "
            + "SELECT {[Measures].[Store Sales]} ON COLUMNS, "
            + "NON EMPTY Order(TopCount([Customers].[Name].Members,3,[Measures].[Store Sales]),[Measures].[Store Sales],BASC) ON ROWS "
            + "FROM [Sales] "
            + "WHERE ([Time].[Aggr])",

            "Axis #0:\n"
            + "{[Time].[Aggr]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n");
    }

    /**
     * Test case for an issue where mondrian failed to use native evaluation
     * for evaluating crossjoin. With the issue, performance is poor because
     * mondrian is doing crossjoins in memory; and the test case throws because
     * the result limit is exceeded.
     */
    public void testAnalyzerPerformanceIssue() {
        final MondrianProperties mondrianProperties =
            MondrianProperties.instance();
        propSaver.set(mondrianProperties.EnableNativeCrossJoin, true);
        propSaver.set(mondrianProperties.EnableNativeTopCount, false);
        propSaver.set(mondrianProperties.EnableNativeFilter, true);
        propSaver.set(mondrianProperties.EnableNativeNonEmpty, false);
        propSaver.set(mondrianProperties.ResultLimit, 5000000);

        assertQueryReturns(
            "with set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Education Level], NonEmptyCrossJoin([*BASE_MEMBERS_Product], NonEmptyCrossJoin([*BASE_MEMBERS_Customers], [*BASE_MEMBERS_Time])))' "
            + "set [*METRIC_CJ_SET] as 'Filter([*NATIVE_CJ_SET], ([Measures].[*TOP_Unit Sales_SEL~SUM] <= 2.0))' "
            + "set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS], [Product].CurrentMember.OrderKey, BASC, Ancestor([Product].CurrentMember, [Product].[Brand Name]).OrderKey, BASC, [Customers].CurrentMember.OrderKey, BASC, Ancestor([Customers].CurrentMember, [Customers].[City]).OrderKey, BASC)' "
            + "set [*SORTED_COL_AXIS] as 'Order([*CJ_COL_AXIS], [Education Level].CurrentMember.OrderKey, BASC)' "
            + "set [*BASE_MEMBERS_Time] as '{[Time].[1997].[Q1]}' "
            + "set [*NATIVE_MEMBERS_Customers] as 'Generate([*NATIVE_CJ_SET], {[Customers].CurrentMember})' "
            + "set [*TOP_SET] as 'Order(Generate([*NATIVE_CJ_SET], {[Product].CurrentMember}), ([Measures].[Unit Sales], [Customers].[*CTX_MEMBER_SEL~SUM], [Education Level].[*CTX_MEMBER_SEL~SUM], [Time].[*CTX_MEMBER_SEL~AGG]), BDESC)' "
            + "set [*BASE_MEMBERS_Education Level] as '[Education Level].[Education Level].Members' "
            + "set [*NATIVE_MEMBERS_Education Level] as 'Generate([*NATIVE_CJ_SET], {[Education Level].CurrentMember})' "
            + "set [*METRIC_MEMBERS_Time] as 'Generate([*METRIC_CJ_SET], {[Time].[Time].CurrentMember})' "
            + "set [*NATIVE_MEMBERS_Time] as 'Generate([*NATIVE_CJ_SET], {[Time].[Time].CurrentMember})' "
            + "set [*BASE_MEMBERS_Customers] as '[Customers].[Name].Members' "
            + "set [*BASE_MEMBERS_Product] as '[Product].[Product Name].Members' "
            + "set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}' "
            + "set [*CJ_COL_AXIS] as 'Generate([*METRIC_CJ_SET], {[Education Level].CurrentMember})' "
            + "set [*CJ_ROW_AXIS] as 'Generate([*METRIC_CJ_SET], {([Product].CurrentMember, [Customers].CurrentMember)})' "
            + "member [Customers].[*DEFAULT_MEMBER] as '[Customers].DefaultMember', SOLVE_ORDER = (- 500.0) "
            + "member [Product].[*TOTAL_MEMBER_SEL~SUM] as 'Sum(Generate([*METRIC_CJ_SET], {([Product].CurrentMember, [Customers].CurrentMember)}))', SOLVE_ORDER = (- 100.0) "
            + "member [Customers].[*TOTAL_MEMBER_SEL~SUM] as 'Sum(Generate(Exists([*METRIC_CJ_SET], {[Product].CurrentMember}), {([Product].CurrentMember, [Customers].CurrentMember)}))', SOLVE_ORDER = (- 101.0) "
            + "member [Measures].[*TOP_Unit Sales_SEL~SUM] as 'Rank([Product].CurrentMember, [*TOP_SET])', SOLVE_ORDER = 300.0 "
            + "member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Unit Sales]', FORMAT_STRING = \"Standard\", SOLVE_ORDER = 400.0 "
            + "member [Customers].[*CTX_MEMBER_SEL~SUM] as 'Sum({[Customers].[All Customers]})', SOLVE_ORDER = (- 101.0) "
            + "member [Education Level].[*TOTAL_MEMBER_SEL~SUM] as 'Sum(Generate([*METRIC_CJ_SET], {[Education Level].CurrentMember}))', SOLVE_ORDER = (- 102.0) "
            + "member [Education Level].[*CTX_MEMBER_SEL~SUM] as 'Sum({[Education Level].[All Education Levels]})', SOLVE_ORDER = (- 102.0) "
            + "member [Time].[Time].[*CTX_MEMBER_SEL~AGG] as 'Aggregate([*NATIVE_MEMBERS_Time])', SOLVE_ORDER = (- 402.0) "
            + "member [Time].[Time].[*SLICER_MEMBER] as 'Aggregate([*METRIC_MEMBERS_Time])', SOLVE_ORDER = (- 400.0) "
            + "select Union(Crossjoin({[Education Level].[*TOTAL_MEMBER_SEL~SUM]}, [*BASE_MEMBERS_Measures]), Crossjoin([*SORTED_COL_AXIS], [*BASE_MEMBERS_Measures])) ON COLUMNS, "
            + "NON EMPTY Union(Crossjoin({[Product].[*TOTAL_MEMBER_SEL~SUM]}, {[Customers].[*DEFAULT_MEMBER]}), Union(Crossjoin(Generate([*METRIC_CJ_SET], {[Product].CurrentMember}), {[Customers].[*TOTAL_MEMBER_SEL~SUM]}), [*SORTED_ROW_AXIS])) ON ROWS "
            + "from [Sales] "
            + "where [Time].[*SLICER_MEMBER] ",
            "Axis #0:\n"
            + "{[Time].[*SLICER_MEMBER]}\n"
            + "Axis #1:\n"
            + "{[Education Level].[*TOTAL_MEMBER_SEL~SUM], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Education Level].[Bachelors Degree], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Education Level].[Graduate Degree], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Education Level].[High School Degree], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Education Level].[Partial College], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Education Level].[Partial High School], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Product].[*TOTAL_MEMBER_SEL~SUM], [Customers].[*DEFAULT_MEMBER]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[WA].[Puyallup].[Cheryl Herring]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[OR].[Salem].[Robert Ahlering]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[WA].[Port Orchard].[Judy Zugelder]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[WA].[Marysville].[Brian Johnston]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[OR].[Corvallis].[Judy Doolittle]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[WA].[Spokane].[Greg Morgan]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[CA].[West Covina].[Sandra Young]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[CA].[Long Beach].[Dana Chappell]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[CA].[La Mesa].[Georgia Thompson]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[WA].[Tacoma].[Jessica Dugan]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[OR].[Milwaukie].[Adrian Torrez]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[WA].[Spokane].[Grace McLaughlin]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[WA].[Bremerton].[Julia Stewart]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[WA].[Port Orchard].[Maureen Overholser]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[WA].[Yakima].[Mary Craig]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[CA].[Spring Valley].[Deborah Adams]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[CA].[Woodland Hills].[Warren Kaufman]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[OR].[Woodburn].[David Moss]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[CA].[Newport Beach].[Michael Sample]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[OR].[Portland].[Ofelia Trembath]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[WA].[Bremerton].[Alexander Case]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[WA].[Bremerton].[Gloria Duncan]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[WA].[Olympia].[Jeanette Foster]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper], [Customers].[USA].[CA].[Lakewood].[Shyla Bettis]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[OR].[Portland].[Tomas Manzanares]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Bremerton].[Kerry Westgaard]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Yakima].[Beatrice Barney]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Seattle].[James La Monica]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Spokane].[Martha Griego]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Bremerton].[Michelle Neri]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Spokane].[Herman Webb]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Spokane].[Bob Alexander]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Issaquah].[Gery Scott]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Spokane].[Grace McLaughlin]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Kirkland].[Brandon Rohlke]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Port Orchard].[Elwood Carter]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[CA].[Beverly Hills].[Samuel Arden]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[OR].[Woodburn].[Ida Cezar]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Olympia].[Barbara Smith]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Spokane].[Matt Bellah]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Sedro Woolley].[William Akin]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[OR].[Albany].[Karie Taylor]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[OR].[Milwaukie].[Bertie Wherrett]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[CA].[Lincoln Acres].[L. Troy Barnes]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Tacoma].[Patricia Martin]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Bremerton].[Martha Clifton]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic], [Customers].[USA].[WA].[Bremerton].[Marla Bell]}\n"
            + "Row #0: 170\n"
            + "Row #0: 45\n"
            + "Row #0: 7\n"
            + "Row #0: 47\n"
            + "Row #0: 16\n"
            + "Row #0: 55\n"
            + "Row #1: 87\n"
            + "Row #1: 25\n"
            + "Row #1: 5\n"
            + "Row #1: 21\n"
            + "Row #1: 8\n"
            + "Row #1: 28\n"
            + "Row #2: 83\n"
            + "Row #2: 20\n"
            + "Row #2: 2\n"
            + "Row #2: 26\n"
            + "Row #2: 8\n"
            + "Row #2: 27\n"
            + "Row #3: 4\n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: 4\n"
            + "Row #3: \n"
            + "Row #4: 4\n"
            + "Row #4: \n"
            + "Row #4: \n"
            + "Row #4: \n"
            + "Row #4: 4\n"
            + "Row #4: \n"
            + "Row #5: 3\n"
            + "Row #5: 3\n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #6: 4\n"
            + "Row #6: 4\n"
            + "Row #6: \n"
            + "Row #6: \n"
            + "Row #6: \n"
            + "Row #6: \n"
            + "Row #7: 4\n"
            + "Row #7: \n"
            + "Row #7: \n"
            + "Row #7: \n"
            + "Row #7: \n"
            + "Row #7: 4\n"
            + "Row #8: 4\n"
            + "Row #8: 4\n"
            + "Row #8: \n"
            + "Row #8: \n"
            + "Row #8: \n"
            + "Row #8: \n"
            + "Row #9: 3\n"
            + "Row #9: \n"
            + "Row #9: \n"
            + "Row #9: \n"
            + "Row #9: \n"
            + "Row #9: 3\n"
            + "Row #10: 2\n"
            + "Row #10: 2\n"
            + "Row #10: \n"
            + "Row #10: \n"
            + "Row #10: \n"
            + "Row #10: \n"
            + "Row #11: 3\n"
            + "Row #11: \n"
            + "Row #11: \n"
            + "Row #11: \n"
            + "Row #11: \n"
            + "Row #11: 3\n"
            + "Row #12: 3\n"
            + "Row #12: \n"
            + "Row #12: \n"
            + "Row #12: 3\n"
            + "Row #12: \n"
            + "Row #12: \n"
            + "Row #13: 4\n"
            + "Row #13: 4\n"
            + "Row #13: \n"
            + "Row #13: \n"
            + "Row #13: \n"
            + "Row #13: \n"
            + "Row #14: 4\n"
            + "Row #14: \n"
            + "Row #14: \n"
            + "Row #14: 4\n"
            + "Row #14: \n"
            + "Row #14: \n"
            + "Row #15: 3\n"
            + "Row #15: \n"
            + "Row #15: \n"
            + "Row #15: \n"
            + "Row #15: \n"
            + "Row #15: 3\n"
            + "Row #16: 4\n"
            + "Row #16: \n"
            + "Row #16: \n"
            + "Row #16: 4\n"
            + "Row #16: \n"
            + "Row #16: \n"
            + "Row #17: 5\n"
            + "Row #17: \n"
            + "Row #17: 5\n"
            + "Row #17: \n"
            + "Row #17: \n"
            + "Row #17: \n"
            + "Row #18: 4\n"
            + "Row #18: \n"
            + "Row #18: \n"
            + "Row #18: \n"
            + "Row #18: \n"
            + "Row #18: 4\n"
            + "Row #19: 3\n"
            + "Row #19: \n"
            + "Row #19: \n"
            + "Row #19: 3\n"
            + "Row #19: \n"
            + "Row #19: \n"
            + "Row #20: 3\n"
            + "Row #20: \n"
            + "Row #20: \n"
            + "Row #20: 3\n"
            + "Row #20: \n"
            + "Row #20: \n"
            + "Row #21: 4\n"
            + "Row #21: \n"
            + "Row #21: \n"
            + "Row #21: 4\n"
            + "Row #21: \n"
            + "Row #21: \n"
            + "Row #22: 4\n"
            + "Row #22: 4\n"
            + "Row #22: \n"
            + "Row #22: \n"
            + "Row #22: \n"
            + "Row #22: \n"
            + "Row #23: 4\n"
            + "Row #23: \n"
            + "Row #23: \n"
            + "Row #23: \n"
            + "Row #23: \n"
            + "Row #23: 4\n"
            + "Row #24: 4\n"
            + "Row #24: \n"
            + "Row #24: \n"
            + "Row #24: \n"
            + "Row #24: \n"
            + "Row #24: 4\n"
            + "Row #25: 3\n"
            + "Row #25: \n"
            + "Row #25: \n"
            + "Row #25: \n"
            + "Row #25: \n"
            + "Row #25: 3\n"
            + "Row #26: 4\n"
            + "Row #26: 4\n"
            + "Row #26: \n"
            + "Row #26: \n"
            + "Row #26: \n"
            + "Row #26: \n"
            + "Row #27: 4\n"
            + "Row #27: 4\n"
            + "Row #27: \n"
            + "Row #27: \n"
            + "Row #27: \n"
            + "Row #27: \n"
            + "Row #28: 4\n"
            + "Row #28: 4\n"
            + "Row #28: \n"
            + "Row #28: \n"
            + "Row #28: \n"
            + "Row #28: \n"
            + "Row #29: 3\n"
            + "Row #29: \n"
            + "Row #29: \n"
            + "Row #29: 3\n"
            + "Row #29: \n"
            + "Row #29: \n"
            + "Row #30: 2\n"
            + "Row #30: \n"
            + "Row #30: \n"
            + "Row #30: \n"
            + "Row #30: \n"
            + "Row #30: 2\n"
            + "Row #31: 4\n"
            + "Row #31: \n"
            + "Row #31: \n"
            + "Row #31: \n"
            + "Row #31: 4\n"
            + "Row #31: \n"
            + "Row #32: 5\n"
            + "Row #32: \n"
            + "Row #32: \n"
            + "Row #32: 5\n"
            + "Row #32: \n"
            + "Row #32: \n"
            + "Row #33: 3\n"
            + "Row #33: \n"
            + "Row #33: \n"
            + "Row #33: \n"
            + "Row #33: \n"
            + "Row #33: 3\n"
            + "Row #34: 4\n"
            + "Row #34: 4\n"
            + "Row #34: \n"
            + "Row #34: \n"
            + "Row #34: \n"
            + "Row #34: \n"
            + "Row #35: 3\n"
            + "Row #35: 3\n"
            + "Row #35: \n"
            + "Row #35: \n"
            + "Row #35: \n"
            + "Row #35: \n"
            + "Row #36: 4\n"
            + "Row #36: \n"
            + "Row #36: \n"
            + "Row #36: 4\n"
            + "Row #36: \n"
            + "Row #36: \n"
            + "Row #37: 4\n"
            + "Row #37: \n"
            + "Row #37: \n"
            + "Row #37: 4\n"
            + "Row #37: \n"
            + "Row #37: \n"
            + "Row #38: 3\n"
            + "Row #38: \n"
            + "Row #38: \n"
            + "Row #38: 3\n"
            + "Row #38: \n"
            + "Row #38: \n"
            + "Row #39: 3\n"
            + "Row #39: 3\n"
            + "Row #39: \n"
            + "Row #39: \n"
            + "Row #39: \n"
            + "Row #39: \n"
            + "Row #40: 2\n"
            + "Row #40: \n"
            + "Row #40: 2\n"
            + "Row #40: \n"
            + "Row #40: \n"
            + "Row #40: \n"
            + "Row #41: 4\n"
            + "Row #41: \n"
            + "Row #41: \n"
            + "Row #41: \n"
            + "Row #41: 4\n"
            + "Row #41: \n"
            + "Row #42: 4\n"
            + "Row #42: \n"
            + "Row #42: \n"
            + "Row #42: \n"
            + "Row #42: \n"
            + "Row #42: 4\n"
            + "Row #43: 2\n"
            + "Row #43: 2\n"
            + "Row #43: \n"
            + "Row #43: \n"
            + "Row #43: \n"
            + "Row #43: \n"
            + "Row #44: 3\n"
            + "Row #44: \n"
            + "Row #44: \n"
            + "Row #44: 3\n"
            + "Row #44: \n"
            + "Row #44: \n"
            + "Row #45: 4\n"
            + "Row #45: \n"
            + "Row #45: \n"
            + "Row #45: 4\n"
            + "Row #45: \n"
            + "Row #45: \n"
            + "Row #46: 4\n"
            + "Row #46: \n"
            + "Row #46: \n"
            + "Row #46: \n"
            + "Row #46: \n"
            + "Row #46: 4\n"
            + "Row #47: 3\n"
            + "Row #47: \n"
            + "Row #47: \n"
            + "Row #47: \n"
            + "Row #47: \n"
            + "Row #47: 3\n"
            + "Row #48: 4\n"
            + "Row #48: \n"
            + "Row #48: \n"
            + "Row #48: \n"
            + "Row #48: \n"
            + "Row #48: 4\n"
            + "Row #49: 7\n"
            + "Row #49: \n"
            + "Row #49: \n"
            + "Row #49: \n"
            + "Row #49: \n"
            + "Row #49: 7\n");
    }

    public void testBug1961163() throws Exception {
        assertQueryReturns(
            "with member [Measures].[AvgRevenue] as 'Avg([Store].[Store Name].Members, [Measures].[Store Sales])' "
            + "select NON EMPTY {[Measures].[Store Sales], [Measures].[AvgRevenue]} ON COLUMNS, "
            + "NON EMPTY Filter([Store].[Store Name].Members, ([Measures].[AvgRevenue] < [Measures].[Store Sales])) ON ROWS "
            + "from [Sales]",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[AvgRevenue]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "Row #0: 45,750.24\n"
            + "Row #0: 43,479.86\n"
            + "Row #1: 54,545.28\n"
            + "Row #1: 43,479.86\n"
            + "Row #2: 54,431.14\n"
            + "Row #2: 43,479.86\n"
            + "Row #3: 55,058.79\n"
            + "Row #3: 43,479.86\n"
            + "Row #4: 87,218.28\n"
            + "Row #4: 43,479.86\n"
            + "Row #5: 52,896.30\n"
            + "Row #5: 43,479.86\n"
            + "Row #6: 52,644.07\n"
            + "Row #6: 43,479.86\n"
            + "Row #7: 49,634.46\n"
            + "Row #7: 43,479.86\n"
            + "Row #8: 74,843.96\n"
            + "Row #8: 43,479.86\n");
    }

    public void testTopCountWithCalcMemberInSlicer() {
        // Internal error: can not restrict SQL to calculated Members
        TestContext ctx = getTestContext();
        ctx.assertQueryReturns(
            "with member [Time].[Time].[First Term] as 'Aggregate({[Time].[1997].[Q1], [Time].[1997].[Q2]})' "
            + "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "TopCount([Product].[Product Subcategory].Members, 3, [Measures].[Unit Sales]) ON ROWS "
            + "from [Sales] "
            + "where ([Time].[First Term]) ",
            "Axis #0:\n"
            + "{[Time].[First Term]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables]}\n"
            + "{[Product].[Food].[Produce].[Fruit].[Fresh Fruit]}\n"
            + "{[Product].[Food].[Canned Foods].[Canned Soup].[Soup]}\n"
            + "Row #0: 10,215\n"
            + "Row #1: 5,711\n"
            + "Row #2: 3,926\n");
    }

    public void testTopCountCacheKeyMustIncludeCount() {
        /**
         * When caching topcount results, the number of elements must
         * be part of the cache key
         */
        TestContext ctx = getTestContext();
        // fill cache
        ctx.assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "TopCount([Product].[Product Subcategory].Members, 2, [Measures].[Unit Sales]) ON ROWS "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables]}\n"
            + "{[Product].[Food].[Produce].[Fruit].[Fresh Fruit]}\n"
            + "Row #0: 20,739\n"
            + "Row #1: 11,767\n");
        // run again with different count
        ctx.assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "TopCount([Product].[Product Subcategory].Members, 3, [Measures].[Unit Sales]) ON ROWS "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables]}\n"
            + "{[Product].[Food].[Produce].[Fruit].[Fresh Fruit]}\n"
            + "{[Product].[Food].[Canned Foods].[Canned Soup].[Soup]}\n"
            + "Row #0: 20,739\n"
            + "Row #1: 11,767\n"
            + "Row #2: 8,006\n");
    }

    public void testStrMeasure() {
        TestContext ctx = TestContext.instance().create(
            null,
            "<Cube name=\"StrMeasure\"> \n"
            + "  <Table name=\"promotion\"/> \n"
            + "  <Dimension name=\"Promotions\"> \n"
            + "    <Hierarchy hasAll=\"true\" > \n"
            + "      <Level name=\"Promotion Name\" column=\"promotion_name\" uniqueMembers=\"true\"/> \n"
            + "    </Hierarchy> \n"
            + "  </Dimension> \n"
            + "  <Measure name=\"Media\" column=\"media_type\" aggregator=\"max\" datatype=\"String\"/> \n"
            + "</Cube> \n",
            null,
            null,
            null,
            null);

        ctx.assertQueryReturns(
            "select {[Measures].[Media]} on columns " + "from [StrMeasure]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Media]}\n"
            + "Row #0: TV\n");
    }

    public void testBug1515302() {
        TestContext ctx = TestContext.instance().create(
            null,
            "<Cube name=\"Bug1515302\"> \n"
            + "  <Table name=\"sales_fact_1997\"/> \n"
            + "  <Dimension name=\"Promotions\" foreignKey=\"promotion_id\"> \n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"promotion_id\"> \n"
            + "      <Table name=\"promotion\"/> \n"
            + "      <Level name=\"Promotion Name\" column=\"promotion_name\" uniqueMembers=\"true\"/> \n"
            + "    </Hierarchy> \n"
            + "  </Dimension> \n"
            + "  <Dimension name=\"Customers\" foreignKey=\"customer_id\"> \n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKey=\"customer_id\"> \n"
            + "      <Table name=\"customer\"/> \n"
            + "      <Level name=\"Country\" column=\"country\" uniqueMembers=\"true\"/> \n"
            + "      <Level name=\"State Province\" column=\"state_province\" uniqueMembers=\"true\"/> \n"
            + "      <Level name=\"City\" column=\"city\" uniqueMembers=\"false\"/> \n"
            + "      <Level name=\"Name\" column=\"customer_id\" type=\"Numeric\" uniqueMembers=\"true\"/> \n"
            + "    </Hierarchy> \n"
            + "  </Dimension> \n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"/> \n"
            + "</Cube> \n",
            null,
            null,
            null,
            null);

        ctx.assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns, "
            + "non empty crossjoin({[Promotions].[Big Promo]}, "
            + "Descendants([Customers].[USA], [City], "
            + "SELF_AND_BEFORE)) on rows "
            + "from [Bug1515302]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Promotions].[Big Promo], [Customers].[USA]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Anacortes]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Ballard]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Bellingham]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Burien]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Everett]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Issaquah]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Kirkland]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Lynnwood]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Marysville]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Olympia]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Puyallup]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Redmond]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Renton]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Seattle]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Sedro Woolley]}\n"
            + "{[Promotions].[Big Promo], [Customers].[USA].[WA].[Tacoma]}\n"
            + "Row #0: 1,789\n"
            + "Row #1: 1,789\n"
            + "Row #2: 20\n"
            + "Row #3: 35\n"
            + "Row #4: 15\n"
            + "Row #5: 18\n"
            + "Row #6: 60\n"
            + "Row #7: 42\n"
            + "Row #8: 36\n"
            + "Row #9: 79\n"
            + "Row #10: 58\n"
            + "Row #11: 520\n"
            + "Row #12: 438\n"
            + "Row #13: 14\n"
            + "Row #14: 20\n"
            + "Row #15: 65\n"
            + "Row #16: 3\n"
            + "Row #17: 366\n");
    }

    /**
     * Must not use native sql optimization because it chooses the wrong
     * RolapStar in SqlContextConstraint/SqlConstraintUtils.  Test ensures that
     * no exception is thrown.
     */
    public void testVirtualCube() {
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            return;
        }
        TestCase c = new TestCase(
            99,
            3,
            "select NON EMPTY {[Measures].[Unit Sales], [Measures].[Warehouse Sales]} ON COLUMNS, "
            + "NON EMPTY [Product].[All Products].Children ON ROWS "
            + "from [Warehouse and Sales]");
        c.run();
    }

    public void testVirtualCubeMembers() throws Exception {
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            return;
        }
        // ok to use native sql optimization for members on a virtual cube
        TestCase c = new TestCase(
            6,
            3,
            "select NON EMPTY {[Measures].[Unit Sales], [Measures].[Warehouse Sales]} ON COLUMNS, "
            + "NON EMPTY {[Product].[Product Family].Members} ON ROWS "
            + "from [Warehouse and Sales]");
        c.run();
    }

    /**
     *  verifies that redundant set braces do not prevent native evaluation
     *  for example, {[Store].[Store Name].members} and
     *  {{[Store Type].[Store Type].members}}
     */
    public void testNativeCJWithRedundantSetBraces() {
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;
        checkNative(
            0,
            20,
            "select non empty {CrossJoin({[Store].[Store Name].members}, "
            + "                        {{" + STORE_TYPE_LEVEL + ".members}})}"
            + "                         on rows, "
            + "{[Measures].[Store Sqft]} on columns "
            + "from [Store]",
            null,
            requestFreshConnection);
    }

    /**
     * Verifies that CrossJoins with two non native inputs can be natively
     * evaluated.
     */
    public void testExpandAllNonNativeInputs() {
        // This query will not run natively unless the <Dimension>.Children
        // expression is expanded to a member list.
        //
        // Note: Both dimensions only have one hierarchy, which has the All
        // member. <Dimension>.Children is interpreted as the children of
        // the All member.
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;
        checkNative(
            0,
            2,
            "select "
            + "NonEmptyCrossJoin([Gender].Children, [Store].Children) on columns "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Store].[USA]}\n"
            + "{[Gender].[M], [Store].[USA]}\n"
            + "Row #0: 131,558\n"
            + "Row #0: 135,215\n",
            requestFreshConnection);
    }

    /**
     * Verifies that CrossJoins with one non native inputs can be natively
     * evaluated.
     */
    public void testExpandOneNonNativeInput() {
        // This query will not be evaluated natively unless the Filter
        // expression is expanded to a member list.
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;
        checkNative(
            0, 1,
            "With "
            + "Set [*Filtered_Set] as Filter([Product].[Product Name].Members, [Product].CurrentMember IS [Product].[Product Name].[Fast Raisins]) "
            + "Set [*NECJ_Set] as NonEmptyCrossJoin([Store].[Store Country].Members, [*Filtered_Set]) "
            + "select [*NECJ_Set] on columns "
            + "From [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA], [Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Raisins]}\n"
            + "Row #0: 152\n",
            requestFreshConnection);
    }

    /**
     * Check that the ExpandNonNative does not create Joins with input lists
     * containing large number of members.
     */
    public void testExpandNonNativeResourceLimitFailure() {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, true);
        propSaver.set(MondrianProperties.instance().ResultLimit, 2);

        try {
            executeQuery(
                "select "
                + "NonEmptyCrossJoin({[Gender].Children, [Gender].[F]}, {[Store].Children, [Store].[Mexico]}) on columns "
                + "from [Sales]");
            fail("Expected error did not occur");
        } catch (Throwable e) {
            String expectedErrorMsg =
                "Mondrian Error:Size of CrossJoin result (3) exceeded limit (2)";
            assertEquals(expectedErrorMsg, e.getMessage());
        }
    }

    /**
     * Verify that the presence of All member in all the inputs disables native
     * evaluation, even when ExpandNonNative is true.
     */
    public void testExpandAllMembersInAllInputs() {
        // This query will not be evaluated natively, even if the Hierarchize
        // expression is expanded to a member list. The reason is that the
        // expanded list contains ALL members.
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, true);
        checkNotNative(
            1, "select NON EMPTY {[Time].[1997]} ON COLUMNS,\n"
               + "       NON EMPTY Crossjoin(Hierarchize(Union({[Store].[All Stores]},\n"
               + "           [Store].[USA].[CA].[San Francisco].[Store 14].Children)), {[Product].[All Products]}) \n"
               + "           ON ROWS\n"
               + "    from [Sales]\n"
               + "    where [Measures].[Unit Sales]",
            "Axis #0:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #1:\n"
            + "{[Time].[1997]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores], [Product].[All Products]}\n"
            + "Row #0: 266,773\n");
    }

    /**
     * Verifies that the presence of calculated member in all the inputs
     * disables native evaluation, even when ExpandNonNative is true.
     */
    public void testExpandCalcMembersInAllInputs() {
        // This query will not be evaluated natively, even if the Hierarchize
        // expression is expanded to a member list. The reason is that the
        // expanded list contains ALL members.
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, true);
        checkNotNative(
            1,
            "With "
            + "Member [Product].[*CTX_MEMBER_SEL~SUM] as 'Sum({[Product].[Product Family].Members})' "
            + "Member [Gender].[*CTX_MEMBER_SEL~SUM] as 'Sum({[Gender].[All Gender]})' "
            + "Select "
            + "NonEmptyCrossJoin({[Gender].[*CTX_MEMBER_SEL~SUM]},{[Product].[*CTX_MEMBER_SEL~SUM]}) "
            + "on columns "
            + "From [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[*CTX_MEMBER_SEL~SUM], [Product].[*CTX_MEMBER_SEL~SUM]}\n"
            + "Row #0: 266,773\n");
    }

    /**
     * Check that if both inputs to NECJ are either
     * AllMember(currentMember, defaultMember are also AllMember)
     * or Calcculated member
     * native CJ is not used.
     */
    public void testExpandCalcMemberInputNECJ() {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        checkNotNative(
            1,
            "With \n"
            + "Member [Product].[All Products].[Food].[CalcSum] as \n"
            + "'Sum({[Product].[All Products].[Food]})', SOLVE_ORDER=-100\n"
            + "Select\n"
            + "{[Measures].[Store Cost]} on columns,\n"
            + "NonEmptyCrossJoin({[Product].[All Products].[Food].[CalcSum]},\n"
            + "                  {[Education Level].DefaultMember}) on rows\n"
            + "From [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Cost]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[CalcSum], [Education Level].[All Education Levels]}\n"
            + "Row #0: 163,270.72\n");
    }

    /**
     * Native evaluation is no longer possible after the fix to
     * {@link #testCjEnumCalcMembersBug()} test.
     */
    public void testExpandCalcMembers() {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        checkNotNative(
            9,
            "with "
            + "member [Store Type].[All Store Types].[S] as sum({[Store Type].[All Store Types]}) "
            + "set [Enum Store Types] as {"
            + "    [Store Type].[All Store Types].[Small Grocery], "
            + "    [Store Type].[All Store Types].[Supermarket], "
            + "    [Store Type].[All Store Types].[HeadQuarters], "
            + "    [Store Type].[All Store Types].[S]} "
            + "set [Filtered Enum Store Types] as Filter([Enum Store Types], [Measures].[Unit Sales] > 0)"
            + "select NonEmptyCrossJoin([Product].[All Products].Children, [Filtered Enum Store Types])  on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Drink], [Store Type].[Small Grocery]}\n"
            + "{[Product].[Drink], [Store Type].[Supermarket]}\n"
            + "{[Product].[Drink], [Store Type].[All Store Types].[S]}\n"
            + "{[Product].[Food], [Store Type].[Small Grocery]}\n"
            + "{[Product].[Food], [Store Type].[Supermarket]}\n"
            + "{[Product].[Food], [Store Type].[All Store Types].[S]}\n"
            + "{[Product].[Non-Consumable], [Store Type].[Small Grocery]}\n"
            + "{[Product].[Non-Consumable], [Store Type].[Supermarket]}\n"
            + "{[Product].[Non-Consumable], [Store Type].[All Store Types].[S]}\n"
            + "Row #0: 574\n"
            + "Row #0: 14,092\n"
            + "Row #0: 24,597\n"
            + "Row #0: 4,764\n"
            + "Row #0: 108,188\n"
            + "Row #0: 191,940\n"
            + "Row #0: 1,219\n"
            + "Row #0: 28,275\n"
            + "Row #0: 50,236\n");
    }

    /**
     * Verify that evaluation is native for expressions with nested non native
     * inputs that preduce MemberList results.
     */
    public void testExpandNestedNonNativeInputs() {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;
        checkNative(
            0,
            6,
            "select "
            + "NonEmptyCrossJoin("
            + "  NonEmptyCrossJoin([Gender].Children, [Store].Children), "
            + "  [Product].Children) on columns "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Store].[USA], [Product].[Drink]}\n"
            + "{[Gender].[F], [Store].[USA], [Product].[Food]}\n"
            + "{[Gender].[F], [Store].[USA], [Product].[Non-Consumable]}\n"
            + "{[Gender].[M], [Store].[USA], [Product].[Drink]}\n"
            + "{[Gender].[M], [Store].[USA], [Product].[Food]}\n"
            + "{[Gender].[M], [Store].[USA], [Product].[Non-Consumable]}\n"
            + "Row #0: 12,202\n"
            + "Row #0: 94,814\n"
            + "Row #0: 24,542\n"
            + "Row #0: 12,395\n"
            + "Row #0: 97,126\n"
            + "Row #0: 25,694\n",
            requestFreshConnection);
    }

    /**
     * Verify that a low value for maxConstraints disables native evaluation,
     * even when ExpandNonNative is true.
     */
    public void testExpandLowMaxConstraints() {
        propSaver.set(MondrianProperties.instance().MaxConstraints, 2);
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        checkNotNative(
            12,
            "select NonEmptyCrossJoin("
            + "    Filter([Store Type].Children, [Measures].[Unit Sales] > 10000), "
            + "    [Product].Children) on columns "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store Type].[Deluxe Supermarket], [Product].[Drink]}\n"
            + "{[Store Type].[Deluxe Supermarket], [Product].[Food]}\n"
            + "{[Store Type].[Deluxe Supermarket], [Product].[Non-Consumable]}\n"
            + "{[Store Type].[Gourmet Supermarket], [Product].[Drink]}\n"
            + "{[Store Type].[Gourmet Supermarket], [Product].[Food]}\n"
            + "{[Store Type].[Gourmet Supermarket], [Product].[Non-Consumable]}\n"
            + "{[Store Type].[Mid-Size Grocery], [Product].[Drink]}\n"
            + "{[Store Type].[Mid-Size Grocery], [Product].[Food]}\n"
            + "{[Store Type].[Mid-Size Grocery], [Product].[Non-Consumable]}\n"
            + "{[Store Type].[Supermarket], [Product].[Drink]}\n"
            + "{[Store Type].[Supermarket], [Product].[Food]}\n"
            + "{[Store Type].[Supermarket], [Product].[Non-Consumable]}\n"
            + "Row #0: 6,827\n"
            + "Row #0: 55,358\n"
            + "Row #0: 14,652\n"
            + "Row #0: 1,945\n"
            + "Row #0: 15,438\n"
            + "Row #0: 3,950\n"
            + "Row #0: 1,159\n"
            + "Row #0: 8,192\n"
            + "Row #0: 2,140\n"
            + "Row #0: 14,092\n"
            + "Row #0: 108,188\n"
            + "Row #0: 28,275\n");
    }

    /**
     * Verify that native evaluation is not enabled if expanded member list will
     * contain members from different levels, even if ExpandNonNative is set.
     */
    public void testExpandDifferentLevels() {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        checkNotNative(
            278,
            "select NonEmptyCrossJoin("
            + "    Descendants([Customers].[All Customers].[USA].[WA].[Yakima]), "
            + "    [Product].Children) on columns "
            + "from [Sales]",
            null);
    }

    /**
     * Verify that native evaluation is turned off for tuple inputs, even if
     * ExpandNonNative is set.
     */
    public void testExpandTupleInputs1() {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        checkNotNative(
            1,
            "with "
            + "set [Tuple Set] as {([Store Type].[All Store Types].[HeadQuarters], [Product].[All Products].[Drink]), ([Store Type].[All Store Types].[Supermarket], [Product].[All Products].[Food])} "
            + "set [Filtered Tuple Set] as Filter([Tuple Set], 1=1) "
            + "set [NECJ] as NonEmptyCrossJoin([Store].Children, [Filtered Tuple Set]) "
            + "select [NECJ] on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA], [Store Type].[Supermarket], [Product].[Food]}\n"
            + "Row #0: 108,188\n");
    }

    /**
     * Verify that native evaluation is turned off for tuple inputs, even if
     * ExpandNonNative is set.
     */
    public void testExpandTupleInputs2() {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        checkNotNative(
            1,
            "with "
            + "set [Tuple Set] as {([Store Type].[All Store Types].[HeadQuarters], [Product].[All Products].[Drink]), ([Store Type].[All Store Types].[Supermarket], [Product].[All Products].[Food])} "
            + "set [Filtered Tuple Set] as Filter([Tuple Set], 1=1) "
            + "set [NECJ] as NonEmptyCrossJoin([Filtered Tuple Set], [Store].Children) "
            + "select [NECJ] on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store Type].[Supermarket], [Product].[Food], [Store].[USA]}\n"
            + "Row #0: 108,188\n");
    }

    /**
     * Verify that native evaluation is on when ExpendNonNative is set, even if
     * the input list is empty.
     */
    public void testExpandWithOneEmptyInput() {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        boolean requestFreshConnection = true;
        // Query should return empty result.
        checkNative(
            0,
            0,
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Gender],[*BASE_MEMBERS_Product])' "
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}' "
            + "Set [*BASE_MEMBERS_Gender] as 'Filter([Gender].[Gender].Members,[Gender].CurrentMember.Name Matches (\"abc\"))' "
            + "Set [*NATIVE_MEMBERS_Gender] as 'Generate([*NATIVE_CJ_SET], {[Gender].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Product] as '[Product].[Product Name].Members' "
            + "Set [*NATIVE_MEMBERS_Product] as 'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})' "
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Unit Sales]', FORMAT_STRING = '#,##0', SOLVE_ORDER=400 "
            + "Select "
            + "[*BASE_MEMBERS_Measures] on columns, "
            + "Non Empty Generate([*NATIVE_CJ_SET], {([Gender].CurrentMember,[Product].CurrentMember)}) on rows "
            + "From [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n",
            requestFreshConnection);
    }

    public void testExpandWithTwoEmptyInputs() {
        getConnection().getCacheControl(null).flushSchemaCache();
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        // Query should return empty result.
        checkNotNative(
            0,
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Gender],[*BASE_MEMBERS_Product])' "
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}' "
            + "Set [*BASE_MEMBERS_Gender] as '{}' "
            + "Set [*NATIVE_MEMBERS_Gender] as 'Generate([*NATIVE_CJ_SET], {[Gender].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Product] as '{}' "
            + "Set [*NATIVE_MEMBERS_Product] as 'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})' "
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Unit Sales]', FORMAT_STRING = '#,##0', SOLVE_ORDER=400 "
            + "Select "
            + "[*BASE_MEMBERS_Measures] on columns, "
            + "Non Empty Generate([*NATIVE_CJ_SET], {([Gender].CurrentMember,[Product].CurrentMember)}) on rows "
            + "From [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n");
    }

    /**
     * Verify that native MemberLists inputs are subject to SQL constriant
     * limitation. If mondrian.rolap.maxConstraints is set too low, native
     * evaluations will be turned off.
     */
    public void testEnumLowMaxConstraints() {
        propSaver.set(MondrianProperties.instance().MaxConstraints, 2);
        checkNotNative(
            12,
            "with "
            + "set [All Store Types] as {"
            + "[Store Type].[Deluxe Supermarket], "
            + "[Store Type].[Gourmet Supermarket], "
            + "[Store Type].[Mid-Size Grocery], "
            + "[Store Type].[Small Grocery], "
            + "[Store Type].[Supermarket]} "
            + "set [All Products] as {"
            + "[Product].[Drink], "
            + "[Product].[Food], "
            + "[Product].[Non-Consumable]} "
            + "select "
            + "NonEmptyCrossJoin("
            + "Filter([All Store Types], ([Measures].[Unit Sales] > 10000)), "
            + "[All Products]) on columns "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store Type].[Deluxe Supermarket], [Product].[Drink]}\n"
            + "{[Store Type].[Deluxe Supermarket], [Product].[Food]}\n"
            + "{[Store Type].[Deluxe Supermarket], [Product].[Non-Consumable]}\n"
            + "{[Store Type].[Gourmet Supermarket], [Product].[Drink]}\n"
            + "{[Store Type].[Gourmet Supermarket], [Product].[Food]}\n"
            + "{[Store Type].[Gourmet Supermarket], [Product].[Non-Consumable]}\n"
            + "{[Store Type].[Mid-Size Grocery], [Product].[Drink]}\n"
            + "{[Store Type].[Mid-Size Grocery], [Product].[Food]}\n"
            + "{[Store Type].[Mid-Size Grocery], [Product].[Non-Consumable]}\n"
            + "{[Store Type].[Supermarket], [Product].[Drink]}\n"
            + "{[Store Type].[Supermarket], [Product].[Food]}\n"
            + "{[Store Type].[Supermarket], [Product].[Non-Consumable]}\n"
            + "Row #0: 6,827\n"
            + "Row #0: 55,358\n"
            + "Row #0: 14,652\n"
            + "Row #0: 1,945\n"
            + "Row #0: 15,438\n"
            + "Row #0: 3,950\n"
            + "Row #0: 1,159\n"
            + "Row #0: 8,192\n"
            + "Row #0: 2,140\n"
            + "Row #0: 14,092\n"
            + "Row #0: 108,188\n"
            + "Row #0: 28,275\n");
    }

    /**
     * Verify that the presence of All member in all the inputs disables native
     * evaluation.
     */
    public void testAllMembersNECJ1() {
        // This query cannot be evaluated natively because of the "All" member.
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, true);
        checkNotNative(
            1,
            "select "
            + "NonEmptyCrossJoin({[Store].[All Stores]}, {[Product].[All Products]}) on columns "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[All Stores], [Product].[All Products]}\n"
            + "Row #0: 266,773\n");
    }

    /**
     * Verify that the native evaluation is possible if one input does not
     * contain the All member.
     */
    public void testAllMembersNECJ2() {
        // This query can be evaluated natively because there is at least one
        // non "All" member.
        //
        // It can also be rewritten to use
        // Filter([Product].Children, Is
        // NotEmpty([Measures].[Unit Sales]))
        // which can be natively evaluated
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;
        checkNative(
            0,
            3,
            "select "
            + "NonEmptyCrossJoin([Product].[All Products].Children, {[Store].[All Stores]}) on columns "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Drink], [Store].[All Stores]}\n"
            + "{[Product].[Food], [Store].[All Stores]}\n"
            + "{[Product].[Non-Consumable], [Store].[All Stores]}\n"
            + "Row #0: 24,597\n"
            + "Row #0: 191,940\n"
            + "Row #0: 50,236\n",
            requestFreshConnection);
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
        checkNative(
            9,
            9,
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "NON EMPTY Crossjoin("
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
            + "        [Measures].[Store Sales]) ON ROWS"
            + " from [Sales] where ("
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    /**
     * Checks that TopCount is executed natively unless disabled.
     */
    public void testNativeTopCount() {
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case INFOBRIGHT:
            // Hits same Infobright bug as NamedSetTest.testNamedSetOnMember.
            return;
        }

        String query =
            "select {[Measures].[Store Sales]} on columns,"
            + "  NON EMPTY TopCount("
            + "        CrossJoin([Customers].[All Customers].[USA].children, [Promotions].[Promotion Name].Members), "
            + "        3, (3 * [Measures].[Store Sales]) - 100) ON ROWS"
            + " from [Sales] where ("
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])";

        propSaver.set(MondrianProperties.instance().EnableNativeTopCount, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;
        checkNative(3, 3, query, null, requestFreshConnection);
    }

    /**
     * Checks that TopCount is executed natively with calculated member.
     */
    public void testCmNativeTopCount() {
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case INFOBRIGHT:
            // Hits same Infobright bug as NamedSetTest.testNamedSetOnMember.
            return;
        }
        String query =
            "with member [Measures].[Store Profit Rate] as '([Measures].[Store Sales]-[Measures].[Store Cost])/[Measures].[Store Cost]', format = '#.00%' "
            + "select {[Measures].[Store Sales]} on columns,"
            + "  NON EMPTY TopCount("
            + "        [Customers].[All Customers].[USA].children, "
            + "        3, [Measures].[Store Profit Rate] / 2) ON ROWS"
            + " from [Sales]";

        propSaver.set(MondrianProperties.instance().EnableNativeTopCount, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;
        checkNative(3, 3, query, null, requestFreshConnection);
    }

    public void testMeasureAndAggregateInSlicer() {
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
            "Axis #0:\n"
            + "{[Store Type].[All Store Types].[All Types], [Measures].[Unit Sales], [Customers].[USA], [Product].[Drink]}\n"
            + "Axis #1:\n"
            + "{[Time].[1997]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "Row #0: 1,945\n"
            + "Row #1: 2,422\n"
            + "Row #2: 2,560\n"
            + "Row #3: 175\n");
    }

    public void testMeasureInSlicer() {
        assertQueryReturns(
            "select NON EMPTY {[Time].[1997]} ON COLUMNS,   "
            + "NON EMPTY [Store].[All Stores].[USA].[CA].Children ON ROWS  "
            + "from [Sales]  "
            + "where ([Measures].[Unit Sales], [Customers].[All Customers].[USA], [Product].[All Products].[Drink])",
            "Axis #0:\n"
            + "{[Measures].[Unit Sales], [Customers].[USA], [Product].[Drink]}\n"
            + "Axis #1:\n"
            + "{[Time].[1997]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "Row #0: 1,945\n"
            + "Row #1: 2,422\n"
            + "Row #2: 2,560\n"
            + "Row #3: 175\n");
    }

    /**
     * Calc Member in TopCount: this topcount can not be calculated native
     * because its set contains calculated members.
     */
    public void testCmInTopCount() {
        checkNotNative(
            1,
            "with member [Time].[Time].[Jan] as  "
            + "'Aggregate({[Time].[1998].[Q1].[1], [Time].[1997].[Q1].[1]})'  "
            + "select NON EMPTY {[Measures].[Unit Sales]} ON columns,  "
            + "NON EMPTY TopCount({[Time].[Jan]}, 2) ON rows from [Sales] ");
    }

    /**
     * Calc member in slicer cannot be executed natively.
     */
    public void testCmInSlicer() {
        checkNotNative(
            3,
            "with member [Time].[Time].[Jan] as  "
            + "'Aggregate({[Time].[1998].[Q1].[1], [Time].[1997].[Q1].[1]})'  "
            + "select NON EMPTY {[Measures].[Unit Sales]} ON columns,  "
            + "NON EMPTY [Product].Children ON rows from [Sales] "
            + "where ([Time].[Jan]) ");
    }

    public void testCmInSlicerResults() {
        assertQueryReturns(
            "with member [Time].[Time].[Jan] as  "
            + "'Aggregate({[Time].[1998].[Q1].[1], [Time].[1997].[Q1].[1]})'  "
            + "select NON EMPTY {[Measures].[Unit Sales]} ON columns,  "
            + "NON EMPTY [Product].Children ON rows from [Sales] "
            + "where ([Time].[Jan]) ",
            "Axis #0:\n"
            + "{[Time].[Jan]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 1,910\n"
            + "Row #1: 15,604\n"
            + "Row #2: 4,114\n");
    }

    public void testSetInSlicerResults() {
        assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales]} ON columns,  "
            + "NON EMPTY [Product].Children ON rows from [Sales] "
            + "where {[Time].[1998].[Q1].[1], [Time].[1997].[Q1].[1]} ",
            "Axis #0:\n"
            + "{[Time].[1998].[Q1].[1]}\n"
            + "{[Time].[1997].[Q1].[1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 1,910\n"
            + "Row #1: 15,604\n"
            + "Row #2: 4,114\n");
    }

    public void testCjMembersMembersMembers() {
        checkNative(
            67,
            67,
            "select {[Measures].[Store Sales]} on columns,"
            + "  NON EMPTY Crossjoin("
            + "    Crossjoin("
            + "        [Customers].[Name].Members,"
            + "        [Product].[Product Name].Members), "
            + "    [Promotions].[Promotion Name].Members) ON rows "
            + " from [Sales] where ("
            + "  [Store].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjMembersWithHideIfBlankLeafAndNoAll() {
        setTestContext(TestContext.instance().createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Product Ragged\" foreignKey=\"product_id\">\n"
            + "  <Hierarchy hasAll=\"false\" primaryKey=\"product_id\">\n"
            + "    <Table name=\"product\"/>\n"
            + "    <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
            + "    <Level name=\"Product Name\" table=\"product\" column=\"product_name\" uniqueMembers=\"true\"\n"
            + "        hideMemberIf=\"IfBlankName\""
            + "        />\n"
            + "  </Hierarchy>\n"
            + "</Dimension>"));

        // No 'all' level, and ragged because [Product Name] is hidden if
        // blank.  Native evaluation should be able to handle this query.
        checkNative(
            9999,  // Don't know why resultLimit needs to be so high.
            67,
            "select {[Measures].[Store Sales]} on columns,"
            + "  NON EMPTY Crossjoin("
            + "    Crossjoin("
            + "        [Customers].[Name].Members,"
            + "        [Product Ragged].[Product Name].Members), "
            + "    [Promotions].[Promotion Name].Members) ON rows "
            + " from [Sales] where ("
            + "  [Store].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjMembersWithHideIfBlankLeaf() {
        setTestContext(TestContext.instance().createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Product Ragged\" foreignKey=\"product_id\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"product_id\">\n"
            + "    <Table name=\"product\"/>\n"
            + "    <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
            + "    <Level name=\"Product Name\" table=\"product\" column=\"product_name\" uniqueMembers=\"true\"\n"
            + "        hideMemberIf=\"IfBlankName\""
            + "        />\n"
            + "  </Hierarchy>\n"
            + "</Dimension>"));

        // [Product Name] can be hidden if it is blank, but native evaluation
        // should be able to handle the query.
        checkNative(
            67,
            67,
            "select {[Measures].[Store Sales]} on columns,"
            + "  NON EMPTY Crossjoin("
            + "    Crossjoin("
            + "        [Customers].[Name].Members,"
            + "        [Product Ragged].[Product Name].Members), "
            + "    [Promotions].[Promotion Name].Members) ON rows "
            + " from [Sales] where ("
            + "  [Store].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjMembersWithHideIfParentsNameLeaf() {
        setTestContext(TestContext.instance().createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Product Ragged\" foreignKey=\"product_id\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"product_id\">\n"
            + "    <Table name=\"product\"/>\n"
            + "    <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
            + "    <Level name=\"Product Name\" table=\"product\" column=\"product_name\" uniqueMembers=\"true\"\n"
            + "        hideMemberIf=\"IfParentsName\""
            + "        />\n"
            + "  </Hierarchy>\n"
            + "</Dimension>"));

        // [Product Name] can be hidden if it it matches its parent name, so
        // native evaluation can not handle this query.
        checkNotNative(
            67,
            "select {[Measures].[Store Sales]} on columns,"
            + "  NON EMPTY Crossjoin("
            + "    Crossjoin("
            + "        [Customers].[Name].Members,"
            + "        [Product Ragged].[Product Name].Members), "
            + "    [Promotions].[Promotion Name].Members) ON rows "
            + " from [Sales] where ("
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjMembersWithHideIfBlankNameAncestor() {
        setTestContext(TestContext.instance().createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Product Ragged\" foreignKey=\"product_id\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"product_id\">\n"
            + "    <Table name=\"product\"/>\n"
            + "    <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\""
            + "        hideMemberIf=\"IfBlankName\""
            + "        />\n"
            + "    <Level name=\"Product Name\" table=\"product\" column=\"product_name\"\n uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>"));

        // Since the parent of [Product Name] can be hidden, native evaluation
        // can't handle the query.
        checkNative(
            67,
            67,
            "select {[Measures].[Store Sales]} on columns,"
            + "  NON EMPTY Crossjoin("
            + "    Crossjoin("
            + "        [Customers].[Name].Members,"
            + "        [Product Ragged].[Product Name].Members), "
            + "    [Promotions].[Promotion Name].Members) ON rows "
            + " from [Sales] where ("
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjMembersWithHideIfParentsNameAncestor() {
        setTestContext(TestContext.instance().createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Product Ragged\" foreignKey=\"product_id\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"product_id\">\n"
            + "    <Table name=\"product\"/>\n"
            + "    <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\""
            + "        hideMemberIf=\"IfParentsName\""
            + "        />\n"
            + "    <Level name=\"Product Name\" table=\"product\" column=\"product_name\"\n uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>"));

        // Since the parent of [Product Name] can be hidden, native evaluation
        // can't handle the query.
        checkNative(
            67,
            67,
            "select {[Measures].[Store Sales]} on columns,"
            + "  NON EMPTY Crossjoin("
            + "    Crossjoin("
            + "        [Customers].[Name].Members,"
            + "        [Product Ragged].[Product Name].Members), "
            + "    [Promotions].[Promotion Name].Members) ON rows "
            + " from [Sales] where ("
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjEnumWithHideIfBlankLeaf() {
        setTestContext(TestContext.instance().createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Product Ragged\" foreignKey=\"product_id\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"product_id\">\n"
            + "    <Table name=\"product\"/>\n"
            + "    <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
            + "    <Level name=\"Product Name\" table=\"product\" column=\"product_name\" uniqueMembers=\"true\"\n"
            + "        hideMemberIf=\"IfBlankName\""
            + "        />\n"
            + "  </Hierarchy>\n"
            + "</Dimension>"));

        // [Product Name] can be hidden if it is blank, but native evaluation
        // should be able to handle the query.
        // Note there's an existing bug with result ordering in native
        // non-empty evaluation of enumerations. This test intentionally
        // avoids this bug by explicitly lilsting [High Top Cauliflower]
        // before [Sphinx Bagels].
        checkNative(
            999,
            7,
            "select {[Measures].[Store Sales]} on columns,"
            + "  NON EMPTY Crossjoin("
            + "    Crossjoin("
            + "        [Customers].[Name].Members,"
            + "        { [Product Ragged].[Kiwi].[Kiwi Scallops],"
            + "          [Product Ragged].[Fast].[Fast Avocado Dip],"
            + "          [Product Ragged].[High Top].[High Top Lemons],"
            + "          [Product Ragged].[Moms].[Moms Sliced Turkey],"
            + "          [Product Ragged].[High Top].[High Top Cauliflower],"
            + "          [Product Ragged].[Sphinx].[Sphinx Bagels]"
            + "        }), "
            + "    [Promotions].[Promotion Name].Members) ON rows "
            + " from [Sales] where ("
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    /**
     * use SQL even when all members are known
     */
    public void testCjEnumEnum() {
        // Make sure maxConstraint settting is high enough
        int minConstraints = 2;
        if (MondrianProperties.instance().MaxConstraints.get()
            < minConstraints)
        {
            propSaver.set(
                MondrianProperties.instance().MaxConstraints, minConstraints);
        }
        checkNative(
            4,
            4,
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "NonEmptyCrossjoin({[Product].[All Products].[Drink].[Beverages], [Product].[All Products].[Drink].[Dairy]}, {[Customers].[All Customers].[USA].[OR].[Portland], [Customers].[All Customers].[USA].[OR].[Salem]}) ON ROWS "
            + "from [Sales] ");
    }

    /**
     * Set containing only null member should not prevent usage of native.
     */
    public void testCjNullInEnum() {
        propSaver.set(
            MondrianProperties.instance().IgnoreInvalidMembersDuringQuery,
            true);
        checkNative(
            20,
            0,
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "NON EMPTY Crossjoin({[Gender].[All Gender].[emale]}, [Customers].[All Customers].[USA].children) ON ROWS "
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
        // Make sure maxConstraint settting is high enough
        int minConstraints = 2;
        if (MondrianProperties.instance().MaxConstraints.get()
            < minConstraints)
        {
            propSaver.set(
                MondrianProperties.instance().MaxConstraints,
                minConstraints);
        }
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
        // Make sure maxConstraint settting is high enough
        // Make sure maxConstraint settting is high enough
        int minConstraints = 2;
        if (MondrianProperties.instance().MaxConstraints.get()
            < minConstraints)
        {
            propSaver.set(
                MondrianProperties.instance().MaxConstraints,
                minConstraints);
        }
        checkNative(
            3,
            3,
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "NON EMPTY Crossjoin("
            + "  {[Product].[All Products].[Drink].[Beverages], [Product].[All Products].[Drink].[Dairy]}, "
            + "  [Customers].[All Customers].[USA].[WA].Children) ON ROWS "
            + "from [Sales] "
            + "where ([Promotions].[All Promotions].[Bag Stuffers])");
    }

    /**
     * {} contains members from different levels, this can not be handled by
     * the current native crossjoin.
     */
    public void testCjEnumDifferentLevelsChildren() {
        // Don't run the test if we're testing expression dependencies.
        // Expression dependencies cause spurious interval calls to
        // 'level.getMembers()' which create false negatives in this test.
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            return;
        }

        TestCase c = new TestCase(
            8,
            5,
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "NON EMPTY Crossjoin("
            + "  {[Product].[All Products].[Food], [Product].[All Products].[Drink].[Dairy]}, "
            + "  [Customers].[All Customers].[USA].[WA].Children) ON ROWS "
            + "from [Sales] "
            + "where ([Promotions].[All Promotions].[Bag Stuffers])");
        c.run();
    }

    public void testCjDescendantsMembers() {
        checkNative(
            67,
            67,
            "select {[Measures].[Store Sales]} on columns,"
            + " NON EMPTY Crossjoin("
            + "   Descendants([Customers].[All Customers].[USA].[CA], [Customers].[Name]),"
            + "     [Product].[Product Name].Members) ON rows "
            + " from [Sales] where ("
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

    // testcase for bug MONDRIAN-506
    public void testCjMembersDescendantsWithNumericArgument() {
        checkNative(
            67,
            67,
            "select {[Measures].[Store Sales]} on columns,"
            + " NON EMPTY Crossjoin("
            + "  {[Product].[Product Name].Members},"
            + "  {Descendants([Customers].[All Customers].[USA].[CA], 2)}) ON rows "
            + " from [Sales] where ("
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjChildrenMembers() {
        checkNative(
            67,
            67,
            "select {[Measures].[Store Sales]} on columns,"
            + "  NON EMPTY Crossjoin([Customers].[All Customers].[USA].[CA].children,"
            + "    [Product].[Product Name].Members) ON rows "
            + " from [Sales] where ("
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjMembersChildren() {
        checkNative(
            67,
            67,
            "select {[Measures].[Store Sales]} on columns,"
            + "  NON EMPTY Crossjoin([Product].[Product Name].Members,"
            + "    [Customers].[All Customers].[USA].[CA].children) ON rows "
            + " from [Sales] where ("
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjMembersMembers() {
        checkNative(
            67,
            67,
            "select {[Measures].[Store Sales]} on columns,"
            + "  NON EMPTY Crossjoin([Customers].[Name].Members,"
            + "    [Product].[Product Name].Members) ON rows "
            + " from [Sales] where ("
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    public void testCjChildrenChildren() {
        checkNative(
            3,
            3,
            "select {[Measures].[Store Sales]} on columns, "
            + "  NON EMPTY Crossjoin("
            + "    [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].children, "
            + "    [Customers].[All Customers].[USA].[CA].CHILDREN) ON rows"
            + " from [Sales] where ("
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    /**
     * Checks that multi-level member list generates compact form of SQL where
     * clause:
     * (1) Use IN list if possible
     * (2) Group members sharing the same parent
     * (3) Only need to compare up to the first unique parent level.
     */
    public void testMultiLevelMemberConstraintNonNullParent() {
        String query =
            "with "
            + "set [Filtered Store City Set] as "
            + "{[Store].[USA].[OR].[Portland], "
            + " [Store].[USA].[OR].[Salem], "
            + " [Store].[USA].[CA].[San Francisco], "
            + " [Store].[USA].[WA].[Tacoma]} "
            + "set [NECJ] as NonEmptyCrossJoin([Filtered Store City Set], {[Product].[Product Family].Food}) "
            + "select [NECJ] on columns from [Sales]";

        String necjSqlDerby =
            "select "
            + "\"store\".\"store_country\", \"store\".\"store_state\", \"store\".\"store_city\", "
            + "\"product_class\".\"product_family\" "
            + "from "
            + "\"store\" as \"store\", \"sales_fact_1997\" as \"sales_fact_1997\", "
            + "\"product\" as \"product\", \"product_class\" as \"product_class\" "
            + "where "
            + "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
            + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" "
            + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" "
            + "and ((\"store\".\"store_state\" = 'OR' and \"store\".\"store_city\" in ('Portland', 'Salem'))"
            + " or (\"store\".\"store_state\" = 'CA' and \"store\".\"store_city\" = 'San Francisco')"
            + " or (\"store\".\"store_state\" = 'WA' and \"store\".\"store_city\" = 'Tacoma')) "
            + "and (\"product_class\".\"product_family\" = 'Food') "
            + "group by \"store\".\"store_country\", \"store\".\"store_state\", \"store\".\"store_city\", \"product_class\".\"product_family\" "
            + "order by CASE WHEN \"store\".\"store_country\" IS NULL THEN 1 ELSE 0 END, \"store\".\"store_country\" ASC, CASE WHEN \"store\".\"store_state\" IS NULL THEN 1 ELSE 0 END, \"store\".\"store_state\" ASC, CASE WHEN \"store\".\"store_city\" IS NULL THEN 1 ELSE 0 END, \"store\".\"store_city\" ASC, CASE WHEN \"product_class\".\"product_family\" IS NULL THEN 1 ELSE 0 END, \"product_class\".\"product_family\" ASC";

        String necjSqlMySql =
            "select "
            + "`store`.`store_country` as `c0`, `store`.`store_state` as `c1`, "
            + "`store`.`store_city` as `c2`, `product_class`.`product_family` as `c3` "
            + "from "
            + "`store` as `store`, `sales_fact_1997` as `sales_fact_1997`, "
            + "`product` as `product`, `product_class` as `product_class` "
            + "where "
            + "`sales_fact_1997`.`store_id` = `store`.`store_id` "
            + "and `product`.`product_class_id` = `product_class`.`product_class_id` "
            + "and `sales_fact_1997`.`product_id` = `product`.`product_id` "
            + "and ((`store`.`store_city`, `store`.`store_state`) in (('Portland', 'OR'), ('Salem', 'OR'), ('San Francisco', 'CA'), ('Tacoma', 'WA'))) "
            + "and (`product_class`.`product_family` = 'Food') "
            + "group by `store`.`store_country`, `store`.`store_state`, `store`.`store_city`, `product_class`.`product_family` order by "
            + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "ISNULL(`c0`) ASC, `c0` ASC, ISNULL(`c1`) ASC, `c1` ASC, "
                + "ISNULL(`c2`) ASC, `c2` ASC, ISNULL(`c3`) ASC, `c3` ASC"
                : "ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC, ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC, "
                + "ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC, ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC");

        if (MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get())
        {
            // slightly different sql expected, uses agg table now for join
            necjSqlMySql = necjSqlMySql.replaceAll(
                "sales_fact_1997", "agg_c_14_sales_fact_1997");
            necjSqlDerby = necjSqlDerby.replaceAll(
                "sales_fact_1997", "agg_c_14_sales_fact_1997");
        }

        if (!MondrianProperties.instance().FilterChildlessSnowflakeMembers
            .get())
        {
            necjSqlMySql = necjSqlMySql.replaceAll(
                "`product` as `product`, `product_class` as `product_class`",
                "`product_class` as `product_class`, `product` as `product`");
            necjSqlMySql = necjSqlMySql.replaceAll(
                "`product`.`product_class_id` = `product_class`.`product_class_id` and "
                + "`sales_fact_1997`.`product_id` = `product`.`product_id` and ",
                "`sales_fact_1997`.`product_id` = `product`.`product_id` and "
                + "`product`.`product_class_id` = `product_class`.`product_class_id` and ");
            necjSqlDerby = necjSqlDerby.replaceAll(
                "\"product\" as \"product\", \"product_class\" as \"product_class\"",
                "\"product_class\" as \"product_class\", \"product\" as \"product\"");
            necjSqlDerby = necjSqlDerby.replaceAll(
                "\"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and "
                + "\"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and ",
                "\"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and "
                + "\"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and ");
        }

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.DERBY, necjSqlDerby, necjSqlDerby),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL, necjSqlMySql, necjSqlMySql)
        };

        assertQuerySql(query, patterns);
    }

    /**
     * Checks that multi-level member list generates compact form of SQL where
     * clause:
     * (1) Use IN list if possible(not possible if there are null values because
     *     NULLs in IN lists do not match)
     * (2) Group members sharing the same parent, including parents with NULLs.
     * (3) If parent levels include NULLs, comparision includes any unique
     * level.
     */
    public void testMultiLevelMemberConstraintNullParent() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        if (!MondrianProperties.instance().FilterChildlessSnowflakeMembers
            .get())
        {
            return;
        }
        String dimension =
            "<Dimension name=\"Warehouse2\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"warehouse_id\">\n"
            + "    <Table name=\"warehouse\"/>\n"
            + "    <Level name=\"address3\" column=\"wa_address3\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"address2\" column=\"wa_address2\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"address1\" column=\"wa_address1\" uniqueMembers=\"false\"/>\n"
            + "    <Level name=\"name\" column=\"warehouse_name\" uniqueMembers=\"false\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n";

        String cube =
            "<Cube name=\"Warehouse2\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Warehouse2\" source=\"Warehouse2\" foreignKey=\"warehouse_id\"/>\n"
            + "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Warehouse Sales\" column=\"warehouse_sales\" aggregator=\"sum\"/>\n"
            + "</Cube>";

        String query =
            "with\n"
            + "set [Filtered Warehouse Set] as "
            + "{[Warehouse2].[#null].[#null].[5617 Saclan Terrace].[Arnold and Sons],"
            + " [Warehouse2].[#null].[#null].[3377 Coachman Place].[Jones International]} "
            + "set [NECJ] as NonEmptyCrossJoin([Filtered Warehouse Set], {[Product].[Product Family].Food}) "
            + "select [NECJ] on columns from [Warehouse2]";

        String necjSqlMySql =
            "select\n"
            + "    `warehouse`.`wa_address3` as `c0`,\n"
            + "    `warehouse`.`wa_address2` as `c1`,\n"
            + "    `warehouse`.`wa_address1` as `c2`,\n"
            + "    `warehouse`.`warehouse_name` as `c3`,\n"
            + "    `product_class`.`product_family` as `c4`\n"
            + "from\n"
            + "    `warehouse` as `warehouse`,\n"
            + "    `inventory_fact_1997` as `inventory_fact_1997`,\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`\n"
            + "where\n"
            + "    `inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `inventory_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    ((( `warehouse`.`wa_address2` IS NULL ) and (`warehouse`.`warehouse_name`, `warehouse`.`wa_address1`) in (('Arnold and Sons', '5617 Saclan Terrace'), ('Jones International', '3377 Coachman Place'))))\n"
            + "and\n"
            + "    (`product_class`.`product_family` = 'Food')\n"
            + "group by\n"
            + "    `warehouse`.`wa_address3`,\n"
            + "    `warehouse`.`wa_address2`,\n"
            + "    `warehouse`.`wa_address1`,\n"
            + "    `warehouse`.`warehouse_name`,\n"
            + "    `product_class`.`product_family`\n"
            + "order by\n"
            + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                + "    ISNULL(`c1`) ASC, `c1` ASC,\n"
                + "    ISNULL(`c2`) ASC, `c2` ASC,\n"
                + "    ISNULL(`c3`) ASC, `c3` ASC,\n"
                + "    ISNULL(`c4`) ASC, `c4` ASC"
                : "    ISNULL(`warehouse`.`wa_address3`) ASC, `warehouse`.`wa_address3` ASC,\n"
                + "    ISNULL(`warehouse`.`wa_address2`) ASC, `warehouse`.`wa_address2` ASC,\n"
                + "    ISNULL(`warehouse`.`wa_address1`) ASC, `warehouse`.`wa_address1` ASC,\n"
                + "    ISNULL(`warehouse`.`warehouse_name`) ASC, `warehouse`.`warehouse_name` ASC,\n"
                + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC");

        TestContext testContext =
            TestContext.instance().create(
                dimension,
                cube,
                null,
                null,
                null,
                null);

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL, necjSqlMySql, necjSqlMySql)
        };

        assertQuerySql(testContext, query, patterns);
    }

    /**
     * Check that multi-level member list generates compact form of SQL where
     * clause:
     * (1) Use IN list if possible(not possible if there are null values because
     *     NULLs in IN lists do not match)
     * (2) Group members sharing the same parent, including parents with NULLs.
     * (3) If parent levels include NULLs, comparision includes any unique
     *     level.
     * (4) Can handle predicates correctly if the member list contains both NULL
     * and non NULL parent levels.
     */
    public void testMultiLevelMemberConstraintMixedNullNonNullParent() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        if (!MondrianProperties.instance().FilterChildlessSnowflakeMembers
            .get())
        {
            return;
        }
        String dimension =
            "<Dimension name=\"Warehouse2\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"warehouse_id\">\n"
            + "    <Table name=\"warehouse\"/>\n"
            + "    <Level name=\"fax\" column=\"warehouse_fax\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"address1\" column=\"wa_address1\" uniqueMembers=\"false\"/>\n"
            + "    <Level name=\"name\" column=\"warehouse_name\" uniqueMembers=\"false\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n";

        String cube =
            "<Cube name=\"Warehouse2\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Warehouse2\" source=\"Warehouse2\" foreignKey=\"warehouse_id\"/>\n"
            + "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Warehouse Sales\" column=\"warehouse_sales\" aggregator=\"sum\"/>\n"
            + "</Cube>";

        String query =
            "with\n"
            + "set [Filtered Warehouse Set] as "
            + "{[Warehouse2].[#null].[234 West Covina Pkwy].[Freeman And Co],"
            + " [Warehouse2].[971-555-6213].[3377 Coachman Place].[Jones International]} "
            + "set [NECJ] as NonEmptyCrossJoin([Filtered Warehouse Set], {[Product].[Product Family].Food}) "
            + "select [NECJ] on columns from [Warehouse2]";

        String necjSqlMySql =
            "select\n"
            + "    `warehouse`.`warehouse_fax` as `c0`,\n"
            + "    `warehouse`.`wa_address1` as `c1`,\n"
            + "    `warehouse`.`warehouse_name` as `c2`,\n"
            + "    `product_class`.`product_family` as `c3`\n"
            + "from\n"
            + "    `warehouse` as `warehouse`,\n"
            + "    `inventory_fact_1997` as `inventory_fact_1997`,\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`\n"
            + "where\n"
            + "    `inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `inventory_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    ((`warehouse`.`warehouse_name`, `warehouse`.`wa_address1`, `warehouse`.`warehouse_fax`) in (('Jones International', '3377 Coachman Place', '971-555-6213')) or (( `warehouse`.`warehouse_fax` IS NULL ) and (`warehouse`.`warehouse_name`, `warehouse`.`wa_address1`) in (('Freeman And Co', '234 West Covina Pkwy'))))\n"
            + "and\n"
            + "    (`product_class`.`product_family` = 'Food')\n"
            + "group by\n"
            + "    `warehouse`.`warehouse_fax`,\n"
            + "    `warehouse`.`wa_address1`,\n"
            + "    `warehouse`.`warehouse_name`,\n"
            + "    `product_class`.`product_family`\n"
            + "order by\n"
            + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                + "    ISNULL(`c1`) ASC, `c1` ASC,\n"
                + "    ISNULL(`c2`) ASC, `c2` ASC,\n"
                + "    ISNULL(`c3`) ASC, `c3` ASC"
                : "    ISNULL(`warehouse`.`warehouse_fax`) ASC, `warehouse`.`warehouse_fax` ASC,\n"
                + "    ISNULL(`warehouse`.`wa_address1`) ASC, `warehouse`.`wa_address1` ASC,\n"
                + "    ISNULL(`warehouse`.`warehouse_name`) ASC, `warehouse`.`warehouse_name` ASC,\n"
                + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC");

        TestContext testContext =
            TestContext.instance().create(
                dimension,
                cube,
                null,
                null,
                null,
                null);

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL, necjSqlMySql, necjSqlMySql)
        };

        assertQuerySql(testContext, query, patterns);
    }

    /**
     * Check that multi-level member list generates compact form of SQL where
     * clause:
     * (1) Use IN list if possible(not possible if there are null values because
     *     NULLs in IN lists do not match)
     * (2) Group members sharing the same parent
     * (3) Only need to compare up to the first unique parent level.
     * (4) Can handle predicates correctly if the member list contains both NULL
     * and non NULL child levels.
     */
    public void testMultiLevelMemberConstraintWithMixedNullNonNullChild() {
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        if (!MondrianProperties.instance().FilterChildlessSnowflakeMembers
            .get())
        {
            return;
        }
        String dimension =
            "<Dimension name=\"Warehouse2\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"warehouse_id\">\n"
            + "    <Table name=\"warehouse\"/>\n"
            + "    <Level name=\"address3\" column=\"wa_address3\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"address2\" column=\"wa_address2\" uniqueMembers=\"false\"/>\n"
            + "    <Level name=\"fax\" column=\"warehouse_fax\" uniqueMembers=\"false\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n";

        String cube =
            "<Cube name=\"Warehouse2\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Warehouse2\" source=\"Warehouse2\" foreignKey=\"warehouse_id\"/>\n"
            + "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Warehouse Sales\" column=\"warehouse_sales\" aggregator=\"sum\"/>\n"
            + "</Cube>";

        String query =
            "with\n"
            + "set [Filtered Warehouse Set] as "
            + "{[Warehouse2].[#null].[#null].[#null],"
            + " [Warehouse2].[#null].[#null].[971-555-6213]} "
            + "set [NECJ] as NonEmptyCrossJoin([Filtered Warehouse Set], {[Product].[Product Family].Food}) "
            + "select [NECJ] on columns from [Warehouse2]";

        String necjSqlDerby =
            "select \"warehouse\".\"wa_address3\", \"warehouse\".\"wa_address2\", \"warehouse\".\"warehouse_fax\", \"product_class\".\"product_family\" "
            + "from \"warehouse\" as \"warehouse\", \"inventory_fact_1997\" as \"inventory_fact_1997\", \"product\" as \"product\", \"product_class\" as \"product_class\" "
            + "where \"inventory_fact_1997\".\"warehouse_id\" = \"warehouse\".\"warehouse_id\" and "
            + "\"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and \"inventory_fact_1997\".\"product_id\" = \"product\".\"product_id\" "
            + "and ((\"warehouse\".\"warehouse_fax\" = '971-555-6213' or \"warehouse\".\"warehouse_fax\" is null) and "
            + "\"warehouse\".\"wa_address2\" is null and \"warehouse\".\"wa_address3\" is null) and "
            + "(\"product_class\".\"product_family\" = 'Food') "
            + "group by \"warehouse\".\"wa_address3\", \"warehouse\".\"wa_address2\", \"warehouse\".\"warehouse_fax\", \"product_class\".\"product_family\" "
            + "order by CASE WHEN \"warehouse\".\"wa_address3\" IS NULL THEN 1 ELSE 0 END, \"warehouse\".\"wa_address3\" ASC, CASE WHEN \"warehouse\".\"wa_address2\" IS NULL THEN 1 ELSE 0 END, \"warehouse\".\"wa_address2\" ASC, CASE WHEN \"warehouse\".\"warehouse_fax\" IS NULL THEN 1 ELSE 0 END, \"warehouse\".\"warehouse_fax\" ASC, CASE WHEN \"product_class\".\"product_family\" IS NULL THEN 1 ELSE 0 END, \"product_class\".\"product_family\" ASC";

        String necjSqlMySql =
            "select `warehouse`.`wa_address3` as `c0`, `warehouse`.`wa_address2` as `c1`, `warehouse`.`warehouse_fax` as `c2`, "
            + "`product_class`.`product_family` as `c3` from `warehouse` as `warehouse`, `inventory_fact_1997` as `inventory_fact_1997`, "
            + "`product` as `product`, `product_class` as `product_class` "
            + "where `inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id` and `product`.`product_class_id` = `product_class`.`product_class_id` and "
            + "`inventory_fact_1997`.`product_id` = `product`.`product_id` and "
            + "((`warehouse`.`warehouse_fax` = '971-555-6213' or `warehouse`.`warehouse_fax` is null) and "
            + "`warehouse`.`wa_address2` is null and `warehouse`.`wa_address3` is null) and "
            + "(`product_class`.`product_family` = 'Food') "
            + "group by `warehouse`.`wa_address3`, `warehouse`.`wa_address2`, `warehouse`.`warehouse_fax`, "
            + "`product_class`.`product_family` "
            + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "order by ISNULL(`c0`) ASC, `c0` ASC, ISNULL(`c1`) ASC, "
                + "`c1` ASC, ISNULL(`c2`) ASC, `c2` ASC, "
                + "ISNULL(`c3`) ASC, `c3` ASC"
                : "order by ISNULL(`warehouse`.`wa_address3`) ASC, `warehouse`.`wa_address3` ASC, ISNULL(`warehouse`.`wa_address2`) ASC, "
                + "`warehouse`.`wa_address2` ASC, ISNULL(`warehouse`.`warehouse_fax`) ASC, `warehouse`.`warehouse_fax` ASC, "
                + "ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC");

        TestContext testContext =
            TestContext.instance().create(
                dimension,
                cube,
                null,
                null,
                null,
                null);

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.DERBY, necjSqlDerby, necjSqlDerby),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL, necjSqlMySql, necjSqlMySql)
        };

        assertQuerySql(testContext, query, patterns);
    }

    public void testNonEmptyUnionQuery() {
        Result result = executeQuery(
            "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns,\n"
            + " NON EMPTY Hierarchize(\n"
            + "   Union(\n"
            + "     Crossjoin(\n"
            + "       Crossjoin([Gender].[All Gender].children,\n"
            + "                 [Marital Status].[All Marital Status].children),\n"
            + "       Crossjoin([Customers].[All Customers].children,\n"
            + "                 [Product].[All Products].children) ),\n"
            + "     Crossjoin({([Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M])},\n"
            + "       Crossjoin(\n"
            + "         [Customers].[All Customers].[USA].children,\n"
            + "         [Product].[All Products].children) ) )) on rows\n"
            + "from Sales where ([Time].[1997])");
        final Axis rowsAxis = result.getAxes()[1];
        Assert.assertEquals(21, rowsAxis.getPositions().size());
    }

    /**
     * when Mondrian parses a string like
     * "[Store].[All Stores].[USA].[CA].[San Francisco]"
     * it shall not lookup additional members.
     */
    public void testLookupMemberCache() {
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            // Dependency testing causes extra SQL reads, and screws up this
            // test.
            return;
        }

        // there currently isn't a cube member to children cache, only
        // a shared cache so use the shared smart member reader
        SmartMemberReader smr = getSmartMemberReader("Store");
        MemberCacheHelper smrch = smr.cacheHelper;
        MemberCacheHelper rcsmrch =
            ((RolapCubeHierarchy.RolapCubeHierarchyMemberReader) smr)
            .getRolapCubeMemberCacheHelper();
        SmartMemberReader ssmr = getSharedSmartMemberReader("Store");
        MemberCacheHelper ssmrch = ssmr.cacheHelper;
        clearAndHardenCache(smrch);
        clearAndHardenCache(rcsmrch);
        clearAndHardenCache(ssmrch);

        RolapResult result =
            (RolapResult) executeQuery(
                "select {[Store].[All Stores].[USA].[CA].[San Francisco]} on columns from [Sales]");
        assertTrue(
            "no additional members should be read:"
            + ssmrch.mapKeyToMember.size(),
            ssmrch.mapKeyToMember.size() <= 5);
        RolapMember sf =
            (RolapMember) result.getAxes()[0].getPositions().get(0).get(0);
        RolapMember ca = sf.getParentMember();

        // convert back to shared members
        ca = ((RolapCubeMember) ca).getRolapMember();
        sf = ((RolapCubeMember) sf).getRolapMember();

        List<RolapMember> list = ssmrch.mapMemberToChildren.get(
            ca, scf.getMemberChildrenConstraint(null));
        assertNull("children of [CA] are not in cache", list);

        Collection caChildren = ssmrch.mapParentToNamedChildren.get(ca);

        assertNotNull("child [San Francisco] of [CA] is in cache", caChildren);
        assertTrue("[San Francisco] expected", caChildren.contains(sf));
    }

    /**
     * When looking for [Month] Mondrian generates SQL that tries to find
     * 'Month' as a member of the time dimension. This resulted in an
     * SQLException because the year level is numeric and the constant 'Month'
     * in the WHERE condition is not.  Its probably a bug that Mondrian does not
     * take into account [Time].[1997] when looking up [Month].
     */
    public void testLookupMember() {
        // ok if no exception occurs
        executeQuery(
            "SELECT DESCENDANTS([Time].[1997], [Month]) ON COLUMNS FROM [Sales]");
    }


    /**
     * Non Empty CrossJoin (A,B) gets turned into CrossJoin (Non Empty(A), Non
     * Empty(B)).  Verify that there is no crash when the length of B could be
     * non-zero length before the non empty and 0 after the non empty.
     */
    public void testNonEmptyCrossJoinList() {
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, false);
        boolean oldEnableNativeNonEmpty =
            MondrianProperties.instance().EnableNativeNonEmpty.get();
        MondrianProperties.instance().EnableNativeNonEmpty.set(false);

        executeQuery(
            "select non empty CrossJoin([Customers].[Name].Members, "
            + "{[Promotions].[All Promotions].[Fantastic Discounts]}) "
            + "ON COLUMNS FROM [Sales]");
        MondrianProperties.instance().EnableNativeNonEmpty.set(
            oldEnableNativeNonEmpty);
    }

    /**
     * SQL Optimization must be turned off in ragged hierarchies.
     */
    public void testLookupMember2() {
        // ok if no exception occurs
        executeQuery(
            "select {[Store].[USA].[Washington]} on columns from [Sales Ragged]");
    }

    /**
     * Make sure that the Crossjoin in [Measures].[CustomerCount]
     * is not evaluated in NON EMPTY context.
     */
    public void testCalcMemberWithNonEmptyCrossJoin() {
        getConnection().getCacheControl(null).flushSchemaCache();
        Result result = executeQuery(
            "with member [Measures].[CustomerCount] as \n"
            + "'Count(CrossJoin({[Product].[All Products]}, [Customers].[Name].Members))'\n"
            + "select \n"
            + "NON EMPTY{[Measures].[CustomerCount]} ON columns,\n"
            + "NON EMPTY{[Product].[All Products]} ON rows\n"
            + "from [Sales]\n"
            + "where ([Store].[All Stores].[USA].[CA].[San Francisco].[Store 14], [Time].[1997].[Q1].[1])");
        Cell c = result.getCell(new int[] {0, 0});
        // we expect 10281 customers, although there are only 20 non-empty ones
        // @see #testLevelMembers
        assertEquals("10,281", c.getFormattedValue());
    }

    public void testLevelMembers() {
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            // Dependency testing causes extra SQL reads, and screws up this
            // test.
            return;
        }
        SmartMemberReader smr = getSmartMemberReader("Customers");
        // use the RolapCubeHierarchy's member cache for levels
        MemberCacheHelper smrch =
            ((RolapCubeHierarchy.CacheRolapCubeHierarchyMemberReader) smr)
            .rolapCubeCacheHelper;
        clearAndHardenCache(smrch);
        MemberCacheHelper smrich = smr.cacheHelper;
        clearAndHardenCache(smrich);

        // use the shared member cache for mapMemberToChildren
        SmartMemberReader ssmr = getSharedSmartMemberReader("Customers");
        MemberCacheHelper ssmrch = ssmr.cacheHelper;
        clearAndHardenCache(ssmrch);

        TestCase c = new TestCase(
            50,
            21,
            "select \n"
            + "{[Measures].[Unit Sales]} ON columns,\n"
            + "NON EMPTY {[Customers].[All Customers], [Customers].[Name].Members} ON rows\n"
            + "from [Sales]\n"
            + "where ([Store].[All Stores].[USA].[CA].[San Francisco].[Store 14], [Time].[1997].[Q1].[1])");
        Result r = c.run();
        Level[] levels = smr.getHierarchy().getLevels();
        Level nameLevel = levels[levels.length - 1];

        // evaluator for [All Customers], [Store 14], [1/1/1997]
        Evaluator context = getEvaluator(r, new int[]{0, 0});

        // make sure that [Customers].[Name].Members is NOT in cache
        TupleConstraint lmc = scf.getLevelMembersConstraint(null);
        assertNull(smrch.mapLevelToMembers.get((RolapLevel) nameLevel, lmc));
        // make sure that NON EMPTY [Customers].[Name].Members IS in cache
        context.setNonEmpty(true);
        lmc = scf.getLevelMembersConstraint(context);
        List<RolapMember> list =
            smrch.mapLevelToMembers.get((RolapLevel) nameLevel, lmc);
        if (MondrianProperties.instance().EnableRolapCubeMemberCache.get()) {
            assertNotNull(list);
            assertEquals(20, list.size());
        }
        // make sure that the parent/child for the context are cached

        // [Customers].[USA].[CA].[Burlingame].[Peggy Justice]
        Member member = r.getAxes()[1].getPositions().get(1).get(0);
        Member parent = member.getParentMember();
        parent = ((RolapCubeMember) parent).getRolapMember();
        member = ((RolapCubeMember) member).getRolapMember();

        // lookup all children of [Burlingame] -> not in cache
        MemberChildrenConstraint mcc = scf.getMemberChildrenConstraint(null);
        assertNull(ssmrch.mapMemberToChildren.get((RolapMember) parent, mcc));

        // lookup NON EMPTY children of [Burlingame] -> yes these are in cache
        mcc = scf.getMemberChildrenConstraint(context);
        list = smrich.mapMemberToChildren.get((RolapMember) parent, mcc);
        assertNotNull(list);
        assertTrue(list.contains(member));
    }

    public void testLevelMembersWithoutNonEmpty() {
        SmartMemberReader smr = getSmartMemberReader("Customers");

        MemberCacheHelper smrch =
            ((RolapCubeHierarchy.CacheRolapCubeHierarchyMemberReader) smr)
            .rolapCubeCacheHelper;
        clearAndHardenCache(smrch);

        MemberCacheHelper smrich = smr.cacheHelper;
        clearAndHardenCache(smrich);

        SmartMemberReader ssmr = getSharedSmartMemberReader("Customers");
        MemberCacheHelper ssmrch = ssmr.cacheHelper;
        clearAndHardenCache(ssmrch);

        Result r = executeQuery(
            "select \n"
            + "{[Measures].[Unit Sales]} ON columns,\n"
            + "{[Customers].[All Customers], [Customers].[Name].Members} ON rows\n"
            + "from [Sales]\n"
            + "where ([Store].[All Stores].[USA].[CA].[San Francisco].[Store 14], [Time].[1997].[Q1].[1])");
        Level[] levels = smr.getHierarchy().getLevels();
        Level nameLevel = levels[levels.length - 1];

        // evaluator for [All Customers], [Store 14], [1/1/1997]
        Evaluator context = getEvaluator(r, new int[] {0, 0});

        // make sure that [Customers].[Name].Members IS in cache
        TupleConstraint lmc = scf.getLevelMembersConstraint(null);
        List<RolapMember> list =
            smrch.mapLevelToMembers.get((RolapLevel) nameLevel, lmc);
        if (MondrianProperties.instance().EnableRolapCubeMemberCache.get()) {
            assertNotNull(list);
            assertEquals(10281, list.size());
        }
        // make sure that NON EMPTY [Customers].[Name].Members is NOT in cache
        context.setNonEmpty(true);
        lmc = scf.getLevelMembersConstraint(context);
        assertNull(smrch.mapLevelToMembers.get((RolapLevel) nameLevel, lmc));

        // make sure that the parent/child for the context are cached

        // [Customers].[Canada].[BC].[Burnaby]
        Member member = r.getAxes()[1].getPositions().get(1).get(0);
        Member parent = member.getParentMember();

        parent = ((RolapCubeMember) parent).getRolapMember();
        member = ((RolapCubeMember) member).getRolapMember();

        // lookup all children of [Burnaby] -> yes, found in cache
        MemberChildrenConstraint mcc = scf.getMemberChildrenConstraint(null);
        list = ssmrch.mapMemberToChildren.get((RolapMember) parent, mcc);
        assertNotNull(list);
        assertTrue(list.contains(member));

        // lookup NON EMPTY children of [Burlingame] -> not in cache
        mcc = scf.getMemberChildrenConstraint(context);
        list = ssmrch.mapMemberToChildren.get((RolapMember) parent, mcc);
        assertNull(list);
    }

    /**
     * Tests that <Dimension>.Members exploits the same optimization as
     * <Level>.Members.
     */
    public void testDimensionMembers() {
        // No query should return more than 20 rows. (1 row at 'all' level,
        // 1 row at nation level, 1 at state level, 20 at city level, and 11
        // at customers level = 34.)
        TestCase c = new TestCase(
            34,
            34,
            "select \n"
            + "{[Measures].[Unit Sales]} ON columns,\n"
            + "NON EMPTY [Customers].Members ON rows\n"
            + "from [Sales]\n"
            + "where ([Store].[All Stores].[USA].[CA].[San Francisco].[Store 14], [Time].[1997].[Q1].[1])");
        c.run();
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
            + "where ([Store].[All Stores].[USA].[CA].[San Francisco].[Store 14], [Time].[1997].[Q1].[1])");
        c.run();
    }

    /**
     * Tests non empty children of All member
     */
    public void testMemberChildrenOfAllMember() {
        TestCase c = new TestCase(
            50,
            14,
            "select {[Measures].[Unit Sales]} ON columns,\n"
            + "NON EMPTY [Promotions].[All Promotions].Children ON rows from [Sales]\n"
            + "where ([Time].[1997].[Q1].[1])");
        c.run();
    }

    /**
     * Tests non empty children of All member w/o WHERE clause
     */
    public void testMemberChildrenNoWhere() {
        // The time dimension is joined because there is no (All) level in the
        // Time hierarchy:
        //
        //      select
        //        `promotion`.`promotion_name` as `c0`
        //      from
        //        `time_by_day` as `time_by_day`,
        //        `sales_fact_1997` as `sales_fact_1997`,
        //        `promotion` as `promotion`
        //      where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`
        //        and `time_by_day`.`the_year` = 1997
        //        and `sales_fact_1997`.`promotion_id`
        //                = `promotion`.`promotion_id`
        //      group by
        //        `promotion`.`promotion_name`
        //      order by
        //        `promotion`.`promotion_name`

        TestCase c =
            new TestCase(
                50,
                48,
                "select {[Measures].[Unit Sales]} ON columns,\n"
                + "NON EMPTY [Promotions].[All Promotions].Children ON rows "
                + "from [Sales]\n");
        c.run();
    }

    /**
     * Testcase for bug 1379068, which causes no children of [Time].[1997].[Q2]
     * to be found, because it incorrectly constrains on the level's key column
     * rather than name column.
     */
    public void testMemberChildrenNameCol() {
        // Expression dependency testing casues false negatives.
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            return;
        }
        TestCase c = new TestCase(
            3,
            1,
            "select "
            + " {[Measures].[Count]} ON columns,"
            + " {[Time].[1997].[Q2].[April]} on rows "
            + "from [HR]");
        c.run();
    }

    /**
     * When a member is expanded in JPivot with mulitple hierarchies visible it
     * generates a
     *   <code>CrossJoin({[member from left hierarchy]}, [member to
     * expand].Children)</code>
     *
     * <p>This should behave the same as if <code>[member from left
     * hierarchy]</code> was put into the slicer.
     */
    public void testCrossjoin() {
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            // Dependency testing causes extra SQL reads, and makes this
            // test fail.
            return;
        }

        TestCase c =
            new TestCase(
                45,
                4,
                "select \n"
                + "{[Measures].[Unit Sales]} ON columns,\n"
                + "NON EMPTY Crossjoin("
                + "{[Store].[USA].[CA].[San Francisco].[Store 14]},"
                + " [Customers].[USA].[CA].[Palo Alto].Children) ON rows\n"
                + "from [Sales] where ([Time].[1997].[Q1].[1])");
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

        Connection con = getTestContext().withSchemaPool(false).getConnection();
        SmartMemberReader smr = getSmartMemberReader(con, "Customers");
        MemberCacheHelper smrch = smr.cacheHelper;
        clearAndHardenCache(smrch);

        SmartMemberReader ssmr = getSmartMemberReader(con, "Customers");
        MemberCacheHelper ssmrch = ssmr.cacheHelper;
        clearAndHardenCache(ssmrch);

        TestCase c =
            new TestCase(
                con,
                45,
                21,
                "select \n"
                + "{[Measures].[Unit Sales]} ON columns, "
                + "NON EMPTY {[Customers].[All Customers], Descendants([Customers].[All Customers].[USA].[CA], [Customers].[Name])} on rows "
                + "from [Sales] "
                + "where ([Store].[All Stores].[USA].[CA].[San Francisco].[Store 14], [Time].[1997].[Q1].[1])");
        Result result = c.run();
        // [Customers].[All Customers].[USA].[CA].[Burlingame].[Peggy Justice]
        RolapMember peggy =
            (RolapMember) result.getAxes()[1].getPositions().get(1).get(0);
        RolapMember burlingame = peggy.getParentMember();

        peggy = ((RolapCubeMember) peggy).getRolapMember();
        burlingame = ((RolapCubeMember) burlingame).getRolapMember();

        // all children of burlingame are not in cache
        MemberChildrenConstraint mcc = scf.getMemberChildrenConstraint(null);
        assertNull(ssmrch.mapMemberToChildren.get(burlingame, mcc));
        // but non empty children is
        Evaluator evaluator = getEvaluator(result, new int[] {0, 0});
        evaluator.setNonEmpty(true);
        mcc = scf.getMemberChildrenConstraint(evaluator);
        List<RolapMember> list =
            ssmrch.mapMemberToChildren.get(burlingame, mcc);
        assertNotNull(list);
        assertTrue(list.contains(peggy));

        // now we run the same query again, this time everything must come out
        // of the cache
        RolapNativeRegistry reg = getRegistry(con);
        reg.setListener(
            new Listener() {
                public void foundEvaluator(NativeEvent e) {
                }

                public void foundInCache(TupleEvent e) {
                }

                public void executingSql(TupleEvent e) {
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
        assertQueryReturns(
            "select NON EMPTY {[Time].[1997]} ON COLUMNS,\n"
            + "NON EMPTY Hierarchize(Union({[Customers].[All Customers]},\n"
            + "[Customers].[All Customers].Children)) ON ROWS\n"
            + "from [Sales]\n"
            + "where [Measures].[Profit]",
            "Axis #0:\n"
            + "{[Measures].[Profit]}\n"
            + "Axis #1:\n"
            + "{[Time].[1997]}\n"
            + "Axis #2:\n"
            + "{[Customers].[All Customers]}\n"
            + "{[Customers].[USA]}\n"
            + "Row #0: $339,610.90\n"
            + "Row #1: $339,610.90\n");
    }

    public void testVirtualCubeCrossJoin() {
        checkNative(
            18,
            3,
            "select "
            + "{[Measures].[Units Ordered], [Measures].[Store Sales]} on columns, "
            + "non empty crossjoin([Product].[All Products].children, "
            + "[Store].[All Stores].children) on rows "
            + "from [Warehouse and Sales]");
    }

    public void testVirtualCubeNonEmptyCrossJoin() {
        checkNative(
            18,
            3,
            "select "
            + "{[Measures].[Units Ordered], [Measures].[Store Sales]} on columns, "
            + "NonEmptyCrossJoin([Product].[All Products].children, "
            + "[Store].[All Stores].children) on rows "
            + "from [Warehouse and Sales]");
    }

    public void testVirtualCubeNonEmptyCrossJoin3Args() {
        checkNative(
            3,
            3,
            "select "
            + "{[Measures].[Store Sales]} on columns, "
            + "nonEmptyCrossJoin([Product].[All Products].children, "
            + "nonEmptyCrossJoin([Customers].[All Customers].children,"
            + "[Store].[All Stores].children)) on rows "
            + "from [Warehouse and Sales]");
    }

    public void testNotNativeVirtualCubeCrossJoin1() {
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case INFOBRIGHT:
            // Hits same Infobright bug as NamedSetTest.testNamedSetOnMember.
            return;
        }
        // for this test, verify that no alert is raised even though
        // native evaluation isn't supported, because query
        // doesn't use explicit NonEmptyCrossJoin
        propSaver.set(
            MondrianProperties.instance().AlertNativeEvaluationUnsupported,
            "ERROR");
        // native cross join cannot be used due to AllMembers
        checkNotNative(
            3,
            "select "
            + "{[Measures].AllMembers} on columns, "
            + "non empty crossjoin([Product].[All Products].children, "
            + "[Store].[All Stores].children) on rows "
            + "from [Warehouse and Sales]");
    }

    public void testNotNativeVirtualCubeCrossJoin2() {
        // native cross join cannot be used due to the range operator
        checkNotNative(
            3,
            "select "
            + "{[Measures].[Sales Count] : [Measures].[Unit Sales]} on columns, "
            + "non empty crossjoin([Product].[All Products].children, "
            + "[Store].[All Stores].children) on rows "
            + "from [Warehouse and Sales]");
    }

    public void testNotNativeVirtualCubeCrossJoinUnsupported() {
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case INFOBRIGHT:
            // Hits same Infobright bug as NamedSetTest.testNamedSetOnMember.
            return;
        }
        final BooleanProperty enableProperty =
            MondrianProperties.instance().EnableNativeCrossJoin;
        final StringProperty alertProperty =
            MondrianProperties.instance().AlertNativeEvaluationUnsupported;
        if (!enableProperty.get()) {
            // When native cross joins are explicitly disabled, no alerts
            // are supposed to be raised.
            return;
        }

        String mdx =
            "select "
            + "{[Measures].AllMembers} on columns, "
            + "NonEmptyCrossJoin([Product].[All Products].children, "
            + "[Store].[All Stores].children) on rows "
            + "from [Warehouse and Sales]";

        final List<LoggingEvent> events = new ArrayList<LoggingEvent>();

        // set up log4j listener to detect alerts
        Appender alertListener = new AppenderSkeleton() {
            protected void append(LoggingEvent event) {
                events.add(event);
            }

            public void close() {
            }

            public boolean requiresLayout() {
                return false;
            }
        };
        final Logger rolapUtilLogger = Logger.getLogger(RolapUtil.class);
        propSaver.setAtLeast(rolapUtilLogger, org.apache.log4j.Level.WARN);
        rolapUtilLogger.addAppender(alertListener);
        String expectedMessage =
            "Unable to use native SQL evaluation for 'NonEmptyCrossJoin'";

        // verify that exception is thrown if alerting is set to ERROR
        propSaver.set(
            alertProperty, org.apache.log4j.Level.ERROR.toString());
        try {
            checkNotNative(3, mdx);
            fail("Expected NativeEvaluationUnsupportedException");
        } catch (Exception ex) {
            Throwable t = ex;
            while (t.getCause() != null && t != t.getCause()) {
                t = t.getCause();
            }
            if (!(t instanceof NativeEvaluationUnsupportedException)) {
                fail();
            }
            // Expected
        } finally {
            propSaver.reset();
            propSaver.setAtLeast(rolapUtilLogger, org.apache.log4j.Level.WARN);
        }

        // should have gotten one ERROR
        int nEvents = countFilteredEvents(
            events, org.apache.log4j.Level.ERROR, expectedMessage);
        assertEquals("logged error count check", 1, nEvents);
        events.clear();

        // verify that exactly one warning is posted but execution succeeds
        // if alerting is set to WARN
        propSaver.set(
            alertProperty, org.apache.log4j.Level.WARN.toString());
        try {
            checkNotNative(3, mdx);
        } finally {
            propSaver.reset();
            propSaver.setAtLeast(rolapUtilLogger, org.apache.log4j.Level.WARN);
        }

        // should have gotten one WARN
        nEvents = countFilteredEvents(
            events, org.apache.log4j.Level.WARN, expectedMessage);
        assertEquals("logged warning count check", 1, nEvents);
        events.clear();

        // verify that no warning is posted if native evaluation is
        // explicitly disabled
        propSaver.set(
            alertProperty, org.apache.log4j.Level.WARN.toString());
        propSaver.set(
            enableProperty, false);
        try {
            checkNotNative(3, mdx);
        } finally {
            propSaver.reset();
            propSaver.setAtLeast(rolapUtilLogger, org.apache.log4j.Level.WARN);
        }

        // should have gotten no WARN
        nEvents = countFilteredEvents(
            events, org.apache.log4j.Level.WARN, expectedMessage);
        assertEquals("logged warning count check", 0, nEvents);
        events.clear();

        // no biggie if we don't get here for some reason; just being
        // half-heartedly clean
        rolapUtilLogger.removeAppender(alertListener);
    }

    private int countFilteredEvents(
        List<LoggingEvent> events,
        org.apache.log4j.Level level,
        String pattern)
    {
        int filteredEventCount = 0;
        for (LoggingEvent event : events) {
            if (!event.getLevel().equals(level)) {
                continue;
            }
            if (event.getMessage().toString().indexOf(pattern) == -1) {
                continue;
            }
            filteredEventCount++;
        }
        return filteredEventCount;
    }

    public void testVirtualCubeCrossJoinCalculatedMember1() {
        // calculated member appears in query
        checkNative(
            18,
            3,
            "WITH MEMBER [Measures].[Total Cost] as "
            + "'[Measures].[Store Cost] + [Measures].[Warehouse Cost]' "
            + "select "
            + "{[Measures].[Total Cost]} on columns, "
            + "non empty crossjoin([Product].[All Products].children, "
            + "[Store].[All Stores].children) on rows "
            + "from [Warehouse and Sales]");
    }

    public void testVirtualCubeCrossJoinCalculatedMember2() {
        // calculated member defined in schema
        checkNative(
            18,
            3,
            "select "
            + "{[Measures].[Profit Per Unit Shipped]} on columns, "
            + "non empty crossjoin([Product].[All Products].children, "
            + "[Store].[All Stores].children) on rows "
            + "from [Warehouse and Sales]");
    }

    public void testNotNativeVirtualCubeCrossJoinCalculatedMember() {
        // native cross join cannot be used due to CurrentMember in the
        // calculated member
        checkNotNative(
            3,
            "WITH MEMBER [Measures].[CurrMember] as "
            + "'[Measures].CurrentMember' "
            + "select "
            + "{[Measures].[CurrMember]} on columns, "
            + "non empty crossjoin([Product].[All Products].children, "
            + "[Store].[All Stores].children) on rows "
            + "from [Warehouse and Sales]");
    }

    public void testCjEnumCalcMembers() {
        // 3 cross joins -- 2 of the 4 arguments to the cross joins are
        // enumerated sets with calculated members
        // should be non-native due to the fix to testCjEnumCalcMembersBug()
        checkNotNative(
            30,
            "with "
            + "member [Product].[All Products].[Drink].[*SUBTOTAL_MEMBER_SEL~SUM] as "
            + "    'sum({[Product].[All Products].[Drink]})' "
            + "member [Product].[All Products].[Non-Consumable].[*SUBTOTAL_MEMBER_SEL~SUM] as "
            + "    'sum({[Product].[All Products].[Non-Consumable]})' "
            + "member [Customers].[All Customers].[USA].[CA].[*SUBTOTAL_MEMBER_SEL~SUM] as "
            + "    'sum({[Customers].[All Customers].[USA].[CA]})' "
            + "member [Customers].[All Customers].[USA].[OR].[*SUBTOTAL_MEMBER_SEL~SUM] as "
            + "    'sum({[Customers].[All Customers].[USA].[OR]})' "
            + "member [Customers].[All Customers].[USA].[WA].[*SUBTOTAL_MEMBER_SEL~SUM] as "
            + "    'sum({[Customers].[All Customers].[USA].[WA]})' "
            + "select "
            + "{[Measures].[Unit Sales]} on columns, "
            + "non empty "
            + "    crossjoin("
            + "        crossjoin("
            + "            crossjoin("
            + "                {[Product].[All Products].[Drink].[*SUBTOTAL_MEMBER_SEL~SUM], "
            + "                    [Product].[All Products].[Non-Consumable].[*SUBTOTAL_MEMBER_SEL~SUM]}, "
            + "                " + EDUCATION_LEVEL_LEVEL + ".Members), "
            + "            {[Customers].[All Customers].[USA].[CA].[*SUBTOTAL_MEMBER_SEL~SUM], "
            + "                [Customers].[All Customers].[USA].[OR].[*SUBTOTAL_MEMBER_SEL~SUM], "
            + "                [Customers].[All Customers].[USA].[WA].[*SUBTOTAL_MEMBER_SEL~SUM]}), "
            + "        [Time].[Year].members)"
            + "    on rows "
            + "from [Sales]");
    }

    public void testCjEnumCalcMembersBug() {
        // make sure NECJ is forced to be non-native
        // before the fix, the query is natively evaluated and result
        // has empty rows for [Store Type].[All Store Types].[HeadQuarters]
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, true);
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        checkNotNative(
            9,
            "with "
            + "member [Store Type].[All Store Types].[S] as sum({[Store Type].[All Store Types]}) "
            + "set [Enum Store Types] as {"
            + "    [Store Type].[All Store Types].[HeadQuarters], "
            + "    [Store Type].[All Store Types].[Small Grocery], "
            + "    [Store Type].[All Store Types].[Supermarket], "
            + "    [Store Type].[All Store Types].[S]}"
            + "select [Measures] on columns,\n"
            + "    NonEmptyCrossJoin([Product].[All Products].Children, [Enum Store Types]) on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink], [Store Type].[Small Grocery]}\n"
            + "{[Product].[Drink], [Store Type].[Supermarket]}\n"
            + "{[Product].[Drink], [Store Type].[All Store Types].[S]}\n"
            + "{[Product].[Food], [Store Type].[Small Grocery]}\n"
            + "{[Product].[Food], [Store Type].[Supermarket]}\n"
            + "{[Product].[Food], [Store Type].[All Store Types].[S]}\n"
            + "{[Product].[Non-Consumable], [Store Type].[Small Grocery]}\n"
            + "{[Product].[Non-Consumable], [Store Type].[Supermarket]}\n"
            + "{[Product].[Non-Consumable], [Store Type].[All Store Types].[S]}\n"
            + "Row #0: 574\n"
            + "Row #1: 14,092\n"
            + "Row #2: 24,597\n"
            + "Row #3: 4,764\n"
            + "Row #4: 108,188\n"
            + "Row #5: 191,940\n"
            + "Row #6: 1,219\n"
            + "Row #7: 28,275\n"
            + "Row #8: 50,236\n");
    }

    public void testCjEnumEmptyCalcMembers() {
        // Make sure maxConstraint settting is high enough
        int minConstraints = 3;
        if (MondrianProperties.instance().MaxConstraints.get()
            < minConstraints)
        {
            propSaver.set(
                MondrianProperties.instance().MaxConstraints, minConstraints);
        }

        // enumerated list of calculated members results in some empty cells
        checkNotNative(
            5,
            "with "
            + "member [Customers].[All Customers].[USA].[*SUBTOTAL_MEMBER_SEL~SUM] as "
            + "    'sum({[Customers].[All Customers].[USA]})' "
            + "member [Customers].[All Customers].[Mexico].[*SUBTOTAL_MEMBER_SEL~SUM] as "
            + "    'sum({[Customers].[All Customers].[Mexico]})' "
            + "member [Customers].[All Customers].[Canada].[*SUBTOTAL_MEMBER_SEL~SUM] as "
            + "    'sum({[Customers].[All Customers].[Canada]})' "
            + "select "
            + "{[Measures].[Unit Sales]} on columns, "
            + "non empty "
            + "    crossjoin("
            + "        {[Customers].[All Customers].[Mexico].[*SUBTOTAL_MEMBER_SEL~SUM], "
            + "            [Customers].[All Customers].[Canada].[*SUBTOTAL_MEMBER_SEL~SUM], "
            + "            [Customers].[All Customers].[USA].[*SUBTOTAL_MEMBER_SEL~SUM]}, "
            + "        " + EDUCATION_LEVEL_LEVEL + ".Members) "
            + "    on rows "
            + "from [Sales]");
    }

    public void testCjUnionEnumCalcMembers() {
        // non-native due to the fix to testCjEnumCalcMembersBug()
        checkNotNative(
            46,
            "with "
            + "member [Education Level].[*SUBTOTAL_MEMBER_SEL~SUM] as "
            + "    'sum({[Education Level].[All Education Levels]})' "
            + "member [Education Level].[*SUBTOTAL_MEMBER_SEL~AVG] as "
            + "   'avg([Education Level].[Education Level].Members)' select "
            + "{[Measures].[Unit Sales]} on columns, "
            + "non empty union (Crossjoin("
            + "    [Product].[Product Department].Members, "
            + "    {[Education Level].[*SUBTOTAL_MEMBER_SEL~AVG]}), "
            + "crossjoin("
            + "    [Product].[Product Department].Members, "
            + "    {[Education Level].[*SUBTOTAL_MEMBER_SEL~SUM]})) on rows "
            + "from [Sales]");
    }

    /**
     * Tests the behavior if you have NON EMPTY on both axes, and the default
     * member of a hierarchy is not 'all' or the first child.
     */
    public void testNonEmptyWithWeirdDefaultMember() {
        if (!Bug.BugMondrian229Fixed) {
            return;
        }
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Time\" type=\"TimeDimension\" foreignKey=\"time_id\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\" defaultMember=\"[Time].[1997].[Q1].[1]\" >\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
            + "          levelType=\"TimeYears\"/>\n"
            + "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n"
            + "          levelType=\"TimeQuarters\"/>\n"
            + "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n"
            + "          levelType=\"TimeMonths\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>");

        // Check that the grand total is different than when [Time].[1997] is
        // the default member.
        testContext.assertQueryReturns(
            "select from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "21,628");

        // Results of this query agree with MSAS 2000 SP1.
        // The query gives the same results if the default member of [Time]
        // is [Time].[1997] or [Time].[1997].[Q1].[1].
        testContext.assertQueryReturns(
            "select\n"
            + "NON EMPTY Crossjoin({[Time].[1997].[Q2].[4]}, [Customers].[Country].members) on columns,\n"
            + "NON EMPTY [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].children on rows\n"
            + "from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997].[Q2].[4], [Customers].[USA]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Light Beer]}\n"
            + "Row #0: 3\n"
            + "Row #1: 21\n");
    }

    public void testCrossJoinNamedSets1() {
        checkNative(
            3,
            3,
            "with "
            + "SET [ProductChildren] as '[Product].[All Products].children' "
            + "SET [StoreMembers] as '[Store].[Store Country].members' "
            + "select {[Measures].[Store Sales]} on columns, "
            + "non empty crossjoin([ProductChildren], [StoreMembers]) "
            + "on rows from [Sales]");
    }

    public void testCrossJoinNamedSets2() {
        // Make sure maxConstraint settting is high enough
        int minConstraints = 3;
        if (MondrianProperties.instance().MaxConstraints.get()
            < minConstraints)
        {
            propSaver.set(
                MondrianProperties.instance().MaxConstraints, minConstraints);
        }

        checkNative(
            3, 3,
            "with "
            + "SET [ProductChildren] as '{[Product].[All Products].[Drink], "
            + "[Product].[All Products].[Food], "
            + "[Product].[All Products].[Non-Consumable]}' "
            + "SET [StoreChildren] as '[Store].[All Stores].children' "
            + "select {[Measures].[Store Sales]} on columns, "
            + "non empty crossjoin([ProductChildren], [StoreChildren]) on rows from "
            + "[Sales]");
    }

    public void testCrossJoinSetWithDifferentParents() {
        // Verify that only the members explicitly referenced in the set
        // are returned.  Note that different members are referenced in
        // each level in the time dimension.
        checkNative(
            5,
            5,
            "select "
            + "{[Measures].[Unit Sales]} on columns, "
            + "NonEmptyCrossJoin(" + EDUCATION_LEVEL_LEVEL + ".Members, "
            + "{[Time].[1997].[Q1], [Time].[1998].[Q2]}) on rows from Sales");
    }

    public void testCrossJoinSetWithCrossProdMembers() {
        // Make sure maxConstraint settting is high enough
        int minConstraints = 6;
        if (MondrianProperties.instance().MaxConstraints.get()
            < minConstraints)
        {
            propSaver.set(
                MondrianProperties.instance().MaxConstraints, minConstraints);
        }

        // members in set are a cross product of (1997, 1998) and (Q1, Q2, Q3)
        checkNative(
            50, 15,
            "select "
            + "{[Measures].[Unit Sales]} on columns, "
            + "NonEmptyCrossJoin(" + EDUCATION_LEVEL_LEVEL + ".Members, "
            + "{[Time].[1997].[Q1], [Time].[1997].[Q2], [Time].[1997].[Q3], "
            + "[Time].[1998].[Q1], [Time].[1998].[Q2], [Time].[1998].[Q3]})"
            + "on rows from Sales");
    }

    public void testCrossJoinSetWithSameParent() {
        // Make sure maxConstraint settting is high enough
        int minConstraints = 2;
        if (MondrianProperties.instance().MaxConstraints.get()
            < minConstraints)
        {
            propSaver.set(
                MondrianProperties.instance().MaxConstraints, minConstraints);
        }

        // members in set have the same parent
        checkNative(
            10, 10,
            "select "
            + "{[Measures].[Unit Sales]} on columns, "
            + "NonEmptyCrossJoin(" + EDUCATION_LEVEL_LEVEL + ".Members, "
            + "{[Store].[All Stores].[USA].[CA].[Beverly Hills], "
            + "[Store].[All Stores].[USA].[CA].[San Francisco]}) "
            + "on rows from Sales");
    }

    public void testCrossJoinSetWithUniqueLevel() {
        // Make sure maxConstraint settting is high enough
        int minConstraints = 2;
        if (MondrianProperties.instance().MaxConstraints.get()
            < minConstraints)
        {
            propSaver.set(
                MondrianProperties.instance().MaxConstraints, minConstraints);
        }

        // members in set have different parents but there is a unique level
        checkNative(
            10, 10,
            "select "
            + "{[Measures].[Unit Sales]} on columns, "
            + "NonEmptyCrossJoin(" + EDUCATION_LEVEL_LEVEL + ".Members, "
            + "{[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6], "
            + "[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]}) "
            + "on rows from Sales");
    }

    public void testCrossJoinMultiInExprAllMember() {
        checkNative(
            10,
            10,
            "select "
            + "{[Measures].[Unit Sales]} on columns, "
            + "NonEmptyCrossJoin(" + EDUCATION_LEVEL_LEVEL + ".Members, "
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages], "
            + "[Product].[All Products].[Food].[Breakfast Foods]}) "
            + "on rows from Sales");
    }

    public void testCrossJoinEvaluatorContext1() {
        // This test ensures that the proper measure members context is
        // set when evaluating a non-empty cross join.  The context should
        // not include the calculated measure [*TOP_BOTTOM_SET].  If it
        // does, the query will result in an infinite loop because the cross
        // join will try evaluating the calculated member (when it shouldn't)
        // and the calculated member references the cross join, resulting
        // in the loop
        assertQueryReturns(
            "With "
            + "Set [*NATIVE_CJ_SET] as "
            + "'NonEmptyCrossJoin([*BASE_MEMBERS_Store], [*BASE_MEMBERS_Products])' "
            + "Set [*TOP_BOTTOM_SET] as "
            + "'Order([*GENERATED_MEMBERS_Store], ([Measures].[Unit Sales], "
            + "[Product].[All Products].[*TOP_BOTTOM_MEMBER]), BDESC)' "
            + "Set [*BASE_MEMBERS_Store] as '[Store].members' "
            + "Set [*GENERATED_MEMBERS_Store] as 'Generate([*NATIVE_CJ_SET], {[Store].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Products] as "
            + "'{[Product].[All Products].[Food], [Product].[All Products].[Drink], "
            + "[Product].[All Products].[Non-Consumable]}' "
            + "Set [*GENERATED_MEMBERS_Products] as "
            + "'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})' "
            + "Member [Product].[All Products].[*TOP_BOTTOM_MEMBER] as "
            + "'Aggregate([*GENERATED_MEMBERS_Products])'"
            + "Member [Measures].[*TOP_BOTTOM_MEMBER] as 'Rank([Store].CurrentMember,[*TOP_BOTTOM_SET])' "
            + "Member [Store].[All Stores].[*SUBTOTAL_MEMBER_SEL~SUM] as "
            + "'sum(Filter([*GENERATED_MEMBERS_Store], [Measures].[*TOP_BOTTOM_MEMBER] <= 10))'"
            + "Select {[Measures].[Store Cost]} on columns, "
            + "Non Empty Filter(Generate([*NATIVE_CJ_SET], {([Store].CurrentMember)}), "
            + "[Measures].[*TOP_BOTTOM_MEMBER] <= 10) on rows From [Sales]",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Cost]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores]}\n"
            + "{[Store].[USA]}\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "Row #0: 225,627.23\n"
            + "Row #1: 225,627.23\n"
            + "Row #2: 63,530.43\n"
            + "Row #3: 56,772.50\n"
            + "Row #4: 21,948.94\n"
            + "Row #5: 34,823.56\n"
            + "Row #6: 34,823.56\n"
            + "Row #7: 105,324.31\n"
            + "Row #8: 29,959.28\n"
            + "Row #9: 29,959.28\n");
    }

    public void testCrossJoinEvaluatorContext2() {
        // Make sure maxConstraint settting is high enough
        int minConstraints = 2;
        if (MondrianProperties.instance().MaxConstraints.get()
            < minConstraints)
        {
            propSaver.set(
                MondrianProperties.instance().MaxConstraints, minConstraints);
        }

        // calculated measure contains a calculated member
        assertQueryReturns(
            "With Set [*NATIVE_CJ_SET] as "
            + "'NonEmptyCrossJoin([*BASE_MEMBERS_Dates], [*BASE_MEMBERS_Stores])' "
            + "Set [*BASE_MEMBERS_Dates] as '{[Time].[1997].[Q1], [Time].[1997].[Q2]}' "
            + "Set [*GENERATED_MEMBERS_Dates] as "
            + "'Generate([*NATIVE_CJ_SET], {[Time].[Time].CurrentMember})' "
            + "Set [*GENERATED_MEMBERS_Measures] as '{[Measures].[*SUMMARY_METRIC_0]}' "
            + "Set [*BASE_MEMBERS_Stores] as '{[Store].[USA].[CA], [Store].[USA].[WA]}' "
            + "Set [*GENERATED_MEMBERS_Stores] as "
            + "'Generate([*NATIVE_CJ_SET], {[Store].CurrentMember})' "
            + "Member [Time].[Time].[*SM_CTX_SEL] as 'Aggregate([*GENERATED_MEMBERS_Dates])' "
            + "Member [Measures].[*SUMMARY_METRIC_0] as "
            + "'[Measures].[Unit Sales]/([Measures].[Unit Sales],[Time].[*SM_CTX_SEL])', "
            + "FORMAT_STRING = '0.00%' "
            + "Member [Time].[Time].[*SUBTOTAL_MEMBER_SEL~SUM] as 'sum([*GENERATED_MEMBERS_Dates])' "
            + "Member [Store].[*SUBTOTAL_MEMBER_SEL~SUM] as "
            + "'sum(Filter([*GENERATED_MEMBERS_Stores], "
            + "([Measures].[Unit Sales], [Time].[*SUBTOTAL_MEMBER_SEL~SUM]) > 0.0))' "
            + "Select Union "
            + "(CrossJoin "
            + "(Filter "
            + "(Generate([*NATIVE_CJ_SET], {([Time].[Time].CurrentMember)}), "
            + "Not IsEmpty ([Measures].[Unit Sales])), "
            + "[*GENERATED_MEMBERS_Measures]), "
            + "CrossJoin "
            + "(Filter "
            + "({[Time].[*SUBTOTAL_MEMBER_SEL~SUM]}, "
            + "Not IsEmpty ([Measures].[Unit Sales])), "
            + "[*GENERATED_MEMBERS_Measures])) on columns, "
            + "Non Empty Union "
            + "(Filter "
            + "(Filter "
            + "(Generate([*NATIVE_CJ_SET], "
            + "{([Store].CurrentMember)}), "
            + "([Measures].[Unit Sales], "
            + "[Time].[*SUBTOTAL_MEMBER_SEL~SUM]) > 0.0), "
            + "Not IsEmpty ([Measures].[Unit Sales])), "
            + "Filter("
            + "{[Store].[*SUBTOTAL_MEMBER_SEL~SUM]}, "
            + "Not IsEmpty ([Measures].[Unit Sales]))) on rows "
            + "From [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997].[Q1], [Measures].[*SUMMARY_METRIC_0]}\n"
            + "{[Time].[1997].[Q2], [Measures].[*SUMMARY_METRIC_0]}\n"
            + "{[Time].[*SUBTOTAL_MEMBER_SEL~SUM], [Measures].[*SUMMARY_METRIC_0]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "{[Store].[*SUBTOTAL_MEMBER_SEL~SUM]}\n"
            + "Row #0: 48.34%\n"
            + "Row #0: 51.66%\n"
            + "Row #0: 100.00%\n"
            + "Row #1: 50.53%\n"
            + "Row #1: 49.47%\n"
            + "Row #1: 100.00%\n"
            + "Row #2: 49.72%\n"
            + "Row #2: 50.28%\n"
            + "Row #2: 100.00%\n");
    }

    public void testVCNativeCJWithIsEmptyOnMeasure() {
        // Don't use checkNative method here because in the case where
        // native cross join isn't used, the query causes a stack overflow.
        //
        // A measures member is referenced in the IsEmpty() function.  This
        // shouldn't prevent native cross join from being used.
        assertQueryReturns(
            "with "
            + "set BM_PRODUCT as {[Product].[All Products].[Drink]} "
            + "set BM_EDU as [Education Level].[Education Level].Members "
            + "set BM_GENDER as {[Gender].[Gender].[M]} "
            + "set CJ as NonEmptyCrossJoin(BM_GENDER,NonEmptyCrossJoin(BM_EDU,BM_PRODUCT)) "
            + "set GM_PRODUCT as Generate(CJ, {[Product].CurrentMember}) "
            + "set GM_EDU as Generate(CJ, {[Education Level].CurrentMember}) "
            + "set GM_GENDER as Generate(CJ, {[Gender].CurrentMember}) "
            + "set GM_MEASURE as {[Measures].[Unit Sales]} "
            + "member [Education Level].FILTER1 as Aggregate(GM_EDU) "
            + "member [Gender].FILTER2 as Aggregate(GM_GENDER) "
            + "select "
            + "Filter(GM_PRODUCT, Not IsEmpty([Measures].[Unit Sales])) on rows, "
            + "GM_MEASURE on columns "
            + "from [Warehouse and Sales] "
            + "where ([Education Level].FILTER1, [Gender].FILTER2)",
            "Axis #0:\n"
            + "{[Education Level].[FILTER1], [Gender].[FILTER2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "Row #0: 12,395\n");
    }

    public void testVCNativeCJWithTopPercent() {
        // The reference to [Store Sales] inside the topPercent function
        // should not prevent native cross joins from being used
        checkNative(
            92,
            1,
            "select {topPercent(nonemptycrossjoin([Product].[Product Department].members, "
            + "[Time].[1997].children),10,[Measures].[Store Sales])} on columns, "
            + "{[Measures].[Store Sales]} on rows from "
            + "[Warehouse and Sales]");
    }

    public void testVCOrdinalExpression() {
        // [Customers].[Name] is an ordinal expression.  Make sure ordering
        // is done on the column corresponding to that expression.
        checkNative(
            67,
            67,
            "select {[Measures].[Store Sales]} on columns,"
            + "  NON EMPTY Crossjoin([Customers].[Name].Members,"
            + "    [Product].[Product Name].Members) ON rows "
            + " from [Warehouse and Sales] where ("
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],"
            + "  [Time].[1997].[Q1].[1])");
    }

    /**
     * Test for bug #1696772
     * Modified which calculations are tested for non native, non empty joins
     */
    public void testNonEmptyWithCalcMeasure() {
        checkNative(
            15,
            6,
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Store],NonEmptyCrossJoin([*BASE_MEMBERS_Education Level],[*BASE_MEMBERS_Product]))' "
            + "Set [*METRIC_CJ_SET] as 'Filter([*NATIVE_CJ_SET],[Measures].[*Store Sales_SEL~SUM] > 50000.0 And [Measures].[*Unit Sales_SEL~MAX] > 50000.0)' "
            + "Set [*BASE_MEMBERS_Store] as '[Store].[Store Country].Members' "
            + "Set [*NATIVE_MEMBERS_Store] as 'Generate([*NATIVE_CJ_SET], {[Store].CurrentMember})' "
            + "Set [*METRIC_MEMBERS_Store] as 'Generate([*METRIC_CJ_SET], {[Store].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[Store Sales],[Measures].[Unit Sales]}' "
            + "Set [*BASE_MEMBERS_Education Level] as '" + EDUCATION_LEVEL_LEVEL
            + ".Members' "
            + "Set [*NATIVE_MEMBERS_Education Level] as 'Generate([*NATIVE_CJ_SET], {[Education Level].CurrentMember})' "
            + "Set [*METRIC_MEMBERS_Education Level] as 'Generate([*METRIC_CJ_SET], {[Education Level].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Product] as '[Product].[Product Family].Members' "
            + "Set [*NATIVE_MEMBERS_Product] as 'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})' "
            + "Set [*METRIC_MEMBERS_Product] as 'Generate([*METRIC_CJ_SET], {[Product].CurrentMember})' "
            + "Member [Product].[*CTX_METRIC_MEMBER_SEL~SUM] as 'Sum({[Product].[All Products]})' "
            + "Member [Store].[*CTX_METRIC_MEMBER_SEL~SUM] as 'Sum({[Store].[All Stores]})' "
            + "Member [Measures].[*Store Sales_SEL~SUM] as '([Measures].[Store Sales],[Education Level].CurrentMember,[Product].[*CTX_METRIC_MEMBER_SEL~SUM],[Store].[*CTX_METRIC_MEMBER_SEL~SUM])' "
            + "Member [Product].[*CTX_METRIC_MEMBER_SEL~MAX] as 'Max([*NATIVE_MEMBERS_Product])' "
            + "Member [Store].[*CTX_METRIC_MEMBER_SEL~MAX] as 'Max([*NATIVE_MEMBERS_Store])' "
            + "Member [Measures].[*Unit Sales_SEL~MAX] as '([Measures].[Unit Sales],[Education Level].CurrentMember,[Product].[*CTX_METRIC_MEMBER_SEL~MAX],[Store].[*CTX_METRIC_MEMBER_SEL~MAX])' "
            + "Select "
            + "Non Empty CrossJoin(Generate([*METRIC_CJ_SET], {([Store].CurrentMember)}),[*BASE_MEMBERS_Measures]) on columns, "
            + "Non Empty Generate([*METRIC_CJ_SET], {([Education Level].CurrentMember,[Product].CurrentMember)}) on rows "
            + "From [Sales]");
    }

    public void testCalculatedSlicerMember() {
        // This test verifies that members(the FILTER members in the query
        // below) on the slicer are ignored in CrossJoin emptiness check.
        // Otherwise, if they are not ignored, stack over flow will occur
        // because emptiness check depends on a calculated slicer member
        // which references the non-empty set being computed.
        //
        // Bcause native evaluation already ignores calculated members on
        // the slicer, both native and non-native evaluation should return
        // the same result.
        checkNative(
            20,
            1,
            "With "
            + "Set BM_PRODUCT as '{[Product].[All Products].[Drink]}' "
            + "Set BM_EDU as '" + EDUCATION_LEVEL_LEVEL + ".Members' "
            + "Set BM_GENDER as '{[Gender].[Gender].[M]}' "
            + "Set NECJ_SET as 'NonEmptyCrossJoin(BM_GENDER, NonEmptyCrossJoin(BM_EDU,BM_PRODUCT))' "
            + "Set GM_PRODUCT as 'Generate(NECJ_SET, {[Product].CurrentMember})' "
            + "Set GM_EDU as 'Generate(NECJ_SET, {[Education Level].CurrentMember})' "
            + "Set GM_GENDER as 'Generate(NECJ_SET, {[Gender].CurrentMember})' "
            + "Set GM_MEASURE as '{[Measures].[Unit Sales]}' "
            + "Member [Education Level].FILTER1 as 'Aggregate(GM_EDU)' "
            + "Member [Gender].FILTER2 as 'Aggregate(GM_GENDER)' "
            + "Select "
            + "GM_PRODUCT on rows, GM_MEASURE on columns "
            + "From [Sales] Where ([Education Level].FILTER1, [Gender].FILTER2)");
    }

    // next two verify that when NECJ references dimension from slicer,
    // slicer is correctly ignored for purposes of evaluating NECJ emptiness,
    // regardless of whether evaluation is native or non-native

    public void testIndependentSlicerMemberNonNative() {
        checkIndependentSlicerMemberNative(false);
    }

    public void testIndependentSlicerMemberNative() {
        checkIndependentSlicerMemberNative(true);
    }

    private void checkIndependentSlicerMemberNative(boolean useNative) {
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, useNative);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        final TestContext context = getTestContext().withFreshConnection();
        try {
            context.assertQueryReturns(
                "with set [p] as '[Product].[Product Family].members' "
                + "set [s] as '[Store].[Store Country].members' "
                + "set [ne] as 'nonemptycrossjoin([p],[s])' "
                + "set [nep] as 'Generate([ne],{[Product].CurrentMember})' "
                + "select [nep] on columns from sales "
                + "where ([Store].[Store Country].[Mexico])",
                "Axis #0:\n"
                + "{[Store].[Mexico]}\n"
                + "Axis #1:\n"
                + "{[Product].[Drink]}\n"
                + "{[Product].[Food]}\n"
                + "{[Product].[Non-Consumable]}\n"
                + "Row #0: \n"
                + "Row #0: \n"
                + "Row #0: \n");
        } finally {
            context.close();
        }
    }

    public void testDependentSlicerMemberNonNative() {
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, false);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        final TestContext context = getTestContext().withFreshConnection();
        try {
            context.assertQueryReturns(
                "with set [p] as '[Product].[Product Family].members' "
                + "set [s] as '[Store].[Store Country].members' "
                + "set [ne] as 'nonemptycrossjoin([p],[s])' "
                + "set [nep] as 'Generate([ne],{[Product].CurrentMember})' "
                + "select [nep] on columns from sales "
                + "where ([Time].[1998])",
                "Axis #0:\n"
                + "{[Time].[1998]}\n"
                + "Axis #1:\n");
        } finally {
            context.close();
        }
    }

    public void testDependentSlicerMemberNative() {
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        final TestContext context = getTestContext().withFreshConnection();
        try {
            context.assertQueryReturns(
                "with set [p] as '[Product].[Product Family].members' "
                + "set [s] as '[Store].[Store Country].members' "
                + "set [ne] as 'nonemptycrossjoin([p],[s])' "
                + "set [nep] as 'Generate([ne],{[Product].CurrentMember})' "
                + "select [nep] on columns from sales "
                + "where ([Time].[1998])",
                "Axis #0:\n"
                + "{[Time].[1998]}\n"
                + "Axis #1:\n");
        } finally {
            context.close();
        }
    }

    /**
     * Tests bug 1791609, "CrossJoin non empty optimizer eliminates calculated
     * member".
     */
    public void testBug1791609NonEmptyCrossJoinEliminatesCalcMember() {
        if (!Bug.BugMondrian328Fixed) {
            return;
        }
        // From the bug:
        //   With NON EMPTY (mondrian.rolap.nonempty) behavior set to true
        //   the following mdx return no result. The same mdx returns valid
        // result when NON EMPTY is turned off.
        assertQueryReturns(
            "WITH \n"
            + "MEMBER Measures.Calc AS '[Measures].[Profit] * 2', SOLVE_ORDER=1000\n"
            + "MEMBER Product.Conditional as 'Iif (Measures.CurrentMember IS Measures.[Calc], "
            + "Measures.CurrentMember, null)', SOLVE_ORDER=2000\n"
            + "SET [S2] AS '{[Store].MEMBERS}' \n"
            + "SET [S1] AS 'CROSSJOIN({[Customers].[All Customers]},{Product.Conditional})' \n"
            + "SELECT \n"
            + "NON EMPTY GENERATE({Measures.[Calc]}, \n"
            + "          CROSSJOIN(HEAD( {([Measures].CURRENTMEMBER)}, \n"
            + "                           1\n"
            + "                        ), \n"
            + "                     {[S1]}\n"
            + "                  ), \n"
            + "                   ALL\n"
            + "                 ) \n"
            + "                                   ON AXIS(0), \n"
            + "NON EMPTY [S2] ON AXIS(1) \n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Calc], [Customers].[All Customers], [Product].[Conditional]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores]}\n"
            + "{[Store].[USA]}\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "{[Store].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "{[Store].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[USA].[WA].[Walla Walla]}\n"
            + "{[Store].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "{[Store].[USA].[WA].[Yakima].[Store 23]}\n"
            + "Row #0: $679,221.79\n"
            + "Row #1: $679,221.79\n"
            + "Row #2: $191,274.83\n"
            + "Row #3: $54,967.60\n"
            + "Row #4: $54,967.60\n"
            + "Row #5: $65,547.49\n"
            + "Row #6: $65,547.49\n"
            + "Row #7: $65,435.21\n"
            + "Row #8: $65,435.21\n"
            + "Row #9: $5,324.53\n"
            + "Row #10: $5,324.53\n"
            + "Row #11: $171,009.14\n"
            + "Row #12: $66,219.69\n"
            + "Row #13: $66,219.69\n"
            + "Row #14: $104,789.45\n"
            + "Row #15: $104,789.45\n"
            + "Row #16: $316,937.82\n"
            + "Row #17: $5,685.23\n"
            + "Row #18: $5,685.23\n"
            + "Row #19: $63,548.67\n"
            + "Row #20: $63,548.67\n"
            + "Row #21: $63,374.53\n"
            + "Row #22: $63,374.53\n"
            + "Row #23: $59,677.94\n"
            + "Row #24: $59,677.94\n"
            + "Row #25: $89,769.36\n"
            + "Row #26: $89,769.36\n"
            + "Row #27: $5,651.26\n"
            + "Row #28: $5,651.26\n"
            + "Row #29: $29,230.83\n"
            + "Row #30: $29,230.83\n");
    }

    /**
     * Test that executes &lt;Level&gt;.Members and applies a non-empty
     * constraint. Must work regardless of whether
     * {@link MondrianProperties#EnableNativeNonEmpty native} is enabled.
     * Testcase for bug
     * 1722959, "NON EMPTY Level.MEMBERS fails if nonempty.enable=false"
     */
    public void testNonEmptyLevelMembers() {
        boolean currentNativeNonEmpty =
                MondrianProperties.instance().EnableNativeNonEmpty.get();
        boolean currentNonEmptyOnAllAxis =
                MondrianProperties.instance().EnableNonEmptyOnAllAxis.get();
        try {
            MondrianProperties.instance().EnableNativeNonEmpty.set(false);
            MondrianProperties.instance().EnableNonEmptyOnAllAxis.set(true);
            assertQueryReturns(
                "WITH MEMBER [Measures].[One] AS '1' "
                + "SELECT "
                + "NON EMPTY {[Measures].[One], [Measures].[Store Sales]} ON rows, "
                + "NON EMPTY [Store].[Store State].MEMBERS on columns "
                + "FROM sales",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[Canada].[BC]}\n"
                + "{[Store].[Mexico].[DF]}\n"
                + "{[Store].[Mexico].[Guerrero]}\n"
                + "{[Store].[Mexico].[Jalisco]}\n"
                + "{[Store].[Mexico].[Veracruz]}\n"
                + "{[Store].[Mexico].[Yucatan]}\n"
                + "{[Store].[Mexico].[Zacatecas]}\n"
                + "{[Store].[USA].[CA]}\n"
                + "{[Store].[USA].[OR]}\n"
                + "{[Store].[USA].[WA]}\n"
                + "Axis #2:\n"
                + "{[Measures].[One]}\n"
                + "{[Measures].[Store Sales]}\n"
                + "Row #0: 1\n"
                + "Row #0: 1\n"
                + "Row #0: 1\n"
                + "Row #0: 1\n"
                + "Row #0: 1\n"
                + "Row #0: 1\n"
                + "Row #0: 1\n"
                + "Row #0: 1\n"
                + "Row #0: 1\n"
                + "Row #0: 1\n"
                + "Row #1: \n"
                + "Row #1: \n"
                + "Row #1: \n"
                + "Row #1: \n"
                + "Row #1: \n"
                + "Row #1: \n"
                + "Row #1: \n"
                + "Row #1: 159,167.84\n"
                + "Row #1: 142,277.07\n"
                + "Row #1: 263,793.22\n");

            if (Bug.BugMondrian446Fixed) {
                MondrianProperties.instance().EnableNativeNonEmpty.set(true);
                assertQueryReturns(
                    "WITH MEMBER [Measures].[One] AS '1' "
                    + "SELECT "
                    + "NON EMPTY {[Measures].[One], [Measures].[Store Sales]} ON rows, "
                    + "NON EMPTY [Store].[Store State].MEMBERS on columns "
                    + "FROM sales",
                    "Axis #0:\n"
                    + "{}\n"
                    + "Axis #1:\n"
                    + "{[Store].[Canada].[BC]}\n"
                    + "{[Store].[Mexico].[DF]}\n"
                    + "{[Store].[Mexico].[Guerrero]}\n"
                    + "{[Store].[Mexico].[Jalisco]}\n"
                    + "{[Store].[Mexico].[Veracruz]}\n"
                    + "{[Store].[Mexico].[Yucatan]}\n"
                    + "{[Store].[Mexico].[Zacatecas]}\n"
                    + "{[Store].[USA].[CA]}\n"
                    + "{[Store].[USA].[OR]}\n"
                    + "{[Store].[USA].[WA]}\n"
                    + "Axis #2:\n"
                    + "{[Measures].[One]}\n"
                    + "{[Measures].[Store Sales]}\n"
                    + "Row #0: 1\n"
                    + "Row #0: 1\n"
                    + "Row #0: 1\n"
                    + "Row #0: 1\n"
                    + "Row #0: 1\n"
                    + "Row #0: 1\n"
                    + "Row #0: 1\n"
                    + "Row #0: 1\n"
                    + "Row #0: 1\n"
                    + "Row #0: 1\n"
                    + "Row #1: \n"
                    + "Row #1: \n"
                    + "Row #1: \n"
                    + "Row #1: \n"
                    + "Row #1: \n"
                    + "Row #1: \n"
                    + "Row #1: \n"
                    + "Row #1: 159,167.84\n"
                    + "Row #1: 142,277.07\n"
                    + "Row #1: 263,793.22\n");
            }
        } finally {
            MondrianProperties.instance().EnableNativeNonEmpty.set(
                currentNativeNonEmpty);
            MondrianProperties.instance().EnableNonEmptyOnAllAxis.set(
                currentNonEmptyOnAllAxis);
        }
    }

    public void testNonEmptyResults() {
        // This unit test was failing with a NullPointerException in JPivot
        // after the highcardinality feature was added, I've included it
        // here to make sure it continues to work.
        assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales], [Measures].[Store Cost]} ON columns, "
            + "NON EMPTY Filter([Product].[Brand Name].Members, ([Measures].[Unit Sales] > 100000.0)) ON rows "
            + "from [Sales] where [Time].[1997]",
            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "Axis #2:\n");
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-412">
     * MONDRIAN-412, "NON EMPTY and Filter() breaking aggregate
     * calculations"</a>.
     */
    public void testBugMondrian412() {
        TestContext ctx = getTestContext();
        ctx.assertQueryReturns(
            "with member [Measures].[AvgRevenue] as 'Avg([Store].[Store Name].Members, [Measures].[Store Sales])' "
            + "select NON EMPTY {[Measures].[Store Sales], [Measures].[AvgRevenue]} ON COLUMNS, "
            + "NON EMPTY Filter([Store].[Store Name].Members, ([Measures].[AvgRevenue] < [Measures].[Store Sales])) ON ROWS "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[AvgRevenue]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "Row #0: 45,750.24\n"
            + "Row #0: 43,479.86\n"
            + "Row #1: 54,545.28\n"
            + "Row #1: 43,479.86\n"
            + "Row #2: 54,431.14\n"
            + "Row #2: 43,479.86\n"
            + "Row #3: 55,058.79\n"
            + "Row #3: 43,479.86\n"
            + "Row #4: 87,218.28\n"
            + "Row #4: 43,479.86\n"
            + "Row #5: 52,896.30\n"
            + "Row #5: 43,479.86\n"
            + "Row #6: 52,644.07\n"
            + "Row #6: 43,479.86\n"
            + "Row #7: 49,634.46\n"
            + "Row #7: 43,479.86\n"
            + "Row #8: 74,843.96\n"
            + "Row #8: 43,479.86\n");
    }

    public void testNonEmpyOnVirtualCubeWithNonJoiningDimension() {
        assertQueryReturns(
            "select non empty {[Warehouse].[Warehouse name].members} on 0,"
            + "{[Measures].[Units Shipped],[Measures].[Unit Sales]} on 1"
            + " from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Warehouse].[USA].[CA].[Beverly Hills].[Big  Quality Warehouse]}\n"
            + "{[Warehouse].[USA].[CA].[Los Angeles].[Artesia Warehousing, Inc.]}\n"
            + "{[Warehouse].[USA].[CA].[San Diego].[Jorgensen Service Storage]}\n"
            + "{[Warehouse].[USA].[CA].[San Francisco].[Food Service Storage, Inc.]}\n"
            + "{[Warehouse].[USA].[OR].[Portland].[Quality Distribution, Inc.]}\n"
            + "{[Warehouse].[USA].[OR].[Salem].[Treehouse Distribution]}\n"
            + "{[Warehouse].[USA].[WA].[Bellingham].[Foster Products]}\n"
            + "{[Warehouse].[USA].[WA].[Bremerton].[Destination, Inc.]}\n"
            + "{[Warehouse].[USA].[WA].[Seattle].[Quality Warehousing and Trucking]}\n"
            + "{[Warehouse].[USA].[WA].[Spokane].[Jones International]}\n"
            + "{[Warehouse].[USA].[WA].[Tacoma].[Jorge Garcia, Inc.]}\n"
            + "{[Warehouse].[USA].[WA].[Walla Walla].[Valdez Warehousing]}\n"
            + "{[Warehouse].[USA].[WA].[Yakima].[Maddock Stored Foods]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Units Shipped]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 10759.0\n"
            + "Row #0: 24587.0\n"
            + "Row #0: 23835.0\n"
            + "Row #0: 1696.0\n"
            + "Row #0: 8515.0\n"
            + "Row #0: 32393.0\n"
            + "Row #0: 2348.0\n"
            + "Row #0: 22734.0\n"
            + "Row #0: 24110.0\n"
            + "Row #0: 11889.0\n"
            + "Row #0: 32411.0\n"
            + "Row #0: 1860.0\n"
            + "Row #0: 10589.0\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n");
    }

    public void testNonEmptyOnNonJoiningValidMeasure() {
        assertQueryReturns(
            "with member [Measures].[vm] as 'ValidMeasure([Measures].[Unit Sales])'"
            + "select non empty {[Warehouse].[Warehouse name].members} on 0,"
            + "{[Measures].[Units Shipped],[Measures].[vm]} on 1"
            + " from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Warehouse].[USA].[CA].[Beverly Hills].[Big  Quality Warehouse]}\n"
            + "{[Warehouse].[USA].[CA].[Los Angeles].[Artesia Warehousing, Inc.]}\n"
            + "{[Warehouse].[USA].[CA].[San Diego].[Jorgensen Service Storage]}\n"
            + "{[Warehouse].[USA].[CA].[San Francisco].[Food Service Storage, Inc.]}\n"
            + "{[Warehouse].[USA].[OR].[Portland].[Quality Distribution, Inc.]}\n"
            + "{[Warehouse].[USA].[OR].[Salem].[Treehouse Distribution]}\n"
            + "{[Warehouse].[USA].[WA].[Bellingham].[Foster Products]}\n"
            + "{[Warehouse].[USA].[WA].[Bremerton].[Destination, Inc.]}\n"
            + "{[Warehouse].[USA].[WA].[Seattle].[Quality Warehousing and Trucking]}\n"
            + "{[Warehouse].[USA].[WA].[Spokane].[Jones International]}\n"
            + "{[Warehouse].[USA].[WA].[Tacoma].[Jorge Garcia, Inc.]}\n"
            + "{[Warehouse].[USA].[WA].[Walla Walla].[Valdez Warehousing]}\n"
            + "{[Warehouse].[USA].[WA].[Yakima].[Maddock Stored Foods]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Units Shipped]}\n"
            + "{[Measures].[vm]}\n"
            + "Row #0: 10759.0\n"
            + "Row #0: 24587.0\n"
            + "Row #0: 23835.0\n"
            + "Row #0: 1696.0\n"
            + "Row #0: 8515.0\n"
            + "Row #0: 32393.0\n"
            + "Row #0: 2348.0\n"
            + "Row #0: 22734.0\n"
            + "Row #0: 24110.0\n"
            + "Row #0: 11889.0\n"
            + "Row #0: 32411.0\n"
            + "Row #0: 1860.0\n"
            + "Row #0: 10589.0\n"
            + "Row #1: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #1: 266,773\n");
    }

    public void testCrossjoinWithTwoDimensionsJoiningToOppositeBaseCubes() {
        // This test formerly expected an empty result set,
        // which is actually inconsistent with SSAS.  Since ValidMeasure forces
        // Warehouse to the [All] level when evaluating the [vm] measure,
        // the results should include each [warehouse name] member intersected
        // with the non-empty Gender members.
        assertQueryReturns(
            "with member [Measures].[vm] as 'ValidMeasure([Measures].[Unit Sales])'\n"
            + "select non empty Crossjoin([Warehouse].[Warehouse Name].members, [Gender].[Gender].members) on 0,\n"
            + "{[Measures].[Units Shipped],[Measures].[vm]} on 1\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Warehouse].[USA].[CA].[Beverly Hills].[Big  Quality Warehouse], [Gender].[F]}\n"
            + "{[Warehouse].[USA].[CA].[Beverly Hills].[Big  Quality Warehouse], [Gender].[M]}\n"
            + "{[Warehouse].[USA].[CA].[Los Angeles].[Artesia Warehousing, Inc.], [Gender].[F]}\n"
            + "{[Warehouse].[USA].[CA].[Los Angeles].[Artesia Warehousing, Inc.], [Gender].[M]}\n"
            + "{[Warehouse].[USA].[CA].[San Diego].[Jorgensen Service Storage], [Gender].[F]}\n"
            + "{[Warehouse].[USA].[CA].[San Diego].[Jorgensen Service Storage], [Gender].[M]}\n"
            + "{[Warehouse].[USA].[CA].[San Francisco].[Food Service Storage, Inc.], [Gender].[F]}\n"
            + "{[Warehouse].[USA].[CA].[San Francisco].[Food Service Storage, Inc.], [Gender].[M]}\n"
            + "{[Warehouse].[USA].[OR].[Portland].[Quality Distribution, Inc.], [Gender].[F]}\n"
            + "{[Warehouse].[USA].[OR].[Portland].[Quality Distribution, Inc.], [Gender].[M]}\n"
            + "{[Warehouse].[USA].[OR].[Salem].[Treehouse Distribution], [Gender].[F]}\n"
            + "{[Warehouse].[USA].[OR].[Salem].[Treehouse Distribution], [Gender].[M]}\n"
            + "{[Warehouse].[USA].[WA].[Bellingham].[Foster Products], [Gender].[F]}\n"
            + "{[Warehouse].[USA].[WA].[Bellingham].[Foster Products], [Gender].[M]}\n"
            + "{[Warehouse].[USA].[WA].[Bremerton].[Destination, Inc.], [Gender].[F]}\n"
            + "{[Warehouse].[USA].[WA].[Bremerton].[Destination, Inc.], [Gender].[M]}\n"
            + "{[Warehouse].[USA].[WA].[Seattle].[Quality Warehousing and Trucking], [Gender].[F]}\n"
            + "{[Warehouse].[USA].[WA].[Seattle].[Quality Warehousing and Trucking], [Gender].[M]}\n"
            + "{[Warehouse].[USA].[WA].[Spokane].[Jones International], [Gender].[F]}\n"
            + "{[Warehouse].[USA].[WA].[Spokane].[Jones International], [Gender].[M]}\n"
            + "{[Warehouse].[USA].[WA].[Tacoma].[Jorge Garcia, Inc.], [Gender].[F]}\n"
            + "{[Warehouse].[USA].[WA].[Tacoma].[Jorge Garcia, Inc.], [Gender].[M]}\n"
            + "{[Warehouse].[USA].[WA].[Walla Walla].[Valdez Warehousing], [Gender].[F]}\n"
            + "{[Warehouse].[USA].[WA].[Walla Walla].[Valdez Warehousing], [Gender].[M]}\n"
            + "{[Warehouse].[USA].[WA].[Yakima].[Maddock Stored Foods], [Gender].[F]}\n"
            + "{[Warehouse].[USA].[WA].[Yakima].[Maddock Stored Foods], [Gender].[M]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Units Shipped]}\n"
            + "{[Measures].[vm]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #1: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #1: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #1: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #1: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #1: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #1: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #1: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #1: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #1: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #1: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #1: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #1: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #1: 131,558\n"
            + "Row #1: 135,215\n");
    }

    public void testCrossjoinWithOneDimensionThatDoesNotJoinToBothBaseCubes() {
        assertQueryReturns(
            "with member [Measures].[vm] as 'ValidMeasure([Measures].[Units Shipped])'"
            + "select non empty Crossjoin([Store].[Store Name].members, [Gender].[Gender].members) on 0,"
            + "{[Measures].[Unit Sales],[Measures].[vm]} on 1"
            + " from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Beverly Hills].[Store 6], [Gender].[F]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills].[Store 6], [Gender].[M]}\n"
            + "{[Store].[USA].[CA].[Los Angeles].[Store 7], [Gender].[F]}\n"
            + "{[Store].[USA].[CA].[Los Angeles].[Store 7], [Gender].[M]}\n"
            + "{[Store].[USA].[CA].[San Diego].[Store 24], [Gender].[F]}\n"
            + "{[Store].[USA].[CA].[San Diego].[Store 24], [Gender].[M]}\n"
            + "{[Store].[USA].[CA].[San Francisco].[Store 14], [Gender].[F]}\n"
            + "{[Store].[USA].[CA].[San Francisco].[Store 14], [Gender].[M]}\n"
            + "{[Store].[USA].[OR].[Portland].[Store 11], [Gender].[F]}\n"
            + "{[Store].[USA].[OR].[Portland].[Store 11], [Gender].[M]}\n"
            + "{[Store].[USA].[OR].[Salem].[Store 13], [Gender].[F]}\n"
            + "{[Store].[USA].[OR].[Salem].[Store 13], [Gender].[M]}\n"
            + "{[Store].[USA].[WA].[Bellingham].[Store 2], [Gender].[F]}\n"
            + "{[Store].[USA].[WA].[Bellingham].[Store 2], [Gender].[M]}\n"
            + "{[Store].[USA].[WA].[Bremerton].[Store 3], [Gender].[F]}\n"
            + "{[Store].[USA].[WA].[Bremerton].[Store 3], [Gender].[M]}\n"
            + "{[Store].[USA].[WA].[Seattle].[Store 15], [Gender].[F]}\n"
            + "{[Store].[USA].[WA].[Seattle].[Store 15], [Gender].[M]}\n"
            + "{[Store].[USA].[WA].[Spokane].[Store 16], [Gender].[F]}\n"
            + "{[Store].[USA].[WA].[Spokane].[Store 16], [Gender].[M]}\n"
            + "{[Store].[USA].[WA].[Tacoma].[Store 17], [Gender].[F]}\n"
            + "{[Store].[USA].[WA].[Tacoma].[Store 17], [Gender].[M]}\n"
            + "{[Store].[USA].[WA].[Walla Walla].[Store 22], [Gender].[F]}\n"
            + "{[Store].[USA].[WA].[Walla Walla].[Store 22], [Gender].[M]}\n"
            + "{[Store].[USA].[WA].[Yakima].[Store 23], [Gender].[F]}\n"
            + "{[Store].[USA].[WA].[Yakima].[Store 23], [Gender].[M]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[vm]}\n"
            + "Row #0: 10,771\n"
            + "Row #0: 10,562\n"
            + "Row #0: 12,089\n"
            + "Row #0: 13,574\n"
            + "Row #0: 12,835\n"
            + "Row #0: 12,800\n"
            + "Row #0: 1,064\n"
            + "Row #0: 1,053\n"
            + "Row #0: 12,488\n"
            + "Row #0: 13,591\n"
            + "Row #0: 20,548\n"
            + "Row #0: 21,032\n"
            + "Row #0: 1,096\n"
            + "Row #0: 1,141\n"
            + "Row #0: 11,640\n"
            + "Row #0: 12,936\n"
            + "Row #0: 13,513\n"
            + "Row #0: 11,498\n"
            + "Row #0: 12,068\n"
            + "Row #0: 11,523\n"
            + "Row #0: 17,420\n"
            + "Row #0: 17,837\n"
            + "Row #0: 1,019\n"
            + "Row #0: 1,184\n"
            + "Row #0: 5,007\n"
            + "Row #0: 6,484\n"
            + "Row #1: 10759.0\n"
            + "Row #1: 10759.0\n"
            + "Row #1: 24587.0\n"
            + "Row #1: 24587.0\n"
            + "Row #1: 23835.0\n"
            + "Row #1: 23835.0\n"
            + "Row #1: 1696.0\n"
            + "Row #1: 1696.0\n"
            + "Row #1: 8515.0\n"
            + "Row #1: 8515.0\n"
            + "Row #1: 32393.0\n"
            + "Row #1: 32393.0\n"
            + "Row #1: 2348.0\n"
            + "Row #1: 2348.0\n"
            + "Row #1: 22734.0\n"
            + "Row #1: 22734.0\n"
            + "Row #1: 24110.0\n"
            + "Row #1: 24110.0\n"
            + "Row #1: 11889.0\n"
            + "Row #1: 11889.0\n"
            + "Row #1: 32411.0\n"
            + "Row #1: 32411.0\n"
            + "Row #1: 1860.0\n"
            + "Row #1: 1860.0\n"
            + "Row #1: 10589.0\n"
            + "Row #1: 10589.0\n");
    }

    public void testLeafMembersOfParentChildDimensionAreNativelyEvaluated() {
        final String query = "SELECT"
            + " NON EMPTY "
            + "Crossjoin("
            + "{"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin].[Gabriel Walton],"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin].[Bishop Meastas],"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin].[Paula Duran],"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin].[Margaret Earley],"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin].[Elizabeth Horne]"
            + "},"
            + "[Store].[Store Name].members"
            + ") on 0 from hr";
        checkNative(50, 5, query);
    }

    public void testNonLeafMembersOfPCDimensionAreNotNativelyEvaluated() {
        final String query = "SELECT"
            + " NON EMPTY "
            + "Crossjoin("
            + "{"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker],"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin].[Gabriel Walton],"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin],"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays],"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley],"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin].[Elizabeth Horne]"
            + "},"
            + "[Store].[Store Name].members"
            + ") on 0 from hr";
        checkNotNative(9, query);
    }

    public void testNativeWithOverriddenNullMemberRepAndNullConstraint() {
        String preMdx = "SELECT FROM [Sales]";

        String mdx =
            "SELECT \n"
            + "  [Gender].[Gender].MEMBERS ON ROWS\n"
            + " ,{[Measures].[Unit Sales]} ON COLUMNS\n"
            + "FROM [Sales]\n"
            + "WHERE \n"
            + "  [Store Size in SQFT].[All Store Size in SQFTs].[~Missing ]";

        // run an mdx query with the default NullMemberRepresentation
        executeQuery(preMdx);

        propSaver.set(
            MondrianProperties.instance().NullMemberRepresentation,
            "~Missing ");
        propSaver.set(
            MondrianProperties.instance().EnableNonEmptyOnAllAxis,
            true);
        RolapUtil.reloadNullLiteral();
        executeQuery(mdx);
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-321">
     * MONDRIAN-321, "CrossJoin has no nulls when
     * EnableNativeNonEmpty=true"</a>.
     */
    public void testBugMondrian321() {
        assertQueryReturns(
            "WITH SET [#DataSet#] AS 'Crossjoin({Descendants([Customers].[All Customers], 2)}, {[Product].[All Products]})' \n"
            + "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns, \n"
            + "NON EMPTY Hierarchize({[#DataSet#]}) on rows FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA].[CA], [Product].[All Products]}\n"
            + "{[Customers].[USA].[OR], [Product].[All Products]}\n"
            + "{[Customers].[USA].[WA], [Product].[All Products]}\n"
            + "Row #0: 74,748\n"
            + "Row #0: 159,167.84\n"
            + "Row #1: 67,659\n"
            + "Row #1: 142,277.07\n"
            + "Row #2: 124,366\n"
            + "Row #2: 263,793.22\n");

        verifySameNativeAndNot(
                "WITH SET [#DataSet#] AS 'Crossjoin({Descendants([Customers].[All Customers], 2)}, {[Product].[All Products]})' \n"
                        + "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns, \n"
                        + "NON EMPTY Hierarchize({[#DataSet#]}) on rows FROM [Sales]",
                "testBugMondrian321 failed",
                getTestContext()
        );
    }

    public void testNativeCrossjoinWillConstrainUsingArgsFromAllAxes() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx = "select "
            + "non empty Crossjoin({[Gender].[Gender].[F]},{[Measures].[Unit Sales]}) on 0,"
            + "non empty Crossjoin({[Time].[1997]},{[Promotions].[All Promotions].[Bag Stuffers],[Promotions].[All Promotions].[Best Savings]}) on 1"
            + " from [Warehouse and Sales]";
        SqlPattern oraclePattern = new SqlPattern(
            Dialect.DatabaseProduct.ORACLE,
            propSaver.properties.UseAggregates.get()
                ? "select\n"
                + "    \"agg_c_14_sales_fact_1997\".\"the_year\" as \"c0\",\n"
                + "    \"promotion\".\"promotion_name\" as \"c1\"\n"
                + "from\n"
                + "    \"agg_c_14_sales_fact_1997\" \"agg_c_14_sales_fact_1997\",\n"
                + "    \"promotion\" \"promotion\",\n"
                + "    \"customer\" \"customer\"\n"
                + "where\n"
                + "    \"agg_c_14_sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
                + "and\n"
                + "    \"agg_c_14_sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
                + "and\n"
                + "    (\"customer\".\"gender\" = 'F')\n"
                + "and\n"
                + "    (\"agg_c_14_sales_fact_1997\".\"the_year\" = 1997)\n"
                + "and\n"
                + "    (\"promotion\".\"promotion_name\" in ('Bag Stuffers', 'Best Savings'))\n"
                + "group by\n"
                + "    \"agg_c_14_sales_fact_1997\".\"the_year\",\n"
                + "    \"promotion\".\"promotion_name\"\n"
                + "order by\n"
                + "    \"agg_c_14_sales_fact_1997\".\"the_year\" ASC NULLS LAST,\n"
                + "    \"promotion\".\"promotion_name\" ASC NULLS LAST"
                : "select\n"
                + "    \"time_by_day\".\"the_year\" as \"c0\",\n"
                + "    \"promotion\".\"promotion_name\" as \"c1\"\n"
                + "from\n"
                + "    \"time_by_day\" \"time_by_day\",\n"
                + "    \"sales_fact_1997\" \"sales_fact_1997\",\n"
                + "    \"promotion\" \"promotion\",\n"
                + "    \"customer\" \"customer\"\n"
                + "where\n"
                + "    \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
                + "and\n"
                + "    \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
                + "and\n"
                + "    \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
                + "and\n"
                + "    (\"customer\".\"gender\" = 'F')\n"
                + "and\n"
                + "    (\"time_by_day\".\"the_year\" = 1997)\n"
                + "and\n"
                + "    (\"promotion\".\"promotion_name\" in ('Bag Stuffers', 'Best Savings'))\n"
                + "group by\n"
                + "    \"time_by_day\".\"the_year\",\n"
                + "    \"promotion\".\"promotion_name\"\n"
                + "order by\n"
                + "    \"time_by_day\".\"the_year\" ASC NULLS LAST,\n"
                + "    \"promotion\".\"promotion_name\" ASC NULLS LAST",
            611);
        assertQuerySql(mdx, new SqlPattern[]{oraclePattern});
    }

    public void testLevelMembersWillConstrainUsingArgsFromAllAxes() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx = "select "
            + "non empty Crossjoin({[Gender].[Gender].[F]},{[Measures].[Unit Sales]}) on 0,"
            + "non empty [Promotions].[Promotions].members on 1"
            + " from [Warehouse and Sales]";
        SqlPattern oraclePattern = new SqlPattern(
            Dialect.DatabaseProduct.ORACLE,
            propSaver.properties.UseAggregates.get()
                ? "select\n"
                + "    \"promotion\".\"promotion_name\" as \"c0\"\n"
                + "from\n"
                + "    \"promotion\" \"promotion\",\n"
                + "    \"agg_c_14_sales_fact_1997\" \"agg_c_14_sales_fact_1997\",\n"
                + "    \"customer\" \"customer\"\n"
                + "where\n"
                + "    \"agg_c_14_sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
                + "and\n"
                + "    \"agg_c_14_sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
                + "and\n"
                + "    (\"customer\".\"gender\" = 'F')\n"
                + "group by\n"
                + "    \"promotion\".\"promotion_name\"\n"
                + "order by\n"
                + "    \"promotion\".\"promotion_name\" ASC NULLS LAST"
                : "select\n"
                + "    \"promotion\".\"promotion_name\" as \"c0\"\n"
                + "from\n"
                + "    \"promotion\" \"promotion\",\n"
                + "    \"sales_fact_1997\" \"sales_fact_1997\",\n"
                + "    \"customer\" \"customer\"\n"
                + "where\n"
                + "    \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
                + "and\n"
                + "    \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
                + "and\n"
                + "    (\"customer\".\"gender\" = 'F')\n"
                + "group by\n"
                + "    \"promotion\".\"promotion_name\"\n"
                + "order by\n"
                + "    \"promotion\".\"promotion_name\" ASC NULLS LAST",
            347);
        assertQuerySql(mdx, new SqlPattern[]{oraclePattern});
    }

    public void testNativeCrossjoinWillExpandFirstLastChild() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx = "select "
            + "non empty Crossjoin({[Gender].firstChild,[Gender].lastChild},{[Measures].[Unit Sales]}) on 0,"
            + "non empty Crossjoin({[Time].[1997]},{[Promotions].[All Promotions].[Bag Stuffers],[Promotions].[All Promotions].[Best Savings]}) on 1"
            + " from [Warehouse and Sales]";
        final SqlPattern pattern = new SqlPattern(
            Dialect.DatabaseProduct.ORACLE,
            propSaver.properties.UseAggregates.get()
                ? "select\n"
                + "    \"agg_c_14_sales_fact_1997\".\"the_year\" as \"c0\",\n"
                + "    \"promotion\".\"promotion_name\" as \"c1\"\n"
                + "from\n"
                + "    \"agg_c_14_sales_fact_1997\" \"agg_c_14_sales_fact_1997\",\n"
                + "    \"promotion\" \"promotion\",\n"
                + "    \"customer\" \"customer\"\n"
                + "where\n"
                + "    \"agg_c_14_sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
                + "and\n"
                + "    \"agg_c_14_sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
                + "and\n"
                + "    (\"customer\".\"gender\" in ('F', 'M'))\n"
                + "and\n"
                + "    (\"agg_c_14_sales_fact_1997\".\"the_year\" = 1997)\n"
                + "and\n"
                + "    (\"promotion\".\"promotion_name\" in ('Bag Stuffers', 'Best Savings'))\n"
                + "group by\n"
                + "    \"agg_c_14_sales_fact_1997\".\"the_year\",\n"
                + "    \"promotion\".\"promotion_name\"\n"
                + "order by\n"
                + "    \"agg_c_14_sales_fact_1997\".\"the_year\" ASC NULLS LAST,\n"
                + "    \"promotion\".\"promotion_name\" ASC NULLS LAST"
                : "select\n"
                + "    \"time_by_day\".\"the_year\" as \"c0\",\n"
                + "    \"promotion\".\"promotion_name\" as \"c1\"\n"
                + "from\n"
                + "    \"time_by_day\" \"time_by_day\",\n"
                + "    \"sales_fact_1997\" \"sales_fact_1997\",\n"
                + "    \"promotion\" \"promotion\",\n"
                + "    \"customer\" \"customer\"\n"
                + "where\n"
                + "    \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
                + "and\n"
                + "    \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
                + "and\n"
                + "    \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
                + "and\n"
                + "    (\"customer\".\"gender\" in ('F', 'M'))\n"
                + "and\n"
                + "    (\"time_by_day\".\"the_year\" = 1997)\n"
                + "and\n"
                + "    (\"promotion\".\"promotion_name\" in ('Bag Stuffers', 'Best Savings'))\n"
                + "group by\n"
                + "    \"time_by_day\".\"the_year\",\n"
                + "    \"promotion\".\"promotion_name\"\n"
                + "order by\n"
                + "    \"time_by_day\".\"the_year\" ASC NULLS LAST,\n"
                + "    \"promotion\".\"promotion_name\" ASC NULLS LAST",
            611);
        assertQuerySql(mdx, new SqlPattern[]{pattern});
    }

    public void testNativeCrossjoinWillExpandLagInNamedSet() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "with set [blah] as '{[Gender].lastChild.lag(1),[Gender].[M]}' "
            + "select "
            + "non empty Crossjoin([blah],{[Measures].[Unit Sales]}) on 0,"
            + "non empty Crossjoin({[Time].[1997]},{[Promotions].[All Promotions].[Bag Stuffers],[Promotions].[All Promotions].[Best Savings]}) on 1"
            + " from [Warehouse and Sales]";
        final SqlPattern pattern = new SqlPattern(
            Dialect.DatabaseProduct.ORACLE,
            propSaver.properties.UseAggregates.get()
                ? "select\n"
                + "    \"agg_c_14_sales_fact_1997\".\"the_year\" as \"c0\",\n"
                + "    \"promotion\".\"promotion_name\" as \"c1\"\n"
                + "from\n"
                + "    \"agg_c_14_sales_fact_1997\" \"agg_c_14_sales_fact_1997\",\n"
                + "    \"promotion\" \"promotion\",\n"
                + "    \"customer\" \"customer\"\n"
                + "where\n"
                + "    \"agg_c_14_sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
                + "and\n"
                + "    \"agg_c_14_sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
                + "and\n"
                + "    (\"customer\".\"gender\" in ('F', 'M'))\n"
                + "and\n"
                + "    (\"agg_c_14_sales_fact_1997\".\"the_year\" = 1997)\n"
                + "and\n"
                + "    (\"promotion\".\"promotion_name\" in ('Bag Stuffers', 'Best Savings'))\n"
                + "group by\n"
                + "    \"agg_c_14_sales_fact_1997\".\"the_year\",\n"
                + "    \"promotion\".\"promotion_name\"\n"
                + "order by\n"
                + "    \"agg_c_14_sales_fact_1997\".\"the_year\" ASC NULLS LAST,\n"
                + "    \"promotion\".\"promotion_name\" ASC NULLS LAST"
                : "select\n"
                + "    \"time_by_day\".\"the_year\" as \"c0\",\n"
                + "    \"promotion\".\"promotion_name\" as \"c1\"\n"
                + "from\n"
                + "    \"time_by_day\" \"time_by_day\",\n"
                + "    \"sales_fact_1997\" \"sales_fact_1997\",\n"
                + "    \"promotion\" \"promotion\",\n"
                + "    \"customer\" \"customer\"\n"
                + "where\n"
                + "    \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
                + "and\n"
                + "    \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
                + "and\n"
                + "    \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
                + "and\n"
                + "    (\"customer\".\"gender\" in ('F', 'M'))\n"
                + "and\n"
                + "    (\"time_by_day\".\"the_year\" = 1997)\n"
                + "and\n"
                + "    (\"promotion\".\"promotion_name\" in ('Bag Stuffers', 'Best Savings'))\n"
                + "group by\n"
                + "    \"time_by_day\".\"the_year\",\n"
                + "    \"promotion\".\"promotion_name\"\n"
                + "order by\n"
                + "    \"time_by_day\".\"the_year\" ASC NULLS LAST,\n"
                + "    \"promotion\".\"promotion_name\" ASC NULLS LAST",
            611);
        assertQuerySql(mdx, new SqlPattern[]{pattern});
    }

    public void testConstrainedMeasureGetsOptimized() {
        String mdx =
            "with member [Measures].[unit sales Male] as '([Measures].[Unit Sales],[Gender].[Gender].[M])' "
            + "member [Measures].[unit sales Female] as '([Measures].[Unit Sales],[Gender].[Gender].[F])' "
            + "member [Measures].[store sales Female] as '([Measures].[Store Sales],[Gender].[Gender].[F])' "
            + "member [Measures].[literal one] as '1' "
            + "select "
            + "non empty {{[Measures].[unit sales Male]}, {([Measures].[literal one])}, "
            + "[Measures].[unit sales Female], [Measures].[store sales Female]} on 0, "
            + "non empty [Customers].[name].members on 1 "
            + "from Sales";
        final String sqlOracle =
            MondrianProperties.instance().UseAggregates.get()
                ? "select \"customer\".\"country\" as \"c0\","
                    + " \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", \"fname\" || ' ' || \"lname\" as \"c4\", \"fname\" || ' ' || \"lname\" as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" \"customer\", \"agg_l_03_sales_fact_1997\" \"agg_l_03_sales_fact_1997\" where \"agg_l_03_sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and (\"customer\".\"gender\" in ('M', 'F')) group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", \"fname\" || ' ' || \"lname\", \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, \"fname\" || ' ' || \"lname\" ASC NULLS LAST"
                : "select \"customer\".\"country\" as \"c0\","
                    + " \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", \"fname\" || ' ' || \"lname\" as \"c4\", \"fname\" || ' ' || \"lname\" as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and (\"customer\".\"gender\" in ('M', 'F')) group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", \"fname\" || ' ' || \"lname\", \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, \"fname\" || ' ' || \"lname\" ASC NULLS LAST";
        assertQuerySql(
            mdx,
            new SqlPattern[]{
                new SqlPattern(
                    Dialect.DatabaseProduct.ORACLE,
                    sqlOracle,
                    sqlOracle.length())});
    }

    public void testNestedMeasureConstraintsGetOptimized() {
        String mdx =
            "with member [Measures].[unit sales Male] as '([Measures].[Unit Sales],[Gender].[Gender].[M])' "
            + "member [Measures].[unit sales Male Married] as '([Measures].[unit sales Male],[Marital Status].[Marital Status].[M])' "
            + "select "
            + "non empty {[Measures].[unit sales Male Married]} on 0, "
            + "non empty [Customers].[name].members on 1 "
            + "from Sales";
        final String sqlOracle =
            MondrianProperties.instance().UseAggregates.get()
                ? "select \"customer\".\"country\" as \"c0\","
                + " \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", \"fname\" || ' ' || \"lname\" as \"c4\", \"fname\" || ' ' || \"lname\" as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" \"customer\", \"agg_l_03_sales_fact_1997\" \"agg_l_03_sales_fact_1997\" where \"agg_l_03_sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and (\"customer\".\"gender\" = 'M') and (\"customer\".\"marital_status\" = 'M') group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", \"fname\" || ' ' || \"lname\", \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, \"fname\" || ' ' || \"lname\" ASC NULLS LAST"
                : "select \"customer\".\"country\" as \"c0\", "
                + "\"customer\".\"state_province\" as \"c1\", "
                + "\"customer\".\"city\" as \"c2\", "
                + "\"customer\".\"customer_id\" as \"c3\", "
                + "\"fname\" || \" \" || \"lname\" as \"c4\", "
                + "\"fname\" || \" \" || \"lname\" as \"c5\", "
                + "\"customer\".\"gender\" as \"c6\", "
                + "\"customer\".\"marital_status\" as \"c7\", "
                + "\"customer\".\"education\" as \"c8\", "
                + "\"customer\".\"yearly_income\" as \"c9\" "
                + "from \"customer\" \"customer\", "
                + "\"sales_fact_1997\" \"sales_fact_1997\" "
                + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "and (\"customer\".\"gender\" = \"M\") "
                + "and (\"customer\".\"marital_status\" = \"M\") "
                + "group by \"customer\".\"country\", "
                + "\"customer\".\"state_province\", "
                + "\"customer\".\"city\", "
                + "\"customer\".\"customer_id\", "
                + "\"fname\" || \" \" || \"lname\", "
                + "\"customer\".\"gender\", "
                + "\"customer\".\"marital_status\", "
                + "\"customer\".\"education\", "
                + "\"customer\".\"yearly_income\" "
                + "order by \"customer\".\"country\" ASC NULLS LAST, "
                + "\"customer\".\"state_province\" ASC NULLS LAST, "
                + "\"customer\".\"city\" ASC NULLS LAST, "
                + "\"fname\" || \" \" || \"lname\" ASC NULLS LAST";
        SqlPattern pattern = new SqlPattern(
            Dialect.DatabaseProduct.ORACLE,
            sqlOracle,
            sqlOracle.length());
        assertQuerySql(mdx, new SqlPattern[]{pattern});
    }

    public void testNonUniformNestedMeasureConstraintsGetOptimized() {
        if (MondrianProperties.instance().UseAggregates.get()) {
            // This test can't work with aggregates becaused
            // the aggregate table doesn't include member properties.
            return;
        }
        String mdx =
            "with member [Measures].[unit sales Male] as '([Measures].[Unit Sales],[Gender].[Gender].[M])' "
            + "member [Measures].[unit sales Female] as '([Measures].[Unit Sales],[Gender].[Gender].[F])' "
            + "member [Measures].[unit sales Male Married] as '([Measures].[unit sales Male],[Marital Status].[Marital Status].[M])' "
            + "select "
            + "non empty {[Measures].[unit sales Male Married],[Measures].[unit sales Female]} on 0, "
            + "non empty [Customers].[name].members on 1 "
            + "from Sales";
        final SqlPattern pattern = new SqlPattern(
            Dialect.DatabaseProduct.ORACLE,
            "select \"customer\".\"country\" as \"c0\", "
            + "\"customer\".\"state_province\" as \"c1\", "
            + "\"customer\".\"city\" as \"c2\", "
            + "\"customer\".\"customer_id\" as \"c3\", "
            + "\"fname\" || ' ' || \"lname\" as \"c4\", "
            + "\"fname\" || ' ' || \"lname\" as \"c5\", "
            + "\"customer\".\"gender\" as \"c6\", "
            + "\"customer\".\"marital_status\" as \"c7\", "
            + "\"customer\".\"education\" as \"c8\", "
            + "\"customer\".\"yearly_income\" as \"c9\" "
            + "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" "
            + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
            + "and (\"customer\".\"gender\" in ('M', 'F')) "
            + "group by \"customer\".\"country\", "
            + "\"customer\".\"state_province\", "
            + "\"customer\".\"city\", "
            + "\"customer\".\"customer_id\", "
            + "\"fname\" || ' ' || \"lname\", "
            + "\"customer\".\"gender\", "
            + "\"customer\".\"marital_status\", "
            + "\"customer\".\"education\", "
            + "\"customer\".\"yearly_income\" "
            + "order by \"customer\".\"country\" ASC NULLS LAST,"
            + " \"customer\".\"state_province\" ASC NULLS LAST,"
            + " \"customer\".\"city\" ASC NULLS LAST, "
            + "\"fname\" || ' ' || \"lname\" ASC NULLS LAST",
            852);
        assertQuerySql(mdx, new SqlPattern[]{pattern});
    }

    public void testNonUniformConstraintsAreNotUsedForOptimization() {
        String mdx =
            "with member [Measures].[unit sales Male] as '([Measures].[Unit Sales],[Gender].[Gender].[M])' "
            + "member [Measures].[unit sales Married] as '([Measures].[Unit Sales],[Marital Status].[Marital Status].[M])' "
            + "select "
            + "non empty {[Measures].[unit sales Male], [Measures].[unit sales Married]} on 0, "
            + "non empty [Customers].[name].members on 1 "
            + "from Sales";
        final String sqlOracle =
            "select \"customer\".\"country\" as \"c0\", \"customer\".\"state_province\" as \"c1\", \"customer\".\"city\" as \"c2\", \"customer\".\"customer_id\" as \"c3\", \"fname\" || ' ' || \"lname\" as \"c4\", \"fname\" || ' ' || \"lname\" as \"c5\", \"customer\".\"gender\" as \"c6\", \"customer\".\"marital_status\" as \"c7\", \"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and (\"customer\".\"gender\" in ('M', 'F')) group by \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\", \"customer\".\"customer_id\", \"fname\" || ' ' || \"lname\", \"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", \"customer\".\"yearly_income\" order by \"customer\".\"country\" ASC NULLS LAST, \"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, \"fname\" || ' ' || \"lname\" ASC NULLS LAST";
        final SqlPattern pattern = new SqlPattern(
            Dialect.DatabaseProduct.ORACLE,
            sqlOracle,
            sqlOracle.length());
        assertQuerySqlOrNot(
            getTestContext(), mdx, new SqlPattern[]{pattern},true, false, true);
    }

    public void testMeasureConstraintsInACrossjoinHaveCorrectResults() {
        //http://jira.pentaho.com/browse/MONDRIAN-715
        propSaver.set(MondrianProperties.instance().EnableNativeNonEmpty, true);
        String mdx =
            "with "
            + "  member [Measures].[aa] as '([Measures].[Store Cost],[Gender].[M])'"
            + "  member [Measures].[bb] as '([Measures].[Store Cost],[Gender].[F])'"
            + " select"
            + "  non empty "
            + "  crossjoin({[Store].[All Stores].[USA].[CA]},"
            + "      {[Measures].[aa], [Measures].[bb]}) on columns,"
            + "  non empty "
            + "  [Marital Status].[Marital Status].members on rows"
            + " from sales";
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA], [Measures].[aa]}\n"
            + "{[Store].[USA].[CA], [Measures].[bb]}\n"
            + "Axis #2:\n"
            + "{[Marital Status].[M]}\n"
            + "{[Marital Status].[S]}\n"
            + "Row #0: 15,339.94\n"
            + "Row #0: 15,941.98\n"
            + "Row #1: 16,598.87\n"
            + "Row #1: 15,649.64\n");
    }

    public void testContextAtAllWorksWithConstraint() {
        TestContext ctx = TestContext.instance().create(
            null,
            "<Cube name=\"onlyGender\"> \n"
            + "  <Table name=\"sales_fact_1997\"/> \n"
            + "<Dimension name=\"Gender\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Gender\" primaryKey=\"customer_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"/> \n"
            + "</Cube> \n",
            null,
            null,
            null,
            null);

        String mdx =
            " select "
            + " NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS, "
            + " NON EMPTY {[Gender].[Gender].Members} ON ROWS "
            + " from [onlyGender] ";
        ctx.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: 131,558\n"
            + "Row #1: 135,215\n");
    }

    /***
     * Before the fix this test would throw an IndexOutOfBounds exception
     * in SqlConstraintUtils.removeDefaultMembers.  The method assumed that the
     * first member in the list would exist and be a measure.  But, when the
     * default measure is calculated, it would have already been removed from
     * the list by removeCalculatedMembers, and thus the assumption was wrong.
     */
    public void testCalculatedDefaultMeasureOnVirtualCubeNoThrowException() {
        propSaver.set(MondrianProperties.instance().EnableNativeNonEmpty, true);
        final TestContext context =
            TestContext.instance().withSchema(
                "<Schema name=\"FoodMart\">"
                + "  <Dimension name=\"Store\">"
                + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">"
                + "      <Table name=\"store\" />"
                + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\" />"
                + "      <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"true\" />"
                + "      <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\" />"
                + "      <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\">"
                + "        <Property name=\"Store Type\" column=\"store_type\" />"
                + "        <Property name=\"Store Manager\" column=\"store_manager\" />"
                + "        <Property name=\"Store Sqft\" column=\"store_sqft\" type=\"Numeric\" />"
                + "        <Property name=\"Grocery Sqft\" column=\"grocery_sqft\" type=\"Numeric\" />"
                + "        <Property name=\"Frozen Sqft\" column=\"frozen_sqft\" type=\"Numeric\" />"
                + "        <Property name=\"Meat Sqft\" column=\"meat_sqft\" type=\"Numeric\" />"
                + "        <Property name=\"Has coffee bar\" column=\"coffee_bar\" type=\"Boolean\" />"
                + "        <Property name=\"Street address\" column=\"store_street_address\" type=\"String\" />"
                + "      </Level>"
                + "    </Hierarchy>"
                + "  </Dimension>"
                + "  <Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">"
                + "    <Table name=\"sales_fact_1997\" />"
                + "    <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\" />"
                + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\" />"
                + "    <CalculatedMember name=\"dummyMeasure\" dimension=\"Measures\">"
                + "      <Formula>1</Formula>"
                + "    </CalculatedMember>"
                + "  </Cube>"
                + "  <VirtualCube defaultMeasure=\"dummyMeasure\" name=\"virtual\">"
                + "    <VirtualCubeDimension name=\"Store\" />"
                + "    <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\" />"
                + "    <VirtualCubeMeasure name=\"[Measures].[dummyMeasure]\" cubeName=\"Sales\" />"
                + "  </VirtualCube>"
                + "</Schema>");
        context.assertQueryReturns(
            "select "
            + " [Measures].[Unit Sales] on COLUMNS, "
            + " NON EMPTY {[Store].[Store State].Members} ON ROWS "
            + " from [virtual] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "Row #0: 74,748\n"
            + "Row #1: 67,659\n"
            + "Row #2: 124,366\n");
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-734">
     * MONDRIAN-734, "Exception thrown when creating a "New Analysis View" with
     * JPivot"</a>.
     */
    public void testExpandNonNativeWithEnableNativeCrossJoin() {
        final MondrianProperties mondrianProperties =
            MondrianProperties.instance();
        propSaver.set(mondrianProperties.EnableNativeCrossJoin, true);
        propSaver.set(mondrianProperties.ExpandNonNative, true);

        String mdx =
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS,"
            + " NON EMPTY Crossjoin(Hierarchize(Crossjoin({[Store].[All Stores]}, Crossjoin({[Store Size in SQFT].[All Store Size in SQFTs]}, Crossjoin({[Store Type].[All Store Types]}, Union(Crossjoin({[Time].[1997]}, {[Product].[All Products]}), Crossjoin({[Time].[1997]}, [Product].[All Products].Children)))))), {([Promotion Media].[All Media], [Promotions].[All Promotions], [Customers].[All Customers], [Education Level].[All Education Levels], [Gender].[All Gender], [Marital Status].[All Marital Status], [Yearly Income].[All Yearly Incomes])}) ON ROWS"
            + " from [Sales]";
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores], [Store Size in SQFT].[All Store Size in SQFTs], [Store Type].[All Store Types], [Time].[1997], [Product].[All Products], [Promotion Media].[All Media], [Promotions].[All Promotions], [Customers].[All Customers], [Education Level].[All Education Levels], [Gender].[All Gender], [Marital Status].[All Marital Status], [Yearly Income].[All Yearly Incomes]}\n"
            + "{[Store].[All Stores], [Store Size in SQFT].[All Store Size in SQFTs], [Store Type].[All Store Types], [Time].[1997], [Product].[Drink], [Promotion Media].[All Media], [Promotions].[All Promotions], [Customers].[All Customers], [Education Level].[All Education Levels], [Gender].[All Gender], [Marital Status].[All Marital Status], [Yearly Income].[All Yearly Incomes]}\n"
            + "{[Store].[All Stores], [Store Size in SQFT].[All Store Size in SQFTs], [Store Type].[All Store Types], [Time].[1997], [Product].[Food], [Promotion Media].[All Media], [Promotions].[All Promotions], [Customers].[All Customers], [Education Level].[All Education Levels], [Gender].[All Gender], [Marital Status].[All Marital Status], [Yearly Income].[All Yearly Incomes]}\n"
            + "{[Store].[All Stores], [Store Size in SQFT].[All Store Size in SQFTs], [Store Type].[All Store Types], [Time].[1997], [Product].[Non-Consumable], [Promotion Media].[All Media], [Promotions].[All Promotions], [Customers].[All Customers], [Education Level].[All Education Levels], [Gender].[All Gender], [Marital Status].[All Marital Status], [Yearly Income].[All Yearly Incomes]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 24,597\n"
            + "Row #2: 191,940\n"
            + "Row #3: 50,236\n");
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-695">
     * MONDRIAN-695, "Unexpected data set may returned when MDX slicer contains
     * multiple dimensions"</a>.
     */
    public void testNonEmptyCJWithMultiPositionSlicer() {
        final String mdx =
            "select NON EMPTY NonEmptyCrossJoin([Measures].[Sales Count], [Store].[USA].Children) ON COLUMNS, "
            + "       NON EMPTY CrossJoin({[Customers].[All Customers]}, {([Promotions].[Bag Stuffers] : [Promotions].[Bye Bye Baby])}) ON ROWS "
            + "from [Sales Ragged] "
            + "where ({[Product].[Drink]} * {[Time].[1997].[Q1], [Time].[1997].[Q2]})";
        final String expected =
            "Axis #0:\n"
            + "{[Product].[Drink], [Time].[1997].[Q1]}\n"
            + "{[Product].[Drink], [Time].[1997].[Q2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sales Count], [Store].[USA].[CA]}\n"
            + "{[Measures].[Sales Count], [Store].[USA].[USA].[Washington]}\n"
            + "{[Measures].[Sales Count], [Store].[USA].[WA]}\n"
            + "Axis #2:\n"
            + "{[Customers].[All Customers], [Promotions].[Bag Stuffers]}\n"
            + "{[Customers].[All Customers], [Promotions].[Best Savings]}\n"
            + "{[Customers].[All Customers], [Promotions].[Big Promo]}\n"
            + "{[Customers].[All Customers], [Promotions].[Big Time Savings]}\n"
            + "{[Customers].[All Customers], [Promotions].[Bye Bye Baby]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 2\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: 13\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: 9\n"
            + "Row #3: \n"
            + "Row #3: 12\n"
            + "Row #3: \n"
            + "Row #4: 1\n"
            + "Row #4: 21\n"
            + "Row #4: \n";
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin,
            true);
        propSaver.set(
            MondrianProperties.instance().ExpandNonNative,
            true);
        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        checkNative(
            0,
            5,
            mdx,
            expected,
            true);
    }


    SmartMemberReader getSmartMemberReader(String hierName) {
        Connection con = getTestContext().getConnection();
        return getSmartMemberReader(con, hierName);
    }

    SmartMemberReader getSmartMemberReader(Connection con, String hierName) {
        RolapCube cube = (RolapCube) con.getSchema().lookupCube("Sales", true);
        RolapSchemaReader schemaReader =
            (RolapSchemaReader) cube.getSchemaReader();
        RolapHierarchy hierarchy =
            (RolapHierarchy) cube.lookupHierarchy(
                new Id.NameSegment(hierName, Id.Quoting.UNQUOTED),
                false);
        assertNotNull(hierarchy);
        return (SmartMemberReader)
            hierarchy.createMemberReader(schemaReader.getRole());
    }

    SmartMemberReader getSharedSmartMemberReader(String hierName) {
        Connection con = getTestContext().getConnection();
        return getSharedSmartMemberReader(con, hierName);
    }

    SmartMemberReader getSharedSmartMemberReader(
        Connection con, String hierName)
    {
        RolapCube cube = (RolapCube) con.getSchema().lookupCube("Sales", true);
        RolapSchemaReader schemaReader =
            (RolapSchemaReader) cube.getSchemaReader();
        RolapCubeHierarchy hierarchy =
            (RolapCubeHierarchy) cube.lookupHierarchy(
                new Id.NameSegment(hierName, Id.Quoting.UNQUOTED), false);
        assertNotNull(hierarchy);
        return (SmartMemberReader) hierarchy.getRolapHierarchy()
            .createMemberReader(schemaReader.getRole());
    }


    RolapEvaluator getEvaluator(Result res, int[] pos) {
        while (res instanceof NonEmptyResult) {
            res = ((NonEmptyResult) res).underlying;
        }
        return (RolapEvaluator) ((RolapResult) res).getEvaluator(pos);
    }

    public void testFilterChildlessSnowflakeMembers2() {
        if (MondrianProperties.instance().FilterChildlessSnowflakeMembers.get())
        {
            // If FilterChildlessSnowflakeMembers is true, then
            // [Product].[Drink].[Baking Goods].[Coffee] does not even exist!
            return;
        }
        // children across a snowflake boundary
        assertQueryReturns(
            "select [Product].[Drink].[Baking Goods].[Dry Goods].[Coffee].Children on 0\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n");
    }

    public void testFilterChildlessSnowflakeMembers() {
        propSaver.set(
            MondrianProperties.instance().FilterChildlessSnowflakeMembers,
            false);
        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                "select `product_class`.`product_family` as `c0` "
                + "from `product_class` as `product_class` "
                + "group by `product_class`.`product_family` "
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                    ? "order by ISNULL(`c0`) ASC,"
                    + " `c0` ASC"
                    : "order by ISNULL(`product_class`.`product_family`) ASC,"
                    + " `product_class`.`product_family` ASC"),
                null)
        };
        final TestContext context = getTestContext().withFreshConnection();
        try {
            assertQuerySql(
                context,
                "select [Product].[Product Family].Members on 0\n"
                + "from [Sales]",
                patterns);

            // note that returns an extra member,
            // [Product].[Drink].[Baking Goods]
            context.assertQueryReturns(
                "select [Product].[Drink].Children on 0\n"
                + "from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Product].[Drink].[Alcoholic Beverages]}\n"
                + "{[Product].[Drink].[Baking Goods]}\n"
                + "{[Product].[Drink].[Beverages]}\n"
                + "{[Product].[Drink].[Dairy]}\n"
                + "Row #0: 6,838\n"
                + "Row #0: \n"
                + "Row #0: 13,573\n"
                + "Row #0: 4,186\n");

            // [Product].[Drink].[Baking Goods] has one child, but no fact data
            context.assertQueryReturns(
                "select [Product].[Drink].[Baking Goods].Children on 0\n"
                + "from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Product].[Drink].[Baking Goods].[Dry Goods]}\n"
                + "Row #0: \n");

            // NON EMPTY filters out that child
            context.assertQueryReturns(
                "select non empty [Product].[Drink].[Baking Goods].Children on 0\n"
                + "from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n");

            // [Product].[Drink].[Baking Goods].[Dry Goods] has one child, but
            // no fact data
            context.assertQueryReturns(
                "select [Product].[Drink].[Baking Goods].[Dry Goods].Children on 0\n"
                + "from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Product].[Drink].[Baking Goods].[Dry Goods].[Coffee]}\n"
                + "Row #0: \n");

            // NON EMPTY filters out that child
            context.assertQueryReturns(
                "select non empty [Product].[Drink].[Baking Goods].[Dry Goods].Children on 0\n"
                + "from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n");

            // [Coffee] has no children
            context.assertQueryReturns(
                "select [Product].[Drink].[Baking Goods].[Dry Goods].[Coffee].Children on 0\n"
                + "from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n");

            context.assertQueryReturns(
                "select [Measures].[Unit Sales] on 0,\n"
                + " [Product].[Product Family].Members on 1\n"
                + "from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[Unit Sales]}\n"
                + "Axis #2:\n"
                + "{[Product].[Drink]}\n"
                + "{[Product].[Food]}\n"
                + "{[Product].[Non-Consumable]}\n"
                + "Row #0: 24,597\n"
                + "Row #1: 191,940\n"
                + "Row #2: 50,236\n");
        } finally {
            context.close();
        }
    }

    /**
    * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-897">
    * MONDRIAN-897, "ClassCastException in
    * CrossJoinArgFactory.allArgsCheapToExpand when defining a NamedSet as
    * another NamedSet"</a>.
    */
    public void testBugMondrian897DoubleNamedSetDefinitions() {
        TestContext ctx = getTestContext();
        ctx.assertQueryReturns(
            "WITH SET [CustomerSet] as {[Customers].[Canada].[BC].[Burnaby].[Alexandra Wellington], [Customers].[USA].[WA].[Tacoma].[Eric Coleman]} "
            + "SET [InterestingCustomers] as [CustomerSet] "
            + "SET [TimeRange] as {[Time].[1998].[Q1], [Time].[1998].[Q2]} "
            + "SELECT {[Measures].[Store Sales]} ON COLUMNS, "
            + "CrossJoin([InterestingCustomers], [TimeRange]) ON ROWS "
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Customers].[Canada].[BC].[Burnaby].[Alexandra Wellington], [Time].[1998].[Q1]}\n"
            + "{[Customers].[Canada].[BC].[Burnaby].[Alexandra Wellington], [Time].[1998].[Q2]}\n"
            + "{[Customers].[USA].[WA].[Tacoma].[Eric Coleman], [Time].[1998].[Q1]}\n"
            + "{[Customers].[USA].[WA].[Tacoma].[Eric Coleman], [Time].[1998].[Q2]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n"
            + "Row #3: \n");
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1133">MONDRIAN-1133</a>
     *
     * <p>RolapNativeFilter would force the join to the fact table.
     * Some queries don't need to be joined to it and gain in performance.
     */
    public void testMondrian1133() {
        propSaver.set(
            propSaver.properties.UseAggregates,
            false);
        propSaver.set(
            propSaver.properties.ReadAggregates,
            false);
        propSaver.set(
            propSaver.properties.GenerateFormattedSql,
            true);
        final String schema =
            "<?xml version=\"1.0\"?>\n"
            + "<Schema name=\"custom\">\n"
            + "  <Dimension name=\"Store\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\">\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Time\" type=\"TimeDimension\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"time_id\">\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
            + "          levelType=\"TimeYears\"/>\n"
            + "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n"
            + "          levelType=\"TimeQuarters\"/>\n"
            + "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n"
            + "          levelType=\"TimeMonths\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Cube name=\"Sales1\" defaultMeasure=\"Unit Sales\">\n"
            + "    <Table name=\"sales_fact_1997\">\n"
            + "        <AggExclude name=\"agg_c_special_sales_fact_1997\" />"
            + "    </Table>\n"
            + "    <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
            + "    <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "    <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###.00\"/>\n"
            + "    <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###.00\"/>\n"
            + "  </Cube>\n"
            + "<Role name=\"Role1\">\n"
            + "  <SchemaGrant access=\"none\">\n"
            + "    <CubeGrant cube=\"Sales1\" access=\"all\">\n"
            + "      <HierarchyGrant hierarchy=\"[Time]\" access=\"custom\" rollupPolicy=\"partial\">\n"
            + "        <MemberGrant member=\"[Time].[Year].[1997]\" access=\"all\"/>\n"
            + "      </HierarchyGrant>\n"
            + "    </CubeGrant>\n"
            + "  </SchemaGrant>\n"
            + "</Role> \n"
            + "</Schema>\n";

        final String query =
            "With\n"
            + "Set [*BASE_MEMBERS_Product] as 'Filter([Store].[Store State].Members,[Store].CurrentMember.Caption Matches (\"(?i).*CA.*\"))'\n"
            + "Select\n"
            + "[*BASE_MEMBERS_Product] on columns\n"
            + "From [Sales1] \n";

        final String nonEmptyQuery =
            "Select\n"
            + "NON EMPTY Filter([Store].[Store State].Members,[Store].CurrentMember.Caption Matches (\"(?i).*CA.*\")) on columns\n"
            + "From [Sales1] \n";

        final String mysql =
            "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`\n"
            + "having\n"
            + "    c1 IS NOT NULL AND UPPER(c1) REGEXP '.*CA.*'\n"
            + "order by\n"
            + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                + "    ISNULL(`c1`) ASC, `c1` ASC"
                : "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
                + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC");

        final String mysqlWithFactJoin =
            "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`\n"
            + "having\n"
            + "    c1 IS NOT NULL AND UPPER(c1) REGEXP '.*CA.*'\n"
            + "order by\n"
            + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                + "    ISNULL(`c1`) ASC, `c1` ASC"
                : "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
                + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC");

        final String oracle =
            "select\n"
            + "    \"store\".\"store_country\" as \"c0\",\n"
            + "    \"store\".\"store_state\" as \"c1\"\n"
            + "from\n"
            + "    \"store\" \"store\"\n"
            + "group by\n"
            + "    \"store\".\"store_country\",\n"
            + "    \"store\".\"store_state\"\n"
            + "having\n"
            + "    \"store\".\"store_state\" IS NOT NULL AND REGEXP_LIKE(\"store\".\"store_state\", '.*CA.*', 'i')\n"
            + "order by\n"
            + "    \"store\".\"store_country\" ASC NULLS LAST,\n"
            + "    \"store\".\"store_state\" ASC NULLS LAST";

        final String oracleWithFactJoin =
            "select\n"
            + "    \"store\".\"store_country\" as \"c0\",\n"
            + "    \"store\".\"store_state\" as \"c1\"\n"
            + "from\n"
            + "    \"store\" \"store\",\n"
            + "    \"sales_fact_1997\" \"sales_fact_1997\",\n"
            + "    \"time_by_day\" \"time_by_day\"\n"
            + "where\n"
            + "    \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
            + "and\n"
            + "    \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
            + "and\n"
            + "    \"time_by_day\".\"the_year\" = 1997\n"
            + "group by\n"
            + "    \"store\".\"store_country\",\n"
            + "    \"store\".\"store_state\"\n"
            + "having\n"
            + "    \"store\".\"store_state\" IS NOT NULL AND REGEXP_LIKE(\"store\".\"store_state\", '.*CA.*', 'i')\n"
            + "order by\n"
            + "    \"store\".\"store_country\" ASC NULLS LAST,\n"
            + "    \"store\".\"store_state\" ASC NULLS LAST";

        final SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL, mysql, mysql),
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE, oracle, oracle)
        };

        final SqlPattern[] patternsWithFactJoin = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                mysqlWithFactJoin, mysqlWithFactJoin),
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE,
                oracleWithFactJoin, oracleWithFactJoin)
        };

        final TestContext context =
            TestContext.instance().withSchema(schema);

        // The filter condition does not require a join to the fact table.
        assertQuerySql(context, query, patterns);
        assertQuerySql(context.withRole("Role1"), query, patterns);

        // in a non-empty context where a role is in effect, the query
        // will pessimistically join the fact table and apply the
        // constraint, since the filter condition could be influenced by
        // role limitations.
        assertQuerySql(
            context.withRole("Role1"), nonEmptyQuery, patternsWithFactJoin);
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1133">MONDRIAN-1133</a>
     *
     * <p>RolapNativeFilter would force the join to the fact table.
     * Some queries don't need to be joined to it and gain in performance.
     *
     * <p>This one is for agg tables turned on.
     */
    public void testMondrian1133WithAggs() {
        propSaver.set(
            propSaver.properties.UseAggregates,
            true);
        propSaver.set(
            propSaver.properties.ReadAggregates,
            true);
        propSaver.set(
            propSaver.properties.GenerateFormattedSql,
            true);
        final String schema =
            "<?xml version=\"1.0\"?>\n"
            + "<Schema name=\"custom\">\n"
            + "  <Dimension name=\"Store\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\">\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Time\" type=\"TimeDimension\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"time_id\">\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
            + "          levelType=\"TimeYears\"/>\n"
            + "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n"
            + "          levelType=\"TimeQuarters\"/>\n"
            + "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n"
            + "          levelType=\"TimeMonths\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Cube name=\"Sales1\" defaultMeasure=\"Unit Sales\">\n"
            + "    <Table name=\"sales_fact_1997\">\n"
            + "        <AggExclude name=\"agg_c_special_sales_fact_1997\" />"
            + "    </Table>\n"
            + "    <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
            + "    <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "    <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###.00\"/>\n"
            + "    <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###.00\"/>\n"
            + "  </Cube>\n"
            + "<Role name=\"Role1\" >\n"
            + "  <SchemaGrant access=\"none\">\n"
            + "    <CubeGrant cube=\"Sales1\" access=\"all\">\n"
            + "      <HierarchyGrant hierarchy=\"[Time]\" access=\"custom\" rollupPolicy=\"partial\">\n"
            + "        <MemberGrant member=\"[Time].[Year].[1997]\" access=\"all\"/>\n"
            + "      </HierarchyGrant>\n"
            + "    </CubeGrant>\n"
            + "  </SchemaGrant>\n"
            + "</Role> \n"
            + "</Schema>\n";

        final String query =
            "With\n"
            + "Set [*BASE_MEMBERS_Product] as 'Filter([Store].[Store State].Members,[Store].CurrentMember.Caption Matches (\"(?i).*CA.*\"))'\n"
            + "Select\n"
            + "[*BASE_MEMBERS_Product] on columns\n"
            + "From [Sales1] \n";

        final String nonEmptyQuery =
                "Select\n"
                + "NON EMPTY Filter([Store].[Store State].Members,[Store].CurrentMember.Caption Matches (\"(?i).*CA.*\")) on columns\n"
                + "From [Sales1] \n";


        final String mysql =
            "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`\n"
            + "having\n"
            + "    c1 IS NOT NULL AND UPPER(c1) REGEXP '.*CA.*'\n"
            + "order by\n"
            + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                + "    ISNULL(`c1`) ASC, `c1` ASC"
                : "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
                + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC");

        final String mysqlWithFactJoin =
            "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`\n"
            + "where\n"
            + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`\n"
            + "having\n"
            + "    c1 IS NOT NULL AND UPPER(c1) REGEXP '.*CA.*'\n"
            + "order by\n"
            + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "    ISNULL(`c0`) ASC, `c0` ASC,\n"
                + "    ISNULL(`c1`) ASC, `c1` ASC"
                : "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
                + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC");

        final String oracle =
            "select\n"
            + "    \"store\".\"store_country\" as \"c0\",\n"
            + "    \"store\".\"store_state\" as \"c1\"\n"
            + "from\n"
            + "    \"store\" \"store\"\n"
            + "group by\n"
            + "    \"store\".\"store_country\",\n"
            + "    \"store\".\"store_state\"\n"
            + "having\n"
            + "    \"store\".\"store_state\" IS NOT NULL AND REGEXP_LIKE(\"store\".\"store_state\", '.*CA.*', 'i')\n"
            + "order by\n"
            + "    \"store\".\"store_country\" ASC NULLS LAST,\n"
            + "    \"store\".\"store_state\" ASC NULLS LAST";

        final String oracleWithFactJoin =
            "select\n"
            + "    \"store\".\"store_country\" as \"c0\",\n"
            + "    \"store\".\"store_state\" as \"c1\"\n"
            + "from\n"
            + "    \"store\" \"store\",\n"
            + "    \"agg_c_14_sales_fact_1997\" \"agg_c_14_sales_fact_1997\"\n"
            + "where\n"
            + "    \"agg_c_14_sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
            + "and\n"
            + "    \"agg_c_14_sales_fact_1997\".\"the_year\" = 1997\n"
            + "group by\n"
            + "    \"store\".\"store_country\",\n"
            + "    \"store\".\"store_state\"\n"
            + "having\n"
            + "    \"store\".\"store_state\" IS NOT NULL AND REGEXP_LIKE(\"store\".\"store_state\", '.*CA.*', 'i')\n"
            + "order by\n"
            + "    \"store\".\"store_country\" ASC NULLS LAST,\n"
            + "    \"store\".\"store_state\" ASC NULLS LAST";

        final SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL, mysql, mysql),
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE, oracle, oracle)
        };

        final SqlPattern[] patternsWithFactJoin = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                mysqlWithFactJoin, mysqlWithFactJoin),
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE,
                oracleWithFactJoin, oracleWithFactJoin)
        };

        final TestContext context =
            TestContext.instance().withSchema(schema);

        // The filter condition does not require a join to the fact table.
        assertQuerySql(context, query, patterns);
        assertQuerySql(context.withRole("Role1"), query, patterns);

        // in a non-empty context where a role is in effect, the query
        // will pessimistically join the fact table and apply the
        // constraint, since the filter condition could be influenced by
        // role limitations.
        assertQuerySql(
            context.withRole("Role1"), nonEmptyQuery, patternsWithFactJoin);
    }


    /**
     * Native CrossJoin with a ranged slicer.
     */
    public void testNonEmptyAggregateSlicerIsNative() {
        final String mdx =
            "select NON EMPTY\n"
            + " Crossjoin([Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]\n"
            + " , [Customers].[USA].[WA].[Puyallup].Children) ON COLUMNS\n"
            + "from [Sales]\n"
            + "where ([Time].[1997].[Q1].[2] : [Time].[1997].[Q2].[5])";

        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mysqlNativeCrossJoinQuery =
            "select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    `time_by_day`.`quarter` as `c1`,\n"
            + "    `time_by_day`.`month_of_year` as `c2`,\n"
            + "    `product_class`.`product_family` as `c3`,\n"
            + "    `product_class`.`product_department` as `c4`,\n"
            + "    `product_class`.`product_category` as `c5`,\n"
            + "    `product_class`.`product_subcategory` as `c6`,\n"
            + "    `product`.`brand_name` as `c7`,\n"
            + "    `customer`.`customer_id` as `c8`,\n"
            + "    sum(`sales_fact_1997`.`unit_sales`) as `m0`\n"
            + "from\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`,\n"
            + "    `customer` as `customer`\n"
            + "where\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`quarter` in ('Q1', 'Q2')\n"
            + "and\n"
            + "    `time_by_day`.`month_of_year` in (2, 3, 4, 5)\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `product_class`.`product_family` = 'Drink'\n"
            + "and\n"
            + "    `product_class`.`product_department` = 'Alcoholic Beverages'\n"
            + "and\n"
            + "    `product_class`.`product_category` = 'Beer and Wine'\n"
            + "and\n"
            + "    `product_class`.`product_subcategory` = 'Beer'\n"
            + "and\n"
            + "    `product`.`brand_name` = 'Portsmouth'\n"
            + "and\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `customer`.`customer_id` = 5219\n"
            + "group by\n"
            + "    `time_by_day`.`the_year`,\n"
            + "    `time_by_day`.`quarter`,\n"
            + "    `time_by_day`.`month_of_year`,\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`,\n"
            + "    `product_class`.`product_category`,\n"
            + "    `product_class`.`product_subcategory`,\n"
            + "    `product`.`brand_name`,\n"
            + "    `customer`.`customer_id`";
        String triggerSql =
            "select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    `time_by_day`.`quarter` as `c1`,\n"
            + "    `time_by_day`.`month_of_year` as `c2`,\n"
            + "    `product_class`.`product_family` as `c3`,\n";

        if (MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get())
        {
            mysqlNativeCrossJoinQuery =
                "select\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` as `c0`,\n"
                + "    `agg_c_14_sales_fact_1997`.`quarter` as `c1`,\n"
                + "    `agg_c_14_sales_fact_1997`.`month_of_year` as `c2`,\n"
                + "    `product_class`.`product_family` as `c3`,\n"
                + "    `product_class`.`product_department` as `c4`,\n"
                + "    `product_class`.`product_category` as `c5`,\n"
                + "    `product_class`.`product_subcategory` as `c6`,\n"
                + "    `product`.`brand_name` as `c7`,\n"
                + "    `customer`.`customer_id` as `c8`,\n"
                + "    sum(`agg_c_14_sales_fact_1997`.`unit_sales`) as `m0`\n"
                + "from\n"
                + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `customer` as `customer`\n"
                + "where\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`quarter` in ('Q1', 'Q2')\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`month_of_year` in (2, 3, 4, 5)\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `product_class`.`product_family` = 'Drink'\n"
                + "and\n"
                + "    `product_class`.`product_department` = 'Alcoholic Beverages'\n"
                + "and\n"
                + "    `product_class`.`product_category` = 'Beer and Wine'\n"
                + "and\n"
                + "    `product_class`.`product_subcategory` = 'Beer'\n"
                + "and\n"
                + "    `product`.`brand_name` = 'Portsmouth'\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
                + "and\n"
                + "    `customer`.`customer_id` = 5219\n"
                + "group by\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year`,\n"
                + "    `agg_c_14_sales_fact_1997`.`quarter`,\n"
                + "    `agg_c_14_sales_fact_1997`.`month_of_year`,\n"
                + "    `product_class`.`product_family`,\n"
                + "    `product_class`.`product_department`,\n"
                + "    `product_class`.`product_category`,\n"
                + "    `product_class`.`product_subcategory`,\n"
                + "    `product`.`brand_name`,\n"
                + "    `customer`.`customer_id`";
            triggerSql =
                "select\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` as `c0`,\n"
                + "    `agg_c_14_sales_fact_1997`.`quarter` as `c1`,\n"
                + "    `agg_c_14_sales_fact_1997`.`month_of_year` as `c2`,\n"
                + "    `product_class`.`product_family` as `c3`,\n";
        }
        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysqlNativeCrossJoinQuery,
                triggerSql);

        assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});

        checkNative(
            20,
            1,
            "select NON EMPTY\n"
            + " Crossjoin([Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]\n"
            + " , [Customers].[USA].[WA].[Puyallup].Children) ON COLUMNS\n"
            + "from [Sales]\n"
            + "where ([Time].[1997].[Q1].[2] : [Time].[1997].[Q2].[5])",
            "Axis #0:\n"
            + "{[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[1997].[Q2].[5]}\n"
            + "Axis #1:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth], [Customers].[USA].[WA].[Puyallup].[Diane Biondo]}\n"
            + "Row #0: 2\n",
            true);
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1658">MONDRIAN-1658</a>
     *
     * <p>Error: Tuple length does not match arity
     *
     * <p>An empty set argument to crossjoin caused native evaluation to return
     * an incorrect type which in turn caused the types for each argument to
     * union to be different
     *
     */
    public void testMondrian1658() {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, true);
        String mdx =
            "Select\n"
            + "  [Measures].[Unit Sales] on columns,\n"
            + "  Non Empty \n"
            + "  Union(\n"
            + "    {([Gender].[M],[Time].[1997].[Q1])},\n"
            + "      Union(\n"
            + "        CrossJoin({[Gender].[F]},{}),\n"
            + "          {([Gender].[F],[Time].[1997].[Q2])}))\n"
            + "  on rows\n"
            + "From [Sales]\n";
        String expected =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[M], [Time].[1997].[Q1]}\n"
            + "{[Gender].[F], [Time].[1997].[Q2]}\n"
            + "Row #0: 33,381\n"
            + "Row #1: 30,992\n";
        assertQueryReturns(mdx, expected);
    }

    public void testMondrian2202WithConflictingMemberInSlicer() {
        // Validates correct behavior of the crossjoin optimizer and
        // native non empty when a calculated member should override the
        // slicer context.
        // In this case the YTD measure should have a value for each
        // of the 5 [Booker] products, even though not all of them have
        // data in the context of the slicer.

        String[] referencesToTimeMember = new String[] {
            "[Time].[1997].[Q3].[9]",
            "[Time].[Time].CurrentMember",
            "[Time].[1997].[Q4].[10].PrevMember"
        };

        for (String timeMember : referencesToTimeMember) {
            assertQueryReturns(
                "with member [Measures].[YTD Unit Sales] as "
                + "'Sum(Ytd(" + timeMember + "), [Measures].[Unit Sales])'\n"
                + "select\n"
                + "{[Measures].[YTD Unit Sales]}\n"
                + "ON COLUMNS,\n"
                + "NON EMPTY Crossjoin(\n"
                + "{[Customers].[All Customers]}\n"
                + ", [Product].[Drink].[Dairy].[Dairy].[Milk].[Booker].Children) ON ROWS\n"
                + "from [Sales]\n"
                + "where\n"
                + "{ [Time].[1997].[Q3].[9]}",
                "Axis #0:\n"
                + "{[Time].[1997].[Q3].[9]}\n"
                + "Axis #1:\n"
                + "{[Measures].[YTD Unit Sales]}\n"
                + "Axis #2:\n"
                + "{[Customers].[All Customers], [Product].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker 1% Milk]}\n"
                + "{[Customers].[All Customers], [Product].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker 2% Milk]}\n"
                + "{[Customers].[All Customers], [Product].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker Buttermilk]}\n"
                + "{[Customers].[All Customers], [Product].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker Chocolate Milk]}\n"
                + "{[Customers].[All Customers], [Product].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker Whole Milk]}\n"
                + "Row #0: 147\n"
                + "Row #1: 136\n"
                + "Row #2: 84\n"
                + "Row #3: 94\n"
                + "Row #4: 101\n");
        }
    }



    public void testMondrian2202WithCrossjoin() {
        // the [overrideContext] measure should have a value for the tuple
        // on rows, given it overrides the time member on the axis.
        assertQueryReturns(
            "WITH  member measures.[overrideContext] as '( measures.[unit sales], Time.[1997].Q1 )'\n"
            + "SELECT measures.[overrideContext] on 0, \n"
            + "NON EMPTY crossjoin( Time.[1998].Q1, [Marital Status].[M]) on 1\n"
            + "FROM sales\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[overrideContext]}\n"
            + "Axis #2:\n"
            + "{[Time].[1998].[Q1], [Marital Status].[M]}\n"
            + "Row #0: 33,101\n");
        // same thing w/ nonemptycrossjoin().
        assertQueryReturns(
            "WITH  member measures.[overrideContext] as '( measures.[unit sales], Time.[1997].Q1 )'\n"
            + "SELECT measures.[overrideContext] on 0, \n"
            + "NonEmptyCrossjoin( Time.[1998].Q1, [Marital Status].[M]) on 1\n"
            + "FROM sales\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[overrideContext]}\n"
            + "Axis #2:\n"
            + "{[Time].[1998].[Q1], [Marital Status].[M]}\n"
            + "Row #0: 33,101\n");
    }


    public void testMondrian2202WithLevelMembers() {
        // verifies SqlConstraintFactory.getLevelMembersConstraint() doesn't
        // generate a conflicting constraint.  Since CJAF attempts to collect
        // constraints from all axes, it's possible for it to construct
        // a constraint which includes the same hierarchy more than once.
        // In this case it would result in
        //    (year = 1997 AND year = 1998)
        // if potential conflicts aren't removed.
        assertQueryReturns(
            "WITH  member measures.[overrideContext] as '( measures.[unit sales], Time.[1997].Q1 )'\n"
            + "SELECT measures.[overrideContext] on 0, \n"
            + "NON EMPTY [Marital Status].[Marital Status].members on 1,\n"
            + "NON EMPTY Time.[1998].Q1 on 2\n"
            + "FROM sales\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[overrideContext]}\n"
            + "Axis #2:\n"
            + "{[Marital Status].[M]}\n"
            + "{[Marital Status].[S]}\n"
            + "Axis #3:\n"
            + "{[Time].[1998].[Q1]}\n"
            + "Row #0: 33,101\n"
            + "Row #1: 33,190\n");
    }

    public void testMondrian2202WithAggTopCountSet() {
        // in slicer
        assertQueryReturns(
            "with member measures.top5Prod as "
            + "'aggregate(topcount("
            + "crossjoin( {time.[1997]}, product.[product name].members), 5, measures.[unit sales]), measures.[unit sales])'"
            + " select measures.top5Prod on 0, non empty crossjoin({[Marital Status].[M]}, gender.gender.members) on 1"
            + " from sales where time.[1998].[Q1]",
            "Axis #0:\n"
            + "{[Time].[1998].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[top5Prod]}\n"
            + "Axis #2:\n"
            + "{[Marital Status].[M], [Gender].[F]}\n"
            + "{[Marital Status].[M], [Gender].[M]}\n"
            + "Row #0: 398\n"
            + "Row #1: 385\n");
        // in CJ
        assertQueryReturns(
            "with member measures.top5Prod as "
            + "'aggregate(topcount("
            + "crossjoin( {time.[1997]}, product.[product name].members), 5, measures.[unit sales]), measures.[unit sales])'"
            + " select measures.top5Prod on 0, non empty crossjoin({[Time].[1998].[Q1]}, gender.gender.members) on 1"
            + " from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[top5Prod]}\n"
            + "Axis #2:\n"
            + "{[Time].[1998].[Q1], [Gender].[F]}\n"
            + "{[Time].[1998].[Q1], [Gender].[M]}\n"
            + "Row #0: 699\n"
            + "Row #1: 699\n");
    }

    public void testMondrian2202WithParameter() {
        assertQueryReturns(
            "WITH "
            + "member measures.[overrideContext] as "
            + "'( measures.[unit sales], "
            + "Parameter(\"timeParam\",[Time],[Time].[1997].[Q1],\"?\") )'\n"
            + "SELECT measures.[overrideContext] on 0, \n"
            + "NON EMPTY crossjoin( Time.[1998].Q1, [Marital Status].[M]) on 1\n"
            + "FROM sales\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[overrideContext]}\n"
            + "Axis #2:\n"
            + "{[Time].[1998].[Q1], [Marital Status].[M]}\n"
            + "Row #0: 33,101\n");
        assertQueryReturns(
            "WITH member Time.param as 'Parameter(\"timeParam\",[Time],[Time].[1997].[Q1],\"?\")' "
            + "member measures.[overrideContext] as '( measures.[unit sales], Time.param )'\n"
            + "SELECT measures.[overrideContext] on 0, \n"
            + "NON EMPTY crossjoin( Time.[1998].Q1, [Marital Status].[M]) on 1\n"
            + "FROM sales\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[overrideContext]}\n"
            + "Axis #2:\n"
            + "{[Time].[1998].[Q1], [Marital Status].[M]}\n"
            + "Row #0: 33,101\n");
    }

    public void testMondrian2202WithFilter() {
        // Validates correct results when a filtered set contains a member
        // overriden by the filter condition.
        // (This worked before the fix for MONDRIAN-2202, since
        // RolapNativeSql cannot nativize tuple calculations.)
        assertQueryReturns(
            "WITH  member measures.[overrideContext] as "
            + " '( measures.[unit sales], Time.[1997].Q1 )'\n"
            + "SELECT measures.[overrideContext] on 0, \n"
            + "filter ( Crossjoin(Time.[1998].Q1, [Marital Status].[marital status].members), "
            + "measures.[overrideContext] >= 0) on 1\n"
            + "FROM sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[overrideContext]}\n"
            + "Axis #2:\n"
            + "{[Time].[1998].[Q1], [Marital Status].[M]}\n"
            + "{[Time].[1998].[Q1], [Marital Status].[S]}\n"
            + "Row #0: 33,101\n"
            + "Row #1: 33,190\n");
    }

    public void testMondrian2202WithTopCount() {
        // Validates correct results when a topcount set contains a member
        // overriden by the filter condition.
        // (This worked before the fix for MONDRIAN-2202, since
        // RolapNativeSql cannot nativize tuple calculations.)
        assertQueryReturns(
            "WITH  member measures.[overrideContext] as "
            + " '( measures.[unit sales], Time.[1997].Q1 )'\n"
            + "SELECT measures.[overrideContext] on 0, \n"
            + "TopCount ( Crossjoin(Time.[1998].Q1.children, "
            + "[Marital Status].[marital status].members), "
            + "2, measures.[overrideContext] ) on 1\n"
            + "FROM sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[overrideContext]}\n"
            + "Axis #2:\n"
            + "{[Time].[1998].[Q1].[1], [Marital Status].[S]}\n"
            + "{[Time].[1998].[Q1].[2], [Marital Status].[S]}\n"
            + "Row #0: 33,190\n"
            + "Row #1: 33,190\n");
    }

    public void testMondrian2202WithMeasureContainingCJ() {
        // NECJ nested within a measure expression
        assertQueryReturns(
            "with  "
            + " member gender.agg as 'aggregate(NonEmptyCrossJoin({Gender.F}, {[Marital Status].[Marital Status].members}))' "
            + "member measures.lastYear as '(parallelperiod([Time].[Year], 1, [Time].CurrentMember), Measures.[unit sales])' "
            + "member measures.ratioCurrentOverAgg as 'Measures.[Unit Sales] / (gender.agg, Measures.[Unit Sales])' "
            + " select gender.agg on 0, {measures.lastYear, measures.ratioCurrentOverAgg} on 1 from sales where [Time].[1998].[Q2]",
            "Axis #0:\n"
            + "{[Time].[1998].[Q2]}\n"
            + "Axis #1:\n"
            + "{[Gender].[agg]}\n"
            + "Axis #2:\n"
            + "{[Measures].[lastYear]}\n"
            + "{[Measures].[ratioCurrentOverAgg]}\n"
            + "Row #0: 30,992\n"
            + "Row #1: \n");
    }

    public void testMon2202RunningSum() {
        assertQueryReturns(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Time_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Education Level_],[*BASE_MEMBERS__Customers_])))'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_1],[Measures].[*SUMMARY_MEASURE_0]}'\n"
            + "SET [*BASE_MEMBERS__Time_] AS 'FILTER([Time].[Month].MEMBERS,ANCESTOR([Time].CURRENTMEMBER, [Time].[Year]) IN {[Time].[1997]})'\n"
            + "SET [*BASE_MEMBERS__Customers_] AS '{[Customers].[USA].[WA].[Ballard]}'\n"
            + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Education Level].CURRENTMEMBER,[Customers].CURRENTMEMBER)})'\n"
            + "SET [*BASE_MEMBERS__Education Level_] AS '{[Education Level].[Partial College]}'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Time].CURRENTMEMBER)})'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Time].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Time].CURRENTMEMBER,[Time].[Quarter]).ORDERKEY,BASC)'\n"
            + "MEMBER [Measures].[*FORMATTED_MEASURE_1] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n"
            + "MEMBER [Measures].[*SUMMARY_MEASURE_0] AS 'SUM(HEAD([*SORTED_ROW_AXIS],RANK(([Time].CURRENTMEMBER),[*SORTED_ROW_AXIS])),[Measures].[Unit Sales])', SOLVE_ORDER=200\n"
            + "SELECT\n"
            + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            + ",NON EMPTY [*SORTED_ROW_AXIS] ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE ([*CJ_SLICER_AXIS])",
            "Axis #0:\n"
            + "{[Education Level].[Partial College], [Customers].[USA].[WA].[Ballard]}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_1]}\n"
            + "{[Measures].[*SUMMARY_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[1997].[Q4].[11]}\n"
            + "{[Time].[1997].[Q4].[12]}\n"
            + "Row #0: 24\n"
            + "Row #0: 24\n"
            + "Row #1: 11\n"
            + "Row #1: 35\n"
            + "Row #2: \n"
            + "Row #2: 35\n"
            + "Row #3: \n"
            + "Row #3: 35\n"
            + "Row #4: \n"
            + "Row #4: 35\n"
            + "Row #5: 112\n"
            + "Row #5: 147\n"
            + "Row #6: \n"
            + "Row #6: 147\n"
            + "Row #7: 14\n"
            + "Row #7: 161\n"
            + "Row #8: 42\n"
            + "Row #8: 203\n"
            + "Row #9: 56\n"
            + "Row #9: 259\n"
            + "Row #10: \n"
            + "Row #10: 259\n");
    }

    public void testMon2202AnalyzerTopCount() {
        // will throw an exception if native cj is not used.
        propSaver.set(
            propSaver.properties.AlertNativeEvaluationUnsupported, "ERROR");
        executeQuery(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Marital Status_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],[*BASE_MEMBERS__Time_]))'\n"
            + "SET [*METRIC_CJ_SET] AS 'FILTER(FILTER([*NATIVE_CJ_SET],[Measures].[*TOP_Unit Sales_SEL~SUM] <= 3), NOT ISEMPTY ([Measures].[Unit Sales]))'\n"
            + "SET [*BASE_MEMBERS__Marital Status_] AS '[Marital Status].[Marital Status].MEMBERS'\n"
            + "SET [*SORTED_COL_AXIS] AS 'ORDER([*CJ_COL_AXIS],[Marital Status].CURRENTMEMBER.ORDERKEY,BASC)'\n"
            + "SET [*TOP_SET] AS 'ORDER(GENERATE([*NATIVE_CJ_SET],{[Product].CURRENTMEMBER}),([Measures].[Unit Sales],[Marital Status].[*CTX_MEMBER_SEL~SUM],[Time].[*CTX_MEMBER_SEL~AGG]),BDESC)'\n"
            + "SET [*NATIVE_MEMBERS__Time_] AS 'GENERATE([*NATIVE_CJ_SET], {[Time].CURRENTMEMBER})'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "SET [*NATIVE_MEMBERS__Marital Status_] AS 'GENERATE([*NATIVE_CJ_SET], {[Marital Status].CURRENTMEMBER})'\n"
            + "SET [*BASE_MEMBERS__Time_] AS '{[Time].[1997].[Q2].[4],[Time].[1997].[Q1].[2],[Time].[1997].[Q1].[1],[Time].[1997].[Q1].[3]}'\n"
            + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Time].CURRENTMEMBER)})'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Product].CURRENTMEMBER)})'\n"
            + "SET [*BASE_MEMBERS__Product_] AS '[Product].[Brand Name].MEMBERS'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Measures].[*SORTED_MEASURE],BDESC)'\n"
            + "SET [*CJ_COL_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Marital Status].CURRENTMEMBER)})'\n"
            + "MEMBER [Marital Status].[*CTX_MEMBER_SEL~SUM] AS 'SUM([*NATIVE_MEMBERS__Marital Status_])', SOLVE_ORDER=99\n"
            + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n"
            + "MEMBER [Measures].[*SORTED_MEASURE] AS '([Measures].[*FORMATTED_MEASURE_0],[Marital Status].[*CTX_MEMBER_SEL~SUM],[Time].[*CTX_MEMBER_SEL~AGG])', SOLVE_ORDER=400\n"
            + "MEMBER [Measures].[*TOP_Unit Sales_SEL~SUM] AS 'RANK([Product].CURRENTMEMBER,[*TOP_SET])', SOLVE_ORDER=400\n"
            + "MEMBER [Time].[*CTX_MEMBER_SEL~AGG] AS 'AGGREGATE([*NATIVE_MEMBERS__Time_])', SOLVE_ORDER=-302\n"
            + "SELECT\n"
            + "CROSSJOIN([*SORTED_COL_AXIS],[*BASE_MEMBERS__Measures_]) ON COLUMNS\n"
            + ",[*SORTED_ROW_AXIS] ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE ([*CJ_SLICER_AXIS])");
    }

    public void testMon2202AnalyzerFilter() {
        propSaver.set(
            propSaver.properties.AlertNativeEvaluationUnsupported, "ERROR");
        assertQueryReturns(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Education Level_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],[*BASE_MEMBERS__Time_]))'\n"
            + "SET [*METRIC_CJ_SET] AS 'FILTER([*NATIVE_CJ_SET],[Measures].[*Unit Sales_SEL~SUM] > 5.0)'\n"
            + "SET [*NATIVE_MEMBERS__Time_] AS 'GENERATE([*NATIVE_CJ_SET], {[Time].CURRENTMEMBER})'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "SET [*BASE_MEMBERS__Time_] AS '{[Time].[1997].[Q2].[4],[Time].[1997].[Q1].[2],[Time].[1997].[Q1].[1],[Time].[1997].[Q1].[3]}'\n"
            + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Time].CURRENTMEMBER)})'\n"
            + "SET [*BASE_MEMBERS__Education Level_] AS '[Education Level].[Education Level].MEMBERS'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Education Level].CURRENTMEMBER,[Product].CURRENTMEMBER)})'\n"
            + "SET [*BASE_MEMBERS__Product_] AS '{[Product].[Food].[Deli].[Meat].[Deli Meats].[American],[Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best],[Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Education Level].CURRENTMEMBER.ORDERKEY,BASC,[Product].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Product].CURRENTMEMBER,[Product].[Product Subcategory]).ORDERKEY,BASC)'\n"
            + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n"
            + "MEMBER [Measures].[*Unit Sales_SEL~SUM] AS '([Measures].[Unit Sales],[Education Level].CURRENTMEMBER,[Product].CURRENTMEMBER,[Time].[*CTX_MEMBER_SEL~AGG])', SOLVE_ORDER=400\n"
            + "MEMBER [Time].[*CTX_MEMBER_SEL~AGG] AS 'AGGREGATE([*NATIVE_MEMBERS__Time_])', SOLVE_ORDER=-301\n"
            + "SELECT\n"
            + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            + ",NON EMPTY\n"
            + "[*SORTED_ROW_AXIS] ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE ([*CJ_SLICER_AXIS])",
            "Axis #0:\n"
            + "{[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[1997].[Q1].[2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Education Level].[Bachelors Degree], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best]}\n"
            + "{[Education Level].[Bachelors Degree], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "{[Education Level].[Graduate Degree], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[High School Degree], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[High School Degree], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best]}\n"
            + "{[Education Level].[High School Degree], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "{[Education Level].[Partial College], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[Partial High School], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[Partial High School], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best]}\n"
            + "{[Education Level].[Partial High School], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "Row #0: 38\n"
            + "Row #1: 13\n"
            + "Row #2: 28\n"
            + "Row #3: 13\n"
            + "Row #4: 62\n"
            + "Row #5: 13\n"
            + "Row #6: 22\n"
            + "Row #7: 30\n"
            + "Row #8: 68\n"
            + "Row #9: 12\n"
            + "Row #10: 27\n");
    }

    public void testMon2202AnalyzerPercOfMeasure() {
        propSaver.set(
            propSaver.properties.AlertNativeEvaluationUnsupported, "ERROR");
        assertQueryReturns(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Education Level_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],[*BASE_MEMBERS__Time_]))'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*SUMMARY_MEASURE_0]}'\n"
            + "SET [*BASE_MEMBERS__Time_] AS '{[Time].[1997].[Q2].[4],[Time].[1997].[Q1].[2],[Time].[1997].[Q1].[1],[Time].[1997].[Q1].[3]}'\n"
            + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Time].CURRENTMEMBER)})'\n"
            + "SET [*BASE_MEMBERS__Education Level_] AS '[Education Level].[Education Level].MEMBERS'\n"
            + "SET [*NATIVE_MEMBERS__Education Level_] AS 'GENERATE([*NATIVE_CJ_SET], {[Education Level].CURRENTMEMBER})'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Education Level].CURRENTMEMBER,[Product].CURRENTMEMBER)})'\n"
            + "SET [*BASE_MEMBERS__Product_] AS '{[Product].[Food].[Deli].[Meat].[Deli Meats].[American],[Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best],[Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}'\n"
            + "SET [*NATIVE_MEMBERS__Product_] AS 'GENERATE([*NATIVE_CJ_SET], {[Product].CURRENTMEMBER})'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Education Level].CURRENTMEMBER.ORDERKEY,BASC,[Product].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Product].CURRENTMEMBER,[Product].[Product Subcategory]).ORDERKEY,BASC)'\n"
            + "MEMBER [Education Level].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM([*NATIVE_MEMBERS__Education Level_])', SOLVE_ORDER=100\n"
            + "MEMBER [Measures].[*SUMMARY_MEASURE_0] AS '[Measures].[Unit Sales]/([Measures].[Unit Sales],[Education Level].[*TOTAL_MEMBER_SEL~SUM],[Product].[*TOTAL_MEMBER_SEL~SUM])', FORMAT_STRING = '###0.00%', SOLVE_ORDER=200\n"
            + "MEMBER [Product].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM([*NATIVE_MEMBERS__Product_])', SOLVE_ORDER=99\n"
            + "SELECT\n"
            + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            + ",NON EMPTY\n"
            + "[*SORTED_ROW_AXIS] ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE ([*CJ_SLICER_AXIS])",
            "Axis #0:\n"
            + "{[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[1997].[Q1].[2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[*SUMMARY_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Education Level].[Bachelors Degree], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best]}\n"
            + "{[Education Level].[Bachelors Degree], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "{[Education Level].[Graduate Degree], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[Graduate Degree], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "{[Education Level].[High School Degree], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[High School Degree], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best]}\n"
            + "{[Education Level].[High School Degree], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "{[Education Level].[Partial College], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[Partial College], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best]}\n"
            + "{[Education Level].[Partial College], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "{[Education Level].[Partial High School], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[Partial High School], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best]}\n"
            + "{[Education Level].[Partial High School], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "Row #0: 11.34%\n"
            + "Row #1: 3.88%\n"
            + "Row #2: 8.36%\n"
            + "Row #3: 3.88%\n"
            + "Row #4: 0.90%\n"
            + "Row #5: 18.51%\n"
            + "Row #6: 3.88%\n"
            + "Row #7: 6.57%\n"
            + "Row #8: 8.96%\n"
            + "Row #9: 0.90%\n"
            + "Row #10: 0.90%\n"
            + "Row #11: 20.30%\n"
            + "Row #12: 3.58%\n"
            + "Row #13: 8.06%\n");
    }

    public void testMon2202AnalyzerRunningSum() {
        propSaver.set(
            propSaver.properties.AlertNativeEvaluationUnsupported, "ERROR");
        assertQueryReturns(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET] AS 'FILTER(NONEMPTYCROSSJOIN([*BASE_MEMBERS__Education Level_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],[*BASE_MEMBERS__Time_])), NOT ISEMPTY ([Measures].[Unit Sales]))'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_1],[Measures].[*SUMMARY_MEASURE_0]}'\n"
            + "SET [*BASE_MEMBERS__Time_] AS '{[Time].[1997].[Q2].[4],[Time].[1997].[Q1].[2],[Time].[1997].[Q1].[1],[Time].[1997].[Q1].[3]}'\n"
            + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Time].CURRENTMEMBER)})'\n"
            + "SET [*BASE_MEMBERS__Education Level_] AS '[Education Level].[Education Level].MEMBERS'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Education Level].CURRENTMEMBER,[Product].CURRENTMEMBER)})'\n"
            + "SET [*BASE_MEMBERS__Product_] AS '{[Product].[Food].[Deli].[Meat].[Deli Meats].[American],[Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best],[Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Education Level].CURRENTMEMBER.ORDERKEY,BASC,[Product].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Product].CURRENTMEMBER,[Product].[Product Subcategory]).ORDERKEY,BASC)'\n"
            + "MEMBER [Measures].[*FORMATTED_MEASURE_1] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n"
            + "MEMBER [Measures].[*SUMMARY_MEASURE_0] AS 'SUM(HEAD([*SORTED_ROW_AXIS],RANK(([Education Level].CURRENTMEMBER,[Product].CURRENTMEMBER),[*SORTED_ROW_AXIS])),[Measures].[Unit Sales])', SOLVE_ORDER=200\n"
            + "SELECT\n"
            + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            + ",[*SORTED_ROW_AXIS] ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE ([*CJ_SLICER_AXIS])",
            "Axis #0:\n"
            + "{[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[1997].[Q1].[2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_1]}\n"
            + "{[Measures].[*SUMMARY_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Education Level].[Bachelors Degree], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best]}\n"
            + "{[Education Level].[Bachelors Degree], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "{[Education Level].[Graduate Degree], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[Graduate Degree], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "{[Education Level].[High School Degree], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[High School Degree], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best]}\n"
            + "{[Education Level].[High School Degree], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "{[Education Level].[Partial College], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[Partial College], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best]}\n"
            + "{[Education Level].[Partial College], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "{[Education Level].[Partial High School], [Product].[Food].[Deli].[Meat].[Deli Meats].[American]}\n"
            + "{[Education Level].[Partial High School], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best]}\n"
            + "{[Education Level].[Partial High School], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "Row #0: 38\n"
            + "Row #0: 38\n"
            + "Row #1: 13\n"
            + "Row #1: 51\n"
            + "Row #2: 28\n"
            + "Row #2: 79\n"
            + "Row #3: 13\n"
            + "Row #3: 92\n"
            + "Row #4: 3\n"
            + "Row #4: 95\n"
            + "Row #5: 62\n"
            + "Row #5: 157\n"
            + "Row #6: 13\n"
            + "Row #6: 170\n"
            + "Row #7: 22\n"
            + "Row #7: 192\n"
            + "Row #8: 30\n"
            + "Row #8: 222\n"
            + "Row #9: 3\n"
            + "Row #9: 225\n"
            + "Row #10: 3\n"
            + "Row #10: 228\n"
            + "Row #11: 68\n"
            + "Row #11: 296\n"
            + "Row #12: 12\n"
            + "Row #12: 308\n"
            + "Row #13: 27\n"
            + "Row #13: 335\n");
    }

    public void testMon2202SeveralFilteredHierarchiesPlusMeasureFilter() {
        propSaver.set(
            propSaver.properties.AlertNativeEvaluationUnsupported, "ERROR");
        assertQueryReturns(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Promotion Media_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Store_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Education Level_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Gender_],[*BASE_MEMBERS__Time_])))))'\n"
            + "SET [*METRIC_CJ_SET] AS 'FILTER([*NATIVE_CJ_SET],[Measures].[*Store Cost_SEL~SUM] > 0.0)'\n"
            + "SET [*BASE_MEMBERS__Store_] AS '{[Store].[USA].[OR]}'\n"
            + "SET [*NATIVE_MEMBERS__Time_] AS 'GENERATE([*NATIVE_CJ_SET], {[Time].CURRENTMEMBER})'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "SET [*BASE_MEMBERS__Time_] AS '{[Time].[1997].[Q2].[4],[Time].[1997].[Q1].[2],[Time].[1997].[Q1].[1],[Time].[1997].[Q1].[3]}'\n"
            + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Time].CURRENTMEMBER)})'\n"
            + "SET [*BASE_MEMBERS__Education Level_] AS '[Education Level].[Education Level].MEMBERS'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Promotion Media].CURRENTMEMBER,[Store].CURRENTMEMBER,[Education Level].CURRENTMEMBER,[Product].CURRENTMEMBER,[Gender].CURRENTMEMBER)})'\n"
            + "SET [*BASE_MEMBERS__Promotion Media_] AS '{[Promotion Media].[Daily Paper, Radio],[Promotion Media].[Daily Paper, Radio, TV],[Promotion Media].[In-Store Coupon],[Promotion Media].[No Media]}'\n"
            + "SET [*BASE_MEMBERS__Product_] AS '{[Product].[Food].[Deli].[Meat].[Deli Meats].[American],[Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best],[Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Promotion Media].CURRENTMEMBER.ORDERKEY,BASC,[Store].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Store].CURRENTMEMBER,[Store].[Store Country]).ORDERKEY,BASC,[Education Level].CURRENTMEMBER.ORDERKEY,BASC,[Product].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Product].CURRENTMEMBER,[Product].[Product Subcategory]).ORDERKEY,BASC,[Gender].CURRENTMEMBER.ORDERKEY,BASC)'\n"
            + "SET [*BASE_MEMBERS__Gender_] AS '{[Gender].[F]}'\n"
            + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n"
            + "MEMBER [Measures].[*Store Cost_SEL~SUM] AS '([Measures].[Store Cost],[Promotion Media].CURRENTMEMBER,[Store].CURRENTMEMBER,[Education Level].CURRENTMEMBER,[Product].CURRENTMEMBER,[Gender].CURRENTMEMBER,[Time].[*CTX_MEMBER_SEL~AGG])', SOLVE_ORDER=400\n"
            + "MEMBER [Time].[*CTX_MEMBER_SEL~AGG] AS 'AGGREGATE([*NATIVE_MEMBERS__Time_])', SOLVE_ORDER=-301\n"
            + "SELECT\n"
            + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            + ",NON EMPTY\n"
            + "[*SORTED_ROW_AXIS] ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE ([*CJ_SLICER_AXIS])",
            "Axis #0:\n"
            + "{[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[1997].[Q1].[2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Promotion Media].[Daily Paper, Radio], [Store].[USA].[OR], [Education Level].[Bachelors Degree], [Product].[Food].[Deli].[Meat].[Deli Meats].[American], [Gender].[F]}\n"
            + "{[Promotion Media].[Daily Paper, Radio], [Store].[USA].[OR], [Education Level].[High School Degree], [Product].[Food].[Deli].[Meat].[Deli Meats].[American], [Gender].[F]}\n"
            + "{[Promotion Media].[Daily Paper, Radio], [Store].[USA].[OR], [Education Level].[Partial High School], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time], [Gender].[F]}\n"
            + "{[Promotion Media].[Daily Paper, Radio, TV], [Store].[USA].[OR], [Education Level].[Partial High School], [Product].[Food].[Deli].[Meat].[Deli Meats].[American], [Gender].[F]}\n"
            + "{[Promotion Media].[No Media], [Store].[USA].[OR], [Education Level].[High School Degree], [Product].[Food].[Deli].[Meat].[Deli Meats].[American], [Gender].[F]}\n"
            + "{[Promotion Media].[No Media], [Store].[USA].[OR], [Education Level].[Partial College], [Product].[Food].[Deli].[Meat].[Deli Meats].[American], [Gender].[F]}\n"
            + "{[Promotion Media].[No Media], [Store].[USA].[OR], [Education Level].[Partial High School], [Product].[Food].[Deli].[Meat].[Deli Meats].[American], [Gender].[F]}\n"
            + "{[Promotion Media].[No Media], [Store].[USA].[OR], [Education Level].[Partial High School], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best], [Gender].[F]}\n"
            + "{[Promotion Media].[No Media], [Store].[USA].[OR], [Education Level].[Partial High School], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time], [Gender].[F]}\n"
            + "Row #0: 2\n"
            + "Row #1: 4\n"
            + "Row #2: 5\n"
            + "Row #3: 4\n"
            + "Row #4: 2\n"
            + "Row #5: 5\n"
            + "Row #6: 3\n"
            + "Row #7: 4\n"
            + "Row #8: 3\n");
    }

    public void testMon2202AnalyzerCompoundMeasureFilterPlusTopCount() {
        propSaver.set(
            propSaver.properties.AlertNativeEvaluationUnsupported, "ERROR");
        assertQueryReturns(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Promotion Media_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Gender_],[*BASE_MEMBERS__Time_])))'\n"
            + "SET [*METRIC_CJ_SET] AS 'FILTER(FILTER([*NATIVE_CJ_SET],[Measures].[*Store Cost_SEL~SUM] > 0.0 AND [Measures].[*Unit Sales_SEL~SUM] > 0.0),[Measures].[*TOP_Customer Count_SEL~SUM] <= 2)'\n"
            + "SET [*NATIVE_MEMBERS__Time_] AS 'GENERATE([*NATIVE_CJ_SET], {[Time].CURRENTMEMBER})'\n"
            + "SET [*NATIVE_MEMBERS__Gender_] AS 'GENERATE([*NATIVE_CJ_SET], {[Gender].CURRENTMEMBER})'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "SET [*BASE_MEMBERS__Time_] AS '{[Time].[1997].[Q2].[4],[Time].[1997].[Q1].[2],[Time].[1997].[Q1].[1],[Time].[1997].[Q1].[3]}'\n"
            + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Time].CURRENTMEMBER)})'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Promotion Media].CURRENTMEMBER,[Product].CURRENTMEMBER,[Gender].CURRENTMEMBER)})'\n"
            + "SET [*BASE_MEMBERS__Promotion Media_] AS '[Promotion Media].[Media Type].MEMBERS'\n"
            + "SET [*BASE_MEMBERS__Product_] AS '{[Product].[Food].[Deli].[Meat].[Deli Meats].[American],[Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best],[Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Promotion Media].CURRENTMEMBER.ORDERKEY,BASC,[Product].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Product].CURRENTMEMBER,[Product].[Product Subcategory]).ORDERKEY,BASC,[Gender].CURRENTMEMBER.ORDERKEY,BASC)'\n"
            + "SET [*BASE_MEMBERS__Gender_] AS '{[Gender].[F]}'\n"
            + "MEMBER [Gender].[*CTX_MEMBER_SEL~SUM] AS 'SUM([*NATIVE_MEMBERS__Gender_])', SOLVE_ORDER=98\n"
            + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n"
            + "MEMBER [Measures].[*Store Cost_SEL~SUM] AS '([Measures].[Store Cost],[Promotion Media].CURRENTMEMBER,[Product].CURRENTMEMBER,[Gender].[*CTX_MEMBER_SEL~SUM],[Time].[*CTX_MEMBER_SEL~AGG])', SOLVE_ORDER=400\n"
            + "MEMBER [Measures].[*TOP_Customer Count_SEL~SUM] AS 'RANK([Product].CURRENTMEMBER,ORDER(FILTER(GENERATE(EXISTS([*NATIVE_CJ_SET], {([Promotion Media].CURRENTMEMBER)}),{[Product].CURRENTMEMBER}),[Measures].[*Store Cost_SEL~SUM] > 0.0 AND [Measures].[*Unit Sales_SEL~SUM] > 0.0),([Measures].[Customer Count],[Promotion Media].CURRENTMEMBER,[Gender].[*CTX_MEMBER_SEL~SUM],[Time].[*CTX_MEMBER_SEL~AGG]),BDESC))', SOLVE_ORDER=400\n"
            + "MEMBER [Measures].[*Unit Sales_SEL~SUM] AS '([Measures].[Unit Sales],[Promotion Media].CURRENTMEMBER,[Product].CURRENTMEMBER,[Gender].[*CTX_MEMBER_SEL~SUM],[Time].[*CTX_MEMBER_SEL~AGG])', SOLVE_ORDER=400\n"
            + "MEMBER [Time].[*CTX_MEMBER_SEL~AGG] AS 'AGGREGATE([*NATIVE_MEMBERS__Time_])', SOLVE_ORDER=-301\n"
            + "SELECT\n"
            + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
            + ",NON EMPTY\n"
            + "[*SORTED_ROW_AXIS] ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE ([*CJ_SLICER_AXIS])",
            "Axis #0:\n"
            + "{[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[1997].[Q1].[1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Promotion Media].[Daily Paper], [Product].[Food].[Deli].[Meat].[Deli Meats].[American], [Gender].[F]}\n"
            + "{[Promotion Media].[Daily Paper], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best], [Gender].[F]}\n"
            + "{[Promotion Media].[Daily Paper, Radio], [Product].[Food].[Deli].[Meat].[Deli Meats].[American], [Gender].[F]}\n"
            + "{[Promotion Media].[Daily Paper, Radio], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time], [Gender].[F]}\n"
            + "{[Promotion Media].[Daily Paper, Radio, TV], [Product].[Food].[Deli].[Meat].[Deli Meats].[American], [Gender].[F]}\n"
            + "{[Promotion Media].[In-Store Coupon], [Product].[Food].[Deli].[Meat].[Deli Meats].[American], [Gender].[F]}\n"
            + "{[Promotion Media].[In-Store Coupon], [Product].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time], [Gender].[F]}\n"
            + "{[Promotion Media].[No Media], [Product].[Food].[Deli].[Meat].[Deli Meats].[American], [Gender].[F]}\n"
            + "{[Promotion Media].[No Media], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best], [Gender].[F]}\n"
            + "{[Promotion Media].[Product Attachment], [Product].[Food].[Deli].[Meat].[Deli Meats].[American], [Gender].[F]}\n"
            + "{[Promotion Media].[Street Handout], [Product].[Food].[Deli].[Meat].[Deli Meats].[American], [Gender].[F]}\n"
            + "{[Promotion Media].[Street Handout], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best], [Gender].[F]}\n"
            + "{[Promotion Media].[Sunday Paper, Radio], [Product].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best], [Gender].[F]}\n"
            + "Row #0: 2\n"
            + "Row #1: 3\n"
            + "Row #2: 6\n"
            + "Row #3: 5\n"
            + "Row #4: 4\n"
            + "Row #5: 3\n"
            + "Row #6: 3\n"
            + "Row #7: 69\n"
            + "Row #8: 17\n"
            + "Row #9: 4\n"
            + "Row #10: 5\n"
            + "Row #11: 3\n"
            + "Row #12: 3\n");
    }

    public void testNonEmptyCrossJoinCalcMember() {
        assertQueryReturns("WITH \n" +
                "MEMBER Measures.Calc AS '[Measures].[Profit] * 2', SOLVE_ORDER=1000\n" +
                "MEMBER Product.Conditional as 'Iif(Measures.CurrentMember IS Measures.[Calc], + Measures.CurrentMember, null)', SOLVE_ORDER=2000\n" +
                "SET [S2] AS '{[Store].MEMBERS}' \n" +
                "SET [S1] AS 'CROSSJOIN({[Customers].[All Customers]},{Product.Conditional})' \n" +
                "SELECT \n" +
                "NON EMPTY GENERATE({Measures.[Calc]}, CROSSJOIN(HEAD({([Measures].CURRENTMEMBER)}, 1),{[S1]}), ALL) ON AXIS(0), NON EMPTY [S2] ON AXIS(1) \n" +
                "FROM [Sales]",
                "Axis #0:\n" +
                        "{}\n" +
                        "Axis #1:\n" +
                        "{[Measures].[Calc], [Customers].[All Customers], [Product].[Conditional]}\n" +
                        "Axis #2:\n" +
                        "{[Store].[All Stores]}\n" +
                        "{[Store].[USA]}\n" +
                        "{[Store].[USA].[CA]}\n" +
                        "{[Store].[USA].[CA].[Beverly Hills]}\n" +
                        "{[Store].[USA].[CA].[Beverly Hills].[Store 6]}\n" +
                        "{[Store].[USA].[CA].[Los Angeles]}\n" +
                        "{[Store].[USA].[CA].[Los Angeles].[Store 7]}\n" +
                        "{[Store].[USA].[CA].[San Diego]}\n" +
                        "{[Store].[USA].[CA].[San Diego].[Store 24]}\n" +
                        "{[Store].[USA].[CA].[San Francisco]}\n" +
                        "{[Store].[USA].[CA].[San Francisco].[Store 14]}\n" +
                        "{[Store].[USA].[OR]}\n" +
                        "{[Store].[USA].[OR].[Portland]}\n" +
                        "{[Store].[USA].[OR].[Portland].[Store 11]}\n" +
                        "{[Store].[USA].[OR].[Salem]}\n" +
                        "{[Store].[USA].[OR].[Salem].[Store 13]}\n" +
                        "{[Store].[USA].[WA]}\n" +
                        "{[Store].[USA].[WA].[Bellingham]}\n" +
                        "{[Store].[USA].[WA].[Bellingham].[Store 2]}\n" +
                        "{[Store].[USA].[WA].[Bremerton]}\n" +
                        "{[Store].[USA].[WA].[Bremerton].[Store 3]}\n" +
                        "{[Store].[USA].[WA].[Seattle]}\n" +
                        "{[Store].[USA].[WA].[Seattle].[Store 15]}\n" +
                        "{[Store].[USA].[WA].[Spokane]}\n" +
                        "{[Store].[USA].[WA].[Spokane].[Store 16]}\n" +
                        "{[Store].[USA].[WA].[Tacoma]}\n" +
                        "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n" +
                        "{[Store].[USA].[WA].[Walla Walla]}\n" +
                        "{[Store].[USA].[WA].[Walla Walla].[Store 22]}\n" +
                        "{[Store].[USA].[WA].[Yakima]}\n" +
                        "{[Store].[USA].[WA].[Yakima].[Store 23]}\n" +
                        "Row #0: $679,221.79\n" +
                        "Row #1: $679,221.79\n" +
                        "Row #2: $191,274.83\n" +
                        "Row #3: $54,967.60\n" +
                        "Row #4: $54,967.60\n" +
                        "Row #5: $65,547.49\n" +
                        "Row #6: $65,547.49\n" +
                        "Row #7: $65,435.21\n" +
                        "Row #8: $65,435.21\n" +
                        "Row #9: $5,324.53\n" +
                        "Row #10: $5,324.53\n" +
                        "Row #11: $171,009.14\n" +
                        "Row #12: $66,219.69\n" +
                        "Row #13: $66,219.69\n" +
                        "Row #14: $104,789.45\n" +
                        "Row #15: $104,789.45\n" +
                        "Row #16: $316,937.82\n" +
                        "Row #17: $5,685.23\n" +
                        "Row #18: $5,685.23\n" +
                        "Row #19: $63,548.67\n" +
                        "Row #20: $63,548.67\n" +
                        "Row #21: $63,374.53\n" +
                        "Row #22: $63,374.53\n" +
                        "Row #23: $59,677.94\n" +
                        "Row #24: $59,677.94\n" +
                        "Row #25: $89,769.36\n" +
                        "Row #26: $89,769.36\n" +
                        "Row #27: $5,651.26\n" +
                        "Row #28: $5,651.26\n" +
                        "Row #29: $29,230.83\n" +
                        "Row #30: $29,230.83\n");
    }

    public void testCrossJoinCalcMember() {
        assertQueryReturns("WITH \n" +
                        "MEMBER Measures.Calc AS '[Measures].[Profit] * 2', SOLVE_ORDER=1000\n" +
                        "MEMBER Product.Conditional as 'Iif(Measures.CurrentMember IS Measures.[Calc], + Measures.CurrentMember, null)', SOLVE_ORDER=2000\n" +
                        "SET [S2] AS '{[Store].MEMBERS}' \n" +
                        "SET [S1] AS 'CROSSJOIN({[Customers].[All Customers]},{Product.Conditional})' \n" +
                        "SELECT \n" +
                        "GENERATE({Measures.[Calc]}, CROSSJOIN(HEAD({([Measures].CURRENTMEMBER)}, 1),{[S1]}), ALL) ON AXIS(0), NON EMPTY [S2] ON AXIS(1) \n" +
                        "FROM [Sales]",
                "Axis #0:\n" +
                        "{}\n" +
                        "Axis #1:\n" +
                        "{[Measures].[Calc], [Customers].[All Customers], [Product].[Conditional]}\n" +
                        "Axis #2:\n" +
                        "{[Store].[All Stores]}\n" +
                        "{[Store].[USA]}\n" +
                        "{[Store].[USA].[CA]}\n" +
                        "{[Store].[USA].[CA].[Beverly Hills]}\n" +
                        "{[Store].[USA].[CA].[Beverly Hills].[Store 6]}\n" +
                        "{[Store].[USA].[CA].[Los Angeles]}\n" +
                        "{[Store].[USA].[CA].[Los Angeles].[Store 7]}\n" +
                        "{[Store].[USA].[CA].[San Diego]}\n" +
                        "{[Store].[USA].[CA].[San Diego].[Store 24]}\n" +
                        "{[Store].[USA].[CA].[San Francisco]}\n" +
                        "{[Store].[USA].[CA].[San Francisco].[Store 14]}\n" +
                        "{[Store].[USA].[OR]}\n" +
                        "{[Store].[USA].[OR].[Portland]}\n" +
                        "{[Store].[USA].[OR].[Portland].[Store 11]}\n" +
                        "{[Store].[USA].[OR].[Salem]}\n" +
                        "{[Store].[USA].[OR].[Salem].[Store 13]}\n" +
                        "{[Store].[USA].[WA]}\n" +
                        "{[Store].[USA].[WA].[Bellingham]}\n" +
                        "{[Store].[USA].[WA].[Bellingham].[Store 2]}\n" +
                        "{[Store].[USA].[WA].[Bremerton]}\n" +
                        "{[Store].[USA].[WA].[Bremerton].[Store 3]}\n" +
                        "{[Store].[USA].[WA].[Seattle]}\n" +
                        "{[Store].[USA].[WA].[Seattle].[Store 15]}\n" +
                        "{[Store].[USA].[WA].[Spokane]}\n" +
                        "{[Store].[USA].[WA].[Spokane].[Store 16]}\n" +
                        "{[Store].[USA].[WA].[Tacoma]}\n" +
                        "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n" +
                        "{[Store].[USA].[WA].[Walla Walla]}\n" +
                        "{[Store].[USA].[WA].[Walla Walla].[Store 22]}\n" +
                        "{[Store].[USA].[WA].[Yakima]}\n" +
                        "{[Store].[USA].[WA].[Yakima].[Store 23]}\n" +
                        "Row #0: $679,221.79\n" +
                        "Row #1: $679,221.79\n" +
                        "Row #2: $191,274.83\n" +
                        "Row #3: $54,967.60\n" +
                        "Row #4: $54,967.60\n" +
                        "Row #5: $65,547.49\n" +
                        "Row #6: $65,547.49\n" +
                        "Row #7: $65,435.21\n" +
                        "Row #8: $65,435.21\n" +
                        "Row #9: $5,324.53\n" +
                        "Row #10: $5,324.53\n" +
                        "Row #11: $171,009.14\n" +
                        "Row #12: $66,219.69\n" +
                        "Row #13: $66,219.69\n" +
                        "Row #14: $104,789.45\n" +
                        "Row #15: $104,789.45\n" +
                        "Row #16: $316,937.82\n" +
                        "Row #17: $5,685.23\n" +
                        "Row #18: $5,685.23\n" +
                        "Row #19: $63,548.67\n" +
                        "Row #20: $63,548.67\n" +
                        "Row #21: $63,374.53\n" +
                        "Row #22: $63,374.53\n" +
                        "Row #23: $59,677.94\n" +
                        "Row #24: $59,677.94\n" +
                        "Row #25: $89,769.36\n" +
                        "Row #26: $89,769.36\n" +
                        "Row #27: $5,651.26\n" +
                        "Row #28: $5,651.26\n" +
                        "Row #29: $29,230.83\n" +
                        "Row #30: $29,230.83\n");
    }

    public void testDefaultMemberNonEmptyContext() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
                "Sales",
                "  <Dimension name=\"Store2\"  foreignKey=\"store_id\" >\n"
                        + "    <Hierarchy hasAll=\"false\" primaryKey=\"store_id\"  defaultMember='[Store2].[USA].[OR]'>\n"
                        + "      <Table name=\"store\"/>\n"
                        + "      <Level name=\"Store Country\" column=\"store_country\"  uniqueMembers=\"true\"\n"
                        + "          />\n"
                        + "      <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"true\"\n"
                        + "         />\n"
                        + "      <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\" />\n"
                        + "    </Hierarchy>\n"
                        + "  </Dimension>");
        testContext.assertQueryReturns("with member measures.one as '1' " +
                        "select non empty store2.usa.[OR].children on 0, measures.one on 1 from sales",
                "Axis #0:\n"
                        + "{}\n"
                        + "Axis #1:\n"
                        + "{[Store2].[USA].[OR].[Portland]}\n"
                        + "{[Store2].[USA].[OR].[Salem]}\n"
                        + "Axis #2:\n"
                        + "{[Measures].[one]}\n"
                        + "Row #0: 1\n"
                        + "Row #0: 1\n");
        testContext.assertQueryReturns("with member measures.one as '1' " +
                        "select store2.usa.[OR].children on 0, measures.one on 1 from sales",
                "Axis #0:\n"
                        + "{}\n"
                        + "Axis #1:\n"
                        + "{[Store2].[USA].[OR].[Portland]}\n"
                        + "{[Store2].[USA].[OR].[Salem]}\n"
                        + "Axis #2:\n"
                        + "{[Measures].[one]}\n"
                        + "Row #0: 1\n"
                        + "Row #0: 1\n");
    }

    public void testCalcMeasureInVirtualCubeWithoutBaseComponents() {
        // http://jira.pentaho.com/browse/ANALYZER-3630
        propSaver.set( MondrianProperties.instance().EnableNativeNonEmpty, true );
        final TestContext context =
                TestContext.instance().withSchema(
                        "<Schema name=\"FoodMart\">"
                                + "  <Dimension name=\"Store\">"
                                + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">"
                                + "      <Table name=\"store\" />"
                                + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\" />"
                                + "      <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"true\" />"
                                + "    </Hierarchy>"
                                + "  </Dimension>"
                                + "  <Dimension name=\"Time\" type=\"TimeDimension\">\n"
                                + "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n"
                                + "      <Table name=\"time_by_day\"/>\n"
                                + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
                                + "          levelType=\"TimeYears\"/>\n"
                                + "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n"
                                + "          levelType=\"TimeQuarters\"/>\n"
                                + "    </Hierarchy>\n"
                                + "    </Dimension>"
                                + "  <Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">"
                                + "    <Table name=\"sales_fact_1997\" />"
                                + "    <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\" />"
                                + "    <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\" />"
                                + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\" />"
                                + "    <CalculatedMember name=\"dummyMeasure\" dimension=\"Measures\">"
                                + "      <Formula>[Measures].[Unit Sales]</Formula>"
                                + "    </CalculatedMember>"
                                + "    <CalculatedMember name=\"dummyMeasure2\" dimension=\"Measures\">"
                                + "      <Formula>[Measures].[dummyMeasure]</Formula>"
                                + "    </CalculatedMember>"
                                + "  </Cube>"
                                + "  <VirtualCube defaultMeasure=\"dummyMeasure\" name=\"virtual\">"
                                + "    <VirtualCubeDimension name=\"Store\" />"
                                + "    <VirtualCubeDimension name=\"Time\" />"
                                + "    <VirtualCubeMeasure name=\"[Measures].[dummyMeasure2]\" cubeName=\"Sales\" />"
                                + "  </VirtualCube>"
                                + "</Schema>" );
        verifySameNativeAndNot(
                "select "
                        + " [Measures].[dummyMeasure2] on COLUMNS, "
                        + " NON EMPTY CrossJoin([Store].[Store State].Members, Time.[Year].members) ON ROWS "
                        + " from [virtual] ",
                "", context );
    }

}

// End NonEmptyTest.java
