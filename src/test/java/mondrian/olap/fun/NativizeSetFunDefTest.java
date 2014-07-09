/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2009-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.rolap.BatchTestCase;
import mondrian.rolap.RolapConnection;
import mondrian.server.Locus;
import mondrian.spi.Dialect;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;
import mondrian.util.Bug;

/**
 * Unit test for the {@code NativizeSet} function.
 *
 * @author jrand
 * @since Oct 14, 2009
 */
public class NativizeSetFunDefTest extends BatchTestCase {
    public void setUp() throws Exception {
        super.setUp();
        propSaver.set(propSaver.props.EnableNonEmptyOnAllAxis, true);
        propSaver.set(propSaver.props.NativizeMinThreshold, 0);
        propSaver.set(propSaver.props.UseAggregates, false);
        propSaver.set(propSaver.props.ReadAggregates, false);
        propSaver.set(propSaver.props.EnableNativeCrossJoin, true);
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testIsNoOpWithAggregatesTablesOn() {
        propSaver.set(propSaver.props.UseAggregates, true);
        propSaver.set(propSaver.props.ReadAggregates, true);
        checkNotNative(
            "with  member [gender].[agg] as"
            + "  'aggregate({[gender].[gender].members},[measures].[unit sales])'"
            + "select NativizeSet(CrossJoin( "
            + "{gender.gender.members, gender.agg}, "
            + "{[marital status].[marital status].members}"
            + ")) on 0 from sales");
    }

    public void testLevelHierarchyHighCardinality() {
        // The cardinality for the hierarchy looks like this:
        //    Year: 2 (level * gender cardinality:2)
        //    Quarter: 16 (level * gender cardinality:2)
        //    Month: 48 (level * gender cardinality:2)
        propSaver.set(propSaver.props.NativizeMinThreshold, 17);
        String mdx =
            "select NativizeSet("
            + "CrossJoin( "
            + "gender.gender.members, "
            + "CrossJoin("
            + "{ measures.[unit sales] }, "
            + "[Time].[Month].members"
            + "))) on 0"
            + "from sales";
        checkNative(mdx);
    }

    public void testLevelHierarchyLowCardinality() {
        // The cardinality for the hierarchy looks like this:
        //    Year: 2 (level * gender cardinality:2)
        //    Quarter: 16 (level * gender cardinality:2)
        //    Month: 48 (level * gender cardinality:2)
        propSaver.set(propSaver.props.NativizeMinThreshold, 50);
        String mdx =
            "select NativizeSet("
            + "CrossJoin( "
            + "gender.gender.members, "
            + "CrossJoin("
            + "{ measures.[unit sales] }, "
            + "[Time].[Month].members"
            + "))) on 0"
            + "from sales";
        checkNotNative(mdx);
    }

    public void testNamedSetLowCardinality() {
        propSaver.set(propSaver.props.NativizeMinThreshold, Integer.MAX_VALUE);
        checkNotNative(
            "with "
            + "set [levelMembers] as 'crossjoin( gender.gender.members, "
            + "[marital status].[marital status].members) '"
            + "select  nativizeSet([levelMembers]) on 0 "
            + "from [warehouse and sales]");
    }

    public void testCrossjoinWithNamedSetLowCardinality() {
        propSaver.set(propSaver.props.NativizeMinThreshold, Integer.MAX_VALUE);
        checkNotNative(
            "with "
            + "set [genderMembers] as 'gender.gender.members'"
            + "set [maritalMembers] as '[marital status].[marital status].members'"
            + "set [levelMembers] as 'crossjoin( [genderMembers],[maritalMembers]) '"
            + "select  nativizeSet([levelMembers]) on 0 "
            + "from [warehouse and sales]");
    }

    public void testMeasureInCrossJoinWithTwoDimensions() {
        checkNative(
            "select NativizeSet("
            + "CrossJoin( "
            + "gender.gender.members, "
            + "CrossJoin("
            + "{ measures.[unit sales] }, "
            + "[marital status].[marital status].members"
            + "))) on 0 "
            + "from sales");
    }

    public void testNativeResultLimitAtZero() {
        // This query will return exactly 6 rows:
        // {Female,Male,Agg}x{Married,Single}
        String mdx =
            "with  member [gender].[agg] as"
            + "  'aggregate({[gender].[gender].members},[measures].[unit sales])'"
            + "select NativizeSet(CrossJoin( "
            + "{gender.gender.members, gender.agg}, "
            + "{[marital status].[marital status].members}"
            + ")) on 0 from sales";

        // Set limit to zero (effectively, no limit)
        propSaver.set(propSaver.props.NativizeMaxResults, 0);
        checkNative(mdx);
    }

    public void testNativeResultLimitBeforeMerge() {
        // This query will return exactly 6 rows:
        // {Female,Male,Agg}x{Married,Single}
        String mdx =
            "with  member [gender].[agg] as"
            + "  'aggregate({[gender].[gender].members},[measures].[unit sales])'"
            + "select NativizeSet(CrossJoin( "
            + "{gender.gender.members, gender.agg}, "
            + "{[marital status].[marital status].members}"
            + ")) on 0 from sales";

        // Set limit to exact size of result
        propSaver.set(propSaver.props.NativizeMaxResults, 6);
        checkNative(mdx);

        try {
            // The native list doesn't contain the calculated members,
            // so it will have 4 rows.  Setting the limit to 3 means
            // that the exception will be thrown before calculated
            // members are merged into the result.
            propSaver.set(propSaver.props.NativizeMaxResults, 3);
            checkNative(mdx);
            fail("Should have thrown ResourceLimitExceededException.");
        } catch (ResourceLimitExceededException expected) {
            // ok
        }
    }

    public void testNativeResultLimitDuringMerge() {
        // This query will return exactly 6 rows:
        // {Female,Male,Agg}x{Married,Single}
        String mdx =
            "with  member [gender].[agg] as"
            + "  'aggregate({[gender].[gender].members},[measures].[unit sales])'"
            + "select NativizeSet(CrossJoin( "
            + "{gender.gender.members, gender.agg}, "
            + "{[marital status].[marital status].members}"
            + ")) on 0 from sales";

        // Set limit to exact size of result
        propSaver.set(propSaver.props.NativizeMaxResults, 6);
        checkNative(mdx);

        try {
            // The native list doesn't contain the calculated members,
            // so setting the limit to 5 means the exception won't be
            // thrown until calculated members are merged into the result.
            propSaver.set(propSaver.props.NativizeMaxResults, 5);
            checkNative(mdx);
            fail("Should have thrown ResourceLimitExceededException.");
        } catch (ResourceLimitExceededException expected) {
        }
    }

    public void testMeasureAndDimensionInCrossJoin() {
        checkNotNative(
            // There's no crossjoin left after the measure is set aside,
            // so it's not even a candidate for native evaluation.
            // This test is here to ensure that "NativizeSet" still returns
            // the correct result.
            "select NativizeSet("
            + "CrossJoin("
            + "{ measures.[unit sales] }, "
            + "[marital status].[marital status].members"
            + ")) on 0"
            + "from sales");
    }

    public void testDimensionAndMeasureInCrossJoin() {
        checkNotNative(
            // There's no crossjoin left after the measure is set aside,
            // so it's not even a candidate for native evaluation.
            // This test is here to ensure that "NativizeSet" still returns
            // the correct result.
            "select NativizeSet("
            + "CrossJoin("
            + "[marital status].[marital status].members, "
            + "{ measures.[unit sales] }"
            + ")) on 0"
            + "from sales");
    }

    public void testAllByAll() {
        checkNotNative(
            // There's no crossjoin left after all members are set aside,
            // so it's not even a candidate for native evaluation.
            // This test is here to ensure that "NativizeSet" still returns
            // the correct result.
            "select NativizeSet("
            + "CrossJoin("
            + "{ [gender].[all gender] }, "
            + "{ [marital status].[all marital status] } "
            + ")) on 0"
            + "from sales");
    }

    public void testAllByAllByAll() {
        checkNotNative(
            // There's no crossjoin left after all members are set aside,
            // so it's not even a candidate for native evaluation.
            // This test is here to ensure that "NativizeSet" still returns
            // the correct result.
            "select NativizeSet("
            + "CrossJoin("
            + "{ [product].[all products] }, "
            + "CrossJoin("
            + "{ [gender].[all gender] }, "
            + "{ [marital status].[all marital status] } "
            + "))) on 0"
            + "from sales");
    }

    public void testNativizeTwoAxes() {
        String mdx =
            "select "
            + "NativizeSet("
            + "CrossJoin("
            + "{ [gender].[gender].members }, "
            + "{ [marital status].[marital status].members } "
            + ")) on 0,"
            + "NativizeSet("
            + "CrossJoin("
            + "{ [measures].[unit sales] }, "
            + "{ [Education Level].[Education Level].members } "
            + ")) on 1"
            + "from [warehouse and sales]";

        // Our setUp sets threshold at zero, so should always be native
        // if possible.
        checkNative(mdx);

        // Set the threshold high; same mdx should no longer be natively
        // evaluated.
        propSaver.set(propSaver.props.NativizeMinThreshold, 200000);
        checkNotNative(mdx);
    }

    public void testCurrentMemberAsFunArg() {
        checkNative(
            "with "
            ////////////////////////////////////////////////////////////
            // Having a member of the measures dimension as a function
            // argument will normally disable native evaluation but
            // there is a special case in FunUtil.checkNativeCompatible
            // which allows currentmember
            ////////////////////////////////////////////////////////////
            + "member [gender].[x] "
            + "   as 'iif (measures.currentmember is measures.[unit sales], "
            + "       Aggregate(gender.gender.members), 101010)' "
            + "select "
            + "NativizeSet("
            + "crossjoin("
            + "{time.year.members}, "
            + "crossjoin("
            + "{gender.x},"
            + "[marital status].[marital status].members"
            + "))) "
            + "on axis(0) "
            + "from [warehouse and sales]");
    }

    public void testOnlyMeasureIsLiteral() {
        checkNotNative(
            //////////////////////////////////////////////////////////////////
            // There's no base cube, so this should NOT be natively evaluated.
            //////////////////////////////////////////////////////////////////
            "with "
            + "member [measures].[cog_oqp_int_t1] as '1', solve_order = 65535 "
            + "select NativizeSet(CrossJoin("
            + "   [marital status].[marital status].members, "
            + "   [gender].[gender].members "
            + ")) on 1, "
            + "{ [measures].[cog_oqp_int_t1] } "
            + "on 0 "
            + "from [warehouse and sales]");
    }

    public void testTwoLiteralMeasuresAndUnitAndStoreSales() {
        checkNative(
            // Should be natively evaluated because the unit sales
            // measure will bring in a base cube.
            "with "
            + "member [measures].[cog_oqp_int_t1] as '1', solve_order = 65535 "
            + "member [measures].[cog_oqp_int_t2] as '2', solve_order = 65535 "
            + "select "
            + "   NativizeSet(CrossJoin("
            + "      [marital status].[marital status].members, "
            + "      [gender].[gender].members "
            + "    ))"
            + "on 1, "
            + "{ "
            + "   { [measures].[cog_oqp_int_t1] }, "
            + "   { [measures].[unit sales] }, "
            + "   { [measures].[cog_oqp_int_t2] }, "
            + "   { [measures].[store sales] } "
            + "} "
            + " on 0 "
            + "from [warehouse and sales]");
    }

    public void testLiteralMeasuresWithinParentheses() {
        checkNative(
            // Should be natively evaluated because the unit sales
            // measure will bring in a base cube.  The extra parens
            // around the reference to the calculated member should no
            // longer cause native evaluation to be abandoned.
            "with "
            + "member [measures].[cog_oqp_int_t1] as '1', solve_order = 65535 "
            + "member [measures].[cog_oqp_int_t2] as '2', solve_order = 65535 "
            + "select "
            + "   NativizeSet(CrossJoin("
            + "      [marital status].[marital status].members, "
            + "      [gender].[gender].members "
            + "    ))"
            + "on 1, "
            + "{ "
            + "   { ((( [measures].[cog_oqp_int_t1] ))) }, "
            + "   { [measures].[unit sales] }, "
            + "   { ( [measures].[cog_oqp_int_t2] ) }, "
            + "   { [measures].[store sales] } "
            + "} "
            + " on 0 "
            + "from [warehouse and sales]");
    }

    public void testIsEmptyOnMeasures() {
        checkNative(
            "with "
            ////////////////////////////////////////////////////////
            // isEmpty doesn't pose a problem for native evaluation.
            ////////////////////////////////////////////////////////
            + "member [measures].[cog_oqp_int_t1] "
            + "   as 'iif( isEmpty( measures.[unit sales]), 1010,2020)', solve_order = 65535 "
            + "select "
            + "   NativizeSet(CrossJoin("
            + "      [marital status].[marital status].members, "
            + "      [gender].[gender].members "
            + "    ))"
            + "on 1, "
            + "{ "
            + "   { [measures].[cog_oqp_int_t1] }, "
            + "   { [measures].[unit sales] } "
            + "} "
            + " on 0 "
            + "from [warehouse and sales]");
    }

    public void testLagOnMeasures() {
        checkNotNative(
            "with "
            /////////////////////////////////////////////
            // Lag function is NOT compatible with native.
            /////////////////////////////////////////////
            + "member [measures].[cog_oqp_int_t1] "
            + "   as 'measures.[store sales].lag(1)', solve_order = 65535 "
            + "select "
            + "   NativizeSet(CrossJoin("
            + "      [marital status].[marital status].members, "
            + "      [gender].[gender].members "
            + "    ))"
            + "on 1, "
            + "{ "
            + "   { [measures].[cog_oqp_int_t1] }, "
            + "   { [measures].[unit sales] }, "
            + "   { [measures].[store sales] } "
            + "} "
            + " on 0 "
            + "from [warehouse and sales]");
    }

    public void testLagOnMeasuresWithinParentheses() {
        checkNotNative(
            "with "
            /////////////////////////////////////////////
            // Lag function is NOT compatible with native.
            // Here we're making sure that the lag function
            // disables native eval even when buried in layers
            // of parentheses.
            /////////////////////////////////////////////
            + "member [measures].[cog_oqp_int_t1] "
            + "   as 'measures.[store sales].lag(1)', solve_order = 65535 "
            + "select "
            + "   NativizeSet(CrossJoin("
            + "      [marital status].[marital status].members, "
            + "      [gender].[gender].members "
            + "    ))"
            + "on 1, "
            + "{ "
            + "   { ((( [measures].[cog_oqp_int_t1] ))) }, "
            + "   { [measures].[unit sales] }, "
            + "   { [measures].[store sales] } "
            + "} "
            + " on 0 "
            + "from [warehouse and sales]");
    }

    public void testRangeOfMeasures() {
        checkNotNative(
            "select "
            + "   NativizeSet(CrossJoin("
            + "      [marital status].[marital status].members, "
            + "      [gender].[gender].members "
            + "    ))"
            + "on 1, "
            + "{ "
            ///////////////////////////////////////////////////
            // Range of measures is NOT compatible with native.
            ///////////////////////////////////////////////////
            + "    measures.[unit sales] : measures.[store sales]  "
            + "} "
            + " on 0 "
            + "from [warehouse and sales]");
    }


    public void testOrderOnMeasures() {
        checkNative(
            "with "
            ///////////////////////////////////////////////////
            // Order function should be compatible with native.
            ///////////////////////////////////////////////////
            + "member [measures].[cog_oqp_int_t1] "
            + " as 'aggregate(order({measures.[store sales]}, measures.[store sales]), "
            + "measures.[store sales])', solve_order = 65535 "
            + "select "
            + "   NativizeSet(CrossJoin("
            + "      [marital status].[marital status].members, "
            + "      [gender].[gender].members "
            + "   ))"
            + "on 1, "
            + "{ "
            + "   measures.[cog_oqp_int_t1],"
            + "   measures.[unit sales]"
            + "} "
            + " on 0 "
            + "from [warehouse and sales]");
    }

    public void testLiteralMeasureAndUnitSalesUsingSet() {
        checkNative(
            // Should be natively evaluated because the unit sales
            "with "   // measure will bring in a base cube.
            + "member [measures].[cog_oqp_int_t1] as '1', solve_order = 65535 "
            + "member [measures].[cog_oqp_int_t2] as '2', solve_order = 65535 "
            + "set [cog_oqp_int_s1] as "
            + "   'CrossJoin("
            + "      [marital status].[marital status].members, "
            + "      [gender].[gender].members "
            + "    )'"
            + "select "
            + "   NativizeSet([cog_oqp_int_s1])"
            + "on 1, "
            + "{ "
            + "   [measures].[cog_oqp_int_t1], "
            + "   [measures].[unit sales], "
            + "   [measures].[cog_oqp_int_t1], "
            + "   [measures].[store sales] "
            + "} "
            + " on 0 "
            + "from [warehouse and sales]");
    }

    public void testNoSubstitutionsArityOne() {
        checkNotNative(
            // no crossjoin, so not native
            "SELECT NativizeSet({Gender.F, Gender.M}) on 0 from sales");
    }

    public void testNoSubstitutionsArityTwo() {
        checkNotNative(
            "SELECT NativizeSet(CrossJoin("
            + "{Gender.F, Gender.M}, "
            + "{ [Marital Status].M } "
            + ")) on 0 from sales");
    }

    public void testExplicitCurrentMonth() {
        checkNative(
            "SELECT NativizeSet(CrossJoin( "
            + "   { [Time].[Month].currentmember }, "
            + "   Gender.Gender.members )) " + "on 0 from sales");
    }

    public void disabled_testCalculatedCurrentMonth() {
        checkNative(
            "WITH "
            + "SET [Current Month] AS 'tail([Time].[month].members, 1)'"
            + "SELECT NativizeSet(CrossJoin( "
            + "   { [Current Month] }, "
            + "   Gender.Gender.members )) "
            + "on 0 from sales");
    }

    public void disabled_testCalculatedRelativeMonth() {
        checkNative(
            "with "
            + "member [gender].[cog_oqp_int_t2] as '1', solve_order = 65535 "
            + "select NativizeSet("
            + "   { { [gender].[cog_oqp_int_t2] }, "
            + "       crossjoin( {tail([Time].[month].members, 1)}, [gender].[gender].members ) },"
            + "   { { [gender].[cog_oqp_int_t2] }, "
            + "       crossjoin( {tail([Time].[month].members, 1).lag(1)}, [gender].[gender].members ) },"
            + ") on 0 "
            + "from [sales]");
    }

    public void testAcceptsAllDimensionMembersSetAsInput() {
        checkNotNative(
            // no crossjoin, so not native
            "SELECT NativizeSet({[Marital Status].[Marital Status].members})"
            + " on 0 from sales");
    }

    public void testAcceptsCrossJoinAsInput() {
        checkNative(
            "SELECT NativizeSet( CrossJoin({ Gender.F, Gender.M }, "
            + "{[Marital Status].[Marital Status].members})) on 0 from sales");
    }

    public void testRedundantEnumMembersFirst() {
        checkNative(
            // In the enumerated marital status values { M, S, S }
            // the second S is clearly redundant, but should be
            // included in the result nonetheless. The extra
            // level of parens aren't logically necessary, but
            // are included here because they require special handling.
            "SELECT NativizeSet( CrossJoin("
            + "{ { [Marital Status].M, [Marital Status].S }, "
            + "  { [Marital Status].S } "
            + "},"
            + "CrossJoin( "
            + "{ gender.gender.members }, "
            + "{ time.quarter.members } "
            + "))) on 0 from sales");
    }

    public void testRedundantEnumMembersMiddle() {
        checkNative(
            // In the enumerated gender values { F, M, M, M }
            // the last two M values are redunant, but should be
            // included in the result nonetheless. The extra
            // level of parens aren't logically necessary, but
            // are included here because they require special handling.
            "SELECT NativizeSet( CrossJoin("
            + "{  [Marital Status].[Marital Status].members },"
            + "CrossJoin( "
            + "{ { gender.F, gender.M , gender.M}, "
            + "  { gender.M } "
            + "}, "
            + "{ time.quarter.members } "
            + "))) on 0 from sales");
    }

    public void testRedundantEnumMembersLast() {
        checkNative(
            // In the enumerated time quarter values { Q1, Q2, Q2 }
            // the last two Q2 values are redunant, but should be
            // included in the result nonetheless. The extra
            // level of parens aren't logically necessary, but
            // are included here because they require special handling.
            "SELECT NativizeSet( CrossJoin("
            + "{  [Marital Status].[Marital Status].members },"
            + "CrossJoin( "
            + "{ gender.gender.members }, "
            + "{ { time.[1997].Q1, time.[1997].Q2 }, "
            + "  { time.[1997].Q2 } "
            + "} "
            + "))) on 0 from sales");
    }

    public void testRedundantLevelMembersFirst() {
        checkNative(
            // The second marital status members function is clearly
            // redundant, but should be included in the result
            // nonetheless. The extra level of parens aren't logically
            // necessary, but are included here because they require
            // special handling.
            "SELECT NativizeSet( CrossJoin("
            + "{  [Marital Status].[Marital Status].members, "
            + "   { [Marital Status].[Marital Status].members } "
            + "},"
            + "CrossJoin( "
            + "{ gender.gender.members }, "
            + "{ time.quarter.members } "
            + "))) on 0 from sales");
    }

    public void testRedundantLevelMembersMiddle() {
        checkNative(
            // The second gender members function is clearly
            // redundant, but should be included in the result
            // nonetheless. The extra level of parens aren't logically
            // necessary, but are included here because they require
            // special handling.
            "SELECT NativizeSet( CrossJoin("
            + "{  [Marital Status].[Marital Status].members },"
            + "CrossJoin( "
            + "{ gender.gender.members, "
            + "  { gender.gender.members } "
            + "}, "
            + "{ time.quarter.members } "
            + "))) on 0 from sales");
    }

    public void testRedundantLevelMembersLast() {
        checkNative(
            // The second time.quarter members function is clearly
            // redundant, but should be included in the result
            // nonetheless. The extra level of parens aren't logically
            // necessary, but are included here because they require
            // special handling.
            "SELECT NativizeSet( CrossJoin("
            + "{  [Marital Status].[Marital Status].members },"
            + "CrossJoin( "
            + "{ gender.gender.members }, "
            + "{ time.quarter.members, "
            + "  { time.quarter.members } "
            + "} "
            + "))) on 0 from sales");
    }

    public void testNonEmptyNestedCrossJoins() {
        checkNative(
            "SELECT "
            + "NativizeSet(CrossJoin("
            + "{ Gender.F, Gender.M }, "
            + "CrossJoin("
            + "{ [Marital Status].[Marital Status].members }, "
            + "CrossJoin("
            + "{ [Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR] }, "
            + "{ [Education Level].[Education Level].members } "
            + ")))"
            + ") on 0 from sales");
    }

    public void testLevelMembersAndAll() {
        checkNative(
            "select NativizeSet ("
            + "crossjoin( "
            + "  { gender.gender.members, gender.[all gender] }, "
            + "  [marital status].[marital status].members "
            + ")) on 0 from sales");
    }

    public void testCrossJoinArgInNestedBraces() {
        checkNative(
            "select NativizeSet ("
            + "crossjoin( "
            + "  { { gender.gender.members } }, "
            + "  [marital status].[marital status].members "
            + ")) on 0 from sales");
    }

    public void testLevelMembersAndAllWhereOrderMatters() {
        checkNative(
            "select NativizeSet ("
            + "crossjoin( "
            + "  { gender.gender.members, gender.[all gender] }, "
            + "  { [marital status].S, [marital status].M } "
            + ")) on 0 from sales");
    }

    public void testEnumMembersAndAll() {
        checkNative(
            "select NativizeSet ("
            + "crossjoin( "
            + "  { gender.F, gender.M, gender.[all gender] }, "
            + "  [marital status].[marital status].members "
            + ")) on 0 from sales");
    }

    public void testNativizeWithASetAtTopLevel() {
        checkNative(
            "WITH"
            + "  MEMBER [Gender].[umg1] AS "
            + "  '([Gender].[gender agg], [Measures].[Unit Sales])', SOLVE_ORDER = 8 "
            + "  MEMBER [Gender].[gender agg] AS"
            + "  'AGGREGATE({[Gender].[Gender].MEMBERS},[Measures].[Unit Sales])', SOLVE_ORDER = 8 "
            + " MEMBER [Marital Status].[umg2] AS "
            + " '([Marital Status].[marital agg], [Measures].[Unit Sales])', SOLVE_ORDER = 4 "
            + " MEMBER [Marital Status].[marital agg] AS "
            + "  'AGGREGATE({[Marital Status].[Marital Status].MEMBERS},[Measures].[Unit Sales])', SOLVE_ORDER = 4 "
            + " SET [s2] AS "
            + "  'CROSSJOIN({[Marital Status].[Marital Status].MEMBERS}, {{[Gender].[Gender].MEMBERS}, {[Gender].[umg1]}})' "
            + " SET [s1] AS "
            + "  'CROSSJOIN({[Marital Status].[umg2]}, {[Gender].DEFAULTMEMBER})' "
            + " SELECT "
            + "  NativizeSet({[Measures].[Unit Sales]}) DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON AXIS(0), "
            + "  NativizeSet({[s2],[s1]}) "
            + " DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON AXIS(1)"
            + " FROM [Sales]  CELL PROPERTIES VALUE, FORMAT_STRING");
    }

    public void testNativizeWithASetAtTopLevel3Levels() {
        checkNative(
            "WITH\n"
            + "MEMBER [Gender].[COG_OQP_INT_umg2] AS 'IIF([Measures].CURRENTMEMBER IS [Measures].[Unit Sales], "
            + "([Gender].[COG_OQP_INT_m5], [Measures].[Unit Sales]), "
            + "AGGREGATE({[Gender].[Gender].MEMBERS}))', SOLVE_ORDER = 8\n"
            + "MEMBER [Gender].[COG_OQP_INT_m5] AS "
            + "'AGGREGATE({[Gender].[Gender].MEMBERS}, [Measures].[Unit Sales])', SOLVE_ORDER = 8\n"
            + "MEMBER [Store Type].[COG_OQP_INT_umg1] AS "
            + "'IIF([Measures].CURRENTMEMBER IS [Measures].[Unit Sales], "
            + "([Store Type].[COG_OQP_INT_m4], [Measures].[Unit Sales]), "
            + "AGGREGATE({[Store Type].[Store Type].MEMBERS}))', SOLVE_ORDER = 12\n"
            + "MEMBER [Store Type].[COG_OQP_INT_m4] AS "
            + "'AGGREGATE({[Store Type].[Store Type].MEMBERS}, [Measures].[Unit Sales])', SOLVE_ORDER = 12\n"
            + "MEMBER [Marital Status].[COG_OQP_INT_umg3] AS "
            + "'IIF([Measures].CURRENTMEMBER IS [Measures].[Unit Sales], "
            + "([Marital Status].[COG_OQP_INT_m6], [Measures].[Unit Sales]), "
            + "AGGREGATE({[Marital Status].[Marital Status].MEMBERS}))', SOLVE_ORDER = 4\n"
            + "MEMBER [Marital Status].[COG_OQP_INT_m6] AS "
            + "'AGGREGATE({[Marital Status].[Marital Status].MEMBERS}, [Measures].[Unit Sales])', SOLVE_ORDER = 4\n"
            + "SET [COG_OQP_INT_s5] AS 'CROSSJOIN({[Marital Status].[Marital Status].MEMBERS}, {[COG_OQP_INT_s4], [COG_OQP_INT_s3]})'\n"
            + "SET [COG_OQP_INT_s4] AS 'CROSSJOIN({[Gender].[Gender].MEMBERS}, {{[Store Type].[Store Type].MEMBERS}, "
            + "{[Store Type].[COG_OQP_INT_umg1]}})'\n"
            + "SET [COG_OQP_INT_s3] AS 'CROSSJOIN({[Gender].[COG_OQP_INT_umg2]}, {[Store Type].DEFAULTMEMBER})'\n"
            + "SET [COG_OQP_INT_s2] AS 'CROSSJOIN({[Marital Status].[COG_OQP_INT_umg3]}, [COG_OQP_INT_s1])'\n"
            + "SET [COG_OQP_INT_s1] AS 'CROSSJOIN({[Gender].DEFAULTMEMBER}, {[Store Type].DEFAULTMEMBER})' \n"
            + "SELECT {[Measures].[Unit Sales]} "
            + "DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON AXIS(0), \n"
            + "NativizeSet({[COG_OQP_INT_s5], [COG_OQP_INT_s2]}) "
            + "DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON AXIS(1)\n"
            + "FROM [Sales]  CELL PROPERTIES VALUE, FORMAT_STRING\n");
    }

    public void testNativizeWithASetAtTopLevel2() {
        checkNative(
            "WITH"
            + "  MEMBER [Gender].[umg1] AS "
            + "  '([Gender].[gender agg], [Measures].[Unit Sales])', SOLVE_ORDER = 8 "
            + "  MEMBER [Gender].[gender agg] AS"
            + "  'AGGREGATE({[Gender].[Gender].MEMBERS},[Measures].[Unit Sales])', SOLVE_ORDER = 8 "
            + " MEMBER [Marital Status].[umg2] AS "
            + " '([Marital Status].[marital agg], [Measures].[Unit Sales])', SOLVE_ORDER = 4 "
            + " MEMBER [Marital Status].[marital agg] AS "
            + "  'AGGREGATE({[Marital Status].[Marital Status].MEMBERS},[Measures].[Unit Sales])', SOLVE_ORDER = 4 "
            + " SET [s2] AS "
            + "  'CROSSJOIN({{[Marital Status].[Marital Status].MEMBERS},{[Marital Status].[umg2]}}, "
            + "{{[Gender].[Gender].MEMBERS}, {[Gender].[umg1]}})' "
            + " SET [s1] AS "
            + "  'CROSSJOIN({[Marital Status].[umg2]}, {[Gender].DEFAULTMEMBER})' "
            + " SELECT "
            + "  NativizeSet({[Measures].[Unit Sales]}) "
            + "DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON AXIS(0), "
            + "  NativizeSet({[s2]}) "
            + " DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON AXIS(1)"
            + " FROM [Sales]  CELL PROPERTIES VALUE, FORMAT_STRING");
    }

    public void testGenderMembersAndAggByMaritalStatus() {
        checkNative(
            "with member gender.agg as 'Aggregate( gender.gender.members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  { gender.gender.members, gender.[agg] }, "
            + "  [marital status].[marital status].members "
            + ")) on 0 from sales");
    }

    public void testGenderAggAndMembersByMaritalStatus() {
        checkNative(
            "with member gender.agg as 'Aggregate( gender.gender.members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  { gender.[agg], gender.gender.members }, "
            + "  [marital status].[marital status].members "
            + ")) on 0 from sales");
    }

    public void testGenderAggAndMembersAndAllByMaritalStatus() {
        checkNative(
            "with member gender.agg as 'Aggregate( gender.gender.members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  { gender.[agg], gender.gender.members, gender.[all gender] }, "
            + "  [marital status].[marital status].members "
            + ")) on 0 from sales");
    }

    public void testMaritalStatusByGenderMembersAndAgg() {
        checkNative(
            "with member gender.agg as 'Aggregate( gender.gender.members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  [marital status].[marital status].members, "
            + "  { gender.gender.members, gender.[agg] } "
            + ")) on 0 from sales");
    }

    public void testMaritalStatusByGenderAggAndMembers() {
        checkNative(
            "with member gender.agg as 'Aggregate( gender.gender.members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  [marital status].[marital status].members, "
            + "  { gender.[agg], gender.gender.members } "
            + ")) on 0 from sales");
    }

    public void testAggWithEnumMembers() {
        checkNative(
            "with member gender.agg as 'Aggregate( gender.gender.members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  { gender.gender.members, gender.[agg] }, "
            + "  { [marital status].[marital status].[M], [marital status].[marital status].[S] } "
            + ")) on 0 from sales");
    }

    public void testCrossjoinArgWithMultipleElementTypes() {
        checkNative(
            // Test for correct handling of a crossjoin arg that contains
            // a combination of element types: a members function, an
            // explicit enumerated value, an aggregate, and the all level.
            "with member [gender].agg as 'Aggregate( gender.gender.members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "{ time.quarter.members }, "
            + "CrossJoin( "
            + "{ gender.gender.members, gender.F, gender.[agg], gender.[all gender] }, "
            + "{ [marital status].[marital status].members }"
            + "))) on 0 from sales");
    }

    public void testProductFamilyMembers() {
        checkNative(
            "select non empty NativizeSet("
            + "crossjoin( "
            + "  [product].[product family].members, "
            + "  { [gender].F } "
            + ")) on 0 from sales");
    }

    public void testNestedCrossJoinWhereAllColsHaveNative() {
        checkNative(
            "with "
            + "member gender.agg as 'Aggregate( gender.gender.members )' "
            + "member [marital status].agg as 'Aggregate( [marital status].[marital status].members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  { gender.[all gender], gender.gender.members, gender.[agg] }, "
            + "  crossjoin("
            + "  { [marital status].[marital status].members, [marital status].[agg] },"
            + "  [Education Level].[Education Level].members "
            + "))) on 0 from sales");
    }

    public void testNestedCrossJoinWhereFirstColumnNonNative() {
        checkNative(
            "with "
            + "member gender.agg as 'Aggregate( gender.gender.members )' "
            + "member [marital status].agg as 'Aggregate( [marital status].[marital status].members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  { gender.[all gender], gender.[agg] }, "
            + "  crossjoin("
            + "  { [marital status].[marital status].members, [marital status].[agg] },"
            + "  [Education Level].[Education Level].members "
            + "))) on 0 from sales");
    }

    public void testNestedCrossJoinWhereMiddleColumnNonNative() {
        checkNative(
            "with "
            + "member gender.agg as 'Aggregate( gender.gender.members )' "
            + "member [marital status].agg as 'Aggregate( [marital status].[marital status].members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  { [marital status].[marital status].members, [marital status].[agg] },"
            + "  crossjoin("
            + "  { gender.[all gender], gender.[agg] }, "
            + "  [Education Level].[Education Level].members "
            + "))) on 0 from sales");
    }

    public void testNestedCrossJoinWhereLastColumnNonNative() {
        checkNative(
            "with "
            + "member gender.agg as 'Aggregate( gender.gender.members )' "
            + "member [marital status].agg as 'Aggregate( [marital status].[marital status].members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  { [marital status].[marital status].members, [marital status].[agg] },"
            + "  crossjoin("
            + "  [Education Level].[Education Level].members, "
            + "  { gender.[all gender], gender.[agg] } "
            + "))) on 0 from sales");
    }

    public void testGenderAggByMaritalStatus() {
        checkNotNative(
            // NativizeSet removes the crossjoin, so not native
            "with member gender.agg as 'Aggregate( gender.gender.members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  { gender.[agg] }, "
            + "  [marital status].[marital status].members "
            + ")) on 0 from sales");
    }

    public void testGenderAggTwiceByMaritalStatus() {
        checkNotNative(
            // NativizeSet removes the crossjoin, so not native
            "with "
            + "member gender.agg1 as 'Aggregate( { gender.M } )' "
            + "member gender.agg2 as 'Aggregate( { gender.F } )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  { gender.[agg1], gender.[agg2] }, "
            + "  [marital status].[marital status].members "
            + ")) on 0 from sales");
    }

    public void testSameGenderAggTwiceByMaritalStatus() {
        checkNotNative(
            // NativizeSet removes the crossjoin, so not native
            "with "
            + "member gender.agg as 'Aggregate( gender.gender.members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  { gender.[agg], gender.[agg] }, "
            + "  [marital status].[marital status].members "
            + ")) on 0 from sales");
    }

    public void testMaritalStatusByGenderAgg() {
        checkNotNative(
            // NativizeSet removes the crossjoin, so not native
            "with member gender.agg as 'Aggregate( gender.gender.members )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  [marital status].[marital status].members, "
            + "  { gender.[agg] } "
            + ")) on 0 from sales");
    }

    public void testMaritalStatusByTwoGenderAggs() {
        checkNotNative(
            // NativizeSet removes the crossjoin, so not native
            "with "
            + "member gender.agg1 as 'Aggregate( { gender.M } )' "
            + "member gender.agg2 as 'Aggregate( { gender.F } )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  [marital status].[marital status].members, "
            + "  { gender.[agg1], gender.[agg2] } "
            + ")) on 0 from sales");
    }

    public void testMaritalStatusBySameGenderAggTwice() {
        checkNotNative(
            // NativizeSet removes the crossjoin, so not native
            "with "
            + "member gender.agg as 'Aggregate( { gender.M } )' "
            + "select NativizeSet("
            + "crossjoin( "
            + "  [marital status].[marital status].members, "
            + "  { gender.[agg], gender.[agg] } "
            + ")) on 0 from sales");
    }

    public void testMultipleLevelsOfSameDimInConcatenatedJoins() {
        checkNotNative(
            // See notes for testMultipleLevelsOfSameDimInSingleArg
            // because the NativizeSetFunDef transforms this mdx into the
            // mdx in that test.
            "select NativizeSet( {"
            + "CrossJoin("
            + "  { [Time].[Year].members },"
            + "  { gender.F, gender. M } ),"
            + "CrossJoin("
            + "  { [Time].[Quarter].members },"
            + "  { gender.F, gender. M } )"
            + "} ) on 0 from sales");
    }

    public void testMultipleLevelsOfSameDimInSingleArg() {
        checkNotNative(
            // Although it's legal MDX, the RolapNativeSet.checkCrossJoinArg
            // can't deal with an arg that contains multiple .members functions.
            // If they were at the same level, the NativizeSetFunDef would
            // deal with them, but since they are at differen levels, we're
            // stuck.
            "select NativizeSet( {"
            + "CrossJoin("
            + "  { [Time].[Year].members,"
            + "    [Time].[Quarter].members },"
            + "  { gender.F, gender. M } )"
            + "} ) on 0 from sales");
    }

    public void testDoesNoHarmToPlainEnumeratedMembers() {
        propSaver.set(propSaver.props.EnableNonEmptyOnAllAxis, false);

        assertQueryIsReWritten(
            "SELECT NativizeSet({Gender.M,Gender.F}) on 0 from sales",
            "select "
            + "NativizeSet({[Customer].[Gender].[M], [Customer].[Gender].[F]}) "
            + "ON COLUMNS\n"
            + "from [Sales]\n");
    }

    public void testDoesNoHarmToPlainDotMembers() {
        propSaver.set(propSaver.props.EnableNonEmptyOnAllAxis, false);

        assertQueryIsReWritten(
            "select NativizeSet({[Marital Status].[Marital Status].members}) "
            + "on 0 from sales",
            "select NativizeSet({[Customer].[Marital Status].[Marital Status].Members}) "
            + "ON COLUMNS\n"
            + "from [Sales]\n");
    }

    public void testTransformsCallToRemoveDotMembersInCrossJoin() {
        propSaver.set(propSaver.props.EnableNonEmptyOnAllAxis, false);

        assertQueryIsReWritten(
            "select NativizeSet(CrossJoin({Gender.M,Gender.F},{[Marital Status].[Marital Status].members})) on 0 from sales",
            "with member [Customer].[Marital Status].[_Nativized_Member_Customer_Marital Status_Marital Status_] as '[Customer].[Marital Status].DefaultMember'\n"
            + "  set [_Nativized_Set_Customer_Marital Status_Marital Status_] as '{[Customer].[Marital Status].[_Nativized_Member_Customer_Marital Status_Marital Status_]}'\n"
            + "  member [Customer].[Gender].[_Nativized_Sentinel_Customer_Gender_(All)_] as '101010'\n"
            + "  member [Customer].[Marital Status].[_Nativized_Sentinel_Customer_Marital Status_(All)_] as '101010'\n"
            + "select NativizeSet(Crossjoin({[Customer].[Gender].[M], [Customer].[Gender].[F]}, {[_Nativized_Set_Customer_Marital Status_Marital Status_]})) ON COLUMNS\n"
            + "from [Sales]\n");
    }

    public void DISABLED_testTransformsWithSeveralDimensionsNestedOnRows() {
        propSaver.set(propSaver.props.EnableNonEmptyOnAllAxis, false);

        assertQueryIsReWritten(
            "WITH SET [COG_OQP_INT_s4] AS 'CROSSJOIN({[Education Level].[Graduate Degree]},"
            + " [COG_OQP_INT_s3])'"
            + " SET [COG_OQP_INT_s3] AS 'CROSSJOIN({[Marital Status].[S]}, [COG_OQP_INT_s2])'"
            + " SET [COG_OQP_INT_s2] AS 'CROSSJOIN({[Gender].[F]}, [COG_OQP_INT_s1])'"
            + " SET [COG_OQP_INT_s1] AS 'CROSSJOIN({[Product].[Product Name].MEMBERS}, {[Customers].[Name].MEMBERS})' "
            + "SELECT {[Measures].[Unit Sales]} DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON AXIS(0),"
            + " NativizeSet([COG_OQP_INT_s4]) DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON AXIS(1) "
            + "FROM [Sales] CELL PROPERTIES VALUE, FORMAT_STRING",
            "with set [COG_OQP_INT_s4] as 'Crossjoin({[Education Level].[Graduate Degree]}, [COG_OQP_INT_s3])'\n"
            + "  set [COG_OQP_INT_s3] as 'Crossjoin({[Marital Status].[S]}, [COG_OQP_INT_s2])'\n"
            + "  set [COG_OQP_INT_s2] as 'Crossjoin({[Gender].[F]}, [COG_OQP_INT_s1])'\n"
            + "  set [COG_OQP_INT_s1] as 'Crossjoin({[_Nativized_Set_Product_Product Name_]}, {[_Nativized_Set_Customers_Name_]})'\n"
            + "  member [Customers].[_Nativized_Member_Customers_Name_] as '[Customers].DefaultMember'\n"
            + "  set [_Nativized_Set_Customers_Name_] as '{[Customers].[_Nativized_Member_Customers_Name_]}'\n"
            + "  member [Product].[_Nativized_Member_Product_Product Name_] as '[Product].DefaultMember'\n"
            + "  set [_Nativized_Set_Product_Product Name_] as '{[Product].[_Nativized_Member_Product_Product Name_]}'\n"
            + "  member [Education Level].[_Nativized_Sentinel_Education Level_(All)_] as '101010'\n"
            + "  member [Marital Status].[_Nativized_Sentinel_Marital Status_(All)_] as '101010'\n"
            + "  member [Gender].[_Nativized_Sentinel_Gender_(All)_] as '101010'\n"
            + "  member [Product].[_Nativized_Sentinel_Product_(All)_] as '101010'\n"
            + "  member [Customers].[_Nativized_Sentinel_Customers_(All)_] as '101010'\n"
            + "select {[Measures].[Unit Sales]} DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON COLUMNS,\n"
            + "  NativizeSet([COG_OQP_INT_s4]) DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON ROWS\n"
            + "from [Sales]\n");
    }

    public void testTransformsComplexQueryWithGenerateAndAggregate() {
        propSaver.set(propSaver.props.EnableNonEmptyOnAllAxis, false);

        assertQueryIsReWritten(
            "WITH MEMBER [Product].[COG_OQP_INT_umg1] AS "
            + "'IIF([Measures].CURRENTMEMBER IS [Measures].[Unit Sales], ([Product].[COG_OQP_INT_m2], [Measures].[Unit Sales]),"
            + " AGGREGATE({[Product].[Product Name].MEMBERS}))', SOLVE_ORDER = 4 "
            + "MEMBER [Product].[COG_OQP_INT_m2] AS 'AGGREGATE({[Product].[Product Name].MEMBERS},"
            + " [Measures].[Unit Sales])', SOLVE_ORDER = 4 "
            + "SET [COG_OQP_INT_s5] AS 'CROSSJOIN({[Marital Status].[S]}, [COG_OQP_INT_s4])'"
            + " SET [COG_OQP_INT_s4] AS 'CROSSJOIN({[Gender].[F]}, [COG_OQP_INT_s2])'"
            + " SET [COG_OQP_INT_s3] AS 'CROSSJOIN({[Gender].[F]}, {[COG_OQP_INT_s2], [COG_OQP_INT_s1]})' "
            + "SET [COG_OQP_INT_s2] AS 'CROSSJOIN({[Product].[Product Name].MEMBERS}, {[Customers].[Name].MEMBERS})' "
            + "SET [COG_OQP_INT_s1] AS 'CROSSJOIN({[Product].[COG_OQP_INT_umg1]}, {[Customers].DEFAULTMEMBER})' "
            + "SELECT {[Measures].[Unit Sales]} DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON AXIS(0),"
            + " NativizeSet(GENERATE({[Education Level].[Graduate Degree]}, \n"
            + "CROSSJOIN(HEAD({([Education Level].CURRENTMEMBER)}, IIF(COUNT([COG_OQP_INT_s5], INCLUDEEMPTY) > 0, 1, 0)), "
            + "GENERATE({[Marital Status].[S]}, CROSSJOIN(HEAD({([Marital Status].CURRENTMEMBER)}, "
            + "IIF(COUNT([COG_OQP_INT_s4], INCLUDEEMPTY) > 0, 1, 0)), [COG_OQP_INT_s3]), ALL)), ALL))"
            + " DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON AXIS(1)"
            + " FROM [Sales]  CELL PROPERTIES VALUE, FORMAT_STRING",
            "with member [Product].[Products].[COG_OQP_INT_umg1] as "
            + "'IIf(([Measures].CurrentMember IS [Measures].[Unit Sales]), ([Product].[Products].[COG_OQP_INT_m2], [Measures].[Unit Sales]), "
            + "Aggregate({[Product].[Products].[Product Name].Members}))', SOLVE_ORDER = 4\n"
            + "  member [Product].[Products].[COG_OQP_INT_m2] as "
            + "'Aggregate({[Product].[Products].[Product Name].Members}, [Measures].[Unit Sales])', SOLVE_ORDER = 4\n"
            + "  set [COG_OQP_INT_s5] as 'Crossjoin({[Customer].[Marital Status].[S]}, [COG_OQP_INT_s4])'\n"
            + "  set [COG_OQP_INT_s4] as 'Crossjoin({[Customer].[Gender].[F]}, [COG_OQP_INT_s2])'\n"
            + "  set [COG_OQP_INT_s3] as 'Crossjoin({[Customer].[Gender].[F]}, {[COG_OQP_INT_s2], [COG_OQP_INT_s1]})'\n"
            + "  set [COG_OQP_INT_s2] as 'Crossjoin({[Product].[Products].[Product Name].Members}, {[Customer].[Customers].[Name].Members})'\n"
            + "  set [COG_OQP_INT_s1] as 'Crossjoin({[Product].[Products].[COG_OQP_INT_umg1]}, {[Customer].[Customers].DefaultMember})'\n"
            + "select {[Measures].[Unit Sales]} DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON COLUMNS,\n"
            + "  NativizeSet(Generate({[Customer].[Education Level].[Graduate Degree]}, "
            + "Crossjoin(Head({[Customer].[Education Level].CurrentMember}, IIf((Count([COG_OQP_INT_s5], INCLUDEEMPTY) > 0), 1, 0)), "
            + "Generate({[Customer].[Marital Status].[S]}, "
            + "Crossjoin(Head({[Customer].[Marital Status].CurrentMember}, "
            + "IIf((Count([COG_OQP_INT_s4], INCLUDEEMPTY) > 0), 1, 0)), [COG_OQP_INT_s3]), ALL)), ALL)) "
            + "DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON ROWS\n"
            + "from [Sales]\n");
    }

    public void DISABLED_testParallelCrossjoins() {
        checkNative(
            // DE2185
            "select NativizeSet( {"
            + "  CrossJoin( { [Marital Status].[Marital Status].members }, { gender.F, gender. M } ),"
            + "  CrossJoin( { [Marital Status].[Marital Status].members }, { gender.F, gender. M } )"
            + "} ) on 0 from sales");
    }

    public void testMultipleHierarchySsasTrue() {
        propSaver.set(propSaver.props.EnableNonEmptyOnAllAxis, false);

        // Ssas compatible: time.[weekly].[week]
        // Use fresh connection -- unique names are baked in when schema is
        // loaded, depending the Ssas setting at that time.
        assertQueryIsReWritten(
            getTestContext().withFreshConnection(),
            "select nativizeSet(crossjoin(time.[week].members, { gender.m })) on 0 "
            + "from sales",
            "with member [Time].[Weekly].[_Nativized_Member_Time_Weekly_Week_] as '[Time].[Weekly].DefaultMember'\n"
            + "  set [_Nativized_Set_Time_Weekly_Week_] as '{[Time].[Weekly].[_Nativized_Member_Time_Weekly_Week_]}'\n"
            + "  member [Time].[Weekly].[_Nativized_Sentinel_Time_Weekly_(All)_] as '101010'\n"
            + "  member [Customer].[Gender].[_Nativized_Sentinel_Customer_Gender_(All)_] as '101010'\n"
            + "select NativizeSet(Crossjoin([_Nativized_Set_Time_Weekly_Week_], {[Customer].[Gender].[M]})) ON COLUMNS\n"
            + "from [Sales]\n");
    }

    public void testComplexCrossjoinAggInMiddle() {
        checkNative(
            "WITH\n"
            + "\tMEMBER [Time].[Time].[COG_OQP_USR_Aggregate(Time Values)] AS "
            + "'IIF([Measures].CURRENTMEMBER IS [Measures].[Unit Sales], ([Time].[1997], [Measures].[Unit Sales]), ([Time].[1997]))',\n"
            + "\tSOLVE_ORDER = 4 MEMBER [Store Type].[COG_OQP_INT_umg1] AS "
            + "'IIF([Measures].CURRENTMEMBER IS [Measures].[Unit Sales], ([Store Type].[COG_OQP_INT_m2], [Measures].[Unit Sales]), "
            + "AGGREGATE({[Store Type].[Store Type].MEMBERS}))',\n"
            + "\tSOLVE_ORDER = 8 MEMBER [Store Type].[COG_OQP_INT_m2] AS "
            + "'AGGREGATE({[Store Type].[Store Type].MEMBERS}, [Measures].[Unit Sales])',\n"
            + "\tSOLVE_ORDER = 8 \n"
            + "SET\n"
            + "\t[COG_OQP_INT_s9] AS 'CROSSJOIN({[Marital Status].[Marital Status].MEMBERS}, {[COG_OQP_INT_s8], [COG_OQP_INT_s6]})' \n"
            + "SET\n"
            + "\t[COG_OQP_INT_s8] AS 'CROSSJOIN({[Store Type].[Store Type].MEMBERS}, [COG_OQP_INT_s7])' \n"
            + "SET\n"
            + "\t[COG_OQP_INT_s7] AS 'CROSSJOIN({[Promotion].[Promotions].MEMBERS}, "
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl].[Pearl Imported Beer]})' \n"
            + "SET\n"
            + "\t[COG_OQP_INT_s6] AS 'CROSSJOIN({[Store Type].[COG_OQP_INT_umg1]}, [COG_OQP_INT_s1])' \n"
            + "SET\n"
            + "\t[COG_OQP_INT_s5] AS 'CROSSJOIN({[Time].[COG_OQP_USR_Aggregate(Time Values)]}, [COG_OQP_INT_s4])' \n"
            + "SET\n"
            + "\t[COG_OQP_INT_s4] AS 'CROSSJOIN({[Gender].DEFAULTMEMBER}, [COG_OQP_INT_s3])' \n"
            + "SET\n"
            + "\t[COG_OQP_INT_s3] AS 'CROSSJOIN({[Marital Status].DEFAULTMEMBER}, [COG_OQP_INT_s2])' \n"
            + "SET\n"
            + "\t[COG_OQP_INT_s2] AS 'CROSSJOIN({[Store Type].DEFAULTMEMBER}, [COG_OQP_INT_s1])' \n"
            + "SET\n"
            + "\t[COG_OQP_INT_s11] AS 'CROSSJOIN({[Gender].[Gender].MEMBERS}, [COG_OQP_INT_s10])' \n"
            + "SET\n"
            + "\t[COG_OQP_INT_s10] AS 'CROSSJOIN({[Marital Status].[Marital Status].MEMBERS}, [COG_OQP_INT_s8])' \n"
            + "SET\n"
            + "\t[COG_OQP_INT_s1] AS 'CROSSJOIN({[Promotion Name].DEFAULTMEMBER}, "
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl].[Pearl Imported Beer]})' \n"
            + "SELECT\n"
            + "\t{[Measures].[Unit Sales]} DIMENSION PROPERTIES PARENT_LEVEL,\n"
            + "\tCHILDREN_CARDINALITY,\n"
            + "\tPARENT_UNIQUE_NAME ON AXIS(0),\n"
            + "NativizeSet(\n"
            + "\t{\n"
            + "CROSSJOIN({[Time].[1997]}, CROSSJOIN({[Gender].[Gender].MEMBERS}, [COG_OQP_INT_s9])),\n"
            + "\t[COG_OQP_INT_s5]}\n"
            + ")\n"
            + "ON AXIS(1) \n"
            + "FROM\n"
            + "\t[Sales] ");
    }

    public void testTopCountDoesNotGetTransformed() {
        assertQueryIsReWritten(
            "select "
            + "   NativizeSet(Crossjoin([Gender].[Gender].members,"
            + "TopCount({[Marital Status].[Marital Status].members},1,[Measures].[Unit Sales]))"
            + " ) on 0,"
            + "{[Measures].[Unit Sales]} on 1 FROM [Sales]",
            "with member [Customer].[Gender].[_Nativized_Member_Customer_Gender_Gender_] as '[Customer].[Gender].DefaultMember'\n"
            + "  set [_Nativized_Set_Customer_Gender_Gender_] as '{[Customer].[Gender].[_Nativized_Member_Customer_Gender_Gender_]}'\n"
            + "  member [Customer].[Gender].[_Nativized_Sentinel_Customer_Gender_(All)_] as '101010'\n"
            + "select NON EMPTY NativizeSet(Crossjoin([_Nativized_Set_Customer_Gender_Gender_], TopCount({[Customer].[Marital Status].[Marital Status].Members}, 1, [Measures].[Unit Sales]))) ON COLUMNS,\n"
            + "  NON EMPTY {[Measures].[Unit Sales]} ON ROWS\n"
            + "from [Sales]\n");
    }

    public void testCrossjoinWithFilter() {
        assertQueryReturns(
            "select\n"
            + "NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS,   \n"
            + "NON EMPTY NativizeSet(Crossjoin({[Time].[1997]}, "
            + "Filter({[Gender].[Gender].Members}, ([Measures].[Unit Sales] < 131559)))) ON ROWS \n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997], [Customer].[Gender].[F]}\n"
            + "Row #0: 131,558\n");
    }

    public void testEvaluationIsNonNativeWhenBelowHighcardThreshoold() {
        propSaver.set(propSaver.props.NativizeMinThreshold, 10000);
        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS,
                "select `customer`.`gender` as `c0` "
                + "from `customer` as `customer`, `sales_fact_1997` as `sales_fact_1997` "
                + "where `sales_fact_1997`.`customer_id` = `customer`.`customer_id` "
                + "and `customer`.`marital_status` = 'S' "
                + "group by `customer`.`gender` order by 1 ASC", 251)
        };
        String mdxQuery =
            "select non empty NativizeSet("
            + "Crossjoin([Gender].[Gender].members,{[Time].[1997]})) on 0 "
            + "from [Warehouse and Sales] "
            + "where [Marital Status].[Marital Status].[S]";
        assertQuerySqlOrNot(
            getTestContext(), mdxQuery, patterns, true, false, true);
    }

    public void testCalculatedLevelsDoNotCauseException() {
        String mdx =
            "SELECT "
            + "  Nativizeset"
            + "  ("
            + "    {"
            + "      [Stores].Levels(0).MEMBERS"
            + "    }"
            + "  ) ON COLUMNS"
            + " FROM [Sales]";
        checkNotNative(mdx);
    }

    public void testAxisWithArityOneIsNotNativelyEvaluated() {
        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS,
                "select `promotion`.`media_type` as `c0` "
                + "from `promotion` as `promotion`, `sales_fact_1997` as `sales_fact_1997` "
                + "where `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id` "
                + "group by `promotion`.`media_type` "
                + "order by Iif(`promotion`.`media_type` IS NULL, 1, 0), "
                + "`promotion`.`media_type` ASC", 296)
        };
        String query =
            "select "
            + "  NON EMPTY "
            + "  NativizeSet("
            + "    Except("
            + "      {[Promotion].[Media Type].Members},\n"
            + "      {[Promotion].[Media Type].[Bulk Mail],[Promotion].[Media Type].[All Media].[Daily Paper]}"
            + "    )"
            + "  ) ON COLUMNS,"
            + "  NON EMPTY "
            + "  {[Measures].[Unit Sales]} ON ROWS "
            + "from [Sales] \n"
            + "where [Time].[1997]";
        assertQuerySqlOrNot(
            getTestContext(), query, patterns, true, false, true);
    }

    public void testAxisWithNamedSetArityOneIsNotNativelyEvaluated() {
        checkNotNative(
            "with "
            + "set [COG_OQP_INT_s1] as "
            + "'Intersect({[Gender].[Gender].Members}, {[Gender].[Gender].[M]})' "
            + "select NON EMPTY "
            + "NativizeSet([COG_OQP_INT_s1]) ON COLUMNS "
            + "from [Sales]");
    }

    public void testOneAxisHighAndOneLowGetsNativeEvaluation() {
        propSaver.set(propSaver.props.NativizeMinThreshold, 19);
        checkNative(
            "select NativizeSet("
            + "Crossjoin([Gender].[Gender].members,"
            + "[Marital Status].[Marital Status].members)) on 0,"
            + "NativizeSet("
            + "Crossjoin([Store].[Store State].members,[Time].[Year].members)) on 1 "
            + "from [Warehouse and Sales]");
    }

    public void disabled_testAggregatesInSparseResultsGetSortedCorrectly() {
        propSaver.set(propSaver.props.NativizeMinThreshold, 0);
        checkNative(
            "select non empty NativizeSet("
            + "Crossjoin({[Store Type].[Store Type].members,[Store Type].[all store types]},"
            + "{ [Promotion].[Media Type].members }"
            + ")) on 0 from sales");
    }

    public void testLeafMembersOfParentChildDimensionAreNativelyEvaluated() {
        checkNative(
            "SELECT"
            + " NON EMPTY "
            + "NativizeSet(Crossjoin("
            + "{"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin].[Gabriel Walton],"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin].[Bishop Meastas],"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin].[Paula Duran],"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin].[Margaret Earley],"
            + "[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin].[Elizabeth Horne]"
            + "},"
            + "[Store].[Store Name].members"
            + ")) on 0 from hr");
    }

    public void testAggregatedCrossjoinWithZeroMembersInNativeList() {
        propSaver.set(propSaver.props.NativizeMinThreshold, 0);
        checkNative(
            "with"
            + " member [gender].[agg] as"
            + "  'aggregate({[gender].[gender].members},[measures].[unit sales])'"
            + " member [Marital Status].[agg] as"
            + "  'aggregate({[Marital Status].[Marital Status].members},[measures].[unit sales])'"
            + "select"
            + " non empty "
            + " NativizeSet("
            + "Crossjoin("
            + "{[Marital Status].[Marital Status].members,[Marital Status].[agg]},"
            + "{[Gender].[Gender].members,[gender].[agg]}"
            + ")) on 0 "
            + " from sales "
            + " where [Store].[Canada].[BC].[Vancouver].[Store 19]");
    }

    public void testCardinalityQueriesOnlyExecuteOnce() {
        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE,
                "select count(*) as \"c0\" "
                + "from (select "
                + "distinct \"customer\".\"gender\" as \"c0\" "
                + "from \"customer\" \"customer\") \"init\"",
                108),
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS,
                "select count(*) as `c0` "
                + "from (select "
                + "distinct `customer`.`gender` as `c0` "
                + "from `customer` as `customer`) as `init`",
                108)
        };
        String mdxQuery =
            "select"
            + " non empty"
            + " NativizeSet(Crossjoin("
            + "[Gender].[Gender].members,[Marital Status].[Marital Status].members"
            + ")) on 0 from Sales";
        getConnection().execute(getConnection().parseQuery(mdxQuery));
        assertQuerySqlOrNot(
            getTestContext(), mdxQuery, patterns, true, false, false);
    }

    public void testSingleLevelDotMembersIsNativelyEvaluated() {
        String mdx1 =
            "with member [Customers].[agg] as '"
            + "AGGREGATE({[Customers].[name].MEMBERS}, [Measures].[Unit Sales])'"
            + "select non empty NativizeSet({{[Customers].[name].members}, {[Customers].[agg]}}) on 0,"
            + "non empty NativizeSet("
            + "Crossjoin({[Gender].[Gender].[M]},"
            + "[Measures].[Unit Sales])) on 1 "
            + "from Sales";
        String mdx2 =
            "select non empty NativizeSet({[Customers].[name].members}) on 0,"
            + "non empty NativizeSet("
            + "Crossjoin({[Gender].[Gender].[M]},"
            + "[Measures].[Unit Sales])) on 1 "
            + "from Sales";

        String sql = "select \"customer\".\"country\" as \"c0\", "
            + "\"customer\".\"state_province\" as \"c1\", "
            + "\"customer\".\"city\" as \"c2\", "
            + "\"customer\".\"customer_id\" as \"c3\", \"fname\" || ' ' || \"lname\" as \"c4\", "
            + "\"fname\" || ' ' || \"lname\" as \"c5\", \"customer\".\"gender\" as \"c6\", "
            + "\"customer\".\"marital_status\" as \"c7\", "
            + "\"customer\".\"education\" as \"c8\", \"customer\".\"yearly_income\" as \"c9\" "
            + "from \"customer\" \"customer\", \"sales_fact_1997\" \"sales_fact_1997\" "
            + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
            + "and (\"customer\".\"gender\" = 'M') "
            + "group by \"customer\".\"country\", \"customer\".\"state_province\", "
            + "\"customer\".\"city\", \"customer\".\"customer_id\", \"fname\" || ' ' || \"lname\", "
            + "\"customer\".\"gender\", \"customer\".\"marital_status\", \"customer\".\"education\", "
            + "\"customer\".\"yearly_income\" "
            + "order by \"customer\".\"country\" ASC NULLS LAST, "
            + "\"customer\".\"state_province\" ASC NULLS LAST, \"customer\".\"city\" ASC NULLS LAST, "
            + "\"fname\" || ' ' || \"lname\" ASC NULLS LAST";
        SqlPattern oraclePattern =
            new SqlPattern(Dialect.DatabaseProduct.ORACLE, sql, sql.length());
        TestContext testContext = getTestContext();
        assertQuerySql(testContext, mdx1, new SqlPattern[]{oraclePattern});
        assertQuerySql(testContext, mdx2, new SqlPattern[]{oraclePattern});
    }

    // ~ ====== Helper methods =================================================

    private void checkNotNative(String mdx) {
        final String mdx2 = removeNativize(mdx);
        final Result result = getTestContext().executeQuery(mdx2);
        checkNotNative(mdx, result);
    }

    private void checkNative(String mdx) {
        final String mdx2 = removeNativize(mdx);
        final Result result = getTestContext().executeQuery(mdx2);
        checkNative(mdx, result);
    }

    private static String removeNativize(String mdx) {
        String mdxWithoutNativize = mdx.replaceAll("(?i)NativizeSet", "");
        assertFalse(
            "Query does use NativizeSet", mdx.equals(mdxWithoutNativize));
        return mdxWithoutNativize;
    }

    private void assertQueryIsReWritten(
        final String query, final String expectedQuery)
    {
        assertQueryIsReWritten(getTestContext(), query, expectedQuery);
    }

    private void assertQueryIsReWritten(
        TestContext testContext,
        final String query,
        final String expectedQuery)
    {
        final RolapConnection connection =
            (RolapConnection) testContext.getConnection();
        String actualOutput =
            Locus.execute(
                connection,
                NativizeSetFunDefTest.class.getName(),
                new Locus.Action<String>() {
                    public String execute() {
                        return connection.parseQuery(query).toString();
                    }
                }
            );
        if (!Util.nl.equals("\n")) {
            actualOutput = actualOutput.replace(Util.nl, "\n");
        }
        assertEquals(expectedQuery, actualOutput);
    }


    public void testCompoundSlicerNativeEval() {
        if (!Bug.BugMondrian1420Fixed) {
            return;
        }
        // MONDRIAN-1404 use case
        propSaver.set(
            propSaver.props.GenerateFormattedSql,
            true);
        propSaver.set(
            propSaver.props.UseAggregates,
            false);
        final String mdx =
            "select NON EMPTY [Customers].[USA].[CA].[San Francisco].Children ON COLUMNS \n"
            + "from [Sales] \n"
            + "where ([Time].[1997].[Q1] : [Time].[1997].[Q3]) \n";

        final String mysql =
            "select\n"
            + "    `customer`.`customer_id` as `c0`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c1`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c2`,\n"
            + "    `customer`.`gender` as `c3`,\n"
            + "    `customer`.`marital_status` as `c4`,\n"
            + "    `customer`.`education` as `c5`,\n"
            + "    `customer`.`yearly_income` as `c6`\n"
            + "from\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `customer` as `customer`\n"
            + "where\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            +    "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`quarter` in ('Q1', 'Q2', 'Q3')\n"
            + "and\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `customer`.`state_province` = 'CA'\n"
            + "and\n"
            + "    `customer`.`city` = 'San Francisco'\n"
            + "group by\n"
            + "    `customer`.`customer_id`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`),\n"
            + "    `customer`.`gender`,\n"
            + "    `customer`.`marital_status`,\n"
            + "    `customer`.`education`,\n"
            + "    `customer`.`yearly_income`\n"
            + "order by\n"
            + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                mysql,
                mysql);

        assertQuerySql(getTestContext(), mdx, new SqlPattern[]{mysqlPattern});

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "{[Time].[1997].[Q2]}\n"
            + "{[Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Dennis Messer]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Esther Logsdon]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Karen Moreland]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Kent Brant]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Louise Wakefield]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Reta Mikalas]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Tammy Mihalek]}\n"
            + "Row #0: 8\n"
            + "Row #0: 3\n"
            + "Row #0: 13\n"
            + "Row #0: 5\n"
            + "Row #0: 13\n"
            + "Row #0: 10\n"
            + "Row #0: 1\n");
    }

    public void testSnowflakeDimInSlicerBug1407() {
        // MONDRIAN-1407 use case
        if (!Bug.BugMondrian1420Fixed) {
            return;
        }
        propSaver.set(
            propSaver.props.GenerateFormattedSql,
            true);
        propSaver.set(
            propSaver.props.UseAggregates,
            false);
        final String mdx =
            "select TopCount([Customers].[Name].members, 5, measures.[unit sales]) ON COLUMNS \n"
            + "  from sales where \n"
            + " { [Time].[1997]} * {[Product].[All Products].[Drink], [Product].[All Products].[Food] }";

        final String mysql =
            "select\n"
            + "    `customer`.`country` as `c0`,\n"
            + "    `customer`.`state_province` as `c1`,\n"
            + "    `customer`.`city` as `c2`,\n"
            + "    `customer`.`customer_id` as `c3`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`,\n"
            + "    `customer`.`gender` as `c6`,\n"
            + "    `customer`.`marital_status` as `c7`,\n"
            + "    `customer`.`education` as `c8`,\n"
            + "    `customer`.`yearly_income` as `c9`,\n"
            + "    sum(`sales_fact_1997`.`unit_sales`) as `c10`\n"
            + "from\n"
            + "    `customer` as `customer`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`\n"
            + "where\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `product_class`.`product_family` in ('Drink', 'Food')\n"
            + "group by\n"
            + "    `customer`.`country`,\n"
            + "    `customer`.`state_province`,\n"
            + "    `customer`.`city`,\n"
            + "    `customer`.`customer_id`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`),\n"
            + "    `customer`.`gender`,\n"
            + "    `customer`.`marital_status`,\n"
            + "    `customer`.`education`,\n"
            + "    `customer`.`yearly_income`\n"
            + "order by\n"
            + "    `c10` DESC,\n"
            + "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
            + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
            + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
            + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                mysql,
                mysql);

        assertQuerySql(getTestContext(), mdx, new SqlPattern[]{mysqlPattern});
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997], [Product].[Drink]}\n"
            + "{[Time].[1997], [Product].[Food]}\n"
            + "Axis #1:\n"
            + "{[Customers].[USA].[WA].[Spokane].[Mary Francis Benigar]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[James Horvat]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Wildon Cameron]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Ida Rodriguez]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Joann Mramor]}\n"
            + "Row #0: 427\n"
            + "Row #0: 384\n"
            + "Row #0: 366\n"
            + "Row #0: 357\n"
            + "Row #0: 324\n");
    }

    public void testCompoundSlicerNonUniqueMemberNames1413() {
        // MONDRIAN-1413 use case
        if (!Bug.BugMondrian1420Fixed) {
            return;
        }
        propSaver.set(
            propSaver.props.GenerateFormattedSql,
            true);
        propSaver.set(
            propSaver.props.UseAggregates,
            false);
        final String mdx =
            "select TopCount([Customers].[Name].members, 5, "
            + "measures.[unit sales]) ON COLUMNS \n"
            + "  from sales where \n"
            + "  {[Time.Weekly].[1997].[48].[17] :[Time.Weekly].[1997].[48].[20]} ";

        final String mysql =
            "select\n"
            + "    `customer`.`country` as `c0`,\n"
            + "    `customer`.`state_province` as `c1`,\n"
            + "    `customer`.`city` as `c2`,\n"
            + "    `customer`.`customer_id` as `c3`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`,\n"
            + "    `customer`.`gender` as `c6`,\n"
            + "    `customer`.`marital_status` as `c7`,\n"
            + "    `customer`.`education` as `c8`,\n"
            + "    `customer`.`yearly_income` as `c9`,\n"
            + "    sum(`sales_fact_1997`.`unit_sales`) as `c10`\n"
            + "from\n"
            + "    `customer` as `customer`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`week_of_year` = 48\n"
            + "and\n"
            + "    `time_by_day`.`day_of_month` in (17, 18, 19, 20)\n"
            + "group by\n"
            + "    `customer`.`country`,\n"
            + "    `customer`.`state_province`,\n"
            + "    `customer`.`city`,\n"
            + "    `customer`.`customer_id`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`),\n"
            + "    `customer`.`gender`,\n"
            + "    `customer`.`marital_status`,\n"
            + "    `customer`.`education`,\n"
            + "    `customer`.`yearly_income`\n"
            + "order by\n"
            + "    `c10` DESC,\n"
            + "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
            + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
            + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
            + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                mysql,
                mysql);
        assertQuerySql(getTestContext(), mdx, new SqlPattern[]{mysqlPattern});

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[Weekly].[1997].[48].[17]}\n"
            + "{[Time].[Weekly].[1997].[48].[18]}\n"
            + "{[Time].[Weekly].[1997].[48].[19]}\n"
            + "{[Time].[Weekly].[1997].[48].[20]}\n"
            + "Axis #1:\n"
            + "{[Customers].[USA].[WA].[Yakima].[Joanne Skuderna]}\n"
            + "{[Customers].[USA].[WA].[Yakima].[Paula Stevens]}\n"
            + "{[Customers].[USA].[WA].[Everett].[Sarah Miller]}\n"
            + "{[Customers].[USA].[OR].[Albany].[Kathryn Chamberlin]}\n"
            + "{[Customers].[USA].[OR].[Salem].[Scott Pavicich]}\n"
            + "Row #0: 37\n"
            + "Row #0: 32\n"
            + "Row #0: 29\n"
            + "Row #0: 28\n"
            + "Row #0: 28\n");
    }


}

// End NativizeSetFunDefTest.java
