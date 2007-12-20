/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// ajogleka, 19 December, 2007
*/
package mondrian.rolap.agg;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

/**
 * <code>AggregationOnDistinctCountMeasureTest</code> tests the
 * Distinct Count functionality with tuples and members.
 *
 * @author ajogleka
 * @version $Id$
 * @since 19 December, 2007
 */
public class AggregationOnDistinctCountMeasuresTest extends FoodMartTestCase {

    public TestContext getTestContext() {
        final TestContext testContext =
            TestContext.create(null, null,
                "<VirtualCube name=\"Warehouse and Sales2\" defaultMeasure=\"Store Sales\">\n" +
                "<VirtualCubeDimension cubeName=\"Sales\" name=\"Gender\"/>\n" +
                "<VirtualCubeDimension name=\"Store\"/>\n" +
                "<VirtualCubeDimension name=\"Product\"/>\n" +
                "<VirtualCubeDimension cubeName=\"Warehouse\" name=\"Warehouse\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Customer Count]\"/>\n" +
                "</VirtualCube>",
                null, null, null);
        return testContext;
    }

    public void testTupleWithAllLevelMembersOnly() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({([GENDER].DEFAULTMEMBER,\n" +
            "[STORE].DEFAULTMEMBER)})'\n" +
            "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Gender].[X]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Row #0: 5,581\n"));

    }

    public void testCrossJoinOfAllMembers() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({CROSSJOIN({[GENDER].DEFAULTMEMBER},\n" +
            "{[STORE].DEFAULTMEMBER})})'\n" +
            "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Gender].[X]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Row #0: 5,581\n"));

    }

    public void testCrossJoinMembersWithASingleMember() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members} * " +
            "{[STORE].[ALL STORES].[USA].[CA]})', solve_order=100 " +
            "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Gender].[X]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Row #0: 2,716\n"));

    }

    public void testCrossJoinParticularMembersFromTwoDimensions() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].M} * " +
            "{[STORE].[ALL STORES].[USA].[CA]})', solve_order=100 " +
            "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Gender].[X]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Row #0: 1,389\n"));

    }

    public void testDistinctCountOnSetOfMembersFromOneDimension() {
        assertQueryReturns(
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members})'" +
            "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Gender].[X]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Row #0: 5,581\n"));

    }

    public void testDistinctCountWithAMeasureAsPartOfTuple() {
        assertQueryReturns("SELECT [STORE].[ALL STORES].[USA].[CA] ON 0, " +
            "([MEASURES].[CUSTOMER COUNT], [Gender].[m]) ON 1 FROM SALES",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Store].[All Stores].[USA].[CA]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Customer Count], [Gender].[All Gender].[M]}\n" +
                "Row #0: 1,389\n"));
    }

    public void testDistinctCountOnSetOfMembers() {
        assertQueryReturns(
            "WITH MEMBER STORE.X as 'Aggregate({[STORE].[ALL STORES].[USA].[CA]," +
            "[STORE].[ALL STORES].[USA].[WA]})'" +
            "SELECT STORE.X  ON ROWS, " +
            "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
            "FROM [SALES]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Store].[X]}\n" +
                "Row #0: 4,544\n"));
    }

    public void testDistinctCountOnTuplesWithSomeNonJoiningDimensions() {
        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X as 'Aggregate({WAREHOUSE.[STATE PROVINCE].MEMBERS}*" +
            "{[Gender].Members})'" +
            "SELECT WAREHOUSE.X  ON ROWS, " +
            "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
            "FROM [WAREHOUSE AND SALES2]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Warehouse].[X]}\n" +
                "Row #0: \n"));

    }

    public void testDistinctCountOnMembersWithNonJoiningDimension() {
        assertQueryReturns(
            "WITH MEMBER WAREHOUSE.X as 'Aggregate({WAREHOUSE.[STATE PROVINCE].MEMBERS})'" +
                "SELECT WAREHOUSE.X  ON ROWS, " +
                "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
                "FROM [WAREHOUSE AND SALES2]",
            fold(
                "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Customer Count]}\n" +
                    "Axis #2:\n" +
                    "{[Warehouse].[X]}\n" +
                    "Row #0: \n"));

    }

    public void testDistinctCountOnTuplesWithLargeNumberOfDimensionMembers() {
        assertQueryReturns(
            "WITH MEMBER PRODUCT.X as 'Aggregate({PRODUCT.MEMBERS}*{GENDER.MEMBERS})' " +
            "SELECT PRODUCT.X  ON ROWS, " +
            "{[MEASURES].[CUSTOMER COUNT]} ON COLUMNS\n" +
            "FROM [WAREHOUSE AND SALES2]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Product].[X]}\n" +
                "Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: " +
                "Distinct Count aggregation is not supported over a large list\n"));
    }

}
