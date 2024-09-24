/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2015-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.test.TestContext;

import static mondrian.rolap.RolapNativeTopCountTestCases.*;

/**
 * @author Andrey Khayrutdinov
 */
public class RolapNativeTopCountVersusNonNativeTest extends BatchTestCase {

    private void assertResultsAreEqual(String testCase, String query) {
        assertResultsAreEqual(testCase, query, getTestContext());
    }

    private void assertResultsAreEqual(
        String testCase,
        String query,
        TestContext ctx)
    {
        String message = String.format(
            "[%s]: native and non-native results of the query differ. The query:\n\t\t%s",
            testCase,
            query);
        verifySameNativeAndNot(query, message, ctx);
    }


    public void testTopCount_ImplicitCountMeasure() throws Exception {
        assertResultsAreEqual(
            "Implicit Count Measure", IMPLICIT_COUNT_MEASURE_QUERY);
    }

    public void testTopCount_SumMeasure() throws Exception {
        assertResultsAreEqual(
            "Sum Measure", SUM_MEASURE_QUERY);
    }

    public void testTopCount_CountMeasure() throws Exception {
        final String schema = TestContext.instance()
            .getSchema(null, CUSTOM_COUNT_MEASURE_CUBE, null, null, null, null);

        TestContext ctx = TestContext.instance()
            .withSchema(schema)
            .withCube(CUSTOM_COUNT_MEASURE_CUBE_NAME);

        assertResultsAreEqual(
            "Custom Count Measure", CUSTOM_COUNT_MEASURE_QUERY, ctx);
    }


    public void testEmptyCellsAreShown_Countries() throws Exception {
        assertResultsAreEqual(
            "Empty Cells Are Shown - Countries",
            EMPTY_CELLS_ARE_SHOWN_COUNTRIES_QUERY);
    }

    public void testEmptyCellsAreShown_States() throws Exception {
        assertResultsAreEqual(
            "Empty Cells Are Shown - States",
            EMPTY_CELLS_ARE_SHOWN_STATES_QUERY);
    }

    public void testEmptyCellsAreShown_ButNoMoreThanReallyExist() {
        assertResultsAreEqual(
            "Empty Cells Are Shown - But no more than really exist",
            EMPTY_CELLS_ARE_SHOWN_NOT_MORE_THAN_EXIST_QUERY);
    }

    public void testEmptyCellsAreHidden_WhenNonEmptyIsDeclaredExplicitly() {
        assertResultsAreEqual(
            "Empty Cells Are Hidden - When NON EMPTY is declared explicitly",
            EMPTY_CELLS_ARE_HIDDEN_WHEN_NON_EMPTY_QUERY);
    }


    public void testRoleRestrictionWorks_ForRowWithData() {
        TestContext ctx = TestContext.instance()
            .create(
                null, null, null, null, null,
                ROLE_RESTRICTION_WORKS_WA_ROLE_DEF)
            .withRole(ROLE_RESTRICTION_WORKS_WA_ROLE_NAME);

        assertResultsAreEqual(
            "Role restriction works - For WA state",
            ROLE_RESTRICTION_WORKS_WA_QUERY, ctx);
    }

    public void testRoleRestrictionWorks_ForRowWithOutData() {
        TestContext ctx = TestContext.instance()
            .create(
                null, null, null, null, null,
                ROLE_RESTRICTION_WORKS_DF_ROLE_DEF)
            .withRole(ROLE_RESTRICTION_WORKS_DF_ROLE_NAME);

        assertResultsAreEqual(
            "Role restriction works - For DF state",
            ROLE_RESTRICTION_WORKS_DF_QUERY, ctx);
    }


    public void testMimicsHeadWhenTwoParams_States() {
        assertResultsAreEqual(
            "Two Parameters - States",
            TOPCOUNT_MIMICS_HEAD_WHEN_TWO_PARAMS_STATES_QUERY);
    }

    public void testMimicsHeadWhenTwoParams_Cities() {
        assertResultsAreEqual(
            "Two Parameters - Cities",
            TOPCOUNT_MIMICS_HEAD_WHEN_TWO_PARAMS_CITIES_QUERY);
    }

    public void testMimicsHeadWhenTwoParams_ShowsNotMoreThanExist() {
        assertResultsAreEqual(
            "Two Parameters - Shows not more than really exist",
            RESULTS_ARE_SHOWN_NOT_MORE_THAN_EXIST_2_PARAMS_QUERY);
    }

    public void testMimicsHeadWhenTwoParams_DoesNotIgnoreNonEmpty() {
        assertResultsAreEqual(
            "Two Parameters - Does not ignore NON EMPTY",
            NON_EMPTY_IS_NOT_IGNORED_WHEN_TWO_PARAMS_QUERY);
    }
}

// End RolapNativeTopCountVersusNonNativeTest.java