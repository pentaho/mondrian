/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
package mondrian.rolap;

import mondrian.test.TestContext;

import static mondrian.rolap.RolapNativeTopCountTestCases.*;

/**
 * @author Andrey Khayrutdinov
 * @see RolapNativeTopCountTestCases
 */
public class RolapNativeTopCountTest extends BatchTestCase {

    public void setUp() throws Exception {
        super.setUp();
        propSaver.set(propSaver.properties.EnableNativeTopCount, true);
    }

    public void testTopCount_ImplicitCountMeasure() throws Exception {
        assertQueryReturns(
            IMPLICIT_COUNT_MEASURE_QUERY, IMPLICIT_COUNT_MEASURE_RESULT);
    }

    public void testTopCount_CountMeasure() throws Exception {
        final String schema = TestContext.instance()
                .getSchema(
                    null, CUSTOM_COUNT_MEASURE_CUBE,
                    null, null, null, null);

        TestContext ctx = TestContext.instance()
                .withSchema(schema)
                .withCube(CUSTOM_COUNT_MEASURE_CUBE_NAME);

        ctx.assertQueryReturns(
            CUSTOM_COUNT_MEASURE_QUERY, CUSTOM_COUNT_MEASURE_RESULT);
    }

    public void testTopCount_SumMeasure() throws Exception {
        assertQueryReturns(SUM_MEASURE_QUERY, SUM_MEASURE_RESULT);
    }


    public void testEmptyCellsAreShown_Countries() {
        assertQueryReturns(
            EMPTY_CELLS_ARE_SHOWN_COUNTRIES_QUERY,
            EMPTY_CELLS_ARE_SHOWN_COUNTRIES_RESULT);
    }

    public void testEmptyCellsAreShown_States() {
        assertQueryReturns(
            EMPTY_CELLS_ARE_SHOWN_STATES_QUERY,
            EMPTY_CELLS_ARE_SHOWN_STATES_RESULT);
    }

    public void testEmptyCellsAreShown_ButNoMoreThanReallyExist() {
        assertQueryReturns(
            EMPTY_CELLS_ARE_SHOWN_NOT_MORE_THAN_EXIST_QUERY,
            EMPTY_CELLS_ARE_SHOWN_NOT_MORE_THAN_EXIST_RESULT);
    }

    public void testEmptyCellsAreHidden_WhenNonEmptyIsDeclaredExplicitly() {
        assertQueryReturns(
            EMPTY_CELLS_ARE_HIDDEN_WHEN_NON_EMPTY_QUERY,
            EMPTY_CELLS_ARE_HIDDEN_WHEN_NON_EMPTY_RESULT);
    }


    public void testRoleRestrictionWorks_ForRowWithData() throws Exception {
        TestContext ctx = TestContext.instance()
            .create(
                null, null, null, null, null,
                ROLE_RESTRICTION_WORKS_WA_ROLE_DEF)
            .withRole(ROLE_RESTRICTION_WORKS_WA_ROLE_NAME);

        ctx.assertQueryReturns(
            ROLE_RESTRICTION_WORKS_WA_QUERY,
            ROLE_RESTRICTION_WORKS_WA_RESULT);
    }

    public void testRoleRestrictionWorks_ForRowWithOutData() throws Exception {
        TestContext ctx = TestContext.instance()
            .create(
                null, null, null, null, null,
                ROLE_RESTRICTION_WORKS_DF_ROLE_DEF)
            .withRole(ROLE_RESTRICTION_WORKS_DF_ROLE_NAME);

        ctx.assertQueryReturns(
            ROLE_RESTRICTION_WORKS_DF_QUERY,
            ROLE_RESTRICTION_WORKS_DF_RESULT);
    }


    public void testMimicsHeadWhenTwoParams_States() {
        assertQueryReturns(
            TOPCOUNT_MIMICS_HEAD_WHEN_TWO_PARAMS_STATES_QUERY,
            TOPCOUNT_MIMICS_HEAD_WHEN_TWO_PARAMS_STATES_RESULT);
    }

    public void testMimicsHeadWhenTwoParams_Cities() {
        assertQueryReturns(
            TOPCOUNT_MIMICS_HEAD_WHEN_TWO_PARAMS_CITIES_QUERY,
            TOPCOUNT_MIMICS_HEAD_WHEN_TWO_PARAMS_CITIES_RESULT);
    }

    public void testMimicsHeadWhenTwoParams_ShowsNotMoreThanExist() {
        assertQueryReturns(
            RESULTS_ARE_SHOWN_NOT_MORE_THAN_EXIST_2_PARAMS_QUERY,
            RESULTS_ARE_SHOWN_NOT_MORE_THAN_EXIST_2_PARAMS_RESULT);
    }

    public void testMimicsHeadWhenTwoParams_DoesNotIgnoreNonEmpty() {
        assertQueryReturns(
            NON_EMPTY_IS_NOT_IGNORED_WHEN_TWO_PARAMS_QUERY,
            NON_EMPTY_IS_NOT_IGNORED_WHEN_TWO_PARAMS_RESULT);
    }
}

// End RolapNativeTopCountTest.java
