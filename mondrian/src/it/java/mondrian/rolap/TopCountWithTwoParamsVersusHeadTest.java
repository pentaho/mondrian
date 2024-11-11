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

import mondrian.olap.Result;
import mondrian.test.TestContext;

import static mondrian.rolap.RolapNativeTopCountTestCases.*;

/**
 * According to
 * <a href="https://msdn.microsoft.com/en-us/library/ms144792.aspx">MSDN
 * page</a>, when {@code TOPCOUNT} function is called with two parameters
 * it should mimic the behaviour of {@code HEAD} function.
 * <p/>
 * The idea of these tests is to compare results of both function being
 * called with same parameters - they should be equal.
 * <p/>
 * Queries with {@code HEAD} are made by mere substitution of the name
 * instead of {@code TOPCOUNT}.
 *
 * @author Andrey Khayrutdinov
 */
public class TopCountWithTwoParamsVersusHeadTest extends BatchTestCase {

    private void assertResultsAreEqual(
        String testCase,
        String topCountQuery)
    {
        if (!topCountQuery.contains("TOPCOUNT")) {
            throw new IllegalArgumentException(
                "'TOPCOUNT' was not found. Please ensure you are using upper case:\n\t\t"
                    + topCountQuery);
        }

        String headQuery = topCountQuery.replace("TOPCOUNT", "HEAD");

        TestContext ctx = getTestContext();
        Result topCountResult = ctx.executeQuery(topCountQuery);
        ctx.flushSchemaCache();
        Result headResult = ctx.executeQuery(headQuery);
        assertEquals(
            String.format(
                "[%s]: TOPCOUNT() and HEAD() results of the query differ. The query:\n\t\t%s",
                testCase,
                topCountQuery),
            TestContext.toString(topCountResult),
            TestContext.toString(headResult));
    }


    public void test_States() throws Exception {
        assertResultsAreEqual(
            "States",
            TOPCOUNT_MIMICS_HEAD_WHEN_TWO_PARAMS_STATES_QUERY);
    }

    public void test_Cities() throws Exception {
        assertResultsAreEqual(
            "Cities",
            TOPCOUNT_MIMICS_HEAD_WHEN_TWO_PARAMS_CITIES_QUERY);
    }

    public void test_ShowsNotMoreThanExist() {
        assertResultsAreEqual(
            "Not more than exists",
            RESULTS_ARE_SHOWN_NOT_MORE_THAN_EXIST_2_PARAMS_QUERY);
    }

    public void test_DoesNotIgnoreNonEmpty() {
        assertResultsAreEqual(
            "Does not ignore NON EMPTY",
            NON_EMPTY_IS_NOT_IGNORED_WHEN_TWO_PARAMS_QUERY);
    }
}

// End TopCountWithTwoParamsVersusHeadTest.java