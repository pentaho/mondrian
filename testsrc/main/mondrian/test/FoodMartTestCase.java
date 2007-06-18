/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 29 March, 2002
*/
package mondrian.test;

import junit.framework.Assert;
import junit.framework.TestCase;
import mondrian.olap.*;

import java.util.ArrayList;
import java.util.List;

/**
 * <code>FoodMartTestCase</code> is a unit test which runs against the FoodMart
 * database.
 *
 * @author jhyde
 * @since 29 March, 2002
 * @version $Id$
 */
public class FoodMartTestCase extends TestCase {
    protected static final String nl = Util.nl;

    public FoodMartTestCase(String name) {
        super(name);
    }

    public FoodMartTestCase() {
    }

    /**
     * Returns the test context. Override this method if you wish to use a
     * different source for your FoodMart connection.
     */
    public TestContext getTestContext() {
        return TestContext.instance();
    }

    /**
     * Returns a {@link TestContext} which uses a given cube for executing
     * scalar and set expressions.
     *
     * @param cubeName Name of cube
     */
    protected TestContext getTestContext(final String cubeName) {
        return new DelegatingTestContext(getTestContext()) {
            public String getDefaultCubeName() {
                return cubeName;
            }
        };
    }

    /**
     * Returns a {@link TestContext} which uses a given connection.
     *
     * @param connection Connection
     */
    public TestContext getTestContext(final Connection connection) {
        return new DelegatingTestContext(getTestContext()) {
            public Connection getConnection() {
                return connection;
            }
        };
    }

    protected Connection getConnection() {
        return getTestContext().getFoodMartConnection();
    }

    /**
     * Runs a query, and asserts that the result has a given number of columns
     * and rows.
     */
    protected void assertSize(String queryString, int columnCount, int rowCount) {
        Result result = executeQuery(queryString);
        Axis[] axes = result.getAxes();
        Assert.assertTrue(axes.length == 2);
        Assert.assertTrue(axes[0].getPositions().size() == columnCount);
        Assert.assertTrue(axes[1].getPositions().size() == rowCount);
    }

    /**
     * Runs a query, and asserts that it throws an exception which contains
     * the given pattern.
     */
    public void assertThrows(String queryString, String pattern) {
        getTestContext().assertThrows(queryString, pattern);
    }

    /**
     * Executes a query in a given connection.
     */
    public Result execute(Connection connection, String queryString) {
        Query query = connection.parseQuery(queryString);
        return connection.execute(query);
    }

    /**
     * Executes a set expression which is expected to return 0 or 1 members.
     * It is an error if the expression returns tuples (as opposed to members),
     * or if it returns two or more members.
     *
     * @param expression Expression
     * @return Null if axis returns the empty set, member if axis returns one
     *   member. Throws otherwise.
     */
    public Member executeSingletonAxis(String expression) {
        return getTestContext().executeSingletonAxis(expression);
    }

    /**
     * Runs a query and checks that the result is a given string.
     */
    public void assertQueryReturns(String query, String desiredResult) {
        getTestContext().assertQueryReturns(query, desiredResult);
    }

    /**
     * Runs a query.
     */
    public Result executeQuery(String queryString) {
        return getTestContext().executeQuery(queryString);
    }

    /**
     * Runs a query with a given expression on an axis, and asserts that it
     * throws an error which matches a particular pattern. The expression
     * is evaulated against the Sales cube.
     */
    public void assertAxisThrows(String expression, String pattern) {
        getTestContext().assertAxisThrows(expression, pattern);
    }

    /**
     * Runs a query on the "Sales" cube with a given expression on an axis, and
     * asserts that it returns the expected string.
     */
    public void assertAxisReturns(String expression, String expected) {
        getTestContext().assertAxisReturns(expression, expected);
    }

    /**
     * Executes an expression against the Sales cube in the FoodMart database
     * to form a single cell result set, then returns that cell's formatted
     * value.
     */
    public String executeExpr(String expression) {
        return getTestContext().executeExprRaw(expression).getFormattedValue();
    }

    /**
     * Executes an expression which yields a boolean result, and asserts that
     * the result is the expected one.
     */
    public void assertBooleanExprReturns(String expression, boolean expected) {
        final String iifExpression = "Iif(" + expression + ",\"true\",\"false\")";
        final String actual = executeExpr(iifExpression);
        final String expectedString = expected ? "true" : "false";
        assertEquals(expectedString, actual);
    }

    /**
     * Runs an expression, and asserts that it gives an error which contains
     * a particular pattern. The error might occur during parsing, or might
     * be contained within the cell value.
     */
    public void assertExprThrows(String expression, String pattern) {
        getTestContext().assertExprThrows(expression, pattern);
    }

    /**
     * Runs an expression and asserts that it returns a given result.
     */
    public void assertExprReturns(String expression, String expected) {
        getTestContext().assertExprReturns(expression, expected);
    }

    /**
     * Converts a string constant into locale-specific line endings.
     */
    public static String fold(String string) {
        return TestContext.fold(string);
    }

    /**
     * Executes query1 and query2 and Compares the obtained measure values.
     */
    protected void assertQueriesReturnSimilarResults(String query1, String query2, TestContext testContext) {

        String resultString1 =
                TestContext.toString(testContext.executeQuery(query1));
        String resultString2 =
                TestContext.toString(testContext.executeQuery(query2));
        assertEquals(measureValues(resultString1), measureValues(resultString2));
    }

    /**
     * Truncates the query result to return only measure values.
     */
    private static String measureValues(String resultString) {
        int index = resultString.indexOf("}");
        return index != -1 ? resultString.substring(index) : resultString;

    }

    protected boolean isGroupingSetsSupported() {
        return MondrianProperties.instance().useGroupingSets.get() &&
                getTestContext().getDialect().isGroupingSetSupported();
    }

    static class QueryAndResult {
        String query;
        String result;
        QueryAndResult(String query, String result) {
            this.query = query;
            this.result = fold(result);
        }
    }
}

/**
 * Similar to {@link Runnable}, except classes which implement
 * <code>ChooseRunnable</code> choose what to do based upon an integer
 * parameter.
 */
interface ChooseRunnable {
    void run(int i);
}

/**
 * Runs a test case in several parallel threads, catching exceptions from
 * each one, and succeeding only if they all succeed.
 */
class TestCaseForker {
    BasicQueryTest testCase;
    long timeoutMs;
    Thread[] threads;
    List<Throwable> failures = new ArrayList<Throwable>();
    ChooseRunnable chooseRunnable;

    public TestCaseForker(
            BasicQueryTest testCase, long timeoutMs, int threadCount,
            ChooseRunnable chooseRunnable) {
        this.testCase = testCase;
        this.timeoutMs = timeoutMs;
        this.threads = new Thread[threadCount];
        this.chooseRunnable = chooseRunnable;
    }

    public void run() {
        ThreadGroup threadGroup = null;//new ThreadGroup("TestCaseForker thread group");
        for (int i = 0; i < threads.length; i++) {
            final int threadIndex = i;
            this.threads[i] = new Thread(threadGroup, "thread #" + threadIndex) {
                public void run() {
                    try {
                        chooseRunnable.run(threadIndex);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        failures.add(e);
                    }
                }
            };
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join(timeoutMs);
            } catch (InterruptedException e) {
                failures.add(
                        Util.newInternal(
                                e, "Interrupted after " + timeoutMs + "ms"));
                break;
            }
        }
        if (failures.size() > 0) {
            for (int i = 0; i < failures.size(); i++) {
                Throwable throwable = failures.get(i);
                throwable.printStackTrace();
            }
            TestCase.fail(failures.size() + " threads failed");
        }
    }
}

// End FoodMartTestCase.java
