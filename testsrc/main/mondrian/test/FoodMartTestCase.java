/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 29 March, 2002
*/
package mondrian.test;

import junit.framework.*;
import mondrian.olap.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * <code>FoodMartTestCase</code> is a unit test which runs against the FoodMart
 * database.
 *
 * @author jhyde
 * @since 29 March, 2002
 * @version $Id$
 **/
public class FoodMartTestCase extends TestCase {
    protected static final String nl = System.getProperty("line.separator");
    private static final Pattern LineBreakPattern =
        Pattern.compile("\r\n|\r|\n");
    private static final Pattern TabPattern = Pattern.compile("\t");

    public FoodMartTestCase(String name) {
        super(name);
    }

    public FoodMartTestCase() {
    }

    public Result runQuery(String queryString) {
        Connection connection = getConnection();
        Query query = connection.parseQuery(queryString);
        return connection.execute(query);
    }

    protected Connection getConnection(boolean fresh) {
        return getTestContext().getFoodMartConnection(fresh);
    }

    /**
     * Returns the test context. Override this method if you wish to use a
     * different source for your FoodMart connection.
     */
    protected TestContext getTestContext() {
        return TestContext.instance();
    }

    protected Connection getConnection() {
        return getConnection(false);
    }

    /**
     * Runs a query, and asserts that the result has a given number of columns
     * and rows.
     */
    protected void assertSize(String queryString, int columnCount, int rowCount) {
        Result result = runQuery(queryString);
        Axis[] axes = result.getAxes();
        Assert.assertTrue(axes.length == 2);
        Assert.assertTrue(axes[0].positions.length == columnCount);
        Assert.assertTrue(axes[1].positions.length == rowCount);
    }

    /**
     * Runs a query, and asserts that it throws an exception which contains
     * the given pattern.
     */
    public void assertThrows(String queryString, String pattern) {
        Throwable throwable = getTestContext().executeFoodMartCatch(
                queryString);
        checkThrowable(throwable, pattern);
    }

    private void checkThrowable(Throwable throwable, String pattern) {
        if (throwable == null) {
            Assert.fail("query did not yield an exception");
        }
        String stackTrace = getStackTrace(throwable);
        if (stackTrace.indexOf(pattern) < 0) {
            Assert.fail("query's error does not match pattern '" + pattern +
                    "'; error is [" + stackTrace + "]");
        }
    }

    /** Executes a query in a given connection. **/
    public Result execute(Connection connection, String queryString) {
        Query query = connection.parseQuery(queryString);
        return connection.execute(query);
    }

    /**
     * Runs a query with a given expression on an axis, and returns the whole
     * axis.
     */
    public Axis executeAxis2(String cube, String expression) {
        Result result = execute(
                getTestContext().getFoodMartConnection(false),
                "select {" + expression + "} on columns from " + cube);
        return result.getAxes()[0];
    }

    /**
     * Runs a query with a given expression on an axis, on a given connection,
     * and returns the whole axis.
     */
    Axis executeAxis2(Connection connection, String expression) {
        Result result = execute(connection, "select {" + expression + "} on columns from Sales");
        return result.getAxes()[0];
    }
    /**
     * Runs a query with a given expression on an axis, and returns the single
     * member.
     */
    public Member executeAxis(String expression) {
        return executeAxis("Sales", expression);
    }

    public Member executeAxis(String cubeName, String expression) {
        Result result = getTestContext().executeFoodMart(
                "select {" + expression + "} on columns from " + cubeName);
        Axis axis = result.getAxes()[0];
        switch (axis.positions.length) {
        case 0:
            // The mdx "{...}" operator eliminates null members (that is,
            // members for which member.isNull() is true). So if "expression"
            // yielded just the null member, the array will be empty.
            return null;
        case 1:
            // Java nulls should never happen during expression evaluation.
            Position position = axis.positions[0];
            Util.assertTrue(position.members.length == 1);
            Member member = position.members[0];
            Util.assertTrue(member != null);
            return member;
        default:
            throw Util.newInternal(
                    "expression " + expression + " yielded " +
                    axis.positions.length + " positions");
        }
    }

    /**
     * Runs a query and checks that the result is a given string.
     */
    public void runQueryCheckResult(QueryAndResult queryAndResult) {
        runQueryCheckResult(queryAndResult.query, queryAndResult.result);
    }

    /**
     * Runs a query and checks that the result is a given string.
     */
    public void runQueryCheckResult(String query, String desiredResult) {
        Result result = runQuery(query);
        String resultString = toString(result);
        if (desiredResult != null) {
            assertEqualsVerbose(desiredResult, resultString);
        }
    }

    public static void assertEqualsVerbose(
        String expected,
        String actual)
    {
        if ((expected == null) && (actual == null)) {
            return;
        }
        if ((expected != null) && expected.equals(actual)) {
            return;
        }
        String s = actual;

        // Convert [string with "quotes" split
        // across lines]
        // into ["string with \"quotes\" split" + NL +
        // "across lines
        //
        //
        s = Util.replace(s, "\"", "\\\"");
        final String lineBreak = "\" + NL + " + nl + "\"";
        s = LineBreakPattern.matcher(s).replaceAll(lineBreak);
        s = TabPattern.matcher(s).replaceAll("\\\\t");
        s = "\"" + s + "\"";
        final String spurious = " + " + nl + "\"\"";
        if (s.endsWith(spurious)) {
            s = s.substring(0, s.length() - spurious.length());
        }
        String message =
            "Expected:" + nl + expected + nl
            + "Actual: " + nl + actual + nl
            + "Actual java: " + nl + s + nl;
        throw new ComparisonFailure(message, expected, actual);
    }

    /**
     * Runs a query.
     */
    public Result execute(String queryString) {
        return getTestContext().executeFoodMart(queryString);
    }

    /**
     * Runs a query with a given expression on an axis, and asserts that it
     * throws an error which matches a particular pattern. The expression
     * is evaulated against the Sales cube.
     */
    public void assertAxisThrows(String expression, String pattern) {
        assertAxisThrows(getTestContext().getFoodMartConnection(false),
                expression, pattern);
    }

    /**
     * Runs a query with a given expression on an axis, and asserts that it
     * throws an error which matches a particular pattern. The expression
     * is evaulated against the named cube.
     */
    public void assertAxisThrows(String cubeName, String expression, String pattern) {
        assertAxisThrows(getTestContext().getFoodMartConnection(false),
                cubeName, expression, pattern);
    }

    /**
     * Runs a query with a given expression on an axis, and asserts that it
     * throws an error which matches a particular pattern. The expression is evaulated
     * against the Sales cube.
     */
    public void assertAxisThrows(Connection connection, String expression, String pattern) {
        assertAxisThrows(connection, "Sales", expression, pattern);
    }

    /**
     * Runs a query with a given expression on an axis, and asserts that it
     * throws an error which matches a particular pattern. The expression is evaulated
     * against the named cube.
     */
    public void assertAxisThrows(Connection connection, String cubeName, String expression, String pattern) {
        Throwable throwable = null;
        try {
            Util.discard(execute(connection,
                    "select {" + expression + "} on columns from " + cubeName));
        } catch (Throwable e) {
            throwable = e;
        }
        checkThrowable(throwable, pattern);
    }

    /**
     * Runs a query on the "Sales" cube with a given expression on an axis, and
     * asserts that it returns the expected string.
     */
    public void assertAxisReturns(String expression, String expected) {
        assertAxisReturns("Sales", expression, expected);
    }
    /**
     * Runs a query with a given expression on an axis, and asserts that it
     * returns the expected string.
     */
    public void assertAxisReturns(String cube, String expression, String expected) {
        Axis axis = executeAxis2(cube, expression);
        Assert.assertEquals(expected, toString(axis.positions));
    }
    /**
     * Runs a query with a given expression on an axis, and asserts that it
     * returns the expected string.
     */
    public void assertAxisReturns(Connection connection, String expression, String expected) {
        Axis axis = executeAxis2(connection, expression);
        Assert.assertEquals(expected, toString(axis.positions));
    }
    /**
     * Converts a {@link Throwable} to a stack trace.
     */
    private static String getStackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }

    /** Executes an expression against the FoodMart database to form a single
     * cell result set, then returns that cell's formatted value. **/
    public String executeExpr(String cubeName, String expression) {
        return executeExprRaw(cubeName, expression).getFormattedValue();
    }

    public String executeExpr(String expression) {
        return executeExpr("Sales", expression);
    }

    /**
     * Executes the expression in the context of the cube indicated by <code>cubeName</code>,
     * and returns the result.
     * @param cubeName The name of the cube to use
     * @param expression The expression to evaluate
     * @return Returns a {@link Cell} which is the result of the expression.
     */
    public Cell executeExprRaw(String cubeName, String expression) {
        if (cubeName.indexOf(' ') >= 0) {
            cubeName = Util.quoteMdxIdentifier(cubeName);
        }
        final String queryString = "with member [Measures].[Foo] as '" +
            expression +
            "' select {[Measures].[Foo]} on columns from " + cubeName;
        Result result = getTestContext().executeFoodMart(queryString);

        return result.getCell(new int[]{0});
    }

    /** Executes an expression which returns a boolean result. **/
    public String executeBooleanExpr(String expression) {
        return executeExpr("Iif(" + expression + ",\"true\",\"false\")");
    }

    /**
     * Runs an expression, and asserts that it gives an error which contains
     * a particular pattern. The error might occur during parsing, or might
     * be contained within the cell value.
     */
    public void assertExprThrows(String expression, String pattern) {
        Throwable throwable = null;
        try {
            Result result = getTestContext().executeFoodMart(
                    "with member [Measures].[Foo] as '" +
                    expression +
                    "' select {[Measures].[Foo]} on columns from Sales");
            Cell cell = result.getCell(new int[]{0});
            if (cell.isError()) {
                throwable = (Throwable) cell.getValue();
            }
        } catch (Throwable e) {
            throwable = e;
        }
        checkThrowable(throwable, pattern);
    }

    /**
     * Converts a set of positions into a string. Useful if you want to check
     * that an axis has the results you expected.
     */
    public String toString(Position[] positions) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < positions.length; i++) {
            Position position = positions[i];
            if (i > 0) {
                sb.append(nl);
            }
            if (position.members.length != 1) {
                sb.append("{");
            }
            for (int j = 0; j < position.members.length; j++) {
                Member member = position.members[j];
                if (j > 0) {
                    sb.append(", ");
                }
                sb.append(member.getUniqueName());
            }
            if (position.members.length != 1) {
                sb.append("}");
            }
        }
        return sb.toString();
    }

    /** Formats {@link Result}. **/
    public String toString(Result result) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        result.print(pw);
        pw.flush();
        return sw.toString();
    }


    static class QueryAndResult {
        String query;
        String result;
        QueryAndResult(String query, String result) {
            this.query = query;
            this.result = result;
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
    ArrayList failures = new ArrayList();
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
                Throwable throwable = (Throwable) failures.get(i);
                throwable.printStackTrace();
            }
            TestCase.fail(failures.size() + " threads failed");
        }
    }
}

// End FoodMartTestCase.java
