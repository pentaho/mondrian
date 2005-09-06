/*
// $Id$
// (C) Copyright 2002-2005 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 29 March, 2002
*/
package mondrian.test;

import junit.framework.ComparisonFailure;
import junit.framework.Assert;
import mondrian.olap.*;
import mondrian.rolap.RolapConnectionProperties;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;

/**
 * <code>TestContext</code> is a singleton class which contains the information
 * necessary to run mondrian tests (otherwise we'd have to pass this
 * information into the constructor of TestCases).
 *
 * @author jhyde
 * @since 29 March, 2002
 * @version $Id$
 **/
public class TestContext {
    private static TestContext instance; // the singleton
    private PrintWriter pw;
    /** Connect string for the FoodMart database. Set by the constructor,
     * but the connection is not created until the first call to
     * {@link #getFoodMartConnection}. **/
    private String foodMartConnectString;
    /** Connection to the FoodMart database. Set on the first call to
     * {@link #getFoodMartConnection}. **/
    private Connection foodMartConnection;

    protected static final String nl = System.getProperty("line.separator");
    private static final String lineBreak = "\" + nl + " + nl + "\"";
    private static final Pattern LineBreakPattern =
        Pattern.compile("\r\n|\r|\n");
    private static final Pattern TabPattern = Pattern.compile("\t");

    /**
     * Retrieves the singleton (instantiating if necessary).
     */
    public static TestContext instance() {
        if (instance == null) {
            synchronized (TestContext.class) {
                if (instance == null) {
                    instance = new TestContext();
                }
            }
        }
        return instance;
    }

    /**
     * Creates a TestContext.
     */
    protected TestContext() {
        this.pw = new PrintWriter(System.out, true);
        foodMartConnectString = getConnectString();
    }

    /**
     * Constructs a connect string by which the unit tests can talk to the
     * FoodMart database.
     *
     * The algorithm is as follows:<ul>
     * <li>Starts with {@link MondrianProperties#TestConnectString}, if it is
     *     set.</li>
     * <li>If {@link MondrianProperties#FoodmartJdbcURL} is set, this
     *     overrides the <code>Jdbc</code> property.</li>
     * <li>If the <code>catalog</code> URL is unset or invalid, it assumes that
     *     we are at the root of the source tree, and references
     *     <code>demo/FoodMart.xml</code></li>.
     * </ul>
     */
    private static String getConnectString() {
        String connectString = MondrianProperties.instance().TestConnectString.get();
        final Util.PropertyList connectProperties;
        if (connectString == null || connectString.equals("")) {
            connectProperties = new Util.PropertyList();
            connectProperties.put("Provider","mondrian");
        } else {
             connectProperties = Util.parseConnectString(connectString);
        }
        String jdbcURL = MondrianProperties.instance().FoodmartJdbcURL.get();
        if (jdbcURL != null) {
            connectProperties.put("Jdbc", jdbcURL);
        }
        // Find the catalog. Use the URL specified in the connect string, if
        // it is specified and is valid. Otherwise, reference FoodMart.xml
        // assuming we are at the root of the source tree.
        URL catalogURL = null;
        String catalog = connectProperties.get("catalog");
        if (catalog != null) {
            try {
                catalogURL = new URL(catalog);
            } catch (MalformedURLException e) {
                // ignore
            }
        }
        if (catalogURL == null) {
            // Works if we are running in root directory of source tree
            File file = new File("demo/FoodMart.xml");
            if (!file.exists()) {
                // Works if we are running in bin directory of runtime env
                file = new File("../demo/FoodMart.xml");
            }
            catalogURL = convertPathToURL(file);
        }
        connectProperties.put("catalog", catalogURL.toString());
        return connectProperties.toString();
    }

    /**
     * Creates a file-protocol URL for the given filename.
     **/
    public static URL convertPathToURL(File file)
    {
        try {
            String path = file.getAbsolutePath();
            // This is a bunch of weird code that is required to
            // make a valid URL on the Windows platform, due
            // to inconsistencies in what getAbsolutePath returns.
            String fs = System.getProperty("file.separator");
            if (fs.length() == 1)
            {
                char sep = fs.charAt(0);
                if (sep != '/')
                    path = path.replace(sep, '/');
                if (path.charAt(0) != '/')
                    path = '/' + path;
            }
            path = "file://" + path;
            return new URL(path);
        } catch (MalformedURLException e) {
            throw new java.lang.Error(e.getMessage());
        }
    }

    /** Returns a connection to the FoodMart database. **/
    public synchronized Connection getFoodMartConnection(boolean fresh) {
        if (fresh) {
            return DriverManager.getConnection(
                    foodMartConnectString, null, fresh);
        } else if (foodMartConnection == null) {
            foodMartConnection = DriverManager.getConnection(
                    foodMartConnectString, null, fresh);
        }
        return foodMartConnection;
    }

    /**
     * Returns a connection to the FoodMart database
     * with a dynamic schema processor.
     */
    public synchronized Connection getFoodMartConnection(String dynProc) {
        Util.PropertyList properties =
                Util.parseConnectString(foodMartConnectString);
        properties.put(
                RolapConnectionProperties.DynamicSchemaProcessor,
                dynProc);
        Connection newConnection = DriverManager.getConnection(
                properties, null, null, false);
        return newConnection;
    }

    /**
     * Executes a query against the FoodMart database.
     */
    public Result executeFoodMart(String queryString) {
        Connection connection = getFoodMartConnection(false);
        Query query = connection.parseQuery(queryString);
        Result result = connection.execute(query);
        return result;
    }

    /**
     * Executes a query against the FoodMart database, and returns the
     * exception, or <code>null</code> if there was no exception.
     */
    public Throwable executeFoodMartCatch(String queryString) {
        try {
            Result result = executeFoodMart(queryString);
            mondrian.olap.Util.discard(result);
            return null;
        } catch (Throwable e) {
            return e;
        }
    }

    /**
     * Runs a query, and asserts that it throws an exception which contains
     * the given pattern.
     */
    public void assertThrows(String queryString, String pattern) {
        Throwable throwable = executeFoodMartCatch(queryString);
        checkThrowable(throwable, pattern);
    }

    /**
     * Runs an expression, and asserts that it gives an error which contains
     * a particular pattern. The error might occur during parsing, or might
     * be contained within the cell value.
     */
    public void assertExprThrows(String expression, String pattern) {
        Throwable throwable = null;
        try {
            Result result = executeFoodMart(
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
     * Executes the expression in the context of the cube indicated by
     * <code>cubeName</code>, and returns the result.
     *
     * @param cubeName The name of the cube to use
     * @param expression The expression to evaluate
     * @return Returns a {@link Cell} which is the result of the expression.
     */
    public Cell executeExprRaw(String cubeName, String expression) {
        if (cubeName.indexOf(' ') >= 0) {
            cubeName = Util.quoteMdxIdentifier(cubeName);
        }
        final String queryString = "with member [Measures].[Foo] as " +
            Util.singleQuoteString(expression) +
            " select {[Measures].[Foo]} on columns from " + cubeName;
        Result result = executeFoodMart(queryString);
        return result.getCell(new int[]{0});
    }

    /**
     * Runs an expression and asserts that it returns a given result.
     */
    public void assertExprReturns(String expression, String expected) {
        final Cell cell = executeExprRaw("Sales", expression);
        assertEqualsVerbose(expected, cell.getFormattedValue());
    }


    /**
     * Runs a query with a given expression on an axis, and asserts that it
     * throws an error which matches a particular pattern. The expression is evaulated
     * against the named cube.
     */
    public void assertAxisThrows(
            Connection connection,
            String cubeName,
            String expression,
            String pattern) {
        Throwable throwable = null;
        try {
            final String queryString =
                    "select {" + expression + "} on columns from " + cubeName;
            Query query = connection.parseQuery(queryString);
            connection.execute(query);
        } catch (Throwable e) {
            throwable = e;
        }
        checkThrowable(throwable, pattern);
    }

    private static void checkThrowable(Throwable throwable, String pattern) {
        if (throwable == null) {
            Assert.fail("query did not yield an exception");
        }
        String stackTrace = getStackTrace(throwable);
        if (stackTrace.indexOf(pattern) < 0) {
            Assert.fail("query's error does not match pattern '" + pattern +
                    "'; error is [" + stackTrace + "]");
        }
    }


    /** Returns the output writer. **/
    public PrintWriter getWriter() {
        return pw;
    }

    private Connection getConnection() {
        return getFoodMartConnection(false);
    }

    /**
     * Runs a query and checks that the result is a given string.
     */
    public void assertQueryReturns(String query, String desiredResult) {
        Result result = executeFoodMart(query);
        String resultString = toString(result);
        if (desiredResult != null) {
            assertEqualsVerbose(desiredResult, resultString);
        }
    }

    /**
     * Checks that an actual string matches an expected string.
     * If they do not, throws a {@link ComparisonFailure} and prints the
     * difference, including the actual string as an easily pasted Java string
     * literal.
     */
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
        // into ["string with \"quotes\" split" + nl +
        // "across lines
        //
        s = Util.replace(s, "\"", "\\\"");
        s = LineBreakPattern.matcher(s).replaceAll(lineBreak);
        s = TabPattern.matcher(s).replaceAll("\\\\t");
        s = "\"" + s + "\"";
        final String spurious = " + " + FoodMartTestCase.nl + "\"\"";
        if (s.endsWith(spurious)) {
            s = s.substring(0, s.length() - spurious.length());
        }
        String message =
                "Expected:" + FoodMartTestCase.nl + expected + FoodMartTestCase.nl +
                "Actual: " + FoodMartTestCase.nl + actual + FoodMartTestCase.nl +
                "Actual java: " + FoodMartTestCase.nl + s + FoodMartTestCase.nl;
        throw new ComparisonFailure(message, expected, actual);
    }

    /**
     * Checks that an actual string matches an expected pattern.
     * If they do not, throws a {@link ComparisonFailure} and prints the
     * difference, including the actual string as an easily pasted Java string
     * literal.
     */
    public void assertMatchesVerbose(
        Pattern expected,
        String actual)
    {
        Util.assertPrecondition(expected != null, "expected != null");
        if (expected.matcher(actual).matches()) {
            return;
        }
        String s = actual;

        // Convert [string with "quotes" split
        // across lines]
        // into ["string with \"quotes\" split" + nl +
        // "across lines
        //
        s = Util.replace(s, "\"", "\\\"");
        s = LineBreakPattern.matcher(s).replaceAll(lineBreak);
        s = TabPattern.matcher(s).replaceAll("\\\\t");
        s = "\"" + s + "\"";
        final String spurious = " + " + FoodMartTestCase.nl + "\"\"";
        if (s.endsWith(spurious)) {
            s = s.substring(0, s.length() - spurious.length());
        }
        String message =
                "Expected pattern:" + FoodMartTestCase.nl + expected + FoodMartTestCase.nl +
                "Actual: " + FoodMartTestCase.nl + actual + FoodMartTestCase.nl +
                "Actual java: " + FoodMartTestCase.nl + s + FoodMartTestCase.nl;
        throw new ComparisonFailure(message, expected.pattern(), actual);
    }

    /**
     * Converts a {@link Throwable} to a stack trace.
     */
    public static String getStackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }

    /**
     * Formats {@link mondrian.olap.Result}.
     */
    public static String toString(Result result) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        result.print(pw);
        pw.flush();
        return sw.toString();
    }

}

// End TestContext.java
