/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 29 March, 2002
*/
package mondrian.test;

import junit.framework.ComparisonFailure;
import junit.framework.Assert;
import mondrian.olap.*;
import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.sql.SqlQuery;
import mondrian.resource.MondrianResource;
import mondrian.calc.Calc;
import mondrian.calc.CalcWriter;

import javax.sql.DataSource;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;
import java.util.Locale;
import java.sql.*;

/**
 * <code>TestContext</code> is a singleton class which contains the information
 * necessary to run mondrian tests (otherwise we'd have to pass this
 * information into the constructor of TestCases).
 *
 * <p>The singleton instance (retrieved via the {@link #instance()} method)
 * contains a connection to the FoodMart database, and runs expressions in the
 * context of the <code>Sales</code> cube.
 *
 * <p>Using the {@link DelegatingTestContext} subclass, you can create derived
 * classes which use a different connection or a different cube.
 *
 * @author jhyde
 * @since 29 March, 2002
 * @version $Id$
 */
public class TestContext {
    private static TestContext instance; // the singleton
    private PrintWriter pw;

    /**
     * Connect string for the FoodMart database. Set by the constructor,
     * but the connection is not created until the first call to
     * {@link #getFoodMartConnection}.
     */
    private String foodMartConnectString;

    /**
     * Connection to the FoodMart database. Set on the first call to
     * {@link #getFoodMartConnection}.
     */
    private Connection foodMartConnection;

    protected static final String nl = Util.nl;
    private static final String lineBreak = "\"," + nl + "\"";
    private static final String lineBreak2 = "\\\\n\" +" + nl + "\"";
    private static final String lineBreak3 = "\\n\" +" + nl + "\"";
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
        // Run all tests in the US locale, not the system default locale,
        // because the results all assume the US locale.
        MondrianResource.setThreadLocale(Locale.US);

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
    public static String getConnectString() {
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
        String jdbcUser = MondrianProperties.instance().TestJdbcUser.get();
        if (jdbcUser != null) {
            connectProperties.put("JdbcUser", jdbcUser);
        }
        String jdbcPassword = MondrianProperties.instance().TestJdbcPassword.get();
        if (jdbcPassword != null) {
            connectProperties.put("JdbcPassword", jdbcPassword);
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
            try {
                catalogURL = Util.toURL(file);
            } catch (MalformedURLException e) {
                throw new Error(e.getMessage());
            }
        }
        connectProperties.put("catalog", catalogURL.toString());
        return connectProperties.toString();
    }

    /**
     * Returns the connection to run queries.
     *
     * <p>By default, returns a connection to the FoodMart database.
     */
    public Connection getConnection() {
        return getFoodMartConnection(false);
    }

    /**
     * Returns a connection to the FoodMart database.
     *
     * @param fresh If true, returns a new connection, not one from the
     *   connection pool
     */
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
    public synchronized Connection getFoodMartConnection(Class dynProcClass) {
        Util.PropertyList properties =
                Util.parseConnectString(foodMartConnectString);
        properties.put(
                RolapConnectionProperties.DynamicSchemaProcessor,
                dynProcClass.getName());
        return DriverManager.getConnection(
                properties, null, null, false);
    }

    /**
     * Returns a connection to the FoodMart database
     * with an inline schema.
     */
    public synchronized Connection getFoodMartConnection(String catalogContent) {
        Util.PropertyList properties =
                Util.parseConnectString(foodMartConnectString);
        properties.put(
                RolapConnectionProperties.CatalogContent,
                catalogContent);
        return DriverManager.getConnection(
                properties, null, null, false);
    }

    /**
     * Returns a the XML of the foodmart schema with added parameters and cube
     * definitions.
     */
    public String getFoodMartSchema(
        String parameterDefs,
        String cubeDefs,
        String namedSetDefs,
        String udfDefs)
    {
        // First, get the unadulterated schema.
        String s;
        synchronized (SnoopingSchemaProcessor.class) {
            getFoodMartConnection(SnoopingSchemaProcessor.class);
            s = SnoopingSchemaProcessor.catalogContent;
        }

        // Add parameter definitions, if specified.
        if (parameterDefs != null) {
            int i = s.indexOf("<Dimension name=\"Store\">");
            s = s.substring(0, i) +
                parameterDefs +
                s.substring(i);
        }

        // Add cube definitions, if specified.
        if (cubeDefs != null) {
            int i = s.indexOf("<Cube name=\"Sales\">");
            s = s.substring(0, i) +
                cubeDefs +
                s.substring(i);
        }

        // Add named set definitions, if specified. Schema-level named sets
        // occur after <Cube> and <VirtualCube> and before <Role> elements.
        if (namedSetDefs != null) {
            int i = s.indexOf("<Role");
            if (i < 0) {
                i = s.indexOf("</Schema>");
            }
            s = s.substring(0, i) +
                namedSetDefs +
                s.substring(i);
        }

        // Add definitions of user-defined functions, if specified.
        if (udfDefs != null) {
            int i = s.indexOf("</Schema>");
            s = s.substring(0, i) +
                udfDefs +
                s.substring(i);
        }
        return s;
    }

    /**
     * Executes a query.
     */
    public Result executeQuery(String queryString) {
        Connection connection = getConnection();
        Query query = connection.parseQuery(queryString);
        Result result = connection.execute(query);
        return result;
    }

    /**
     * Executes a query, and asserts that it throws an exception which contains
     * the given pattern.
     */
    public void assertThrows(String queryString, String pattern) {
        Throwable throwable;
        try {
            Result result = executeQuery(queryString);
            Util.discard(result);
            throwable = null;
        } catch (Throwable e) {
            throwable = e;
        }
        checkThrowable(throwable, pattern);
    }

    /**
     * Executes an expression, and asserts that it gives an error which contains
     * a particular pattern. The error might occur during parsing, or might
     * be contained within the cell value.
     */
    public void assertExprThrows(String expression, String pattern) {
        Throwable throwable = null;
        try {
            Result result = executeQuery(
                    "with member [Measures].[Foo] as '" +
                    expression +
                    "' select {[Measures].[Foo]} on columns from " +
                    getDefaultCubeName());
            Cell cell = result.getCell(new int[]{0});
            if (cell.isError()) {
                throwable = (Throwable) cell.getValue();
            }
        } catch (Throwable e) {
            throwable = e;
        }
        checkThrowable(throwable, pattern);
    }

    public String getDefaultCubeName() {
        return "Sales";
    }

    /**
     * Executes the expression in the context of the cube indicated by
     * <code>cubeName</code>, and returns the result.
     *
     * @param expression The expression to evaluate
     * @return Returns a {@link Cell} which is the result of the expression.
     */
    public Cell executeExprRaw(String expression) {
        String cubeName = getDefaultCubeName();
        if (cubeName.indexOf(' ') >= 0) {
            cubeName = Util.quoteMdxIdentifier(cubeName);
        }
        final String queryString = "with member [Measures].[Foo] as " +
            Util.singleQuoteString(expression) +
            " select {[Measures].[Foo]} on columns from " + cubeName;
        Result result = executeQuery(queryString);
        return result.getCell(new int[]{0});
    }

    /**
     * Executes an expression and asserts that it returns a given result.
     */
    public void assertExprReturns(String expression, String expected) {
        final Cell cell = executeExprRaw(expression);
        assertEqualsVerbose(expected, cell.getFormattedValue());
    }

    /**
     * Executes a query with a given expression on an axis, and asserts that it
     * returns the expected string.
     */
    public void assertAxisReturns(
            String expression,
            String expected) {
        Axis axis = executeAxis(expression);
        assertEqualsVerbose(expected, toString(axis.positions));
    }

    /**
     * Compiles a scalar expression in the context of the default cube.
     *
     * @param expression The expression to evaluate
     * @param scalar Whether the expression is scalar
     * @return String form of the program
     */
    public String compileExpression(String expression, final boolean scalar) {
        String cubeName = getDefaultCubeName();
        if (cubeName.indexOf(' ') >= 0) {
            cubeName = Util.quoteMdxIdentifier(cubeName);
        }
        final String queryString;
        if (scalar) {
            queryString = "with member [Measures].[Foo] as " +
                    Util.singleQuoteString(expression) +
                    " select {[Measures].[Foo]} on columns from " + cubeName;
        } else {
            queryString = "SELECT {" + expression + "} ON COLUMNS FROM " + cubeName;
        }
        Connection connection = getConnection();
        Query query = connection.parseQuery(queryString);
        final Exp exp;
        if (scalar) {
            exp = query.formulas[0].getExpression();
        } else {
            exp = query.axes[0].exp;
        }
        final Calc calc = query.compileExpression(exp, scalar);
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        final CalcWriter calcWriter = new CalcWriter(pw);
        calc.accept(calcWriter);
        pw.flush();
        String calcString = sw.toString();
        return calcString;
    }

    /**
     * Executes a set expression which is expected to return 0 or 1 members.
     * It is an error if the expression returns tuples (as opposed to members),
     * or if it returns two or more members.
     *
     * @param expression
     * @return Null if axis returns the empty set, member if axis returns one
     *   member. Throws otherwise.
     */
    public Member executeSingletonAxis(String expression) {
        final String cubeName = getDefaultCubeName();
        Result result = executeQuery(
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
     * Executes a query with a given expression on an axis, and returns the
     * whole axis.
     */
    public Axis executeAxis(String expression) {
        Result result = executeQuery(
                "select {" + expression + "} on columns from " +
                getDefaultCubeName());
        return result.getAxes()[0];
    }

    /**
     * Executes a query with a given expression on an axis, and asserts that it
     * throws an error which matches a particular pattern. The expression is
     * evaulated against the default cube.
     */
    public void assertAxisThrows(
            String expression,
            String pattern) {
        Throwable throwable = null;
        Connection connection = getConnection();
        try {
            final String cubeName = getDefaultCubeName();
            final String queryString =
                    "select {" + expression + "} on columns from " + cubeName;
            Query query = connection.parseQuery(queryString);
            connection.execute(query);
        } catch (Throwable e) {
            throwable = e;
        }
        checkThrowable(throwable, pattern);
    }

    public static void checkThrowable(Throwable throwable, String pattern) {
        if (throwable == null) {
            Assert.fail("query did not yield an exception");
        }
        String stackTrace = getStackTrace(throwable);
        if (stackTrace.indexOf(pattern) < 0) {
            Assert.fail("query's error does not match pattern '" + pattern +
                    "'; error is [" + stackTrace + "]");
        }
    }

    /**
     * Returns the output writer.
     */
    public PrintWriter getWriter() {
        return pw;
    }

    /**
     * Executes a query and checks that the result is a given string.
     */
    public void assertQueryReturns(String query, String desiredResult) {
        Result result = executeQuery(query);
        String resultString = toString(result);
        if (desiredResult != null) {
            assertEqualsVerbose(desiredResult, resultString);
        }
    }

    /**
     * Checks that an actual string matches an expected string.
     * If they do not, throws a {@link junit.framework.ComparisonFailure} and prints the
     * difference, including the actual string as an easily pasted Java string
     * literal.
     */
    public static void assertEqualsVerbose(
        String expected,
        String actual)
    {
        assertEqualsVerbose(expected, actual, true, null);
    }

    /**
     * Checks that an actual string matches an expected string.
     * If they do not, throws a {@link ComparisonFailure} and prints the
     * difference, including the actual string as an easily pasted Java string
     * literal.
     */
    public static void assertEqualsVerbose(
            String expected,
            String actual,
            boolean java,
            String message)
    {
        if ((expected == null) && (actual == null)) {
            return;
        }
        if ((expected != null) && expected.equals(actual)) {
            return;
        }
        if (message == null) {
            message = "";
        } else {
            message += nl;
        }
        message +=
                "Expected:" + nl + expected + nl +
                "Actual:" + nl + actual + nl;
        if (java) {
            message += "Actual java:" + nl + toJavaString(actual) + nl;
        }
        throw new ComparisonFailure(message, expected, actual);
    }

    private static String toJavaStringWithNl(String s) {

        // Convert [string with "quotes" split
        // across lines]
        // into ["string with \"quotes\" split" + nl +
        // "across lines
        //
        s = Util.replace(s, "\"", "\\\"");
        s = LineBreakPattern.matcher(s).replaceAll(lineBreak);
        s = TabPattern.matcher(s).replaceAll("\\\\t");
        s = "\"" + s + "\"";
//        private static final String spurious = "," + nl + "\"\"";
//        if (s.endsWith(spurious)) {
//            s = s.substring(0, s.length() - spurious.length());
//        }
        if (s.indexOf(lineBreak) >= 0) {
            s = "fold(new String[] {" + nl + s + "})";
        }
        return s;
    }

    private static String toJavaString(String s) {

        // Convert [string with "quotes" split
        // across lines]
        // into ["string with \"quotes\" split\n" +
        // "across lines
        //
        s = Util.replace(s, "\"", "\\\"");
        s = LineBreakPattern.matcher(s).replaceAll(lineBreak2);
        s = TabPattern.matcher(s).replaceAll("\\\\t");
        s = "\"" + s + "\"";
        String spurious = " +" + nl + "\"\"";
        if (s.endsWith(spurious)) {
            s = s.substring(0, s.length() - spurious.length());
        }
        if (s.indexOf(lineBreak3) >= 0) {
            s = "fold(" + nl + s + ")";
        }
        return s;
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
        final String spurious = " + " + nl + "\"\"";
        if (s.endsWith(spurious)) {
            s = s.substring(0, s.length() - spurious.length());
        }
        String message =
                "Expected pattern:" + nl + expected + nl +
                "Actual: " + nl + actual + nl +
                "Actual java: " + nl + s + nl;
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

    /**
     * Converts a set of positions into a string. Useful if you want to check
     * that an axis has the results you expected.
     */
    public static String toString(Position[] positions) {
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

    /**
     * Converts an array of strings, each representing a line, into a single
     * string with line separators. There is no line separator after the
     * last string.
     *
     * <p>This function exists because line separators are platform dependent,
     * and IDEs such as Intellij handle large string arrays much better than
     * they handle concatenations of large numbers of string fragments.
     */
    public static String fold(String[] strings) {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < strings.length; i++) {
            if (i > 0) {
                buf.append(nl);
            }
            String string = strings[i];
            buf.append(string);
        }
        return buf.toString();
    }

    /**
     * Converts a string constant into locale-specific line endings.
     */
    public static String fold(String string) {
        if (!nl.equals("\n")) {
            string = Util.replace(string, "\n", nl);
        }
        return string;
    }
    
    public SqlQuery.Dialect getDialect() {
        java.sql.Connection connection = null;
        try {
            DataSource dataSource =
                ((RolapConnection) getConnection()).getDataSource();
            connection = dataSource.getConnection();
            return SqlQuery.Dialect.create(connection.getMetaData());
        } catch (SQLException e) {
            throw Util.newInternal(e, "While opening connection");
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Creates a TestContext which is based on a variant of the FoodMart
     * schema, which parameter, cube, named set, and user-defined function
     * definitions added.
     *
     * @param parameterDefs Parameter definitions. If not null, the string is
     *   is inserted into the schema XML in the appropriate place for
     *   parameter definitions.
     * @param cubeDefs Cube definition(s). If not null, the string is
     *   is inserted into the schema XML in the appropriate place for
     *   cube definitions.
     * @param namedSetDefs Definitions of named sets. If not null, the string
     *   is inserted into the schema XML in the appropriate place for
     *   named set definitions.
     * @param udfDefs Definitions of user-defined functions. If not null, the
     *   string is inserted into the schema XML in the appropriate place for
     *   UDF definitions.
     * @return TestContext which reads from a slightly different hymnbook
     */
    public static TestContext create(
        final String parameterDefs,
        final String cubeDefs,
        final String namedSetDefs,
        final String udfDefs) {
        return new TestContext() {
            public synchronized Connection getFoodMartConnection(boolean fresh) {
                final String schema = getFoodMartSchema(
                    parameterDefs, cubeDefs, namedSetDefs, udfDefs);
                return getFoodMartConnection(schema);
            }
        };
    }

    public static class SnoopingSchemaProcessor
        extends DecoratingSchemaProcessor
    {
        public static String catalogContent;

        protected String filterSchema(String s) {
            catalogContent = s;
            return s;
        }
    }
}

// End TestContext.java
