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
import java.util.List;
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
    public static final String[] AllDims = {
        "[Measures]",
        "[Store]",
        "[Store Size in SQFT]",
        "[Store Type]",
        "[Time]",
        "[Product]",
        "[Promotion Media]",
        "[Promotions]",
        "[Customers]",
        "[Education Level]",
        "[Gender]",
        "[Marital Status]",
        "[Yearly Income]"
    };

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
                RolapConnectionProperties.DynamicSchemaProcessor.name(),
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
            RolapConnectionProperties.CatalogContent.name(),
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
        String virtualCubeDefs,
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

        // Add virtual cube definitions, if specified.
        if (virtualCubeDefs != null) {
            int i = s.indexOf("<VirtualCube name=\"Warehouse and Sales\">");
            s = s.substring(0, i) +
                virtualCubeDefs +
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
     * Returns a the XML of the foodmart schema, adding dimension definitions
     * to the definition of a given cube.
     */
    public String getFoodMartSchemaSubstitutingCube(
        String cubeName,
        String dimensionDefs,
        String memberDefs)
    {
        // First, get the unadulterated schema.
        String s;
        synchronized (SnoopingSchemaProcessor.class) {
            getFoodMartConnection(SnoopingSchemaProcessor.class);
            s = SnoopingSchemaProcessor.catalogContent;
        }

        // Search for the <Cube> or <VirtualCube> element.
        int h = s.indexOf("<Cube name=\"" + cubeName + "\"");
        int end;
        if (h < 0) {
            h = s.indexOf("<VirtualCube name=\"" + cubeName + "\"");
            if (h < 0) {
                throw new RuntimeException("cube '" + cubeName + "' not found");
            } else {
                end = s.indexOf("</VirtualCube", h);
            }
        } else {
            end = s.indexOf("</Cube>", h);
        }

        // Add dimension definitions, if specified.
        if (dimensionDefs != null) {
            int i = s.indexOf("<Dimension ", h);
            s = s.substring(0, i) +
                dimensionDefs +
                s.substring(i);
        }

        // Add calculated member definitions, if specified.
        if (memberDefs != null) {
            int i = s.indexOf("<CalculatedMember ", h);
            if (i < 0 || i > end) {
                i = end;
            }
            s = s.substring(0, i) +
                memberDefs +
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
        if (expected == null) {
            expected = ""; // null values are formatted as empty string
        }
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
        assertEqualsVerbose(expected, toString(axis.getPositions()));
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
            exp = query.axes[0].getSet();
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
        switch (axis.getPositions().size()) {
        case 0:
            // The mdx "{...}" operator eliminates null members (that is,
            // members for which member.isNull() is true). So if "expression"
            // yielded just the null member, the array will be empty.
            return null;
        case 1:
            // Java nulls should never happen during expression evaluation.
            Position position = axis.getPositions().get(0);
            Util.assertTrue(position.size() == 1);
            Member member = position.get(0);
            Util.assertTrue(member != null);
            return member;
        default:
            throw Util.newInternal(
                    "expression " + expression + " yielded " +
                    axis.getPositions().size() + " positions");
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
    public static String toString(List<Position> positions) {
        StringBuilder buf = new StringBuilder();
        int i = 0;
        for (Position position: positions) {
            if (i > 0) {
                buf.append(nl);
            }
            if (position.size() != 1) {
                buf.append("{");
            }
            for (int j = 0; j < position.size(); j++) {
                Member member = position.get(j);
                if (j > 0) {
                    buf.append(", ");
                }
                buf.append(member.getUniqueName());
            }
            if (position.size() != 1) {
                buf.append("}");
            }
            i++;
        }
        return buf.toString();
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
        StringBuilder buf = new StringBuilder();
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
     * Checks that expected SQL equals actual SQL.
     * Performs some normalization on the actual SQL to compensate for
     * differences between dialects.
     */
    public void assertSqlEquals(String expectedSql, String actualSql) {
        final String search = "fname \\+ ' ' \\+ lname";
        final SqlQuery.Dialect dialect = getDialect();
        if (dialect.isMySQL()) {
            // Mysql would generate "CONCAT( ... )"
            expectedSql = expectedSql.replaceAll(
                    search,
                    "CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)");
        } else if (dialect.isPostgres()
            || dialect.isOracle() || dialect.isLucidDB()) {
            expectedSql = expectedSql.replaceAll(
                    search,
                    "`fname` || ' ' || `lname`");
        } else if (dialect.isDerby() || dialect.isCloudscape()) {
            expectedSql = expectedSql.replaceAll(
                    search,
                    "`customer`.`fullname`");
        } else if (dialect.isDB2()) {
            expectedSql = expectedSql.replaceAll(
                    search,
                    "CONCAT(CONCAT(fname, ' '), lname)");
        }

        // DB2 does not have quotes on identifiers
        if (dialect.isDB2()) {
            expectedSql = expectedSql.replaceAll("`", "");
        }

        // the following replacement is for databases in ANSI mode
        //  using '"' to quote identifiers
        actualSql = actualSql.replace('"', '`');

        if (dialect.isOracle()) {
            // " + tableQualifier + "
            expectedSql = expectedSql.replaceAll(" =as= ", " ");
        } else {
            expectedSql = expectedSql.replaceAll(" =as= ", " as ");
        }

        Assert.assertEquals(expectedSql, actualSql);
    }

    /**
     * Asserts that an MDX set-valued expression depends upon a given list of
     * dimensions.
     */
    public void assertSetExprDependsOn(String expr, String dimList) {
        // Construct a query, and mine it for a parsed expression.
        // Use a fresh connection, because some tests define their own dims.
        final boolean fresh = true;
        final Connection connection = getFoodMartConnection(fresh);
        final String queryString =
                "SELECT {" + expr + "} ON COLUMNS FROM [Sales]";
        final Query query = connection.parseQuery(queryString);
        query.resolve();
        final Exp expression = query.getAxes()[0].getSet();

        // Build a list of the dimensions which the expression depends upon,
        // and check that it is as expected.
        checkDependsOn(query, expression, dimList, false);
    }

    /**
     * Asserts that an MDX member-valued depends upon a given list of
     * dimensions.
     */
    public void assertMemberExprDependsOn(String expr, String dimList) {
        assertSetExprDependsOn("{" + expr + "}", dimList);
    }

    /**
     * Asserts that an MDX expression depends upon a given list of dimensions.
     */
    public void assertExprDependsOn(String expr, String dimList) {
        // Construct a query, and mine it for a parsed expression.
        // Use a fresh connection, because some tests define their own dims.
        final boolean fresh = true;
        final Connection connection = getFoodMartConnection(fresh);
        final String queryString =
                "WITH MEMBER [Measures].[Foo] AS " +
                Util.singleQuoteString(expr) +
                " SELECT FROM [Sales]";
        final Query query = connection.parseQuery(queryString);
        query.resolve();
        final Formula formula = query.getFormulas()[0];
        final Exp expression = formula.getExpression();

        // Build a list of the dimensions which the expression depends upon,
        // and check that it is as expected.
        checkDependsOn(query, expression, dimList, true);
    }

    private void checkDependsOn(
            final Query query,
            final Exp expression,
            String expectedDimList,
            final boolean scalar) {
        final Calc calc = query.compileExpression(expression, scalar);
        final Dimension[] dimensions = query.getCube().getDimensions();
        StringBuilder buf = new StringBuilder("{");
        int dependCount = 0;
        for (int i = 0; i < dimensions.length; i++) {
            Dimension dimension = dimensions[i];
            if (calc.dependsOn(dimension)) {
                if (dependCount++ > 0) {
                    buf.append(", ");
                }
                buf.append(dimension.getUniqueName());
            }
        }
        buf.append("}");
        String actualDimList = buf.toString();
        Assert.assertEquals(expectedDimList, actualDimList);
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
     * @param virtualCubeDefs Definitions of virtual cubes. If not null, the
     *   string is inserted into the schema XML in the appropriate place for
     *   virtual cube definitions.
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
        final String virtualCubeDefs,
        final String namedSetDefs,
        final String udfDefs) {
        return new TestContext() {
            public synchronized Connection getFoodMartConnection(boolean fresh) {
                final String schema = getFoodMartSchema(
                    parameterDefs, cubeDefs,
                    virtualCubeDefs, namedSetDefs, udfDefs);
                return getFoodMartConnection(schema);
            }
        };
    }

    /**
     * Creates a TestContext, adding hierarchy definitions to a cube definition.
     *
     * @param cubeName Name of a cube in the schema (cube must exist)
     * @param dimensionDefs String defining dimensions, or null
     * @return TestContext with modified cube defn
     */
    public static TestContext createSubstitutingCube(
        final String cubeName,
        final String dimensionDefs)
    {
        return createSubstitutingCube(cubeName, dimensionDefs, null);
    }

    /**
     * Creates a TestContext, adding hierarchy and calculated member definitions
     * to a cube definition.
     *
     * @param cubeName Name of a cube in the schema (cube must exist)
     * @param dimensionDefs String defining dimensions, or null
     * @param memberDefs String defining calculated members, or null
     * @return TestContext with modified cube defn
     */
    public static TestContext createSubstitutingCube(
        final String cubeName,
        final String dimensionDefs, final String memberDefs
    ) {
        return new TestContext() {
            public synchronized Connection getFoodMartConnection(boolean fresh) {
                final String schema =
                    getFoodMartSchemaSubstitutingCube(
                        cubeName, dimensionDefs, memberDefs);
                return getFoodMartConnection(schema);
            }
        };
    }

    /**
     * Generates a string containing all dimensions except those given.
     * Useful as an argument to {@link #assertExprDependsOn(String, String)}.
     */
    public static String allDimsExcept(String[] dims) {
        for (int i = 0; i < dims.length; i++) {
            assert contains(AllDims, dims[i]) : "unknown dimension " + dims[i];
        }
        StringBuilder buf = new StringBuilder("{");
        int j = 0;
        for (int i = 0; i < AllDims.length; i++) {
            if (!contains(dims, AllDims[i])) {
                if (j++ > 0) {
                    buf.append(", ");
                }
                buf.append(AllDims[i]);
            }
        }
        buf.append("}");
        return buf.toString();
    }

    public static boolean contains(String[] a, String s) {
        for (int i = 0; i < a.length; i++) {
            if (a[i].equals(s)) {
                return true;
            }
        }
        return false;
    }

    public static String allDims() {
        return allDimsExcept(new String[0]);
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
