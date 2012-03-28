/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.calc.*;
import mondrian.olap.Axis;
import mondrian.olap.Cell;
import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.olap.*;
import mondrian.olap.Position;
import mondrian.olap.fun.FunUtil;
import mondrian.resource.MondrianResource;
import mondrian.rolap.*;
import mondrian.spi.*;
import mondrian.spi.impl.FilterDynamicSchemaProcessor;
import mondrian.util.DelegatingInvocationHandler;
import mondrian.util.Pair;

import junit.framework.*;
import junit.framework.Test;

import org.olap4j.*;
import org.olap4j.impl.CoordinateIterator;
import org.olap4j.layout.TraditionalCellSetFormatter;

import java.io.*;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;
import javax.sql.DataSource;

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
 */
public class TestContext {
    private static TestContext instance; // the singleton
    private PrintWriter pw;

    /**
     * Connection to the FoodMart database. Set on the first call to
     * {@link #getConnection}. Soft reference allows garbage-collector to clear
     * it if it's not been used recently.
     */
    private SoftReference<Connection> connectionRef;

    private Dialect dialect;

    private int errorStart;
    private int errorEnd;
    private String schema;

    protected static final String nl = Util.nl;
    private static final String indent = "                ";
    private static final String lineBreak = "\"," + nl + "\"";
    private static final String lineBreak2 = "\\\\n\"" + nl + indent + "+ \"";
    private static final Pattern LineBreakPattern =
        Pattern.compile("\r\n|\r|\n");
    private static final Pattern TabPattern = Pattern.compile("\t");
    private static final String[] AllHiers = {
        "[Customer].[Customers]",
        "[Customer].[Education Level]",
        "[Customer].[Gender]",
        "[Customer].[Marital Status]",
        "[Customer].[Yearly Income]",
        "[Measures]",
        "[Product].[Products]",
        "[Promotion].[Media Type]",
        "[Promotion].[Promotions]",
        "[Store].[Store Size in SQFT]",
        "[Store].[Store Type]",
        "[Store].[Stores]",
        "[Time].[Time]",
        "[Time].[Weekly]",
    };
    private static final Map<String, String> rawSchemas =
        new WeakMap<String, String>();

    /**
     * Retrieves the singleton (instantiating if necessary).
     */
    public static synchronized TestContext instance() {
        if (instance == null) {
            instance = new TestContext().with(DataSet.FOODMART);
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
    }

    /**
     * Returns the connect string by which the unit tests can talk to the
     * FoodMart database.
     *
     * <p>In the base class, the result is the same as the static method
     * {@link #getDefaultConnectString}. If a derived class overrides
     * {@link #getConnectionProperties()}, the result of this method
     * will change also.
     */
    public final String getConnectString() {
        return getConnectionProperties().toString();
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
    public static String getDefaultConnectString() {
        String connectString =
            MondrianProperties.instance().TestConnectString.get();
        final Util.PropertyList connectProperties;
        if (connectString == null || connectString.equals("")) {
            connectProperties = new Util.PropertyList();
            connectProperties.put("Provider", "mondrian");
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
        String jdbcPassword =
            MondrianProperties.instance().TestJdbcPassword.get();
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

    public synchronized void flushSchemaCache() {
        // it's pointless to flush the schema cache if we
        // have a handle on the connection object already
        getConnection().getCacheControl(null).flushSchemaCache();
    }

    /**
     * Returns the connection to run queries.
     *
     * <p>When invoked on the default TestContext instance, returns a connection
     * to the FoodMart database.
     */
    public synchronized Connection getConnection() {
        if (connectionRef != null) {
            final Connection connection = connectionRef.get();
            if (connection != null) {
                return connection;
            }
        }
        final Connection connection =
            DriverManager.getConnection(
                getConnectionProperties(),
                null,
                null);
        connectionRef = new SoftReference<Connection>(connection);
        return connection;
    }

    /**
     * Returns a connection to the FoodMart database
     * with a dynamic schema processor and disables use of RolapSchema Pool.
     */
    public TestContext withSchemaProcessor(
        Class<? extends DynamicSchemaProcessor> dynProcClass)
    {
        final Util.PropertyList properties = getConnectionProperties().clone();
        properties.put(
            RolapConnectionProperties.DynamicSchemaProcessor.name(),
            dynProcClass.getName());
        properties.put(
            RolapConnectionProperties.UseSchemaPool.name(),
            "false");
        return withProperties(properties);
    }

    /**
     * Returns a {@link TestContext} similar to this one, but which uses a fresh
     * connection.
     *
     * @return Test context which uses the a fresh connection
     *
     * @see #withSchemaPool(boolean)
     */
    public final TestContext withFreshConnection() {
        final Connection connection = withSchemaPool(false).getConnection();
        return withConnection(connection);
    }

    public TestContext withSchemaPool(boolean usePool) {
        final Util.PropertyList properties = getConnectionProperties().clone();
        properties.put(
            RolapConnectionProperties.UseSchemaPool.name(),
            Boolean.toString(usePool));
        return withProperties(properties);
    }

    public Util.PropertyList getConnectionProperties() {
        final Util.PropertyList propertyList =
            Util.parseConnectString(getDefaultConnectString());
        if (MondrianProperties.instance().TestHighCardinalityDimensionList
            .get() != null
            && propertyList.get(
                RolapConnectionProperties.DynamicSchemaProcessor.name())
            == null)
        {
            propertyList.put(
                RolapConnectionProperties.DynamicSchemaProcessor.name(),
                HighCardDynamicSchemaProcessor.class.getName());
        }
        return propertyList;
    }

    /**
     * Returns a the XML of the current schema with added parameters and cube
     * definitions.
     */
    public String getSchema(
        String parameterDefs,
        String cubeDefs,
        String virtualCubeDefs,
        String namedSetDefs,
        String udfDefs,
        String roleDefs)
    {
        // First, get the unadulterated schema.
        String s = getRawSchema();

        // Add parameter definitions, if specified.
        if (parameterDefs != null) {
            int firstDimension = s.indexOf("<Dimension ");
            int firstCube = s.indexOf("<Cube ");
            int i = Math.min(firstDimension, firstCube);
            s = s.substring(0, i)
                + parameterDefs
                + s.substring(i);
        }

        // Add cube definitions, if specified.
        if (cubeDefs != null) {
            int i =
                s.indexOf(
                    "<Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">");
            if (i < 0) {
                i = s.indexOf(
                    "<Cube name='Sales' defaultMeasure='Unit Sales'>");
            }
            s = s.substring(0, i)
                + cubeDefs
                + s.substring(i);
        }

        // Add virtual cube definitions, if specified.
        if (virtualCubeDefs != null) {
            int i = s.indexOf(
                "<VirtualCube name=\"Warehouse and Sales\" "
                + "defaultMeasure=\"Store Sales\">");
            if (i < 0) {
                throw new RuntimeException(
                    "VirtualCube may only specified in legacy TestContext; see "
                    + "TestContext.legacy()");
            }
            s = s.substring(0, i)
                + virtualCubeDefs
                + s.substring(i);
        }

        // Add named set definitions, if specified. Schema-level named sets
        // occur after <Cube> and <VirtualCube> and before <Role> elements.
        if (namedSetDefs != null) {
            int i = s.indexOf("<Role");
            if (i < 0) {
                i = s.indexOf("</Schema>");
            }
            s = s.substring(0, i)
                + namedSetDefs
                + s.substring(i);
        }

        // Add definitions of roles, if specified.
        if (roleDefs != null) {
            int i = s.indexOf("<UserDefinedFunction");
            if (i < 0) {
                i = s.indexOf("</Schema>");
            }
            s = s.substring(0, i)
                + roleDefs
                + s.substring(i);
        }

        // Add definitions of user-defined functions, if specified.
        if (udfDefs != null) {
            int i = s.indexOf("</Schema>");
            s = s.substring(0, i)
                + udfDefs
                + s.substring(i);
        }
        return s;
    }

    /**
     * Returns the definition of a schema as stored in the underlying file.
     * Uses a cache to save re-reading.
     *
     * @param dataSet Data set
     * @return XML definition of the FoodMart schema
     */
    public static String getRawSchema(DataSet dataSet) {
        return instance().with(dataSet).getRawSchema();
    }

    /**
     * Returns the definition of the schema.
     *
     * @return XML definition of the FoodMart schema
     */
    public String getRawSchema() {
        final String catalog =
            getConnectionProperties().get(
                RolapConnectionProperties.Catalog.name());
        synchronized (rawSchemas) {
            String rawSchema = rawSchemas.get(catalog);
            if (rawSchema == null) {
                final Connection connection =
                    withSchemaProcessor(SnoopingSchemaProcessor.class)
                        .getConnection();
                connection.close();
                rawSchema = SnoopingSchemaProcessor.THREAD_RESULT.get();
                Util.threadLocalRemove(SnoopingSchemaProcessor.THREAD_RESULT);
                rawSchemas.put(catalog, rawSchema);
            }
            return rawSchema;
        }
    }

    /**
     * Returns a the XML of the foodmart schema, adding dimension definitions
     * to the definition of a given cube.
     */
    private String substituteSchema(
        String rawSchema,
        String cubeName,
        String dimensionDefs,
        String measureDefs,
        String memberDefs,
        String namedSetDefs,
        Map<String, String> dimensionLinks)
    {
        String s = rawSchema;

        final boolean v4 = s.contains("<PhysicalSchema");
        // Search for the <Cube> or <VirtualCube> element.
        int h = s.indexOf("<Cube name=\"" + cubeName + "\"");
        int end;
        if (h < 0) {
            h = s.indexOf("<Cube name='" + cubeName + "'");
        }
        if (h < 0) {
            h = s.indexOf("<VirtualCube name=\"" + cubeName + "\"");
            if (h < 0) {
                h = s.indexOf("<VirtualCube name='" + cubeName + "'");
            }
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
            s = s.substring(0, i)
                + dimensionDefs
                + s.substring(i);
        }

        // Add measure definitions, if specified.
        if (measureDefs != null) {
            int i = s.indexOf("<Measure", h);
            if (i < 0 || i > end) {
                i = end;
            }
            s = s.substring(0, i)
                + measureDefs
                + s.substring(i);
        }

        // Add calculated member definitions, if specified.
        if (memberDefs != null) {
            int i = s.indexOf("<CalculatedMembers>", h);
            if (i >= 0) {
                i += "<CalculatedMembers>".length();
                s = s.substring(0, i)
                    + memberDefs
                    + s.substring(i);
            } else {
                i = s.indexOf("<CalculatedMember", h);
                if (i < 0 || i > end) {
                    i = end;
                }
                if (v4) {
                    memberDefs =
                        "<CalculatedMembers>"
                        + memberDefs
                        + "</CalculatedMembers>";
                }
                s = s.substring(0, i)
                    + memberDefs
                    + s.substring(i);
            }
        }

        if (namedSetDefs != null) {
            int i = s.indexOf("<NamedSet", h);
            if (i < 0 || i > end) {
                i = end;
            }
            s = s.substring(0, i)
                + namedSetDefs
                + s.substring(i);
        }

        if (dimensionLinks == null) {
            dimensionLinks = Collections.emptyMap();
        }
        for (Map.Entry<String, String> entry : dimensionLinks.entrySet()) {
            int i =
                s.indexOf(
                    "<MeasureGroup name='" + entry.getKey() + "' ",
                    h);
            if (i < 0 || i > end) {
                continue;
            }
            i = s.indexOf("</DimensionLinks>", i);
            if (i < 0 || i > end) {
                continue;
            }
            s = s.substring(0, i)
                + entry.getValue()
                + s.substring(i);
        }

        return s;
    }

    /**
     * Executes a query.
     *
     * @param queryString Query string
     */
    public Result executeQuery(String queryString) {
        Connection connection = getConnection();
        queryString = upgradeQuery(queryString);
        Query query = connection.parseQuery(queryString);
        final Result result = connection.execute(query);

        // If we're deep testing, check that we never return the dummy null
        // value when cells are null. TestExpDependencies isn't the perfect
        // switch to enable this, but it will do for now.
        if (MondrianProperties.instance().TestExpDependencies.booleanValue()) {
            assertResultValid(result);
        }
        return result;
    }

    public ResultSet executeStatement(String queryString) throws SQLException {
        OlapConnection connection = getOlap4jConnection();
        queryString = upgradeQuery(queryString);
        OlapStatement stmt = connection.createStatement();
        return stmt.executeQuery(queryString);
    }

    /**
     * Executes a query using olap4j.
     */
    public CellSet executeOlap4jQuery(String queryString) throws SQLException {
        OlapConnection connection = getOlap4jConnection();
        queryString = upgradeQuery(queryString);
        OlapStatement stmt = connection.createStatement();
        final CellSet cellSet = stmt.executeOlapQuery(queryString);

        // If we're deep testing, check that we never return the dummy null
        // value when cells are null. TestExpDependencies isn't the perfect
        // switch to enable this, but it will do for now.
        if (MondrianProperties.instance().TestExpDependencies.booleanValue()) {
            assertCellSetValid(cellSet);
        }
        return cellSet;
    }

    /**
     * Checks that a {@link Result} is valid.
     *
     * @param result Query result
     */
    private void assertResultValid(Result result) {
        for (Cell cell : cellIter(result)) {
            final Object value = cell.getValue();

            // Check that the dummy value used to represent null cells never
            // leaks into the outside world.
            Assert.assertNotSame(value, Util.nullValue);
            Assert.assertFalse(
                value instanceof Number
                && ((Number) value).doubleValue() == FunUtil.DoubleNull);

            // Similarly empty values.
            Assert.assertNotSame(value, Util.EmptyValue);
            Assert.assertFalse(
                value instanceof Number
                && ((Number) value).doubleValue() == FunUtil.DoubleEmpty);

            // Cells should be null if and only if they are null or empty.
            if (cell.getValue() == null) {
                Assert.assertTrue(cell.isNull());
            } else {
                Assert.assertFalse(cell.isNull());
            }
        }

        // There should be no null members.
        for (Axis axis : result.getAxes()) {
            for (Position position : axis.getPositions()) {
                for (Member member : position) {
                    Assert.assertNotNull(member);
                }
            }
        }
    }

    /**
     * Checks that a {@link CellSet} is valid.
     *
     * @param cellSet Cell set
     */
    private void assertCellSetValid(CellSet cellSet) {
        for (org.olap4j.Cell cell : cellIter(cellSet)) {
            final Object value = cell.getValue();

            // Check that the dummy value used to represent null cells never
            // leaks into the outside world.
            Assert.assertNotSame(value, Util.nullValue);
            Assert.assertFalse(
                value instanceof Number
                && ((Number) value).doubleValue() == FunUtil.DoubleNull);

            // Similarly empty values.
            Assert.assertNotSame(value, Util.EmptyValue);
            Assert.assertFalse(
                value instanceof Number
                && ((Number) value).doubleValue() == FunUtil.DoubleEmpty);

            // Cells should be null if and only if they are null or empty.
            if (cell.getValue() == null) {
                Assert.assertTrue(cell.isNull());
            } else {
                Assert.assertFalse(cell.isNull());
            }
        }

        // There should be no null members.
        for (CellSetAxis axis : cellSet.getAxes()) {
            for (org.olap4j.Position position : axis.getPositions()) {
                for (org.olap4j.metadata.Member member : position.getMembers())
                {
                    Assert.assertNotNull(member);
                }
            }
        }
    }

    /**
     * Returns an iterator over cells in a result.
     */
    static Iterable<Cell> cellIter(final Result result) {
        return new Iterable<Cell>() {
            public Iterator<Cell> iterator() {
                int[] axisDimensions = new int[result.getAxes().length];
                int k = 0;
                for (Axis axis : result.getAxes()) {
                    axisDimensions[k++] = axis.getPositions().size();
                }
                final CoordinateIterator
                    coordIter = new CoordinateIterator(axisDimensions);
                return new Iterator<Cell>() {
                    public boolean hasNext() {
                        return coordIter.hasNext();
                    }

                    public Cell next() {
                        final int[] ints = coordIter.next();
                        return result.getCell(ints);
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Returns an iterator over cells in an olap4j cell set.
     */
    static Iterable<org.olap4j.Cell> cellIter(final CellSet cellSet) {
        return new Iterable<org.olap4j.Cell>() {
            public Iterator<org.olap4j.Cell> iterator() {
                int[] axisDimensions = new int[cellSet.getAxes().size()];
                int k = 0;
                for (CellSetAxis axis : cellSet.getAxes()) {
                    axisDimensions[k++] = axis.getPositions().size();
                }
                final CoordinateIterator
                    coordIter = new CoordinateIterator(axisDimensions);
                return new Iterator<org.olap4j.Cell>() {
                    public boolean hasNext() {
                        return coordIter.hasNext();
                    }

                    public org.olap4j.Cell next() {
                        final int[] ints = coordIter.next();
                        final List<Integer> list =
                            new AbstractList<Integer>() {
                                public Integer get(int index) {
                                    return ints[index];
                                }

                                public int size() {
                                    return ints.length;
                                }
                            };
                        return cellSet.getCell(
                            list);
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Executes a query, and asserts that it throws an exception which contains
     * the given pattern.
     *
     * @param queryString Query string
     * @param pattern Pattern which exception must match
     */
    public void assertQueryThrows(String queryString, String pattern) {
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
            String cubeName = getDefaultCubeName();
            if (cubeName.indexOf(' ') >= 0) {
                cubeName = Util.quoteMdxIdentifier(cubeName);
            }
            expression = Util.replace(expression, "'", "''");
            Result result = executeQuery(
                "with member [Measures].[Foo] as '"
                + expression
                + "' select {[Measures].[Foo]} on columns from "
                + cubeName);
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
     * Returns the name of the default cube.
     *
     * <p>Tests which evaluate scalar expressions, such as
     * {@link #assertExprReturns(String, String)}, generate queries against this
     * cube.
     *
     * @return the name of the default cube
     */
    public String getDefaultCubeName() {
        return "Sales";
    }

    /**
     * Executes the expression in the context of the cube indicated by
     * <code>cubeName</code>, and returns the result as a Cell.
     *
     * @param expression The expression to evaluate
     * @return Cell which is the result of the expression
     */
    public Cell executeExprRaw(String expression) {
        final String queryString = generateExpression(expression);
        Result result = executeQuery(queryString);
        return result.getCell(new int[]{0});
    }

    private String generateExpression(String expression) {
        String cubeName = getDefaultCubeName();
        if (cubeName.indexOf(' ') >= 0) {
            cubeName = Util.quoteMdxIdentifier(cubeName);
        }
        return
            "with member [Measures].[Foo] as "
            + Util.singleQuoteString(expression)
            + " select {[Measures].[Foo]} on columns from " + cubeName;
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
     * Asserts that an expression, with a given set of parameter bindings,
     * returns a given result.
     *
     * @param expr Scalar MDX expression
     * @param expected Expected result
     * @param paramValues Array of parameter names and values
     */
    public void assertParameterizedExprReturns(
        String expr,
        String expected,
        Object... paramValues)
    {
        Connection connection = getConnection();
        String queryString = generateExpression(expr);
        Query query = connection.parseQuery(queryString);
        assert paramValues.length % 2 == 0;
        for (int i = 0; i < paramValues.length;) {
            final String paramName = (String) paramValues[i++];
            final Object value = paramValues[i++];
            query.setParameter(paramName, value);
        }
        final Result result = connection.execute(query);
        final Cell cell = result.getCell(new int[]{0});

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
        String expected)
    {
        Axis axis = executeAxis(expression);
        assertEqualsVerbose(
            expected,
            upgradeActual(toString(axis.getPositions())));
    }

    /**
     * Massages the actual result of executing a query to handle differences in
     * unique names betweeen old and new behavior.
     *
     * <p>Since the new naming is now the default, reference logs
     * should be in terms of the new naming.
     *
     * @see mondrian.olap.MondrianProperties#SsasCompatibleNaming
     *
     * @param actual Actual result
     * @return Expected result massaged for backwards compatibility
     */
    public String upgradeActual(String actual) {
        String[] strings = {actual};
        if (true || !MondrianProperties.instance().SsasCompatibleNaming.get()) {
            foo(strings, "Time", "Weekly");
            strings[0] = Util.replace(
                strings[0],
                "[All Time.Weeklys]",
                "[All Weeklys]");
            strings[0] = Util.replace(
                strings[0],
                "<HIERARCHY_NAME>Time.Weekly</HIERARCHY_NAME>",
                "<HIERARCHY_NAME>Weekly</HIERARCHY_NAME>");
            if (false) {
            foo(strings, "Time", "Monthly");
            foo2(strings, "Store", "Stores");
            foo2(strings, "Customer", "Customers");
            foo2(strings, "Customer", "Marital Status");
            foo2(strings, "Customer", "Gender");
            foo2(strings, "Store", "Store Type");
            foo2(strings, "Product", "Products");
            foo1(strings, "Promotion Media");
            foo1(strings, "Time");

            // for a few tests in SchemaTest
            foo(strings, "Store", "MyHierarchy");
            strings[0] = Util.replace(
                strings[0],
                "[All Store.MyHierarchys]",
                "[All MyHierarchys]");
            strings[0] = Util.replace(
                strings[0],
                "[Store2].[All Store2s]",
                "[Store2].[Store].[All Stores]");
            strings[0] = Util.replace(
                strings[0],
                "[Store Type 2.Store Type 2].[All Store Type 2.Store Type 2s]",
                "[Store Type 2].[All Store Type 2s]");
            foo(strings, "TIME", "CALENDAR");
            }
            strings[0] = Util.replace(
                strings[0],
                "<Store>true</Store>",
                "<Store>1</Store>");
            strings[0] = Util.replace(
                strings[0],
                "<Employees>80000.0000</Employees>",
                "<Employees>80000</Employees>");
        }
        if (true) {
            // mondrian-4
            strings[0] = Util.replace(
                strings[0],
                "[Promotion.Media Type]",
                "[Promotion].[Media Type]");
            strings[0] = Util.replace(
                strings[0],
                "[Promotion].[Media Type]",
                "[Promotion].[Media Type]");
            if (false) {
            strings[0] = Util.replace(
                strings[0],
                "[Product].[All Products].",
                "[Product].[Products].");
            }
        }
        return strings[0];
    }

    // converts "[dim.hier]" to "[dim].[hier]"
    private static void foo(String[] strings, String dim, String hier)
    {
        strings[0] = Util.replace(
            strings[0],
            "[" + dim + "." + hier + "]",
            "[" + dim + "].[" + hier + "]");
    }

    // converts "[dim].[hier]" to "[hier]"
    private static void foo2(String[] strings, String dim, String hier)
    {
        strings[0] = Util.replace(
            strings[0],
            "[" + dim + "].[" + hier + "]",
            "[" + hier + "]");
    }

    // converts "[hier].[hier]" to "[hier]"
    private static void foo1(String[] strings, String hier)
    {
        foo2(strings, hier, hier);
    }

    public String upgradeExpected(String actual) {
        String[] strings = {actual};
        if (false)
        strings[0] = strings[0].replaceAll(
            "([^.])\\[Product\\]\\.",
            "$1[Product].[Products].");
        return strings[0];
    }

    /**
     * Massages an MDX query to handle differences in
     * unique names betweeen old and new behavior.
     *
     * <p>The main difference addressed is with level naming. The problem
     * arises when dimension, hierarchy and level have the same name:<ul>
     *
     * <li>In old behavior, the [Gender].[Gender] represents the Gender level,
     * and [Gender].[Gender].[Gender] is invalid.
     *
     * <li>In new behavior, [Gender].[Gender] represents the Gender hierarchy,
     * and [Gender].[Gender].[Gender].members represents the Gender level.
     * </ul></p>
     *
     * <p>So, {@code upgradeQuery("[Gender]")} returns
     * "[Gender].[Gender]" for old behavior,
     * "[Gender].[Gender].[Gender]" for new behavior.</p>
     *
     * @see mondrian.olap.MondrianProperties#SsasCompatibleNaming
     *
     * @param queryString Original query
     * @return Massaged query for backwards compatibility
     */
    public String upgradeQuery(String queryString) {
        if (MondrianProperties.instance().SsasCompatibleNaming.get()) {
            String[] names = {
//                "[Gender]",
//                "[Education Level]",
//                "[Marital Status]",
//                "[Store Type]",
//                "[Yearly Income]",
            };
            for (String name : names) {
                queryString = Util.replace(
                    queryString,
                    name + "." + name,
                    name + "." + name + "." + name);
            }
            queryString = Util.replace(
                queryString,
                "[Time.Weekly].[All Time.Weeklys]",
                "[Time].[Weekly].[All Weeklys]");
        }
        return queryString;
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
            queryString =
                "with member [Measures].[Foo] as "
                + Util.singleQuoteString(expression)
                + " select {[Measures].[Foo]} on columns from " + cubeName;
        } else {
            queryString =
                "SELECT {" + expression + "} ON COLUMNS FROM " + cubeName;
        }
        Connection connection = getConnection();
        Query query = connection.parseQuery(queryString);
        final Exp exp;
        if (scalar) {
            exp = query.getFormulas()[0].getExpression();
        } else {
            exp = query.getAxes()[0].getSet();
        }
        final Calc calc = query.compileExpression(exp, scalar, null);
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        final CalcWriter calcWriter = new CalcWriter(pw, false);
        calc.accept(calcWriter);
        pw.flush();
        return sw.toString();
    }

    /**
     * Executes a set expression which is expected to return 0 or 1 members.
     * It is an error if the expression returns tuples (as opposed to members),
     * or if it returns two or more members.
     *
     * @param expression Expression string
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
                "expression " + expression
                + " yielded " + axis.getPositions().size() + " positions");
        }
    }

    /**
     * Executes a query with a given expression on an axis, and returns the
     * whole axis.
     */
    public Axis executeAxis(String expression) {
        Result result = executeQuery(
            "select {" + expression + "} on columns from "
            + getDefaultCubeName());
        return result.getAxes()[0];
    }

    /**
     * Executes a query with a given expression on an axis, and asserts that it
     * throws an error which matches a particular pattern. The expression is
     * evaulated against the default cube.
     */
    public void assertAxisThrows(
        String expression,
        String pattern)
    {
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
            Assert.fail(
                "query's error does not match pattern '" + pattern
                + "'; error is [" + stackTrace + "]");
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
            assertEqualsVerbose(
                upgradeExpected(desiredResult),
                upgradeActual(resultString));
        }
    }

    /**
     * Executes a very simple query.
     *
     * <p>This forces the schema to be loaded and performs a basic sanity check.
     * If this is a negative schema test, causes schema validation errors to be
     * thrown.
     */
    public void assertSimpleQuery() {
        assertQueryReturns(
            "select from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "266,773");
    }

    /**
     * Checks that an actual string matches an expected string.
     *
     * <p>If they do not, throws a {@link junit.framework.ComparisonFailure} and
     * prints the difference, including the actual string as an easily pasted
     * Java string literal.
     */
    public static void assertEqualsVerbose(
        String expected,
        String actual)
    {
        assertEqualsVerbose(expected, actual, true, null);
    }

    /**
     * Checks that an actual string matches an expected string.
     *
     * <p>If they do not, throws a {@link ComparisonFailure} and prints the
     * difference, including the actual string as an easily pasted Java string
     * literal.
     *
     * @param expected Expected string
     * @param actual Actual string
     * @param java Whether to generate actual string as a Java string literal
     * if the values are not equal
     * @param message Message to display, optional
     */
    public static void assertEqualsVerbose(
        String expected,
        String actual,
        boolean java,
        String message)
    {
        assertEqualsVerbose(
            fold(expected), actual, java, message);
    }

    /**
     * Checks that an actual string matches an expected string.
     *
     * <p>If they do not, throws a {@link ComparisonFailure} and prints the
     * difference, including the actual string as an easily pasted Java string
     * literal.
     *
     * @param safeExpected Expected string, where all line endings have been
     * converted into platform-specific line endings
     * @param actual Actual string
     * @param java Whether to generate actual string as a Java string literal
     * if the values are not equal
     * @param message Message to display, optional
     */
    public static void assertEqualsVerbose(
        SafeString safeExpected,
        String actual,
        boolean java,
        String message)
    {
        String expected = safeExpected == null ? null : safeExpected.s;
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
            "Expected:" + nl + expected + nl
            + "Actual:" + nl + actual + nl;
        if (java) {
            message += "Actual java:" + nl + toJavaString(actual) + nl;
        }
        throw new ComparisonFailure(message, expected, actual);
    }

    private static String toJavaString(String s) {
        // Convert [string with "quotes" split
        // across lines]
        // into ["string with \"quotes\" split\n"
        //                 + "across lines
        //
        s = Util.replace(s, "\"", "\\\"");
        s = LineBreakPattern.matcher(s).replaceAll(lineBreak2);
        s = TabPattern.matcher(s).replaceAll("\\\\t");
        s = "\"" + s + "\"";
        String spurious = nl + indent + "+ \"\"";
        if (s.endsWith(spurious)) {
            s = s.substring(0, s.length() - spurious.length());
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
            "Expected pattern:" + nl + expected + nl
            + "Actual: " + nl + actual + nl
            + "Actual java: " + nl + s + nl;
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
     * Converts a {@link mondrian.olap.Result} to text in traditional format.
     *
     * <p>For more exotic formats, see
     * {@link org.olap4j.layout.CellSetFormatter}.
     *
     * @param result Query result
     * @return Result as text
     */
    public static String toString(Result result) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        result.print(pw);
        pw.flush();
        return sw.toString();
    }

    /**
     * Converts a {@link CellSet} to text in traditional format.
     *
     * <p>For more exotic formats, see
     * {@link org.olap4j.layout.CellSetFormatter}.
     *
     * @param cellSet Query result
     * @return Result as text
     */
    public static String toString(CellSet cellSet) {
        final StringWriter sw = new StringWriter();
        new TraditionalCellSetFormatter().format(
            cellSet, new PrintWriter(sw));
        return sw.toString();
    }

    /**
     * Returns a test context whose {@link #getOlap4jConnection()} method always
     * returns the same connection object, and which has an active
     * {@link org.olap4j.Scenario}, thus enabling writeback.
     *
     * @return Test context with active scenario
     */
    public final TestContext withScenario() {
        return new DelegatingTestContext(this)
        {
            OlapConnection connection;

            public OlapConnection getOlap4jConnection() throws SQLException {
                if (connection == null) {
                    connection = super.getOlap4jConnection();
                    connection.setScenario(
                        connection.createScenario());
                }
                return connection;
            }

            @Override
            public Connection getConnection() {
                try {
                    return getOlap4jConnection().unwrap(Connection.class);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /**
     * Converts a set of positions into a string. Useful if you want to check
     * that an axis has the results you expected.
     */
    public static String toString(List<Position> positions) {
        StringBuilder buf = new StringBuilder();
        int i = 0;
        for (Position position : positions) {
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
     * Makes a copy of a suite, filtering certain tests.
     *
     * @param suite Test suite
     * @param testPattern Regular expression of name of tests to include
     * @return copy of test suite
     */
    public static TestSuite copySuite(
        TestSuite suite,
        Util.Functor1<Boolean, Test> testPattern)
    {
        TestSuite newSuite = new TestSuite(suite.getName());
        //noinspection unchecked
        for (Test test : Collections.list((Enumeration<Test>) suite.tests())) {
            if (!testPattern.apply(test)) {
                continue;
            }
            if (test instanceof TestCase) {
                newSuite.addTest(test);
            } else if (test instanceof TestSuite) {
                TestSuite subSuite = copySuite((TestSuite) test, testPattern);
                if (subSuite.countTestCases() > 0) {
                    newSuite.addTest(subSuite);
                }
            } else {
                // some other kind of test
                newSuite.addTest(test);
            }
        }
        return newSuite;
    }

    public void close() {
        // nothing
    }

    /**
     * Asserts that a particular error is given while validating a schema.
     *
     * <p>At present errors are regarded as fatal, therefore there can be at
     * most one. That may change; if so, this method will behave more like
     * {@link #getSchemaWarnings()} followed by
     * {@link #assertContains(java.util.List, String, String)}.
     *
     * @param expected Expected message
     * @param errorLoc Location of error
     */
    public void assertSchemaError(
        String expected,
        String errorLoc)
    {
        final List<Exception> exceptionList = new ArrayList<Exception>();
        try {
            assertSimpleQuery();
        } catch (Exception e) {
            exceptionList.add(e);
        }
        assertContains(exceptionList, expected, errorLoc);
    }

    /**
     * Returns a test context based on a particular data set.
     *
     * @param dataSet Data set
     * @return Test context based on given data set
     */
    public TestContext with(DataSet dataSet) {
        switch (dataSet) {
        case FOODMART:
            return withPropertiesReplace(
                RolapConnectionProperties.Catalog,
                "FoodMart.xml",
                "NewFoodMart.xml");
        case LEGACY_FOODMART:
            return withPropertiesReplace(
                RolapConnectionProperties.Catalog,
                "NewFoodMart.xml",
                "FoodMart.xml");
        case STEELWHEELS:
            return SteelWheelsTestCase.createContext(this, null);
        default:
            throw Util.unexpected(dataSet);
        }
    }

    private TestContext withPropertiesReplace(
        RolapConnectionProperties property,
        String find,
        String replace)
    {
        final Util.PropertyList properties =
            getConnectionProperties().clone();
        final String catalog =
            properties.get(property.name());
        String catalog2 = Util.replace(catalog, find, replace);
        if (catalog.equals(catalog2)) {
            return this;
        }
        properties.put(property.name(), catalog2);
        return withProperties(properties);
    }

    /**
     * Shorthand for {@code with(LEGACY_FOODMART)} that indicates that the test
     * case should be upgraded.
     */
    public TestContext legacy() {
        return with(DataSet.LEGACY_FOODMART);
    }

    /**
     * Shorthand for {@code with(FOODMART)}.
     */
    public TestContext modern() {
        return with(DataSet.FOODMART);
    }

    public String getCatalogContent() {
        return schema;
    }

    /**
     * Wrapper around a string that indicates that all line endings have been
     * converted to platform-specific line endings.
     *
     * @see TestContext#fold
     */
    public static class SafeString {
        public final String s;

        private SafeString(String s) {
            this.s = s;
        }
    }

    /**
     * Replaces line-endings in a string with the platform-dependent
     * equivalent. If the input string already has platform-dependent
     * line endings, no replacements are made.
     *
     * @param string String whose line endings are to be made platform-
     *               dependent. Typically these are constant "expected
     *               value" string expressions where the linefeed is
     *               represented as linefeed "\n", but sometimes this method
     *               will receive strings created dynamically where the line
     *               endings are already appropriate for the platform.
     * @return String where all linefeeds have been converted to
     *         platform-specific (CR+LF on Windows, LF on Unix/Linux)
     */
    public static SafeString fold(String string) {
        if (string == null) {
            return null;
        }
        if (nl.equals("\n") || string.indexOf(nl) != -1) {
            return new SafeString(string);
        }
        return new SafeString(Util.replace(string, "\n", nl));
    }

    /**
     * Reverses the effect of {@link #fold}; converts platform-specific line
     * endings in a string info linefeeds.
     *
     * @param string String where all linefeeds have been converted to
     * platform-specific (CR+LF on Windows, LF on Unix/Linux)
     * @return String where line endings are represented as linefeed "\n"
     */
    public static String unfold(String string) {
        if (!nl.equals("\n")) {
            string = Util.replace(string, nl, "\n");
        }
        if (string == null) {
            return null;
        } else {
            return string;
        }
    }

    public synchronized Dialect getDialect() {
        if (dialect == null) {
            dialect = getDialectInternal();
        }
        return dialect;
    }

    private Dialect getDialectInternal() {
        DataSource dataSource = getConnection().getDataSource();
        return DialectManager.createDialect(dataSource, null);
    }

    /**
     * Creates a dialect without using a connection.
     *
     * @return dialect of an Access persuasion
     */
    public static Dialect getFakeDialect()
    {
        final DatabaseMetaData metaData =
            (DatabaseMetaData) Proxy.newProxyInstance(
                null,
                new Class<?>[] {DatabaseMetaData.class},
                new DelegatingInvocationHandler() {
                    public boolean supportsResultSetConcurrency(
                        int type, int concurrency)
                    {
                        return false;
                    }
                    public String getDatabaseProductName() {
                        return "Access";
                    }
                    public String getIdentifierQuoteString() {
                        return "\"";
                    }
                    public String getDatabaseProductVersion() {
                        return "1.0";
                    }
                    public boolean isReadOnly() {
                        return true;
                    }
                }
            );
        final java.sql.Connection connection =
            (java.sql.Connection) Proxy.newProxyInstance(
                null,
                new Class<?>[] {java.sql.Connection.class},
                new DelegatingInvocationHandler() {
                    public DatabaseMetaData getMetaData() {
                        return metaData;
                    }
                }
            );
        return DialectManager.createDialect(null, connection);
    }

    /**
     * Checks that expected SQL equals actual SQL.
     * Performs some normalization on the actual SQL to compensate for
     * differences between dialects.
     */
    public void assertSqlEquals(
        String expectedSql,
        String actualSql,
        int expectedRows)
    {
        // if the actual SQL isn't in the current dialect we have some
        // problems... probably with the dialectize method
        assertEqualsVerbose(actualSql, fold(dialectize(actualSql)).s);

        String transformedExpectedSql = removeQuotes(dialectize(expectedSql));
        String transformedActualSql = removeQuotes(actualSql);

        Assert.assertEquals(transformedExpectedSql, transformedActualSql);

        checkSqlAgainstDatasource(actualSql, expectedRows);
    }

    /**
     * Checks that expected SQL equals actual SQL, using a diff repository to
     * get the expected SQL.
     *
     * <p>Performs some normalization on the actual SQL to compensate for
     * differences between dialects.</p>
     */
    public void assertSqlEquals(
        DiffRepository diffRepos,
        String tag,
        String actualSql,
        int expectedRows)
    {
        final Util.Functor1<String, String> filter =
            new Util.Functor1<String, String>()
            {
                public String apply(String param) {
                    return transformQuotes(
                        dialectize(Dialect.DatabaseProduct.MYSQL, param));
                }
            };
        String transformedActualSql = filter.apply(actualSql);

        final String expectedSql = "${" + tag + "}";
        diffRepos.assertEquals(tag, expectedSql, transformedActualSql, filter);

        // if the actual SQL isn't in the current dialect we have some
        // problems... probably with the dialectize method
        diffRepos.assertEquals(tag, actualSql, fold(dialectize(actualSql)).s);

        checkSqlAgainstDatasource(actualSql, expectedRows);
    }

    private static String removeQuotes(String actualSql) {
        String transformedActualSql = actualSql.replaceAll("`", "");
        transformedActualSql = transformedActualSql.replaceAll("\"", "");
        return transformedActualSql;
    }

    private static String transformQuotes(String sql) {
        return sql.replaceAll("\"", "`");
    }

    /**
     * Converts a SQL string into the current dialect.
     *
     * @param sql SQL string in generic dialect
     * @return SQL string converted into current dialect
     */
    private String dialectize(String sql)
    {
        return dialectize(getDialect().getDatabaseProduct(), sql);
    }

    /**
     * Converts a SQL string into a given dialect.
     *
     * <p>This is not intended to be a general purpose method: it looks for
     * specific patterns known to occur in tests, in particular "=as=" and
     * "fname + ' ' + lname".</p>
     *
     * @param databaseProduct Database product
     * @param sql SQL string in generic dialect
     * @return SQL string converted into current dialect
     */
    private static String dialectize(
        Dialect.DatabaseProduct databaseProduct,
        String sql)
    {
        final String fullName = "fname \\+ ' ' \\+ lname";
        final String promotionSales =
            "\\(case when `sales_fact_1997`.`promotion_id` = 0 then 0 else `sales_fact_1997`.`store_sales` end\\)";
        switch (databaseProduct) {
        case MYSQL:
            // Mysql would generate "CONCAT(...)"
            sql = sql.replaceAll(
                fullName,
                "CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)");
            sql = sql.replaceAll(
                promotionSales,
                "`sales_fact_1997`.`store_sales`");
            break;
        case POSTGRESQL:
        case ORACLE:
        case LUCIDDB:
        case TERADATA:
            sql = sql.replaceAll(
                fullName,
                "`fname` || ' ' || `lname`");
            break;
        case DERBY:
            sql = sql.replaceAll(
                fullName,
                "`customer`.`fullname`");
            break;
        case INGRES:
            sql = sql.replaceAll(
                fullName,
                "fullname");
            break;
        case DB2:
        case DB2_AS400:
        case DB2_OLD_AS400:
            sql = sql.replaceAll(
                fullName,
                "CONCAT(CONCAT(`customer`.`fname`, ' '), `customer`.`lname`)");
            break;
        }

        if (databaseProduct == Dialect.DatabaseProduct.ORACLE) {
            // " + tableQualifier + "
            sql = sql.replaceAll(" =as= ", " ");
        } else {
            sql = sql.replaceAll(" =as= ", " as ");
        }

        final String caseStmt =
            " (case when `sales_fact_1997`.`promotion_id` = 0 then 0"
            + " else `sales_fact_1997`.`store_sales` end)";
        final String accessCase =
            " Iif(`sales_fact_1997`.`promotion_id` = 0, 0,"
            + " `sales_fact_1997`.`store_sales`)";
        final String infobrightCase = " `sales_fact_1997`.`store_sales`";
        switch (databaseProduct) {
        case ACCESS:
            sql = sql.replaceAll(accessCase, caseStmt);
            break;
        case INFOBRIGHT:
            sql = sql.replaceAll(infobrightCase, caseStmt);
            break;
        }

        return sql;
    }

    private void checkSqlAgainstDatasource(
        String actualSql,
        int expectedRows)
    {
        Util.PropertyList connectProperties = getConnectionProperties();

        java.sql.Connection jdbcConn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            String jdbcDrivers =
                connectProperties.get(
                    RolapConnectionProperties.JdbcDrivers.name());
            if (jdbcDrivers != null) {
                RolapUtil.loadDrivers(jdbcDrivers);
            }
            final String jdbcDriversProp =
                MondrianProperties.instance().JdbcDrivers.get();
            RolapUtil.loadDrivers(jdbcDriversProp);

            jdbcConn = java.sql.DriverManager.getConnection(
                connectProperties.get(RolapConnectionProperties.Jdbc.name()),
                connectProperties.get(
                    RolapConnectionProperties.JdbcUser.name()),
                connectProperties.get(
                    RolapConnectionProperties.JdbcPassword.name()));
            stmt = jdbcConn.createStatement();

            if (RolapUtil.SQL_LOGGER.isDebugEnabled()) {
                StringBuffer sqllog = new StringBuffer();
                sqllog.append("mondrian.test.TestContext: executing sql [");
                if (actualSql.indexOf('\n') >= 0) {
                    // SQL appears to be formatted as multiple lines. Make it
                    // start on its own line.
                    sqllog.append("\n");
                }
                sqllog.append(actualSql);
                sqllog.append(']');
                RolapUtil.SQL_LOGGER.debug(sqllog.toString());
            }

            long startTime = System.currentTimeMillis();
            rs = stmt.executeQuery(actualSql);
            long time = System.currentTimeMillis();
            final long execMs = time - startTime;
            Util.addDatabaseTime(execMs);

            RolapUtil.SQL_LOGGER.debug(", exec " + execMs + " ms");

            int rows = 0;
            while (rs.next()) {
                rows++;
            }

            Assert.assertEquals("row count", expectedRows, rows);
        } catch (SQLException e) {
            throw new RuntimeException(
                "ERROR in SQL - invalid for database: "
                + connectProperties.get(RolapConnectionProperties.Jdbc.name())
                + "\n" + actualSql,
                e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception e1) {
                // ignore
            }
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (Exception e1) {
                // ignore
            }
            try {
                if (jdbcConn != null) {
                    jdbcConn.close();
                }
            } catch (Exception e1) {
                // ignore
            }
        }
    }

    /**
     * Asserts that an MDX set-valued expression depends upon a given list of
     * dimensions.
     */
    public void assertSetExprDependsOn(String expr, Set<String> hierSet) {
        // Construct a query, and mine it for a parsed expression.
        // Use a fresh connection, because some tests define their own dims.
        final Connection connection = getConnection();
        final String queryString =
                "SELECT {" + expr + "} ON COLUMNS FROM [Sales]";
        final Query query = connection.parseQuery(queryString);
        query.resolve();
        final Exp expression = query.getAxes()[0].getSet();

        // Build a list of the dimensions which the expression depends upon,
        // and check that it is as expected.
        checkDependsOn(query, expression, hierSet, false);
    }

    /**
     * Asserts that an MDX member-valued depends upon a given list of
     * hierarchies.
     */
    public void assertMemberExprDependsOn(String expr, Set<String> hierSet) {
        assertSetExprDependsOn("{" + expr + "}", hierSet);
    }

    /**
     * Asserts that an MDX expression depends upon a given list of dimensions.
     */
    public void assertExprDependsOn(String expr, Set<String> hierSet) {
        // Construct a query, and mine it for a parsed expression.
        // Use a fresh connection, because some tests define their own dims.
        final Connection connection = getConnection();
        final String queryString =
            "WITH MEMBER [Measures].[Foo] AS "
            + Util.singleQuoteString(expr)
            + " SELECT FROM "
            + getDefaultCubeName();
        final Query query = connection.parseQuery(queryString);
        query.resolve();
        final Formula formula = query.getFormulas()[0];
        final Exp expression = formula.getExpression();

        // Build a list of the dimensions which the expression depends upon,
        // and check that it is as expected.
        checkDependsOn(query, expression, hierSet, true);
    }

    private void checkDependsOn(
        final Query query,
        final Exp expression,
        Set<String> expectedHierList,
        final boolean scalar)
    {
        final Calc calc =
            query.compileExpression(
                expression,
                scalar,
                scalar ? null : ResultStyle.ITERABLE);
        final TreeSet<String> actualHierarchyList = new TreeSet<String>();
        final RolapCube cube = (RolapCube) query.getCube();
        for (Hierarchy hierarchy : cube.getHierarchyList()) {
            if (calc.dependsOn(hierarchy)) {
                actualHierarchyList.add(hierarchy.getUniqueName());
            }
        }
        if (!Util.equals(expectedHierList, actualHierarchyList)) {
            String message =
                "In expected but not actual: "
                + minus(expectedHierList, actualHierarchyList)
                + "\n"
                + "In actual but not expected: "
                + minus(actualHierarchyList, expectedHierList);
            assertEqualsVerbose(
                expectedHierList.toString(), actualHierarchyList.toString(),
                false, message);
        }
    }

    private <T> Set<T> minus(Set<T> s1, Set<T> s2) {
        final LinkedHashSet<T> set = new LinkedHashSet<T>(s1);
        set.removeAll(s2);
        return set;
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
     * @param roleDefs Definitions of roles
     * @return TestContext which reads from a slightly different hymnbook
     */
    public final TestContext create(
        String parameterDefs,
        String cubeDefs,
        String virtualCubeDefs,
        String namedSetDefs,
        String udfDefs,
        String roleDefs)
    {
        final String catalogContent = getSchema(
            parameterDefs, cubeDefs, virtualCubeDefs, namedSetDefs,
            udfDefs, roleDefs);
        return withSchema(catalogContent);
    }

    /**
     * Creates a TestContext which contains the given schema text.
     *
     * @param catalogContent XML schema content
     * @return TestContext which contains the given schema
     */
    public final TestContext withSchema(String catalogContent) {
        final Util.PropertyList properties = getConnectionProperties().clone();
        catalogContent = checkErrorLocation(catalogContent);
        properties.put(
            RolapConnectionProperties.CatalogContent.name(),
            catalogContent);
        properties.remove(
            RolapConnectionProperties.Catalog.name());
        return withProperties(properties);
    }

    /**
     * Creates a TestContext that applies a substitution to the schema text.
     *
     * @param substitution Filter to be applied to the schema content
     * @return TestContext which contains the substituted schema
     */
    public final TestContext withSubstitution(
        final Util.Functor1<String, String> substitution)
    {
        return new DelegatingTestContext(this) {
            public Util.PropertyList getConnectionProperties() {
                final Util.PropertyList propertyList =
                    super.getConnectionProperties();
                String catalogContent =
                    propertyList.get(
                        RolapConnectionProperties.CatalogContent.name());
                if (catalogContent == null) {
                    catalogContent = super.getRawSchema();
                }
                String catalogContent2 = substitution.apply(catalogContent);
                Util.PropertyList propertyList2 = propertyList.clone();
                propertyList2.put(
                    RolapConnectionProperties.CatalogContent.name(),
                    catalogContent2);
                return propertyList2;
            }
        };
    }

    protected String checkErrorLocation(String schema) {
        int firstCaret = schema.indexOf('^');
        int secondCaret = -1;
        if (firstCaret >= 0) {
            schema = schema.substring(0, firstCaret)
                + schema.substring(firstCaret + 1);
            secondCaret = schema.indexOf('^', firstCaret);
            if (secondCaret >= 0) {
                schema = schema.substring(0, secondCaret)
                    + schema.substring(secondCaret + 1);
            }
        }
        setErrorLocation(schema, firstCaret, secondCaret);
        return schema;
    }

    /**
     * Sets the position in the schema text where a validation error is
     * expected to occur.
     *
     * @param errorStart Offset of start of error
     * @param errorEnd Offset of end of error, or -1
     */
    protected void setErrorLocation(
        String schema,
        int errorStart,
        int errorEnd)
    {
        this.schema = schema;
        this.errorStart = errorStart;
        this.errorEnd = errorEnd;
     }

    /**
     * Creates a TestContext which is like this one but uses the given
     * connection properties.
     *
     * @param properties Connection properties
     * @return TestContext which contains the given properties
     */
    public TestContext withProperties(final Util.PropertyList properties) {
        return new DelegatingTestContext(this) {
            public Util.PropertyList getConnectionProperties() {
                return properties;
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
    public final TestContext createSubstitutingCube(
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
    public final TestContext createSubstitutingCube(
        final String cubeName,
        final String dimensionDefs,
        final String memberDefs)
    {
        return createSubstitutingCube(
            cubeName, dimensionDefs, null, memberDefs, null);
    }


    /**
     * Creates a TestContext, adding hierarchy and calculated member definitions
     * to a cube definition.
     *
     * @param cubeName Name of a cube in the schema (cube must exist)
     * @param dimensionDefs String defining dimensions, or null
     * @param measureDefs String defining measures, or null
     * @param memberDefs String defining calculated members, or null
     * @param namedSetDefs String defining named set definitions, or null
     * @return TestContext with modified cube defn
     */
    public final TestContext createSubstitutingCube(
        final String cubeName,
        final String dimensionDefs,
        final String measureDefs,
        final String memberDefs,
        final String namedSetDefs)
    {
        return createSubstitutingCube(
            cubeName,
            dimensionDefs,
            measureDefs,
            memberDefs,
            namedSetDefs,
            Collections.<String, String>emptyMap());
    }


    /**
     * Creates a TestContext, adding hierarchy and calculated member definitions
     * to a cube definition.
     *
     * @param cubeName Name of a cube in the schema (cube must exist)
     * @param dimensionDefs String defining dimensions, or null
     * @param measureDefs String defining measures, or null
     * @param memberDefs String defining calculated members, or null
     * @param namedSetDefs String defining named set definitions, or null
     * @param dimensionLinks Dimension links
     * @return TestContext with modified cube defn
     */
    public final TestContext createSubstitutingCube(
        final String cubeName,
        final String dimensionDefs,
        final String measureDefs,
        final String memberDefs,
        final String namedSetDefs,
        Map<String, String> dimensionLinks)
    {
        final String schema =
            substituteSchema(
                getRawSchema(),
                cubeName,
                dimensionDefs,
                measureDefs,
                memberDefs,
                namedSetDefs,
                dimensionLinks);
        return withSchema(schema);
    }

    /**
     * Returns a TestContext similar to this one, but using the given role.
     *
     * @param roleName Role name
     * @return Test context with the given role
     */
    public final TestContext withRole(final String roleName) {
        final Util.PropertyList properties = getConnectionProperties().clone();
        properties.put(
            RolapConnectionProperties.Role.name(),
            roleName);
        return new DelegatingTestContext(this) {
            public Util.PropertyList getConnectionProperties() {
                return properties;
            }
        };
    }

    /**
     * Returns a TestContext similar to this one, but using the given cube as
     * default for tests such as {@link #assertExprReturns(String, String)}.
     *
     * @param cubeName Cube name
     * @return Test context with the given default cube
     */
    public final TestContext withCube(final String cubeName) {
        return new DelegatingTestContext(this) {
            public String getDefaultCubeName() {
                return cubeName;
            }
        };
    }

    /**
     * Returns a {@link TestContext} similar to this one, but which uses a given
     * connection.
     *
     * @param connection Connection
     * @return Test context which uses the given connection
     */
    public final TestContext withConnection(final Connection connection) {
        return new DelegatingTestContext(this) {
            public Connection getConnection() {
                return connection;
            }

            @Override
            public void close() {
                connection.close();
            }
        };
    }

    /**
     * Generates a string containing all dimensions except those given. Useful
     * as an argument to {@link #assertExprDependsOn(String, java.util.Set)}.
     *
     * @return string containing all dimensions except those given
     */
    public static Set<String> allHiersExcept(String ... hiers) {
        for (String hier : hiers) {
            assert contains(AllHiers, hier) : "unknown hierarchy " + hier;
        }
        final LinkedHashSet<String> result =
            new LinkedHashSet<String>(Arrays.asList(AllHiers));
        result.removeAll(Arrays.asList(hiers));
        return result;
    }

    public static boolean contains(String[] a, String s) {
        for (String anA : a) {
            if (anA.equals(s)) {
                return true;
            }
        }
        return false;
    }

    public static Set<String> allHiers() {
        return allHiersExcept();
    }

    /**
     * Creates a FoodMart connection with "Ignore=true" and returns the list
     * of warnings in the schema.
     *
     * @return Warnings encountered while loading schema
     */
    public List<Exception> getSchemaWarnings() {
        final Util.PropertyList propertyList =
            getConnectionProperties().clone();
        propertyList.put(
            RolapConnectionProperties.Ignore.name(),
            "true");
        final Connection connection =
            withProperties(propertyList).getConnection();
        return connection.getSchema().getWarnings();
    }

    /**
     * Asserts that a list of exceptions (probably from
     * {@link mondrian.olap.Schema#getWarnings()}) contains the expected
     * exception.
     *
     * <p>If the expected string contains the token "${pos}", it is replaced
     * with the range indicated by carets when the schema was created: see
     * {@link #setErrorLocation(String, int, int)}.
     *
     * @param exceptionList List of exceptions
     * @param expected Expected message
     * @param errorLoc Location of error
     */
    public void assertContains(
        List<Exception> exceptionList,
        String expected,
        String errorLoc)
    {
        final StringBuilder buf = new StringBuilder();
        final StringWriter sw = new StringWriter();
        final int posPos = expected.indexOf("${pos}");
        for (Exception exception : exceptionList) {
            RolapSchema.XmlLocation xmlLocation = null;
            if (exception instanceof RolapSchema.MondrianSchemaException) {
                final RolapSchema.MondrianSchemaException mse =
                    (RolapSchema.MondrianSchemaException) exception;
                xmlLocation = mse.getXmlLocation();
            }
            String expected2 = expected;
            if (posPos >= 0 && xmlLocation != null) {
                String pos = xmlLocation.toString();
                expected2 =
                    expected.substring(0, posPos)
                    + pos
                    + expected.substring(posPos + "${pos}".length());
            }
            final String message = exception.getMessage();
            if (message != null && message.matches(expected2)) {
                if (xmlLocation == null) {
                    Assert.fail(
                        "Actual message matched expected message, '"
                        + message
                        + "'; but we expected an error location and actual "
                        + "exception had no location");
                    return;
                }
                if (errorLoc == null && errorStart != -1) {
                    throw new AssertionFailedError(
                        "Test must specify expected error location. Either use "
                        + "carets (^) in the schema string, or specify the "
                        + "errorLoc parameter");
                }
                if (errorLoc != null) {
                    int errorStart = -1;
                    final String schema = getCatalogContent();
                    while ((errorStart =
                        schema.indexOf(errorLoc, errorStart + 1)) >= 0)
                    {
                        int errorEnd = errorStart + errorLoc.length();
                        sw.append(schema.substring(errorStart, errorEnd))
                            .append(", start=")
                            .append(String.valueOf(errorStart))
                            .append(", end=")
                            .append(String.valueOf(errorEnd))
                            .append(", range=")
                            .append(xmlLocation.getRange())
                            .append(nl);
                        if (xmlLocation.getRange().equals(
                                errorStart + "-" + errorEnd))
                        {
                            return;
                        }
                    }
                }
                if (errorStart != -1) {
                    if (xmlLocation.getRange().equals(
                            errorStart + "-" + errorEnd))
                    {
                        return;
                    }
                }
                throw new AssertionFailedError(
                    "Actual message matched expected, but actual error "
                    + "location (" + xmlLocation + ") did not match expected."
                    + (sw.getBuffer().length() > 0
                        ? " Other info: "
                        : "")
                    + sw);
            }
            if (buf.length() > 0) {
                buf.append(Util.nl);
            }
            buf.append(Util.getErrorMessage(exception));
        }
        throw new AssertionFailedError(
            "Exception list did not contain expected exception. Exception is:\n"
            + expected
            + "\nException list is:\n"
            + buf
            + "\nOther info:\n" + sw);
    }

    /**
     * Asserts that a list of exceptions (probably from
     * {@link mondrian.olap.Schema#getWarnings()}) contains the expected
     * exception.
     *
     * @param exceptionList List of exceptions
     * @param expected Expected message
     */
    protected void assertContains(
        List<Exception> exceptionList,
        String expected)
    {
        StringBuilder buf = new StringBuilder();
        for (Exception exception : exceptionList) {
            if (exception.getMessage().matches(expected)) {
                return;
            }
            if (buf.length() > 0) {
                buf.append(Util.nl);
            }
            buf.append(exception.getMessage());
        }
        Assert.fail(
            "Exception list did not contain expected exception '"
            + expected
            + "'. Exception list is:"
            + Util.nl
            + buf.toString());
    }

    public OlapConnection getOlap4jConnection() throws SQLException {
        try {
            Class.forName("mondrian.olap4j.MondrianOlap4jDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Driver not found");
        }
        String connectString = getConnectString();
        if (connectString.startsWith("Provider=mondrian; ")) {
            connectString =
                connectString.substring("Provider=mondrian; ".length());
        }
        final java.sql.Connection connection =
            java.sql.DriverManager.getConnection(
                "jdbc:mondrian:" + connectString);
        return ((OlapWrapper) connection).unwrap(OlapConnection.class);
    }

    /**
     * Tests whether the database is valid. Allows tests that depend on optional
     * databases to figure out whether to proceed.
     *
     * @return whether a database is present and correct
     */
    public boolean databaseIsValid() {
        try {
            Connection connection = getConnection();
            String cubeName = getDefaultCubeName();
            if (cubeName.indexOf(' ') >= 0) {
                cubeName = Util.quoteMdxIdentifier(cubeName);
            }
            Query query = connection.parseQuery("select from " + cubeName);
            Result result = connection.execute(query);
            Util.discard(result);
            connection.close();
            return true;
        } catch (RuntimeException e) {
            Util.discard(e);
            return false;
        }
    }

    public static String hierarchyName(String dimension, String hierarchy) {
        return MondrianProperties.instance().SsasCompatibleNaming.get()
            ? "[" + dimension + "].[" + hierarchy + "]"
            : (hierarchy.equals(dimension)
                ? "[" + dimension + "]"
                : "[" + dimension + "." + hierarchy + "]");
    }

    public static String levelName(
        String dimension, String hierarchy, String level)
    {
        return hierarchyName(dimension, hierarchy) + ".[" + level + "]";
    }

    /**
     * Returns count copies of a string. Format strings within string are
     * substituted, per {@link java.lang.String#format}.
     *
     * @param count Number of copies
     * @param format String template
     * @return Multiple copies of a string
     */
    public static String repeatString(
        final int count,
        String format)
    {
        final Formatter formatter = new Formatter();
        for (int i = 0; i < count; i++) {
            formatter.format(format, i);
        }
        return formatter.toString();
    }

    //~ Inner classes ----------------------------------------------------------

    public static class SnoopingSchemaProcessor
        extends FilterDynamicSchemaProcessor
    {
        public static final ThreadLocal<String> THREAD_RESULT =
            new ThreadLocal<String>();

        protected String filter(
            String schemaUrl,
            Util.PropertyList connectInfo,
            InputStream stream) throws Exception
        {
            String catalogContent =
                super.filter(schemaUrl, connectInfo, stream);
            THREAD_RESULT.set(catalogContent);
            return catalogContent;
        }
    }

    /**
     * Schema processor that flags dimensions as high-cardinality if they
     * appear in the list of values in the
     * {@link MondrianProperties#TestHighCardinalityDimensionList} property.
     * It's a convenient way to run the whole suite against high-cardinality
     * dimensions without modifying FoodMart.xml.
     */
    public static class HighCardDynamicSchemaProcessor
        extends FilterDynamicSchemaProcessor
    {
        protected String filter(
            String schemaUrl, Util.PropertyList connectInfo, InputStream stream)
            throws Exception
        {
            String s = super.filter(schemaUrl, connectInfo, stream);
            final String highCardDimensionList =
                MondrianProperties.instance()
                    .TestHighCardinalityDimensionList.get();
            if (highCardDimensionList != null
                && !highCardDimensionList.equals(""))
            {
                for (String dimension : highCardDimensionList.split(",")) {
                    final String match =
                        "<Dimension name=\"" + dimension + "\"";
                    s = s.replaceAll(
                        match, match + " highCardinality=\"true\"");
                }
            }
            return s;
        }
    }

    enum DataSet {
        FOODMART,
        STEELWHEELS,
        LEGACY_FOODMART
    }

    /**
     * Map backed by a hash map of weak references. When a ref is garbage
     * collected, the entry is a candidate for removal from the map. Operations
     * such as put, get, and iterator remove dead entries when they see them.
     *
     * <p>Unlike {@link WeakHashMap}, this map can be used for keys that can
     * be re-created. Such as strings.</p>
     *
     * @param <K> Key type
     * @param <V> Value type
     */
    private static class WeakMap<K, V> implements Map<K, V> {
        private final Map<K, WeakReference<V>> map =
            new HashMap<K, WeakReference<V>>();

        public int size() {
            return map.size();
        }

        public boolean isEmpty() {
            return map.isEmpty();
        }

        public boolean containsKey(Object key) {
            return map.containsKey(key);
        }

        public boolean containsValue(Object value) {
            for (WeakReference<V> ref : map.values()) {
                final V v = ref.get();
                if (v != null && v.equals(value)) {
                    return true;
                }
            }
            return false;
        }

        public V get(Object key) {
            final WeakReference<V> ref = map.get(key);
            if (ref == null) {
                return null;
            }
            final V v = ref.get();
            if (v == null) {
                //noinspection SuspiciousMethodCalls
                map.remove(key);
            }
            return v;
        }

        public V put(K key, V value) {
            final WeakReference<V> ref =
                map.put(key, new WeakReference<V>(value));
            return ref == null ? null : ref.get();
        }

        public V remove(Object key) {
            final WeakReference<V> ref = map.remove(key);
            return ref == null ? null : ref.get();
        }

        public void putAll(Map<? extends K, ? extends V> m) {
            for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
                map.put(entry.getKey(), new WeakReference<V>(entry.getValue()));
            }
        }

        public void clear() {
            map.clear();
        }

        public Set<K> keySet() {
            return map.keySet();
        }

        public Collection<V> values() {
            return new AbstractSet<V>() {
                public Iterator<V> iterator() {
                    return Util.GcIterator.over(map.values()).iterator();
                }

                public int size() {
                    return map.size();
                }
            };
        }

        public Set<Entry<K, V>> entrySet() {
            return new AbstractSet<Entry<K, V>>() {
                @Override
                public Iterator<Entry<K, V>> iterator() {
                    // WARNING: An iteration may return fewer than size()
                    // entries due to the invisible hand of the garbage
                    // collector removing them. Normally collections are
                    // safe in a single-threaded scenario, but not this one.
                    return new GcEntryIterator<K, V>(map.entrySet().iterator());
                }

                @Override
                public int size() {
                    return map.size();
                }
            };
        }
    }

    /**
     * Iterator over a collection of entries that removes entries whose value
     * is null.
     *
     * @see Util.GcIterator
     *
     * @param <K> Key type
     * @param <V> Value type
     */
    static class GcEntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {
        private final Iterator<Map.Entry<K, WeakReference<V>>> iterator;
        private boolean hasNext;
        private Map.Entry<K, V> next;

        public GcEntryIterator(
            Iterator<Map.Entry<K, WeakReference<V>>> iterator)
        {
            this.iterator = iterator;
            this.hasNext = true;
            moveToNext();
        }

        private void moveToNext() {
            while (iterator.hasNext()) {
                final Map.Entry<K, WeakReference<V>> ref = iterator.next();
                V value = ref.getValue().get();
                if (value != null) {
                    next = new Pair<K, V>(ref.getKey(), value);
                    return;
                }
                iterator.remove();
            }
            hasNext = false;
        }

        public boolean hasNext() {
            return hasNext;
        }

        public Map.Entry<K, V> next() {
            final Map.Entry<K, V> next1 = next;
            moveToNext();
            return next1;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

// End TestContext.java
