/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.calc.TupleList;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.olap.*;

import junit.framework.Assert;
import junit.framework.TestCase;

import java.util.*;

/**
 * <code>FoodMartTestCase</code> is a unit test which runs against the FoodMart
 * database.
 *
 * @author jhyde
 * @since 29 March, 2002
 */
public class FoodMartTestCase extends TestCase {

    /**
     * Access properties via this object and their values will be reset on
     * {@link #tearDown()}.
     */
    protected final PropertySaver propSaver = new PropertySaver();

    public FoodMartTestCase(String name) {
        super(name);
    }

    public FoodMartTestCase() {
    }

    protected void tearDown() throws Exception {
        // revert any properties that have been set during this test
        propSaver.reset();
    }

    /**
     * Returns the test context. Override this method if you wish to use a
     * different source for your FoodMart connection.
     */
    public TestContext getTestContext() {
        return TestContext.instance();
    }

    protected Connection getConnection() {
        return getTestContext().getConnection();
    }

    /**
     * Runs a query, and asserts that the result has a given number of columns
     * and rows.
     */
    protected void assertSize(
        String queryString,
        int columnCount,
        int rowCount)
    {
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
    public void assertQueryThrows(String queryString, String pattern) {
        getTestContext().assertQueryThrows(queryString, pattern);
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
     * is evaluated against the Sales cube.
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
        final String iifExpression =
            "Iif (" + expression + ",\"true\",\"false\")";
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
     * Executes query1 and query2 and Compares the obtained measure values.
     */
    protected void assertQueriesReturnSimilarResults(
        String query1,
        String query2,
        TestContext testContext)
    {
        String resultString1 =
            TestContext.toString(testContext.executeQuery(query1));
        String resultString2 =
            TestContext.toString(testContext.executeQuery(query2));
        assertEquals(
            measureValues(resultString1),
            measureValues(resultString2));
    }

    /**
     * Truncates the query result to return only measure values.
     */
    private static String measureValues(String resultString) {
        int index = resultString.indexOf("}");
        return index != -1 ? resultString.substring(index) : resultString;
    }

    protected boolean isGroupingSetsSupported() {
        return MondrianProperties.instance().EnableGroupingSets.get()
            && getTestContext().getDialect().supportsGroupingSets();
    }

    protected boolean isDefaultNullMemberRepresentation() {
        return MondrianProperties.instance().NullMemberRepresentation.get()
                .equals("#null");
    }

    protected Member member(
        List<Id.Segment> segmentList,
        SchemaReader salesCubeSchemaReader)
    {
        return salesCubeSchemaReader.getMemberByUniqueName(segmentList, true);
    }

    protected TupleList storeMembersCAAndOR(
        SchemaReader salesCubeSchemaReader)
    {
        return new UnaryTupleList(Arrays.asList(
            member(
                Id.Segment.toList(
                    "Store", "All Stores", "USA", "CA", "Alameda"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Store", "All Stores", "USA", "CA", "Alameda", "HQ"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Store", "All Stores", "USA", "CA", "Beverly Hills"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Store", "All Stores", "USA", "CA", "Beverly Hills",
                    "Store 6"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Store", "All Stores", "USA", "CA", "Los Angeles"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Store", "All Stores", "USA", "OR", "Portland"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Store", "All Stores", "USA", "OR", "Portland", "Store 11"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Store", "All Stores", "USA", "OR", "Salem"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Store", "All Stores", "USA", "OR", "Salem", "Store 13"),
                salesCubeSchemaReader)));
    }

    protected TupleList productMembersPotScrubbersPotsAndPans(
        SchemaReader salesCubeSchemaReader)
    {
        return new UnaryTupleList(Arrays.asList(
            member(
                Id.Segment.toList(
                    "Product", "All Products", "Non-Consumable", "Household",
                    "Kitchen Products", "Pot Scrubbers", "Cormorant"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Product", "All Products", "Non-Consumable", "Household",
                    "Kitchen Products", "Pot Scrubbers", "Denny"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Product", "All Products", "Non-Consumable", "Household",
                    "Kitchen Products", "Pot Scrubbers", "Red Wing"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Product", "All Products", "Non-Consumable", "Household",
                    "Kitchen Products", "Pots and Pans", "Cormorant"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Product", "All Products", "Non-Consumable", "Household",
                    "Kitchen Products", "Pots and Pans", "Denny"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Product", "All Products", "Non-Consumable", "Household",
                    "Kitchen Products", "Pots and Pans", "High Quality"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Product", "All Products", "Non-Consumable", "Household",
                    "Kitchen Products", "Pots and Pans", "Red Wing"),
                salesCubeSchemaReader),
            member(
                Id.Segment.toList(
                    "Product", "All Products", "Non-Consumable", "Household",
                    "Kitchen Products", "Pots and Pans", "Sunset"),
                salesCubeSchemaReader)));
    }

    protected TupleList genderMembersIncludingAll(
        boolean includeAllMember,
        SchemaReader salesCubeSchemaReader,
        Cube salesCube)
    {
        Member maleMember =
            member(
                Id.Segment.toList("Gender", "All Gender", "M"),
                salesCubeSchemaReader);
        Member femaleMember =
            member(
                Id.Segment.toList("Gender", "All Gender", "F"),
                salesCubeSchemaReader);
        Member [] members;
        if (includeAllMember) {
            members = new Member[] {
                allMember("Customer", "Gender", salesCube),
                maleMember,
                femaleMember
            };
        } else {
            members = new Member[] {maleMember, femaleMember};
        }
        return new UnaryTupleList(Arrays.asList(members));
    }

    protected Member allMember(
        String dimensionName, String hierarchyName, Cube cube)
    {
        return getHierarchy(cube, dimensionName, hierarchyName).getAllMember();
    }

    protected Hierarchy getHierarchy(
        Cube cube,
        String dimensionName,
        String hierarchyName)
    {
        for (Dimension dimension : cube.getDimensionList()) {
            if (dimension.getName().equals(dimensionName)) {
                for (Hierarchy hierarchy : dimension.getHierarchyList()) {
                    if (hierarchy.getName().equals(hierarchyName)) {
                        return hierarchy;
                    }
                }
            }
        }
        return null;
    }

    protected List<Member> warehouseMembersCanadaMexicoUsa(SchemaReader reader)
    {
        return Arrays.asList(
            member(Id.Segment.toList(
                "Warehouse", "All Warehouses", "Canada"), reader),
            member(Id.Segment.toList(
                "Warehouse", "All Warehouses", "Mexico"), reader),
            member(Id.Segment.toList(
                "Warehouse", "All Warehouses", "USA"), reader));
    }

    protected Cube cubeByName(Connection connection, String cubeName) {
        SchemaReader reader = connection.getSchemaReader().withLocus();

        Cube[] cubes = reader.getCubes();
        return cubeByName(cubeName, cubes);
    }

    private Cube cubeByName(String cubeName, Cube[] cubes) {
        Cube resultCube = null;
        for (Cube cube : cubes) {
            if (cubeName.equals(cube.getName())) {
                resultCube = cube;
                break;
            }
        }
        return resultCube;
    }

    protected TupleList storeMembersUsaAndCanada(
        boolean includeAllMember,
        SchemaReader salesCubeSchemaReader,
        Cube salesCube)
    {
        Member usaMember =
            member(
                Id.Segment.toList("Store", "All Stores", "USA"),
                salesCubeSchemaReader);
        Member canadaMember =
            member(
                Id.Segment.toList("Store", "All Stores", "CANADA"),
                salesCubeSchemaReader);
        Member [] members;
        if (includeAllMember) {
            members = new Member[] {
                allMember("Store", "Stores", salesCube),
                usaMember,
                canadaMember
            };
        } else {
            members = new Member[] {usaMember, canadaMember};
        }
        return new UnaryTupleList(Arrays.asList(members));
    }

    static class QueryAndResult {
        final String query;
        final String result;

        QueryAndResult(String query, String result) {
            this.query = query;
            this.result = result;
        }
    }

    /**
     * Checks whether query produces the same results with the native.* props
     * enabled as it does with the props disabled
     * @param query query to run
     * @param message Message to output on test failure
     * @param context test context to use
     */
    public void verifySameNativeAndNot(
        String query, String message, TestContext context)
    {
        propSaver.set(propSaver.props.EnableNativeCrossJoin, true);
        propSaver.set(propSaver.props.EnableNativeFilter, true);
        propSaver.set(propSaver.props.EnableNativeNonEmpty, true);
        propSaver.set(propSaver.props.EnableNativeTopCount, true);

        Result resultNative = context.executeQuery(query);

        propSaver.set(propSaver.props.EnableNativeCrossJoin, false);
        propSaver.set(propSaver.props.EnableNativeFilter, false);
        propSaver.set(propSaver.props.EnableNativeNonEmpty, false);
        propSaver.set(propSaver.props.EnableNativeTopCount, true);

        Result resultNonNative = context.executeQuery(query);

        assertEquals(
            message,
            TestContext.toString(resultNative),
            TestContext.toString(resultNonNative));

        propSaver.reset();
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
        BasicQueryTest testCase,
        long timeoutMs,
        int threadCount,
        ChooseRunnable chooseRunnable)
    {
        this.testCase = testCase;
        this.timeoutMs = timeoutMs;
        this.threads = new Thread[threadCount];
        this.chooseRunnable = chooseRunnable;
    }

    public void run() {
        ThreadGroup threadGroup = null;
        for (int i = 0; i < threads.length; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(threadGroup, "thread #" + threadIndex) {
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
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            try {
                thread.join(timeoutMs);
            } catch (InterruptedException e) {
                failures.add(
                    Util.newInternal(
                        e, "Interrupted after " + timeoutMs + "ms"));
                break;
            }
        }
        if (failures.size() > 0) {
            for (Throwable throwable : failures) {
                throwable.printStackTrace();
            }
            TestCase.fail(failures.size() + " threads failed");
        }
    }
}

// End FoodMartTestCase.java
