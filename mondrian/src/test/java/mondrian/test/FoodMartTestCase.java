/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
//
// jhyde, 29 March, 2002
*/
package mondrian.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;
import mondrian.calc.TupleList;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.olap.Axis;
import mondrian.olap.Connection;
import mondrian.olap.Cube;
import mondrian.olap.Dimension;
import mondrian.olap.Id;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.SchemaReader;

/**
 * <code>FoodMartTestCase</code> is a unit test which runs against the FoodMart
 * database.
 *
 * @author jhyde
 * @since 29 March, 2002
 */
public class FoodMartTestCase {



    /**
     * Returns the test context. Override this method if you wish to use a
     * different source for your FoodMart connection.
     */
    public static FoodmartTestContextImpl getTestContext() {
        return FoodmartTestContextImpl.instance();
    }

    protected static Connection getConnection() {
        return getTestContext().getConnection();
    }

    /**
     * Runs a query, and asserts that the result has a given number of columns
     * and rows.
     */
    protected static void assertSize(
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
    public static void assertQueryThrows(String queryString, String pattern) {
        getTestContext().assertQueryThrows(queryString, pattern);
    }

    /**
     * Executes a query in a given connection.
     */
    public static Result execute(Connection connection, String queryString) {
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
    public static  Member executeSingletonAxis(String expression) {
        return getTestContext().executeSingletonAxis(expression);
    }

    /**
     * Runs a query and checks that the result is a given string.
     */
    public static void assertQueryReturns(String query, String desiredResult) {
        getTestContext().assertQueryReturns(query, desiredResult);
    }

    /**
     * Runs a query.
     */
    public static Result executeQuery(String queryString) {
        return getTestContext().executeQuery(queryString);
    }

    /**
     * Runs a query with a given expression on an axis, and asserts that it
     * throws an error which matches a particular pattern. The expression
     * is evaulated against the Sales cube.
     */
    public static void assertAxisThrows(String expression, String pattern) {
        getTestContext().assertAxisThrows(expression, pattern);
    }

    /**
     * Runs a query on the "Sales" cube with a given expression on an axis, and
     * asserts that it returns the expected string.
     */
    public static void assertAxisReturns(String expression, String expected) {
        getTestContext().assertAxisReturns(expression, expected);
    }

    /**
     * Executes an expression against the Sales cube in the FoodMart database
     * to form a single cell result set, then returns that cell's formatted
     * value.
     */
    public static String executeExpr(String expression) {
        return getTestContext().executeExprRaw(expression).getFormattedValue();
    }

    /**
     * Executes an expression which yields a boolean result, and asserts that
     * the result is the expected one.
     */
    public static void assertBooleanExprReturns(String expression, boolean expected) {
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
    public static void assertExprThrows(String expression, String pattern) {
        getTestContext().assertExprThrows(expression, pattern);
    }

    /**
     * Runs an expression and asserts that it returns a given result.
     */
    public static void assertExprReturns(String expression, String expected) {
        getTestContext().assertExprReturns(expression, expected);
    }

    /**
     * Executes query1 and query2 and Compares the obtained measure values.
     */
    protected static void assertQueriesReturnSimilarResults(
        String query1,
        String query2,
        TestContext testContext)
    {
        String resultString1 =
            FoodmartTestContextImpl.toString(testContext.executeQuery(query1));
        String resultString2 =
            FoodmartTestContextImpl.toString(testContext.executeQuery(query2));
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

    protected  static boolean isGroupingSetsSupported() {
        return MondrianProperties.instance().EnableGroupingSets.get()
            && getTestContext().getDialect().supportsGroupingSets();
    }

    protected static boolean isDefaultNullMemberRepresentation() {
        return MondrianProperties.instance().NullMemberRepresentation.get()
                .equals("#null");
    }

    protected static Member member(
        List<Id.Segment> segmentList,
        SchemaReader salesCubeSchemaReader)
    {
        return salesCubeSchemaReader.getMemberByUniqueName(segmentList, true);
    }

    protected static TupleList storeMembersCAAndOR(
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

    protected static TupleList productMembersPotScrubbersPotsAndPans(
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

    protected static TupleList genderMembersIncludingAll(
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
                allMember("Gender", salesCube),
                maleMember,
                femaleMember};
        } else {
            members = new Member[] {maleMember, femaleMember};
        }
        return new UnaryTupleList(Arrays.asList(members));
    }

    protected static Member allMember(String dimensionName, Cube salesCube) {
        Dimension genderDimension = getDimension(dimensionName, salesCube);
        return genderDimension.getHierarchy().getAllMember();
    }

    private static Dimension getDimension(String dimensionName, Cube salesCube) {
        return getDimensionWithName(dimensionName, salesCube.getDimensions());
    }

    protected static Dimension getDimensionWithName(
        String name,
        Dimension[] dimensions)
    {
        Dimension resultDimension = null;
        for (Dimension dimension : dimensions) {
            if (dimension.getName().equals(name)) {
                resultDimension = dimension;
                break;
            }
        }
        return resultDimension;
    }

    protected static List<Member> warehouseMembersCanadaMexicoUsa(SchemaReader reader)
    {
        return Arrays.asList(
            member(Id.Segment.toList(
                "Warehouse", "All Warehouses", "Canada"), reader),
            member(Id.Segment.toList(
                "Warehouse", "All Warehouses", "Mexico"), reader),
            member(Id.Segment.toList(
                "Warehouse", "All Warehouses", "USA"), reader));
    }

    protected static Cube cubeByName(Connection connection, String cubeName) {
        SchemaReader reader = connection.getSchemaReader().withLocus();

        Cube[] cubes = reader.getCubes();
        return cubeByName(cubeName, cubes);
    }

    private static Cube cubeByName(String cubeName, Cube[] cubes) {
        Cube resultCube = null;
        for (Cube cube : cubes) {
            if (cubeName.equals(cube.getName())) {
                resultCube = cube;
                break;
            }
        }
        return resultCube;
    }

    protected static  TupleList storeMembersUsaAndCanada(
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
            members = new Member[]{
                allMember("Store", salesCube), usaMember, canadaMember};
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
    public static void verifySameNativeAndNot(
        String query, String message, TestContext context)
    {
	PropertySaver5 propSaver=new PropertySaver5();

        propSaver.set(propSaver.properties.EnableNativeCrossJoin, true);
        propSaver.set(propSaver.properties.EnableNativeFilter, true);
        propSaver.set(propSaver.properties.EnableNativeNonEmpty, true);
        propSaver.set(propSaver.properties.EnableNativeTopCount, true);

        Result resultNative = context.executeQuery(query);

        propSaver.set(propSaver.properties.EnableNativeCrossJoin, false);
        propSaver.set(propSaver.properties.EnableNativeFilter, false);
        propSaver.set(propSaver.properties.EnableNativeNonEmpty, false);
        propSaver.set(propSaver.properties.EnableNativeTopCount, false);

        Result resultNonNative = context.executeQuery(query);

        assertEquals(
            FoodmartTestContextImpl.toString(resultNative),
            FoodmartTestContextImpl.toString(resultNonNative),
            message);

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


// End FoodMartTestCase.java
