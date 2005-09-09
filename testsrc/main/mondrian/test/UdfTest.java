/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;


import mondrian.example.InverseNormalFunDef;
import mondrian.example.LastNonEmptyUdf;
import mondrian.olap.*;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;

import java.util.regex.Pattern;

/**
 * Unit-test for {@link UserDefinedFunction user-defined functions}.
 *
 * <p>TODO:
 * 1. test that function which does not return a name, description etc.
 *    gets a sensible error
 * 2. document UDFs
 *
 * @author jhyde
 * @since Apr 29, 2005
 * @version $Id$
 **/
public class UdfTest extends FoodMartTestCase {

    /**
     * Test context which uses the local FoodMart schema.
     */
    private final TestContext tc = new TestContext() {
        public synchronized Connection getFoodMartConnection(boolean fresh) {
            return getFoodMartConnection(FoodmartWithUdf.class.getName());
        }
    };

    public TestContext getTestContext() {
        return tc;
    }

    public void testSanity() {
        // sanity check, make sure the schema is loading correctly
        assertQueryReturns(
                "SELECT {[Measures].[Store Sqft]} ON COLUMNS, {[Store Type]} ON ROWS FROM [Store]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Store Sqft]}" + nl +
                "Axis #2:" + nl +
                "{[Store Type].[All Store Types]}" + nl +
                "Row #0: 571,596" + nl);
    }

    public void testFun() {
        assertQueryReturns(
                "WITH MEMBER [Measures].[Sqft Plus One] AS 'PlusOne([Measures].[Store Sqft])'" + nl +
                "SELECT {[Measures].[Sqft Plus One]} ON COLUMNS, " + nl +
                "  {[Store Type].children} ON ROWS " + nl +
                "FROM [Store]",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Sqft Plus One]}" + nl +
                "Axis #2:" + nl +
                "{[Store Type].[All Store Types].[Deluxe Supermarket]}" + nl +
                "{[Store Type].[All Store Types].[Gourmet Supermarket]}" + nl +
                "{[Store Type].[All Store Types].[HeadQuarters]}" + nl +
                "{[Store Type].[All Store Types].[Mid-Size Grocery]}" + nl +
                "{[Store Type].[All Store Types].[Small Grocery]}" + nl +
                "{[Store Type].[All Store Types].[Supermarket]}" + nl +
                "Row #0: 146,046" + nl +
                "Row #1: 47,448" + nl +
                "Row #2: (null)" + nl +
                "Row #3: 109,344" + nl +
                "Row #4: 75,282" + nl +
                "Row #5: 193,481" + nl);
    }

    public void testLastNonEmpty() {
        assertQueryReturns(
                "WITH MEMBER [Measures].[Last Unit Sales] AS " + nl +
                " '([Measures].[Unit Sales], " + nl +
                "   LastNonEmpty(Descendants([Time]), [Measures].[Unit Sales]))'" + nl +
                "SELECT {[Measures].[Last Unit Sales]} ON COLUMNS," + nl +
                " CrossJoin(" + nl +
                "  {[Time].[1997], [Time].[1997].[Q1], [Time].[1997].[Q1].Children}," + nl +
                "  {[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].children} ) ON ROWS" + nl +
                "FROM [Sales]" + nl +
                "WHERE ( [Store].[All Stores].[USA].[OR].[Portland].[Store 11] )",
                "Axis #0:" + nl +
                "{[Store].[All Stores].[USA].[OR].[Portland].[Store 11]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Last Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Time].[1997], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}" + nl +
                "{[Time].[1997], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}" + nl +
                "{[Time].[1997], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}" + nl +
                "{[Time].[1997], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}" + nl +
                "{[Time].[1997], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}" + nl +
                "{[Time].[1997].[Q1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}" + nl +
                "{[Time].[1997].[Q1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}" + nl +
                "{[Time].[1997].[Q1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}" + nl +
                "{[Time].[1997].[Q1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}" + nl +
                "{[Time].[1997].[Q1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}" + nl +
                "{[Time].[1997].[Q1].[1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}" + nl +
                "{[Time].[1997].[Q1].[1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}" + nl +
                "{[Time].[1997].[Q1].[1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}" + nl +
                "{[Time].[1997].[Q1].[1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}" + nl +
                "{[Time].[1997].[Q1].[1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}" + nl +
                "{[Time].[1997].[Q1].[2], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}" + nl +
                "{[Time].[1997].[Q1].[2], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}" + nl +
                "{[Time].[1997].[Q1].[2], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}" + nl +
                "{[Time].[1997].[Q1].[2], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}" + nl +
                "{[Time].[1997].[Q1].[2], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}" + nl +
                "{[Time].[1997].[Q1].[3], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}" + nl +
                "{[Time].[1997].[Q1].[3], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}" + nl +
                "{[Time].[1997].[Q1].[3], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}" + nl +
                "{[Time].[1997].[Q1].[3], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}" + nl +
                "{[Time].[1997].[Q1].[3], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}" + nl +
                "Row #0: 2" + nl +
                "Row #1: 7" + nl +
                "Row #2: 6" + nl +
                "Row #3: 7" + nl +
                "Row #4: 4" + nl +
                "Row #5: 3" + nl +
                "Row #6: 4" + nl +
                "Row #7: 3" + nl +
                "Row #8: 4" + nl +
                "Row #9: 2" + nl +
                "Row #10: (null)" + nl +
                "Row #11: 4" + nl +
                "Row #12: (null)" + nl +
                "Row #13: 2" + nl +
                "Row #14: (null)" + nl +
                "Row #15: (null)" + nl +
                "Row #16: 2" + nl +
                "Row #17: (null)" + nl +
                "Row #18: 4" + nl +
                "Row #19: (null)" + nl +
                "Row #20: 3" + nl +
                "Row #21: 4" + nl +
                "Row #22: 3" + nl +
                "Row #23: 4" + nl +
                "Row #24: 2" + nl);
    }

    public void testBadFun() {
        final TestContext tc = new TestContext() {
                    public synchronized Connection getFoodMartConnection(boolean fresh) {
                        return getFoodMartConnection(FoodmartWithBadUdf.class.getName());
                    }
            };
        try {
            tc.executeQuery("SELECT {} ON COLUMNS FROM [Sales]");
            fail("Expected exception");
        } catch (Exception e) {
            final String s = e.getMessage();
            assertEquals("Mondrian Error:Internal error: Invalid " +
                    "user-defined function 'BadPlusOne': return type is null",
                    s);
        }

    }

    public void testComplexFun() {
        assertQueryReturns(
                "WITH MEMBER [Measures].[InverseNormal] AS 'InverseNormal([Measures].[Grocery Sqft] / [Measures].[Store Sqft])', FORMAT_STRING = \"0.000\"" + nl +
                "SELECT {[Measures].[InverseNormal]} ON COLUMNS, " + nl +
                "  {[Store Type].children} ON ROWS " + nl +
                "FROM [Store]",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[InverseNormal]}" + nl +
                "Axis #2:" + nl +
                "{[Store Type].[All Store Types].[Deluxe Supermarket]}" + nl +
                "{[Store Type].[All Store Types].[Gourmet Supermarket]}" + nl +
                "{[Store Type].[All Store Types].[HeadQuarters]}" + nl +
                "{[Store Type].[All Store Types].[Mid-Size Grocery]}" + nl +
                "{[Store Type].[All Store Types].[Small Grocery]}" + nl +
                "{[Store Type].[All Store Types].[Supermarket]}" + nl +
                "Row #0: 0.467" + nl +
                "Row #1: 0.463" + nl +
                "Row #2: (null)" + nl +
                "Row #3: 0.625" + nl +
                "Row #4: 0.521" + nl +
                "Row #5: 0.504" + nl);
    }

    public void testException() {
        Result result = executeQuery("WITH MEMBER [Measures].[InverseNormal] " +
                        " AS 'InverseNormal([Measures].[Store Sqft] / [Measures].[Grocery Sqft])'," +
                        " FORMAT_STRING = \"0.000000\"" + nl +
                        "SELECT {[Measures].[InverseNormal]} ON COLUMNS, " + nl +
                        "  {[Store Type].children} ON ROWS " + nl +
                        "FROM [Store]");
        Axis rowAxis = result.getAxes()[0];
        assertTrue(rowAxis.positions.length == 1);
        Axis colAxis = result.getAxes()[1];
        assertTrue(colAxis.positions.length == 6);
        Cell cell = result.getCell(new int[] {0, 0});
        assertTrue(cell.isError());
        getTestContext().assertMatchesVerbose(
                Pattern.compile(".*Invalid value for inverse normal distribution: 1.4708.*"),
                cell.getValue().toString());
        cell = result.getCell(new int[] {0, 5});
        assertTrue(cell.isError());
        getTestContext().assertMatchesVerbose(
                Pattern.compile(".*Invalid value for inverse normal distribution: 1.4435.*"),
                cell.getValue().toString());
    }

    /**
     * Dynamic schema which adds two user-defined functions to any given
     * schema.
     */
    public static class FoodmartWithUdf extends DecoratingSchemaProcessor {
        protected String filterSchema(String s) {
            return Util.replace(
                    s,
                    "</Schema>",
                    "<UserDefinedFunction name=\"PlusOne\" className=\"" +
                    PlusOneUdf.class.getName() + "\"/>" + nl +
                    "<UserDefinedFunction name=\"LastNonEmpty\" className=\"" +
                    LastNonEmptyUdf.class.getName() + "\"/>" + nl +
                    "<UserDefinedFunction name=\"InverseNormal\" className=\"" +
                    InverseNormalFunDef.class.getName() + "\"/>" + nl +
                    "</Schema>");
        }
    }

    /**
     * Dynamic schema which adds the {@link BadPlusOneUdf} user-defined
     * function to any given schema.
     */
    public static class FoodmartWithBadUdf extends DecoratingSchemaProcessor {
        protected String filterSchema(String s) {
            return Util.replace(
                    s,
                    "</Schema>",
                    "<UserDefinedFunction name=\"BadPlusOne\" className=\"" +
                    BadPlusOneUdf.class.getName() + "\"/>" + nl +
                    "</Schema>");
        }
    }

    /**
     * A simple user-defined function which adds one to its argument.
     */
    public static class PlusOneUdf implements UserDefinedFunction {
        public String getName() {
            return "PlusOne";
        }

        public String getDescription() {
            return "Returns its argument plus one";
        }

        public Syntax getSyntax() {
            return Syntax.Function;
        }

        public Type getReturnType(Type[] parameterTypes) {
            return new NumericType();
        }

        public Type[] getParameterTypes() {
            return new Type[] {new NumericType()};
        }

        public Object execute(Evaluator evaluator, Exp[] arguments) {
            final Object argValue = arguments[0].evaluateScalar(evaluator);
            if (argValue instanceof Number) {
                return new Double(((Number) argValue).doubleValue() + 1);
            } else {
                // Argument might be a RuntimeException indicating that
                // the cache does not yet have the required cell value. The
                // function will be called again when the cache is loaded.
                return null;
            }
        }

        public String[] getReservedWords() {
            return null;
        }
    }

    /**
     * A simple user-defined function which adds one to its argument.
     */
    public static class BadPlusOneUdf extends PlusOneUdf {
        private final String name;

        public BadPlusOneUdf(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public Type getReturnType(Type[] parameterTypes) {
            // Will cause error.
            return null;
        }
    }
}

// UdfTest.java
