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

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.NumericType;
import mondrian.rolap.DynamicSchemaProcessor;
import mondrian.spi.UserDefinedFunction;

import java.net.URL;

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

    public static final String NL = FoodMartTestCase.nl;
    /**
     * Test context which uses the local FoodMart schema.
     */
    private final TestContext tc = new TestContext() {
        public synchronized Connection getFoodMartConnection(boolean fresh) {
            return getFoodMartConnection(FoodmartWithUdf.class.getName());
        }
    };

    protected TestContext getTestContext() {
        return tc;
    }

    public void testSanity() {
        // sanity check, make sure the schema is loading correctly
        runQueryCheckResult(
                "SELECT {[Measures].[Store Sqft]} ON COLUMNS, {[Store Type]} ON ROWS FROM [Store]",
                "Axis #0:" + NL +
                "{}" + NL +
                "Axis #1:" + NL +
                "{[Measures].[Store Sqft]}" + NL +
                "Axis #2:" + NL +
                "{[Store Type].[All Store Types]}" + NL +
                "Row #0: 571,596" + NL);
    }

    public void testFun() {
        runQueryCheckResult(
                "WITH MEMBER [Measures].[Sqft Plus One] AS 'PlusOne([Measures].[Store Sqft])'" + NL +
                "SELECT {[Measures].[Sqft Plus One]} ON COLUMNS, " + NL +
                "  {[Store Type].children} ON ROWS " + NL +
                "FROM [Store]",

                "Axis #0:" + NL +
                "{}" + NL +
                "Axis #1:" + NL +
                "{[Measures].[Sqft Plus One]}" + NL +
                "Axis #2:" + NL +
                "{[Store Type].[All Store Types].[Deluxe Supermarket]}" + NL +
                "{[Store Type].[All Store Types].[Gourmet Supermarket]}" + NL +
                "{[Store Type].[All Store Types].[HeadQuarters]}" + NL +
                "{[Store Type].[All Store Types].[Mid-Size Grocery]}" + NL +
                "{[Store Type].[All Store Types].[Small Grocery]}" + NL +
                "{[Store Type].[All Store Types].[Supermarket]}" + NL +
                "Row #0: 146,046" + NL +
                "Row #1: 47,448" + NL +
                "Row #2: (null)" + NL +
                "Row #3: 109,344" + NL +
                "Row #4: 75,282" + NL +
                "Row #5: 193,481" + NL);
    }

    /**
     * Dynamic schema which contains a single cube and a single user-defined
     * function.
     */
    public static class FoodmartWithUdf implements DynamicSchemaProcessor {
        private static final String schema =
                "<?xml version=\"1.0\"?>" + NL +
                "<Schema name=\"FoodMartWithUdfs\">" + NL +
                "<Cube name=\"Store\">" + NL +
                "  <Table name=\"store\"/>" + NL +
                "  <Dimension name=\"Store Type\">" + NL +
                "    <Hierarchy hasAll=\"true\">" + NL +
                "      <Level name=\"Store Type\" column=\"store_type\" uniqueMembers=\"true\"/>" + NL +
                "    </Hierarchy>" + NL +
                "  </Dimension>" + NL +
                "  <Measure name=\"Store Sqft\" column=\"store_sqft\" aggregator=\"sum\"" + NL +
                "      formatString=\"#,###\"/>" + NL +
                "  <Measure name=\"Grocery Sqft\" column=\"grocery_sqft\" aggregator=\"sum\"" + NL +
                "      formatString=\"#,###\"/>" + NL +
                "</Cube>" + NL +
                "<UserDefinedFunction name=\"PlusOne\" className=\"" + PlusOneFunDef.class.getName() + "\"/>" + NL +
                "</Schema>";

        public String processSchema(URL schemaUrl) throws Exception {
            return schema;
        }
    }

    /**
     * A simple user-defined function which adds one to its argument.
     */
    public static class PlusOneFunDef implements UserDefinedFunction {
        public String getName() {
            return "PlusOne";
        }

        public String getDescription() {
            return "Returns its argument plus one";
        }

        public Syntax getSyntax() {
            return Syntax.Function;
        }

        public Type getReturnType() {
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
}

// UdfTest.java
