/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 13, 2003
*/
package mondrian.test;

import junit.framework.Assert;
import mondrian.olap.*;
import mondrian.rolap.RolapConnectionProperties;
import org.eigenbase.util.property.Property;

import java.util.EnumSet;
import java.util.Set;

/**
 * A <code>ParameterTest</code> is a test suite for functionality relating to
 * parameters.
 *
 * @author jhyde
 * @since Feb 13, 2003
 * @version $Id$
 */
public class ParameterTest extends FoodMartTestCase {
    public ParameterTest(String name) {
        super(name);
    }

    // -- Helper methods ----------

    private void assertSetPropertyFails(String propName, String scope) {
        Query q = getConnection().parseQuery("select from [Sales]");
        try {
            q.setParameter(propName, "foo");
            fail("expected exception, trying to set " +
                "non-overrideable property '" + propName + "'");
        } catch (Exception e) {
            assertTrue(e.getMessage().indexOf(
                "Parameter '" + propName + "' (defined at '" +
                    scope + "' scope) is not modifiable") >= 0);
        }
    }

    // -- Tests --------------

    public void testChangeable() {
        // jpivot needs to set a parameters value before the query is executed
        String mdx = "select {Parameter(\"Foo\",[Time],[Time].[1997],\"Foo\")} ON COLUMNS from [Sales]";
        Query query = getConnection().parseQuery(mdx);
        SchemaReader sr = query.getSchemaReader(false);
        Member m = sr.getMemberByUniqueName(new String[]{"Time", "1997", "Q2", "5"}, true);
        Parameter p = sr.getParameter("Foo");
        p.setValue(m);
        assertEquals(m, p.getValue());
        query.resolve();
        p.setValue(m);
        assertEquals(m, p.getValue());
        mdx = query.toString();
        assertEquals("select {Parameter(\"Foo\", [Time], [Time].[1997].[Q2].[5], \"Foo\")} ON COLUMNS" + nl +
        "from [Sales]" + nl,  mdx);
    }

    public void testParameterInFormatString() {
      assertQueryReturns(
          "with member [Measures].[X] as '[Measures].[Store Sales]'," + nl +
          "format_string = Parameter(\"fmtstrpara\", STRING, \"#\")" + nl +
          "select {[Measures].[X]} ON COLUMNS" + nl +
          "from [Sales]",

          "Axis #0:" + nl +
          "{}" + nl +
          "Axis #1:" + nl +
          "{[Measures].[X]}" + nl +
          "Row #0: 565238" + nl

      );
    }

    public void testParameterInFormatString_Bug1584439() {
      String queryString =
        "with member [Measures].[X] as '[Measures].[Store Sales]'," + nl +
        "format_string = Parameter(\"fmtstrpara\", STRING, \"#\")" + nl +
        "select {[Measures].[X]} ON COLUMNS" + nl +
        "from [Sales]";

      // this used to crash
      Connection connection = getConnection();
      Query query = connection.parseQuery(queryString);
      query.toString();
    }

    public void testParameterOnAxis() {
        assertQueryReturns(
                "select {[Measures].[Unit Sales]} on rows," + nl +
                " {Parameter(\"GenderParam\",[Gender],[Gender].[M],\"Which gender?\")} on columns" + nl +
                "from Sales",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Gender].[All Gender].[M]}" + nl +
                "Axis #2:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Row #0: 135,215" + nl);
    }

    public void testNumericParameter() {
        String s = executeExpr("Parameter(\"N\",NUMERIC,2+3,\"A numeric parameter\")");
        Assert.assertEquals("5",s);
    }

    public void testStringParameter() {
        String s = executeExpr("Parameter(\"S\",STRING,\"x\" || \"y\",\"A string parameter\")");
        Assert.assertEquals("xy", s);
    }

    public void testNumericParameterStringValueFails() {
        assertExprThrows("Parameter(\"S\",NUMERIC,\"x\" || \"y\",\"A string parameter\")",
                "Default value of parameter 'S' is inconsistent with its type, NUMERIC");
    }

    public void testParameterDimension() {
        assertExprReturns("Parameter(\"Foo\",[Time],[Time].[1997],\"Foo\").Name",
                "1997");
        assertExprReturns("Parameter(\"Foo\",[Time],[Time].[1997].[Q2].[5],\"Foo\").Name",
                "5");
        // wrong dimension
        assertExprThrows("Parameter(\"Foo\",[Time],[Product].[All Products],\"Foo\").Name",
                "Default value of parameter 'Foo' is not consistent with the parameter type 'MemberType<dimension=[Time]>");
        // non-existent member
        assertExprThrows("Parameter(\"Foo\",[Time],[Time].[1997].[Q5],\"Foo\").Name",
                "MDX object '[Time].[1997].[Q5]' not found in cube 'Sales'");
    }

    public void testParameterHierarchy() {
        assertExprReturns("Parameter(\"Foo\", [Time.Weekly], [Time.Weekly].[1997].[40],\"Foo\").Name",
                "40");
        // right dimension, wrong hierarchy
        assertExprThrows("Parameter(\"Foo\",[Time.Weekly],[Time].[1997].[Q1],\"Foo\").Name",
                "Default value of parameter 'Foo' is not consistent with the parameter type 'MemberType<hierarchy=[Time.Weekly]>");
        // wrong dimension
        assertExprThrows("Parameter(\"Foo\",[Time.Weekly],[Product].[All Products],\"Foo\").Name",
                "Default value of parameter 'Foo' is not consistent with the parameter type 'MemberType<hierarchy=[Time.Weekly]>");
        // garbage
        assertExprThrows("Parameter(\"Foo\",[Time.Weekly],[Widget].[All Widgets],\"Foo\").Name",
                "MDX object '[Widget].[All Widgets]' not found in cube 'Sales'");
    }

    public void testParameterLevel() {
        assertExprReturns("Parameter(\"Foo\",[Time].[Quarter], [Time].[1997].[Q3], \"Foo\").Name",
                "Q3");
        assertExprThrows("Parameter(\"Foo\",[Time].[Quarter], [Time].[1997].[Q3].[8], \"Foo\").Name",
                "Default value of parameter 'Foo' is not consistent with the parameter type 'MemberType<level=[Time].[Quarter]>");
    }

    public void testParameterMemberFails() {
        // type of a param can be dimension, hierarchy, level but not member
        assertExprThrows("Parameter(\"Foo\",[Time].[1997].[Q2],[Time].[1997],\"Foo\")",
                "Invalid type for parameter 'Foo'; expecting NUMERIC, STRING or a hierarchy");
    }

    public void testParameterWithExpressionForHierarchyFails() {
        assertExprThrows("Parameter(\"Foo\",[Gender].DefaultMember.Hierarchy,[Gender].[M],\"Foo\")",
                "Invalid parameter 'Foo'. Type must be a NUMERIC, STRING, or a dimension, hierarchy or level");
    }

    public void _testDerivedParameterFails() {
        assertExprThrows("Parameter(\"X\",NUMERIC,Parameter(\"Y\",NUMERIC,1)+2)",
                "Parameter may not be derived from another parameter");
    }

    public void testParameterInSlicer() {
        assertQueryReturns(
                "select {[Measures].[Unit Sales]} on rows," + nl +
                " {[Marital Status].children} on columns" + nl +
                "from Sales where Parameter(\"GenderParam\",[Gender],[Gender].[M],\"Which gender?\")",
                "Axis #0:" + nl +
                "{[Gender].[All Gender].[M]}" + nl +
                "Axis #1:" + nl +
                "{[Marital Status].[All Marital Status].[M]}" + nl +
                "{[Marital Status].[All Marital Status].[S]}" + nl +
                "Axis #2:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Row #0: 66,460" + nl +
                "Row #0: 68,755" + nl);
    }

    /**
     * Parameter in slicer and expression on columns axis are both of [Gender]
     * hierarchy, which is illegal.
     */
    public void _testParameterDuplicateDimensionFails() {
        assertThrows(
                "select {[Measures].[Unit Sales]} on rows," + nl +
                " {[Gender].[F]} on columns" + nl +
                "from Sales where Parameter(\"GenderParam\",[Gender],[Gender].[M],\"Which gender?\")",
                "Invalid hierarchy for parameter 'GenderParam'");
    }

    /** Mondrian can not handle forward references */
    public void dontTestParamRef() {
        String s = executeExpr(
                "Parameter(\"X\",STRING,\"x\",\"A string\") || " +
                "ParamRef(\"Y\") || " +
                "\".\" ||" +
                "ParamRef(\"X\") || " +
                "Parameter(\"Y\",STRING,\"y\" || \"Y\",\"Other string\")");
        Assert.assertEquals("xyY.xyY",s);
    }

    public void testParamRefWithoutParamFails() {
        assertExprThrows("ParamRef(\"Y\")", "Unknown parameter 'Y'");
    }

    public void testParamDefinedTwiceFails() {
        assertThrows(
                "select {[Measures].[Unit Sales]} on rows," + nl +
                " {Parameter(\"P\",[Gender],[Gender].[M],\"Which gender?\")," + nl +
                "  Parameter(\"P\",[Gender],[Gender].[F],\"Which gender?\")} on columns" + nl +
                "from Sales",
                "Parameter 'P' is defined more than once");
    }

    public void testParamBadTypeFails() {
        assertExprThrows("Parameter(\"P\", 5)",
            "No function matches signature 'Parameter(<String>, <Numeric Expression>)'");
    }

    public void testParamCyclicOk() {
        assertExprReturns(
            "Parameter(\"P\", NUMERIC, ParamRef(\"Q\") + 1) + " +
                "Parameter(\"Q\", NUMERIC, Iif(1 = 0, ParamRef(\"P\"), 2))",
            "5");
    }

    public void testParamCyclicFails() {
        assertExprThrows(
            "Parameter(\"P\", NUMERIC, ParamRef(\"Q\") + 1) + " +
                "Parameter(\"Q\", NUMERIC, Iif(1 = 1, ParamRef(\"P\"), 2))",
            "Cycle occurred while evaluating parameter 'P'");
    }

    public void testParameterMetadata() {
        Connection connection = getConnection();
        Query query = connection.parseQuery(
                "with member [Measures].[A string] as " + nl +
                "   Parameter(\"S\",STRING,\"x\" || \"y\",\"A string parameter\")" + nl +
                " member [Measures].[A number] as " + nl +
                "   Parameter(\"N\",NUMERIC,2+3,\"A numeric parameter\")" + nl +
                "select {[Measures].[Unit Sales]} on rows," + nl +
                " {Parameter(\"P\",[Gender],[Gender].[F],\"Which gender?\")," + nl +
                "  Parameter(\"Q\",[Gender],[Gender].DefaultMember,\"Another gender?\")} on columns" + nl +
                "from Sales");
        Parameter[] parameters = query.getParameters();
        Assert.assertEquals(4, parameters.length);
        Assert.assertEquals("S", parameters[0].getName());
        Assert.assertEquals("N", parameters[1].getName());
        Assert.assertEquals("P", parameters[2].getName());
        Assert.assertEquals("Q", parameters[3].getName());
        final Member member =
                query.getSchemaReader(true).getMemberByUniqueName(
                        new String[] {"Gender", "M"}, true);
        parameters[2].setValue(member);
        Assert.assertEquals("with member [Measures].[A string] as 'Parameter(\"S\", STRING, (\"x\" || \"y\"), \"A string parameter\")'" + nl +
                "  member [Measures].[A number] as 'Parameter(\"N\", NUMERIC, (2.0 + 3.0), \"A numeric parameter\")'" + nl +
                "select {Parameter(\"P\", [Gender], [Gender].[All Gender].[M], \"Which gender?\"), Parameter(\"Q\", [Gender], [Gender].DefaultMember, \"Another gender?\")} ON COLUMNS," + nl +
                "  {[Measures].[Unit Sales]} ON ROWS" + nl +
                "from [Sales]" + nl,
                Util.unparse(query));
    }

    public void testTwoParametersBug1425153() {
        Connection connection = getTestContext().getConnection();
        Query query = connection.parseQuery("select \n" +
                "{[Measures].[Unit Sales]} on columns, \n" +
                "{Parameter(\"ProductMember\", [Product], [Product].[All Products].[Food], \"wat willste?\").children} ON rows \n" +
                "from Sales where Parameter(\"Time\",[Time],[Time].[1997].[Q1])");

        // Execute before setting parameters.
        Result result = connection.execute(query);
        String resultString = TestContext.toString(result);
        TestContext.assertEqualsVerbose(
            fold(
                "Axis #0:\n" +
                    "{[Time].[1997].[Q1]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Food].[Baked Goods]}\n" +
                    "{[Product].[All Products].[Food].[Baking Goods]}\n" +
                    "{[Product].[All Products].[Food].[Breakfast Foods]}\n" +
                    "{[Product].[All Products].[Food].[Canned Foods]}\n" +
                    "{[Product].[All Products].[Food].[Canned Products]}\n" +
                    "{[Product].[All Products].[Food].[Dairy]}\n" +
                    "{[Product].[All Products].[Food].[Deli]}\n" +
                    "{[Product].[All Products].[Food].[Eggs]}\n" +
                    "{[Product].[All Products].[Food].[Frozen Foods]}\n" +
                    "{[Product].[All Products].[Food].[Meat]}\n" +
                    "{[Product].[All Products].[Food].[Produce]}\n" +
                    "{[Product].[All Products].[Food].[Seafood]}\n" +
                    "{[Product].[All Products].[Food].[Snack Foods]}\n" +
                    "{[Product].[All Products].[Food].[Snacks]}\n" +
                    "{[Product].[All Products].[Food].[Starchy Foods]}\n" +
                    "Row #0: 1,932\n" +
                    "Row #1: 5,045\n" +
                    "Row #2: 820\n" +
                    "Row #3: 4,737\n" +
                    "Row #4: 400\n" +
                    "Row #5: 3,262\n" +
                    "Row #6: 2,985\n" +
                    "Row #7: 918\n" +
                    "Row #8: 6,624\n" +
                    "Row #9: 391\n" +
                    "Row #10: 9,499\n" +
                    "Row #11: 412\n" +
                    "Row #12: 7,750\n" +
                    "Row #13: 1,718\n" +
                    "Row #14: 1,316\n"),
            resultString);

        // Set one parameter and execute again.
        query.setParameter("ProductMember", "[Product].[All Products].[Food].[Eggs]");
        result = connection.execute(query);
        resultString = TestContext.toString(result);
        TestContext.assertEqualsVerbose(
            fold(
                "Axis #0:\n" +
                    "{[Time].[1997].[Q1]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Food].[Eggs].[Eggs]}\n" +
                    "Row #0: 918\n"),
            resultString);

        // Now set both parameters and execute again.
        query.setParameter("ProductMember", "[Product].[All Products].[Food].[Deli]");
        query.setParameter("Time", "[Time].[1997].[Q2].[4]");
        result = connection.execute(query);
        resultString = TestContext.toString(result);
        TestContext.assertEqualsVerbose(
            fold(
                "Axis #0:\n" +
                    "{[Time].[1997].[Q2].[4]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Food].[Deli].[Meat]}\n" +
                    "{[Product].[All Products].[Food].[Deli].[Side Dishes]}\n" +
                    "Row #0: 621\n" +
                    "Row #1: 187\n"),
            resultString);
    }

    public void testFoo() {
        Connection connection = getTestContext().getConnection();
        try {
            String mdx = "with member [Measures].[s] as Parameter(\"x\", NUMERIC, 1) select {[Measures].[s]} on columns, {Time.Children} on rows from [Sales]";
            Query query = connection.parseQuery(mdx);
            query.setParameter("x", "8");
        } finally {
            connection.close();
        }
    }

    // -- Tests for connection properties --------------

    /**
     * Tests that certain connection properties which should be null, are.
     */
    public void testConnectionPropsWhichShouldBeNull() {
        // properties which must always return null
        assertExprReturns("ParamRef(\"JdbcPassword\")", "");
        assertExprReturns("ParamRef(\"CatalogContent\")", "");
    }

    /**
     * Tests that non-overrideable properties cannot be overridden in a
     * statement.
     */
    public void testConnectionPropsCannotBeOverridden() {
        Set<RolapConnectionProperties> overrideableProps = Util.enumSetOf(
            RolapConnectionProperties.Catalog,
            RolapConnectionProperties.Locale);
        for (RolapConnectionProperties prop :
            RolapConnectionProperties.class.getEnumConstants())
        {
            if (!overrideableProps.contains(prop)) {
                // try to override prop
                assertSetPropertyFails(prop.name(), "Connection");
            }
        }
    }

    // -- Tests for system properties --------------

    /**
     * Tests accessing system properties as parameters in a statement.
     */
    public void testSystemPropsGet() {
        for (Property property : MondrianProperties.instance().getPropertyList()) {
            assertExprReturns(
                "ParamRef(" +
                    Util.singleQuoteString(property.getPath()) + ")",
                property.stringValue());
        }
    }

    /**
     * Tests getting a java system property.
     */
    public void testSystemPropsGetJava() {
        final String javaVersion = System.getProperty("java.version");
        assertExprReturns(
            "ParamRef(\"java.version\")", javaVersion);
    }

    /**
     * Tests getting a mondrian property.
     */
    public void testMondrianPropsGetJava() {
        final String jdbcDrivers =
            MondrianProperties.instance().JdbcDrivers.get();
        assertExprReturns(
            "ParamRef(\"mondrian.jdbcDrivers\")", jdbcDrivers);
    }

    /**
     * Tests setting system properties.
     */
    public void testSystemPropsSet() {
        for (Property property : MondrianProperties.instance().getPropertyList()) {
            final String propName = property.getPath();
            assertSetPropertyFails(propName, "System");
        }
    }

    // -- Tests for schema properties --------------

    /**
     * Tests a schema property with a default value.
     */
    public void testSchemaProp() {
        final TestContext tc = TestContext.create(
            "<Parameter name=\"prop\" type=\"String\" defaultValue=\" 'foo bar' \" />", null,
            null,
            null, null);
        tc.assertExprReturns("ParamRef(\"prop\")", "foo bar");
    }

    /**
     * Tests a schema property with a default value.
     */
    public void testSchemaPropDupFails() {
        final TestContext tc = TestContext.create(
            "<Parameter name=\"foo\" type=\"Numeric\" defaultValue=\"1\" />\n" +
                "<Parameter name=\"bar\" type=\"Numeric\" defaultValue=\"2\" />\n" +
                "<Parameter name=\"foo\" type=\"Numeric\" defaultValue=\"3\" />\n", null,
            null,
            null, null);
        tc.assertExprThrows("ParamRef(\"foo\")",
            "Duplicate parameter 'foo' in schema");
    }

    public void testSchemaPropIllegalTypeFails() {
        final TestContext tc = TestContext.create(
            "<Parameter name=\"foo\" type=\"Bad type\" defaultValue=\"1\" />", null,
            null,
            null, null);
        tc.assertExprThrows(
            "1",
            "In Schema: In Parameter: " +
                "Value 'Bad type' of attribute 'type' has illegal value 'Bad type'.  " +
                "Legal values: {String, Numeric, Integer, Boolean, Date, Time, Timestamp, Member}");
    }

    public void testSchemaPropInvalidDefaultExpFails() {
        final TestContext tc = TestContext.create(
            "<Parameter name=\"Product Current Member\" type=\"Member\" defaultValue=\"[Product].DefaultMember.Children(2) \" />", null,
            null,
            null, null);
        tc.assertExprThrows("ParamRef(\"Product Current Member\")",
            "No function matches signature '<Member>.Children(<Numeric Expression>)'");
    }

    /**
     * Tests that a schema property fails if it references dimensions which
     * are not available.
     */
    public void testSchemaPropContext() {
        final TestContext tc = TestContext.create(
            "<Parameter name=\"Customer Current Member\" type=\"Member\" defaultValue=\"[Customers].DefaultMember.Children.Item(2) \" />", null,
            null,
            null, null);

        tc.assertQueryReturns(
            "with member [Measures].[Foo] as ' ParamRef(\"Customer Current Member\").Name '\n" +
                "select {[Measures].[Foo]} on columns\n" +
                "from [Sales]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Foo]}\n" +
                "Row #0: USA\n"));

        tc.assertThrows(
            "with member [Measures].[Foo] as ' ParamRef(\"Customer Current Member\").Name '\n" +
                "select {[Measures].[Foo]} on columns\n" +
                "from [Warehouse]",
            "MDX object '[Customers]' not found in cube 'Warehouse'");
    }

}

// End ParameterTest.java
