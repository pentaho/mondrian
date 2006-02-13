/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 13, 2003
*/
package mondrian.test;

import junit.framework.Assert;

import mondrian.olap.*;

/**
 * A <code>ParameterTest</code> is a test suite for functionality relating to
 * parameters.
 *
 * @author jhyde
 * @since Feb 13, 2003
 * @version $Id$
 **/
public class ParameterTest extends FoodMartTestCase {
    public ParameterTest(String name) {
        super(name);
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
        assertExprThrows("ParamRef(\"Y\")", "Parameter 'Y' is referenced but never defined");
    }
    public void testParamDefinedTwiceFails() {
        assertThrows(
                "select {[Measures].[Unit Sales]} on rows," + nl +
                " {Parameter(\"P\",[Gender],[Gender].[M],\"Which gender?\")," + nl +
                "  Parameter(\"P\",[Gender],[Gender].[F],\"Which gender?\")} on columns" + nl +
                "from Sales",
                "Parameter 'P' is defined more than once");
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
                query.toString());
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
                fold(new String[] {
                "Axis #0:",
                "{[Time].[1997].[Q1]}",
                "Axis #1:",
                "{[Measures].[Unit Sales]}",
                "Axis #2:",
                "{[Product].[All Products].[Food].[Baked Goods]}",
                "{[Product].[All Products].[Food].[Baking Goods]}",
                "{[Product].[All Products].[Food].[Breakfast Foods]}",
                "{[Product].[All Products].[Food].[Canned Foods]}",
                "{[Product].[All Products].[Food].[Canned Products]}",
                "{[Product].[All Products].[Food].[Dairy]}",
                "{[Product].[All Products].[Food].[Deli]}",
                "{[Product].[All Products].[Food].[Eggs]}",
                "{[Product].[All Products].[Food].[Frozen Foods]}",
                "{[Product].[All Products].[Food].[Meat]}",
                "{[Product].[All Products].[Food].[Produce]}",
                "{[Product].[All Products].[Food].[Seafood]}",
                "{[Product].[All Products].[Food].[Snack Foods]}",
                "{[Product].[All Products].[Food].[Snacks]}",
                "{[Product].[All Products].[Food].[Starchy Foods]}",
                "Row #0: 1,932",
                "Row #1: 5,045",
                "Row #2: 820",
                "Row #3: 4,737",
                "Row #4: 400",
                "Row #5: 3,262",
                "Row #6: 2,985",
                "Row #7: 918",
                "Row #8: 6,624",
                "Row #9: 391",
                "Row #10: 9,499",
                "Row #11: 412",
                "Row #12: 7,750",
                "Row #13: 1,718",
                "Row #14: 1,316",
                ""}),
                resultString);

        // Now set both parameters and execute again.
        query.setParameter("ProductMember", "[Product].[All Products].[Food].[Deli]");
        query.setParameter("Time", "[Time].[1997].[Q2].[4]");
        result = connection.execute(query);
        resultString = TestContext.toString(result);
        TestContext.assertEqualsVerbose(
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997].[Q2].[4]}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Product].[All Products].[Food].[Deli].[Meat]}",
                    "{[Product].[All Products].[Food].[Deli].[Side Dishes]}",
                    "Row #0: 621",
                    "Row #1: 187",
                    ""}),
                resultString);

    }
}

// End ParameterTest.java
