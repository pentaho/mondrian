/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2006 Julian Hyde and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import junit.framework.TestCase;
import mondrian.olap.fun.BuiltinFunTable;
import mondrian.mdx.UnresolvedFunCall;
import mondrian.mdx.QueryPrintWriter;
import mondrian.test.TestContext;

import java.io.StringWriter;
import java.io.PrintWriter;

/**
 * Tests the MDX parser.
 *
 * @author gjohnson
 * @version $Id$
 */
public class ParserTest extends TestCase {
    public ParserTest(String name) {
        super(name);
    }

    static BuiltinFunTable funTable = BuiltinFunTable.instance();

    public void testAxisParsing() throws Exception {
        checkAxisAllWays(0, "COLUMNS");
        checkAxisAllWays(1, "ROWS");
        checkAxisAllWays(2, "PAGES");
        checkAxisAllWays(3, "CHAPTERS");
        checkAxisAllWays(4, "SECTIONS");
    }

    private void checkAxisAllWays(int axisOrdinal, String axisName) {
        checkAxis(axisOrdinal + "", axisName);
        checkAxis("AXIS(" + axisOrdinal + ")", axisName);
        checkAxis(axisName, axisName);
    }

    private void checkAxis(
            String s,
            String expectedName) {
        Parser p = new TestParser();
        String q = "select [member] on " + s + " from [cube]";
        Query query = p.parseInternal(null, q, false, funTable, false);
        assertNull("Test parser should return null query", query);

        QueryAxis[] axes = ((TestParser) p).getAxes();

        assertEquals("Number of axes must be 1", 1, axes.length);
        assertEquals("Axis index name must be correct",
                expectedName, axes[0].getAxisName());
    }

    public void testNegativeCases() throws Exception {
        assertParseQueryFails("select [member] on axis(1.7) from sales", "The axis number must be an integer");
        assertParseQueryFails("select [member] on axis(-1) from sales", "Syntax error at line");
        assertParseQueryFails("select [member] on axis(5) from sales", "The axis number must be an integer");
        assertParseQueryFails("select [member] on axes(0) from sales", "Syntax error at line");
        assertParseQueryFails("select [member] on 0.5 from sales", "The axis number must be an integer");
        assertParseQueryFails("select [member] on 555 from sales", "The axis number must be an integer");
    }

    public void testScannerPunc() {
        // '$' is OK inside brackets but not outside
        assertParseQuery(
                "select [measures].[$foo] on columns from sales",
                TestContext.fold(
                    "select [measures].[$foo] ON COLUMNS\n" +
                    "from [sales]\n"));
        assertParseQueryFails(
            "select [measures].$foo on columns from sales",
                "Unexpected character '$'");

        // ']' unexcpected
        assertParseQueryFails("select { Customers].Children } on columns from [Sales]",
                "Unexpected character ']'");
    }

    public void testUnparse() {
        checkUnparse(
                TestContext.fold(
                    "with member [Measures].[Foo] as ' 123 '\n" +
                    "select {[Measures].members} on columns,\n" +
                    " CrossJoin([Product].members, {[Gender].Children}) on rows\n" +
                    "from [Sales]\n" +
                    "where [Marital Status].[S]"),
                TestContext.fold(
                    "with member [Measures].[Foo] as '123.0'\n" +
                    "select {[Measures].Members} ON COLUMNS,\n" +
                    "  Crossjoin([Product].Members, {[Gender].Children}) ON ROWS\n" +
                    "from [Sales]\n" +
                    "where [Marital Status].[All Marital Status].[S]\n"));
    }

    private void checkUnparse(String queryString, final String expected) {
        final TestContext testContext = TestContext.instance();

        final Query query = testContext.getConnection().parseQuery(queryString);
        String unparsedQueryString = Util.unparse(query);
        TestContext.assertEqualsVerbose(expected, unparsedQueryString);
    }

    private void assertParseQueryFails(String query, String expected) {
        checkFails(new TestParser(), query, expected);
    }

    private void assertParseExprFails(String expr, String expected) {
        checkFails(new TestParser(), wrapExpr(expr), expected);
    }

    private void checkFails(Parser p, String query, String expected) {
        try {
            p.parseInternal(null, query, false, funTable, false);

            fail("Must return an error");
        } catch (Exception e) {
            Exception nested = (Exception) e.getCause();
            String message = nested.getMessage();
            if (message.indexOf(expected) < 0) {
                fail("Actual result [" + message +
                    "] did not contain [" + expected +
                    "]");
            }
        }
    }

    public void testMultipleAxes() throws Exception {
        Parser p = new TestParser();
        String query = "select {[axis0mbr]} on axis(0), "
                + "{[axis1mbr]} on axis(1) from cube";

        assertNull("Test parser should return null query",
            p.parseInternal(null, query, false, funTable, false));

        QueryAxis[] axes = ((TestParser) p).getAxes();

        assertEquals("Number of axes", 2, axes.length);
        assertEquals("Axis index name must be correct",
            AxisOrdinal.forLogicalOrdinal(0).name(), axes[0].getAxisName());
        assertEquals("Axis index name must be correct",
            AxisOrdinal.forLogicalOrdinal(1).name(), axes[1].getAxisName());

        query = "select {[axis1mbr]} on aXiS(1), "
                + "{[axis0mbr]} on AxIs(0) from cube";

        assertNull("Test parser should return null query",
            p.parseInternal(null, query, false, funTable, false));

        assertEquals("Number of axes", 2, axes.length);
        assertEquals("Axis index name must be correct",
            AxisOrdinal.forLogicalOrdinal(0).name(), axes[0].getAxisName());
        assertEquals("Axis index name must be correct",
            AxisOrdinal.forLogicalOrdinal(1).name(), axes[1].getAxisName());

        Exp colsSetExpr = axes[0].getSet();
        assertNotNull("Column tuples", colsSetExpr);

        UnresolvedFunCall fun = (UnresolvedFunCall)colsSetExpr;
        String id = ((Id)(fun.getArgs()[0])).getElement(0);
        assertEquals("Correct member on axis", "axis0mbr", id);

        Exp rowsSetExpr = axes[1].getSet();
        assertNotNull("Row tuples", rowsSetExpr);

        fun = (UnresolvedFunCall ) rowsSetExpr;
        id = ((Id) (fun.getArgs()[0])).getElement(0);
        assertEquals("Correct member on axis", "axis1mbr", id);
    }

    public void testCaseTest() {
        assertParseQuery(
                "with member [Measures].[Foo] as " +
                " ' case when x = y then \"eq\" when x < y then \"lt\" else \"gt\" end '" +
                "select {[foo]} on axis(0) from cube",
                TestContext.fold(
                    "with member [Measures].[Foo] as 'CASE WHEN (x = y) THEN \"eq\" WHEN (x < y) THEN \"lt\" ELSE \"gt\" END'\n" +
                    "select {[foo]} ON COLUMNS\n" +
                    "from [cube]\n"));
    }

    public void testCaseSwitch() {
        assertParseQuery(
                "with member [Measures].[Foo] as " +
                " ' case x when 1 then 2 when 3 then 4 else 5 end '" +
                "select {[foo]} on axis(0) from cube",
                TestContext.fold(
                    "with member [Measures].[Foo] as 'CASE x WHEN 1.0 THEN 2.0 WHEN 3.0 THEN 4.0 ELSE 5.0 END'\n" +
                    "select {[foo]} ON COLUMNS\n" +
                    "from [cube]\n"));
    }

    public void testDimensionProperties() {
        assertParseQuery(
                "select {[foo]} properties p1,   p2 on columns from [cube]",
                TestContext.fold(
                    "select {[foo]} DIMENSION PROPERTIES p1, p2 ON COLUMNS\n" +
                    "from [cube]\n"));
    }

    public void testIsEmpty() {
        assertParseExpr("[Measures].[Unit Sales] IS EMPTY",
            "([Measures].[Unit Sales] IS EMPTY)");

        assertParseExpr("[Measures].[Unit Sales] IS EMPTY AND 1 IS NULL",
            "(([Measures].[Unit Sales] IS EMPTY) AND (1.0 IS NULL))");

        // FIXME: "NULL" should associate as "IS NULL" rather than "NULL + 56.0"
        assertParseExpr("- x * 5 is empty is empty is null + 56",
            "(((((- x) * 5.0) IS EMPTY) IS EMPTY) IS (NULL + 56.0))");
    }

    public void testIs() {
        assertParseExpr("[Measures].[Unit Sales] IS [Measures].[Unit Sales] AND [Measures].[Unit Sales] IS NULL",
            "(([Measures].[Unit Sales] IS [Measures].[Unit Sales]) AND ([Measures].[Unit Sales] IS NULL))");
    }

    public void testIsNull() {
        assertParseExpr("[Measures].[Unit Sales] IS NULL",
            "([Measures].[Unit Sales] IS NULL)");

        assertParseExpr("[Measures].[Unit Sales] IS NULL AND 1 <> 2",
            "(([Measures].[Unit Sales] IS NULL) AND (1.0 <> 2.0))");

        assertParseExpr("x is null or y is null and z = 5",
            "((x IS NULL) OR ((y IS NULL) AND (z = 5.0)))");

        assertParseExpr("(x is null) + 56 > 6",
            "((((x IS NULL)) + 56.0) > 6.0)");

        // FIXME: Should be
        //  "(((((x IS NULL) AND (a = b)) OR ((c = (d + 5.0))) IS NULL) + 5.0)");
        assertParseExpr("x is null and a = b or c = d + 5 is null + 5",
            "(((x IS NULL) AND (a = b)) OR ((c = (d + 5.0)) IS (NULL + 5.0)))");
    }

    public void testNull() {
        assertParseExpr("Filter({[Measures].[Foo]}, Iif(1 = 2, NULL, 'X'))",
            "Filter({[Measures].[Foo]}, Iif((1.0 = 2.0), NULL, \"X\"))");
    }

    public void testCast() {
        assertParseExpr("Cast([Measures].[Unit Sales] AS Numeric)",
            "CAST([Measures].[Unit Sales] AS Numeric)");

        assertParseExpr("Cast(1 + 2 AS String)",
            "CAST((1.0 + 2.0) AS String)");
    }

    public void testId() {
        assertParseExpr("foo", "foo");
        assertParseExpr("fOo", "fOo");
        assertParseExpr("[Foo].[Bar Baz]", "[Foo].[Bar Baz]");
        assertParseExpr("[Foo].&[Bar]", "[Foo].&[Bar]");
    }

    public void testCloneQuery() {
        Connection connection = TestContext.instance().getFoodMartConnection();
        Query query = connection.parseQuery(
            "select {[Measures].Members} on columns,\n" +
                " {[Store].Members} on rows\n" +
                "from [Sales]\n" +
                "where ([Gender].[M])");

        Object queryClone = query.clone();
        assertTrue(queryClone instanceof Query);
        assertEquals(query.toString(), queryClone.toString());
    }

    /**
     * Tests parsing of numbers.
     */
    public void testNumbers() {
        // Number: [+-] <digits> [ . <digits> ] [e [+-] <digits> ]
        assertParseExpr("2", "2.0");

        // leading '-' is treated as an operator -- that's ok
        assertParseExpr("-3", "(- 3.0)");

        // leading '+' is ignored -- that's ok
        assertParseExpr("+45", "45.0");

        // space bad
        assertParseExprFails("4 5", "Syntax error at line 1, column 35, token '5.0'");

        assertParseExpr("3.14", "3.14");
        assertParseExpr(".12345", "0.12345");

        // lots of digits left and right of point
        assertParseExpr("31415926535.89793", "3.141592653589793E10");
        assertParseExpr("31415926535897.9314159265358979", "3.141592653589793E13");
        assertParseExpr("3.141592653589793", "3.141592653589793");
        assertParseExpr("-3141592653589793.14159265358979", "(- 3.141592653589793E15)");

        // exponents akimbo
        assertParseExpr("1e2", "100.0");
        assertParseExprFails("1e2e3", "Syntax error at line 1, column 37, token 'e3'");
        assertParseExpr("1.2e3", "1200.0");
        assertParseExpr("-1.2345e3", "(- 1234.5)");
        assertParseExprFails("1.2e3.4", "Syntax error at line 1, column 39, token '0.4'");
        assertParseExpr(".00234e0003", "2.34");
        assertParseExpr(".00234e-0067", "2.34E-70");
    }

    /**
     * Testcase for bug 1688645, "High precision number in MDX causes overflow".
     * The problem was that "5000001234" exceeded the precision of the int being
     * used to gather the mantissa.
     */
    public void testLargePrecision() {

        // Now, a query with several numeric literals. This is the original
        // testcase for the bug.
        assertParseQuery(
            "with member [Measures].[Small Number] as '[Measures].[Store Sales] / 9000'\n" +
            "select\n" +
            "{[Measures].[Small Number]} on columns,\n" +
            "{Filter([Product].[Product Department].members, [Measures].[Small Number] >= 0.3\n" +
            "and [Measures].[Small Number] <= 0.5000001234)} on rows\n" +
            "from Sales\n" +
            "where ([Time].[1997].[Q2].[4])",
            TestContext.fold("with member [Measures].[Small Number] as '([Measures].[Store Sales] / 9000.0)'\n" +
                "select {[Measures].[Small Number]} ON COLUMNS,\n" +
                "  {Filter([Product].[Product Department].members, (([Measures].[Small Number] >= 0.3) AND ([Measures].[Small Number] <= 0.5000001234)))} ON ROWS\n" +
                "from [Sales]\n" +
                "where ([Time].[1997].[Q2].[4])\n"));
    }

    /**
     * Parses an MDX query and asserts that the result is as expected when
     * unparsed.
     *
     * @param mdx MDX query
     * @param expected Expected result of unparsing
     */
    private void assertParseQuery(String mdx, final String expected) {
        Parser p = new TestParser();
        final Query query = p.parseInternal(null, mdx, false, funTable, false);
        assertNull("Test parser should return null query", query);
        final String actual = ((TestParser) p).toMdxString();
        TestContext.assertEqualsVerbose(expected, actual);
    }

    /**
     * Parses an MDX expression and asserts that the result is as expected when
     * unparsed.
     *
     * @param expr MDX query
     * @param expected Expected result of unparsing
     */
    private void assertParseExpr(String expr, final String expected) {
        TestParser p = new TestParser();
        final String mdx = wrapExpr(expr);
        final Query query = p.parseInternal(null, mdx, false, funTable, false);
        assertNull("Test parser should return null query", query);
        final String actual = Util.unparse(p.formulas[0].getExpression());
        TestContext.assertEqualsVerbose(expected, actual);
    }

    private String wrapExpr(String expr) {
        return "with member [Measures].[Foo] as " +
            expr +
            "\n select from [Sales]";
    }

    public static class TestParser extends Parser {
        private Formula[] formulas;
        private QueryAxis[] axes;
        private String cube;
        private Exp slicer;
        private QueryPart[] cellProps;

        public TestParser() {
            super();
        }

        protected Query makeQuery(
                Formula[] formulae,
                QueryAxis[] axes,
                String cube,
                Exp slicer,
                QueryPart[] cellProps) {
            setFormulas(formulae);
            setAxes(axes);
            setCube(cube);
            setSlicer(slicer);
            setCellProps(cellProps);
            return null;
        }

        public QueryAxis[] getAxes() {
            return axes;
        }

        public void setAxes(QueryAxis[] axes) {
            this.axes = axes;
        }

        public QueryPart[] getCellProps() {
            return cellProps;
        }

        public void setCellProps(QueryPart[] cellProps) {
            this.cellProps = cellProps;
        }

        public String getCube() {
            return cube;
        }

        public void setCube(String cube) {
            this.cube = cube;
        }

        public Formula[] getFormulas() {
            return formulas;
        }

        public void setFormulas(Formula[] formulas) {
            this.formulas = formulas;
        }

        public Exp getSlicer() {
            return slicer;
        }

        public void setSlicer(Exp slicer) {
            this.slicer = slicer;
        }

        /**
         * Converts this query to a string.
         *
         * @return This query converted to a string
         */
        public String toMdxString() {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new QueryPrintWriter(sw);
            unparse(pw);
            return sw.toString();
        }

        private void unparse(PrintWriter pw) {
            if (formulas != null) {
                for (int i = 0; i < formulas.length; i++) {
                    if (i == 0) {
                        pw.print("with ");
                    } else {
                        pw.print("  ");
                    }
                    formulas[i].unparse(pw);
                    pw.println();
                }
            }
            pw.print("select ");
            if (axes != null) {
                for (int i = 0; i < axes.length; i++) {
                    axes[i].unparse(pw);
                    if (i < axes.length - 1) {
                        pw.println(",");
                        pw.print("  ");
                    } else {
                        pw.println();
                    }
                }
            }
            if (cube != null) {
                pw.println("from [" + cube + "]");
            }
            if (slicer != null) {
                pw.print("where ");
                slicer.unparse(pw);
                pw.println();
            }
        }

        /**
         * Converts an expression to a string.
         *
         * @param exp Expression
         * @return Expression converted to a string
         */
        public String toMdxString(Exp exp) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new QueryPrintWriter(sw);
            unparse(pw);
            return sw.toString();
        }

    }
}

// End ParserTest.java
