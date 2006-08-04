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
        Query query = p.parseInternal(null, q, false, funTable);
        assertNull("Test parser should return null query", query);

        QueryAxis[] axes = ((TestParser) p).getAxes();

        assertEquals("Number of axes must be 1", 1, axes.length);
        assertEquals("Axis index name must be correct",
                expectedName, axes[0].getAxisName());
    }

    public void testNegativeCases() throws Exception {
        Parser p = new TestParser();

        checkFails(p, "select [member] on axis(1.7) from sales", "The axis number must be an integer");
        checkFails(p, "select [member] on axis(-1) from sales", "Syntax error at line");
        checkFails(p, "select [member] on axis(5) from sales", "The axis number must be an integer");
        checkFails(p, "select [member] on axes(0) from sales", "Syntax error at line");
        checkFails(p, "select [member] on 0.5 from sales", "The axis number must be an integer");
        checkFails(p, "select [member] on 555 from sales", "The axis number must be an integer");
    }

    public void testScannerPunc() {
        // '$' is OK inside brackets but not outside
        Parser p = new TestParser();
        assertParserReturns(
                "select [measures].[$foo] on columns from sales",
                TestContext.fold(
                    "select [measures].[$foo] ON COLUMNS\n" +
                    "from [sales]\n"));
        checkFails(p,
                "select [measures].$foo on columns from sales",
                "Unexpected character '$'");

        // ']' unexcpected
        checkFails(p, "select { Customers].Children } on columns from [Sales]",
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

    private void checkFails(Parser p, String query, String expected) {
        try {
            p.parseInternal(null, query, false, funTable);

            fail("Must return an error");
        } catch (Exception e) {
            Exception nested = (Exception) e.getCause();
            assertTrue("Check valid error message", nested.getMessage().indexOf(expected) >= 0);
        }
    }

    public void testMultipleAxes() throws Exception {
        Parser p = new TestParser();
        String query = "select {[axis0mbr]} on axis(0), "
                + "{[axis1mbr]} on axis(1) from cube";

        assertNull("Test parser should return null query", p.parseInternal(null, query, false, funTable));

        QueryAxis[] axes = ((TestParser) p).getAxes();

        assertEquals("Number of axes", 2, axes.length);
        assertEquals("Axis index name must be correct",
            AxisOrdinal.enumeration.getName(0), axes[0].getAxisName());
        assertEquals("Axis index name must be correct",
            AxisOrdinal.enumeration.getName(1), axes[1].getAxisName());

        query = "select {[axis1mbr]} on aXiS(1), "
                + "{[axis0mbr]} on AxIs(0) from cube";

        assertEquals("Number of axes", 2, axes.length);
        assertEquals("Axis index name must be correct",
            AxisOrdinal.enumeration.getName(0), axes[0].getAxisName());
        assertEquals("Axis index name must be correct",
            AxisOrdinal.enumeration.getName(1), axes[1].getAxisName());

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
        assertParserReturns(
                "with member [Measures].[Foo] as " +
                " ' case when x = y then \"eq\" when x < y then \"lt\" else \"gt\" end '" +
                "select {[foo]} on axis(0) from cube",
                TestContext.fold(
                    "with member [Measures].[Foo] as 'CASE WHEN ([x] = [y]) THEN \"eq\" WHEN ([x] < [y]) THEN \"lt\" ELSE \"gt\" END'\n" +
                    "select {[foo]} ON COLUMNS\n" +
                    "from [cube]\n"));
    }

    public void testCaseSwitch() {
        assertParserReturns(
                "with member [Measures].[Foo] as " +
                " ' case x when 1 then 2 when 3 then 4 else 5 end '" +
                "select {[foo]} on axis(0) from cube",
                TestContext.fold(
                    "with member [Measures].[Foo] as 'CASE [x] WHEN 1.0 THEN 2.0 WHEN 3.0 THEN 4.0 ELSE 5.0 END'\n" +
                    "select {[foo]} ON COLUMNS\n" +
                    "from [cube]\n"));
    }

    public void testDimensionProperties() {
        assertParserReturns(
                "select {[foo]} properties p1,   p2 on columns from [cube]",
                TestContext.fold(
                    "select {[foo]} DIMENSION PROPERTIES [p1], [p2] ON COLUMNS\n" +
                    "from [cube]\n"));
    }

    /**
     * Parses an MDX query and asserts that the result is as expected when
     * unparsed.
     *
     * @param mdx MDX query
     * @param expected Expected result of unparsing
     */
    private void assertParserReturns(String mdx, final String expected) {
        Parser p = new TestParser();
        final Query query = p.parseInternal(null, mdx, false, funTable);
        assertNull("Test parser should return null query", query);
        final String actual = ((TestParser) p).toMdxString();
        TestContext.assertEqualsVerbose(expected, actual);
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


    }
}

// End ParserTest.java
