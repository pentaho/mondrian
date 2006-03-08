/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 Julian Hyde and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import junit.framework.TestCase;
import mondrian.olap.fun.BuiltinFunTable;
import mondrian.mdx.UnresolvedFunCall;
import mondrian.test.TestContext;

/**
 * Tests the MDX parser.
 *
 * @author gjohnson
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
        ParserData data = new ParserData();
        Parser p = new TestParser(data);
        String q = "select [member] on " + s + " from [cube]";
        Query query = p.parseInternal(null, q, false, funTable);
        assertNull("Test parser should return null query", query);

        QueryAxis[] axes = data.getAxes();

        assertEquals("Number of axes must be 1", 1, axes.length);
        assertEquals("Axis index name must be correct",
                expectedName, axes[0].getAxisName());
    }

    public void testNegativeCases() throws Exception {
        ParserData data = new ParserData();
        Parser p = new TestParser(data);

        checkFails(p, "select [member] on axis(1.7) from sales", "The axis number must be an integer");
        checkFails(p, "select [member] on axis(-1) from sales", "Syntax error at line");
        checkFails(p, "select [member] on axis(5) from sales", "The axis number must be an integer");
        checkFails(p, "select [member] on axes(0) from sales", "Syntax error at line");
        checkFails(p, "select [member] on 0.5 from sales", "The axis number must be an integer");
        checkFails(p, "select [member] on 555 from sales", "The axis number must be an integer");
    }

    public void testUnparse() {
        checkUnparse(
                TestContext.fold(new String[] {
                    "with member [Measures].[Foo] as ' 123 '",
                    "select {[Measures].members} on columns,",
                    " CrossJoin([Product].members, {[Gender].Children}) on rows",
                    "from [Sales]",
                    "where [Marital Status].[S]"}),
                TestContext.fold(new String[] {
                    "with member [Measures].[Foo] as '123.0'",
                    "select {[Measures].Members} ON COLUMNS,",
                    "  Crossjoin([Product].Members, {[Gender].Children}) ON ROWS",
                    "from [Sales]",
                    "where [Marital Status].[All Marital Status].[S]",
                    ""}));
    }

    private void checkUnparse(String queryString, final String expected) {
        final TestContext testContext = TestContext.instance();

        final Query query = testContext.getConnection().parseQuery(queryString);
        String unparsedQueryString = query.toMdx();
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
        ParserData data = new ParserData();
        Parser p = new TestParser(data);
        String query = "select {[axis0mbr]} on axis(0), "
                + "{[axis1mbr]} on axis(1) from cube";

        assertNull("Test parser should return null query", p.parseInternal(null, query, false, funTable));

        QueryAxis[] axes = data.getAxes();

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

    public static class TestParser extends Parser {
        ParserData parserData;

        public TestParser(ParserData pd) {
            super();
            parserData = pd;
        }

        protected Query makeQuery(Formula[] formulae, QueryAxis[] axes, String cube, Exp slicer, QueryPart[] cellProps) {
            parserData.setFormulae(formulae);
            parserData.setAxes(axes);
            parserData.setCube(cube);
            parserData.setSlicer(slicer);
            parserData.setCellProps(cellProps);

            return null;
        }
    }

    public static class ParserData {
        private Formula[] formulae;
        private QueryAxis[] axes;
        private String cube;
        private Exp slicer;
        private QueryPart[] cellProps;

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

        public Formula[] getFormulae() {
            return formulae;
        }

        public void setFormulae(Formula[] formulae) {
            this.formulae = formulae;
        }

        public Exp getSlicer() {
            return slicer;
        }

        public void setSlicer(Exp slicer) {
            this.slicer = slicer;
        }
    }
}

// End ParserTest.java
