/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2005 Julian Hyde and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import junit.framework.TestCase;

import java.text.MessageFormat;

/**
 * Tests the MDX parser.
 *
 * @author gjohnson
 */
public class ParserTest extends TestCase {
    public ParserTest(String name) {
        super(name);
    }


    public void testAxisParsing() throws Exception {
        final String queryString = "select [member] on axis({0}) from [cube]";

        ParserData data = new ParserData();
        Parser p = new TestParser(data);

        for (int idx = 0; idx < AxisOrdinal.instance.getMax() + 1; idx++) {
            String q = MessageFormat.format(queryString, new Object[] {new Integer(idx)});

            assertNull("Test parser should return null query", p.parseInternal(null, q, false));

            QueryAxis[] axes = data.getAxes();

            assertEquals("Number of axes must be 1", 1, axes.length);
            assertEquals("Axis index name must be correct",
                AxisOrdinal.instance.getName(idx), axes[0].getAxisName());
        }
    }

    public void testNegativeCases() throws Exception {
        ParserData data = new ParserData();
        Parser p = new TestParser(data);

        try {
            p.parseInternal(null, "select [member] on axis(1.7) from sales", false);

            fail("Must return an error");
        }
        catch (Exception e) {
            Exception nested = (Exception) e.getCause();

            assertTrue("Check valid error message", nested.getMessage().indexOf("The axis number must be a non-negative") >= 0);
        }

        try {
            p.parseInternal(null, "select [member] on axis(-1) from sales", false);

            fail("Must return an error");
        }
        catch (Exception e) {
            Throwable t = e.getCause();

            assertNotNull("Expect nested exception", t);
            assertTrue("Check valid error message", t.getMessage().indexOf("Syntax error at line")
                    >= 0);
        }

        try {
            p.parseInternal(null, "select [member] on axis(" + (AxisOrdinal.instance.getMax()  + 1) + ") from sales", false);

            fail("Must return an error");
        }
        catch (Exception e) {
            Exception nested = (Exception) e.getCause();

            assertTrue("Check valid error message", nested.getMessage()
                    .indexOf("The axis number must be a non-negative")
                    >= 0);
        }

        try {
            p.parseInternal(null, "select [member] on axes(0) from sales", false);

            fail("Must return an error");
        }
        catch (Exception e) {
            Throwable t = e.getCause();

            assertNotNull("Expect nested exception", t);
            assertTrue("Check valid error message", t.getMessage().indexOf("Syntax error at line") >= 0);
        }
    }

    public void testMultipleAxes() throws Exception {
        ParserData data = new ParserData();
        Parser p = new TestParser(data);
        String query = "select {[axis0mbr]} on axis(0), "
                + "{[axis1mbr]} on axis(1) from cube";

        assertNull("Test parser should return null query", p.parseInternal(null, query, false));

        QueryAxis[] axes = data.getAxes();

        assertEquals("Number of axes", 2, axes.length);
        assertEquals("Axis index name must be correct",
            AxisOrdinal.instance.getName(0), axes[0].getAxisName());
        assertEquals("Axis index name must be correct",
            AxisOrdinal.instance.getName(1), axes[1].getAxisName());

        query = "select {[axis1mbr]} on aXiS(1), "
                + "{[axis0mbr]} on AxIs(0) from cube";

        assertEquals("Number of axes", 2, axes.length);
        assertEquals("Axis index name must be correct",
            AxisOrdinal.instance.getName(0), axes[0].getAxisName());
        assertEquals("Axis index name must be correct",
            AxisOrdinal.instance.getName(1), axes[1].getAxisName());

        Object[] tuples = axes[0].getChildren();
        assertEquals("Column tuples", 1, tuples.length);

        FunCall fun = (FunCall)tuples[0];
        String id = ((Id)(fun.getChildren()[0])).getElement(0);
        assertEquals("Correct member on axis", "axis0mbr", id);

        tuples = axes[1].getChildren();
        assertEquals("Row tuples", 1, tuples.length);

        fun = (FunCall) tuples[0];
        id = ((Id) (fun.getChildren()[0])).getElement(0);
        assertEquals("Correct member on axis", "axis1mbr", id);
    }

    public static class TestParser extends Parser {
        ParserData mParserData;

        public TestParser(ParserData pd) {
            super();
            mParserData = pd;
        }

        protected Query makeQuery(Formula[] formulae, QueryAxis[] axes, String cube, Exp slicer, QueryPart[] cellProps) {
            mParserData.setFormulae(formulae);
            mParserData.setAxes(axes);
            mParserData.setCube(cube);
            mParserData.setSlicer(slicer);
            mParserData.setCellProps(cellProps);

            return null;
        }
    }

    public static class ParserData {
        private Formula[] mFormulae;
        private QueryAxis[] mAxes;
        private String mCube;
        private Exp mSlicer;
        private QueryPart[] mCellProps;

        public QueryAxis[] getAxes() {
            return mAxes;
        }

        public void setAxes(QueryAxis[] axes) {
            mAxes = axes;
        }

        public QueryPart[] getCellProps() {
            return mCellProps;
        }

        public void setCellProps(QueryPart[] cellProps) {
            mCellProps = cellProps;
        }

        public String getCube() {
            return mCube;
        }

        public void setCube(String cube) {
            mCube = cube;
        }

        public Formula[] getFormulae() {
            return mFormulae;
        }

        public void setFormulae(Formula[] formulae) {
            mFormulae = formulae;
        }

        public Exp getSlicer() {
            return mSlicer;
        }

        public void setSlicer(Exp slicer) {
            mSlicer = slicer;
        }
    }
}
