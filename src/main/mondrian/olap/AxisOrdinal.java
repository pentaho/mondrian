/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 21, 2003
*/
package mondrian.olap;

/**
 * <code>AxisOrdinal</code> describes the allowable values for an axis code.
 *
 * @author jhyde
 * @since Feb 21, 2003
 * @version $Id$
 **/
public class AxisOrdinal extends EnumeratedValues {
    /** The singleton instance of <code>AxisOrdinal</code>. **/
    public static final AxisOrdinal instance = new AxisOrdinal();

    private AxisOrdinal() {
        super(
                new String[] {
                    "none", "slicer", "columns", "rows", "pages", "chapters", "sections",
                },
                new int[] {
                    NONE, SLICER, COLUMNS, ROWS, PAGES, CHAPTERS, SECTIONS,
                }
        );
    }

    /** Returns the singleton instance of <code>AxisOrdinal</code>. **/
    public static final AxisOrdinal instance() {
        return instance;
    }
    /** No axis.*/
    public static final int NONE = -2;
    /** Slicer axis (which JOLAP calls the Page axis, not to be confused with
     * our {@link #PAGES} axis). **/
    public static final int SLICER = -1;
    /** Columns axis (also known as X axis). **/
    public static final int COLUMNS = 0;
    /** Rows axis (also known as Y axis). **/
    public static final int ROWS = 1;
    /** Pages axis. **/
    public static final int PAGES = 2;
    /** Chapters axis. **/
    public static final int CHAPTERS = 3;
    /** Sections axis. **/
    public static final int SECTIONS = 4;
}

// End AxisOrdinal.java