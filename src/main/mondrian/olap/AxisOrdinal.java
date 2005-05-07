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
public class AxisOrdinal extends EnumeratedValues.BasicValue {

    private AxisOrdinal(String name, int ordinal) {
        super(name, ordinal, null);
    }

    /**
     * Converts an ordinal value into an {@link AxisOrdinal}. Returns null if
     * not found.
     */
    public static AxisOrdinal get(int ordinal) {
        return (AxisOrdinal) enumeration.getValue(ordinal);
    }

    /**
     * Converts a name into an {@link AxisOrdinal}. Returns null if
     * not found.
     */
    public static AxisOrdinal get(String name) {
        return (AxisOrdinal) enumeration.getValue(name);
    }

    public static final int NoneOrdinal = -2;
    public static final int SlicerOrdinal = -1;
    public static final int ColumnsOrdinal = 0;
    public static final int RowsOrdinal = 1;
    public static final int PagesOrdinal = 2;
    public static final int ChaptersOrdinal = 3;
    public static final int SectionsOrdinal = 4;
    public static final int MaxOrdinal = SectionsOrdinal;
    public static final int MinOrdinal = NoneOrdinal;

    /** No axis.*/
    public static final AxisOrdinal None =
            new AxisOrdinal("NONE", NoneOrdinal);
    /** Slicer axis (which JOLAP calls the Page axis, not to be confused with
     * our {@link #PagesOrdinal} axis). **/
    public static final AxisOrdinal Slicer =
            new AxisOrdinal("SLICER", SlicerOrdinal);
    /** Columns axis (also known as X axis). **/
    public static final AxisOrdinal Columns =
            new AxisOrdinal("COLUMNS", ColumnsOrdinal);
    /** Rows axis (also known as Y axis). **/
    public static final AxisOrdinal Rows =
            new AxisOrdinal("ROWS", RowsOrdinal);
    /** Pages axis. **/
    public static final AxisOrdinal Pages =
            new AxisOrdinal("PAGES", PagesOrdinal);
    /** Chapters axis. **/
    public static final AxisOrdinal Chapters =
            new AxisOrdinal("CHAPTERS", ChaptersOrdinal);
    /** Sections axis. **/
    public static final AxisOrdinal Sections =
            new AxisOrdinal("SECTIONS", SectionsOrdinal);
    /** Enumerates the valid values {@link AxisOrdinal}. */
    public static final EnumeratedValues enumeration = new EnumeratedValues(
            new AxisOrdinal[] {
                None, Slicer, Columns, Rows, Pages, Chapters, Sections,
            }
    );
}

// End AxisOrdinal.java