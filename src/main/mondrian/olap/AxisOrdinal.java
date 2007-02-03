/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde
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
 */
public enum AxisOrdinal {

    /** No axis.*/
    NONE,

    /** Slicer axis. */
    SLICER,

    /** Columns axis (also known as X axis), logical ordinal = 0. */
    COLUMNS,

    /** Rows axis (also known as Y axis), logical ordinal = 1. */
    ROWS,

    /** Pages axis, logical ordinal = 2. */
    PAGES,

    /** Chapters axis, logical ordinal = 3. */
    CHAPTERS,

    /** Sections axis, logical ordinal = 4. */
    SECTIONS;

    public static AxisOrdinal forLogicalOrdinal(int ordinal) {
        return values()[ordinal + 2];
    }

    /**
     * Returns the ordinal of this axis with {@link #COLUMNS} = 0,
     * {@link #ROWS} = 1, etc.
     */
    public int logicalOrdinal() {
        return ordinal() - 2;
    }

    public static final int MaxLogicalOrdinal = SECTIONS.logicalOrdinal() + 1;
}

// End AxisOrdinal.java