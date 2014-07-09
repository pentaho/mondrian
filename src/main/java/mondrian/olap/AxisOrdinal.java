/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho
// All Rights Reserved.
*/
package mondrian.olap;

/**
 * <code>AxisOrdinal</code> describes the allowable values for an axis code.
 *
 * @author jhyde
 * @since Feb 21, 2003
 */
public interface AxisOrdinal {
    /**
     * Returns the name of this axis, e.g. "COLUMNS", "SLICER", "AXIS(17)".
     *
     * @return Name of the axis
     */
    String name();

    /**
     * Returns the ordinal of this axis.
     * {@link StandardAxisOrdinal#COLUMNS} = 0,
     * {@link StandardAxisOrdinal#ROWS} = 1, etc.
     *
     * @return ordinal of this axis
     */
    int logicalOrdinal();

    /**
     * Returns whether this is the filter (slicer) axis.
     *
     * @return whether this is the filter axis
     */
    boolean isFilter();

    public enum StandardAxisOrdinal implements AxisOrdinal {
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

        /**
         * Returns an axis with a given number.
         *
         * <p>If ordinal is greater than 4, returns a non-standard axis called
         * "AXIS(n)". Never returns null.
         *
         * @param ordinal Ordinal
         * @return Axis
         */
        public static AxisOrdinal forLogicalOrdinal(final int ordinal) {
            if (ordinal + 2 > SECTIONS.ordinal()) {
                return new AxisOrdinal() {
                    public String name() {
                        return "AXIS(" + ordinal + ")";
                    }

                    public int logicalOrdinal() {
                        return ordinal;
                    }

                    public boolean isFilter() {
                        return false;
                    }
                };
            } else {
                return values()[ordinal + 2];
            }
        }

        public int logicalOrdinal() {
            return ordinal() - 2;
        }

        public boolean isFilter() {
            return this == SLICER;
        }
    }
}

// End AxisOrdinal.java
