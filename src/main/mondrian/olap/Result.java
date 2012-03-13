/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import java.io.PrintWriter;

/**
 * A <code>Result</code> is the result of running an MDX query. See {@link
 * Connection#execute}.
 *
 * @author jhyde
 * @since 6 August, 2001
 */
public interface Result {
    /** Returns the query which generated this result. */
    Query getQuery();
    /** Returns the non-slicer axes. */
    Axis[] getAxes();
    /** Returns the slicer axis. */
    Axis getSlicerAxis();
    /** Returns the cell at a given set of coordinates. For example, in a result
     * with 4 columns and 6 rows, the top-left cell has coordinates [0, 0],
     * and the bottom-right cell has coordinates [3, 5]. */
    Cell getCell(int[] pos);
    void print(PrintWriter pw);
    void close();
}

// End Result.java
