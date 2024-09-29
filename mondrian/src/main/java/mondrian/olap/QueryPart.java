/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.olap;

import java.io.PrintWriter;

/**
 * Component of an MDX query (derived classes include Query, Axis, Exp, Level).
 *
 * @author jhyde, 23 January, 1999
 */
public abstract class QueryPart implements Walkable {
    /**
     * Creates a QueryPart.
     */
    QueryPart() {
    }

    /**
     * Writes a string representation of this parse tree
     * node to the given writer.
     *
     * @param pw writer
     */
    public void unparse(PrintWriter pw) {
        pw.print(toString());
    }

    // implement Walkable
    public Object[] getChildren() {
        // By default, a QueryPart is atomic (has no children).
        return null;
    }

    /**
     * Returns the plan that Mondrian intends to use to execute this query.
     *
     * @param pw Print writer
     */
    public void explain(PrintWriter pw) {
        throw new UnsupportedOperationException(
            "explain not implemented for " + this + " (" + getClass() + ")");
    }
}

// End QueryPart.java
