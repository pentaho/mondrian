/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 23 January, 1999
*/

package mondrian.olap;
import java.io.PrintWriter;

/**
 * Component of an MDX query (derived classes include Query, Axis, Exp, Level).
 *
 * @version $Id$
 * @author jhyde
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
}

// End QueryPart.java
