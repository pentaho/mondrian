/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 23 January, 1999
*/

package mondrian.olap;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Component of an MDX query (derived classes include Query, Axis, Exp, Level).
 */
public abstract class QueryPart implements Walkable {
    QueryPart() {
    }

    /**
     * Converts this query or expression into an MDX string.
     */
    public String toMdx()
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        unparse(pw);
        return sw.toString();
    }

    public void unparse(PrintWriter pw) {
        pw.print(toString());
    }

    /**
     * Replaces the <code>ordinal</code>th child (as it appeared in the array
     * returned from {@link #getChildren}) with <code>with</code>.
     */
    public void replaceChild(int ordinal, QueryPart with) {
        // By default, a QueryPart is atomic (has no children).
        throw new Error("unsupported");
    }

    // implement Walkable
    public Object[] getChildren() {
        // By default, a QueryPart is atomic (has no children).
        return null;
    }

    protected Object[] getAllowedChildren(CubeAccess cubeAccess) {
        // By default, a QueryPart is atomic (has no children).
        return null;
    }
}

// End QueryPart.java
