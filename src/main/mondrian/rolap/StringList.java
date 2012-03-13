/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2006 Pentaho and others
// All Rights Reserved.
//
// jhyde, 29 December, 2001
*/
package mondrian.rolap;

import mondrian.olap.Util;

/**
 * <code>StringList</code> makes it easy to build up a comma-separated string.
 *
 * @author jhyde
 * @since 29 December, 2001
 */
class StringList
{
    private final StringBuilder buf;
    private final String first, mid, last;
    private int count;

    StringList(String first, String mid)
    {
        this.buf = new StringBuilder(first);
        this.count = 0;
        this.first = first;
        this.mid = mid;
        this.last = "";
    }
    StringList(String first)
    {
        this(first, ", ");
    }
    int getCount()
    {
        return count;
    }
    boolean isEmpty()
    {
        return count == 0;
    }
    /** Creates a new item. */
    void newItem(String s)
    {
        if (count++ > 0) {
            buf.append(mid);
        }
        buf.append(s);
    }
    /** Appends to an existing item. */
    void append(String s)
    {
        Util.assertTrue(count > 0);
        buf.append(s);
    }
    // override Object
    public String toString()
    {
        buf.append(last);
        return buf.toString();
    }
};


// End StringList.java
