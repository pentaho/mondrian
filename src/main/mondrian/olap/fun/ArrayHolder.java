/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import java.util.Arrays;

/**
 * Holds an array, so that {@link #equals} and {@link #hashCode} work.
 *
 * @author jhyde
 * @version $Id$
 */
public class ArrayHolder<T> {
    private final T[] a;

    ArrayHolder(T[] a) {
        this.a = a;
    }

    public int hashCode() {
        return Arrays.hashCode(a);
    }

    public boolean equals(Object o) {
        return o instanceof ArrayHolder &&
            Arrays.equals(a, ((ArrayHolder) o).a);
    }
}

// End ArrayHolder.java
