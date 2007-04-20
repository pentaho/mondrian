/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import mondrian.olap.Util;

import java.util.Map;

/**
 * Pair of values.
 *
 * <p>Because a pair implements {@link #equals(Object)}, {@link #hashCode()} and
 * {@link #compareTo(Pair)}, it can be used in any kind of
 * {@link java.util.Collection}.
 *
 * @author jhyde
 * @version $Id$
 * @since Apr 19, 2007
 */
public class Pair <L, R>
    implements Comparable<Pair<L, R>>, Map.Entry<L, R> {
    public L left;
    public R right;

    /**
     * Creates a pair.
     *
     * @param left Left value
     * @param right Right value
     */
    public Pair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public boolean equals(Object obj) {
        if (obj instanceof Pair) {
            Pair<L, R> pair = (Pair<L,R>) obj;
            return Util.equals(this.left, pair.left) &&
                Util.equals(this.right, pair.right);
        }
        return false;
    }

    public int hashCode() {
        int k = (left == null) ? 0 : left.hashCode();
        int k1 = (right == null) ? 0 : right.hashCode();
        return ((k << 4) | k) ^ k1;
    }


    public int compareTo(Pair<L, R> that) {
        int c = compare((Comparable) this.left, (Comparable)that.left);
        if (c == 0) {
            c = compare((Comparable) this.right, (Comparable)that.right);
        }
        return c;
    }

    public String toString() {
        return "<" + left + ", " + ">";
    }

    // implement Map.Entry
    public L getKey() {
        return left;
    }

    // implement Map.Entry
    public R getValue() {
        return right;
    }

    // implement Map.Entry
    public R setValue(R value) {
        R previous = right;
        right = value;
        return previous;
    }

    private static <C extends Comparable<C>> int compare(C c1, C c2) {
        if (c1 == null) {
            if (c2 == null) {
                return 0;
            } else {
                return -1;
            }
        } else if (c2 == null) {
            return 1;
        } else {
            return c1.compareTo(c2);
        }
    }
}

// End Pair.java
