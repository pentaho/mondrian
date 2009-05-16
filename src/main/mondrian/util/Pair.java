/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2009 Julian Hyde
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
    implements Comparable<Pair<L, R>>, Map.Entry<L, R>
{
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

    /**
     * Creates a pair representing the same mapping as the
     * specified entry.
     *
     * @param entry the entry to copy
     */
    public Pair(Map.Entry<? extends L, ? extends R> entry) {
        this.left = entry.getKey();
        this.right = entry.getValue();
    }

    public boolean equals(Object obj) {
        if (obj instanceof Pair) {
            //noinspection unchecked
            Pair<L, R> pair = (Pair) obj;
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
        return "<" + left + ", " + right + ">";
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

    /**
     * Compares a pair of comparable values of the same type. Null collates
     * less than everything else, but equal to itself.
     *
     * @param c1 First value
     * @param c2 Second value
     * @return  a negative integer, zero, or a positive integer if c1
     *          is less than, equal to, or greater than c2.
     */
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
