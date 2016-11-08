/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.util;

import mondrian.olap.Util;

import java.util.Iterator;

/**
 * Tuple of three values.
 *
 * <p>Because a triple implements {@link #equals(Object)}, {@link #hashCode()}
 * and {@link #compareTo(mondrian.util.Triple)}, it can be used in any kind of
 * {@link java.util.Collection}.
 *
 * @author jhyde
 */
public class Triple<T0, T1, T2>
    implements Comparable<Triple<T0, T1, T2>>
{
    public T0 v0;
    public T1 v1;
    public T2 v2;

    /**
     * Creates a Triple.
     *
     * @param v0 Value #0
     * @param v1 Value #1
     * @param v2 Value #2
     */
    public Triple(T0 v0, T1 v1, T2 v2) {
        this.v0 = v0;
        this.v1 = v1;
        this.v2 = v2;
    }

    /**
     * Creates a Triple.
     *
     * @param v0 Value #0
     * @param v1 Value #1
     * @param v2 Value #2
     * @return a new Triple
     */
    public static <T0, T1, T2> Triple<T0, T1, T2> of(T0 v0, T1 v1, T2 v2) {
        return new Triple<T0, T1, T2>(v0, v1, v2);
    }

    public boolean equals(Object obj) {
        if (obj instanceof Triple) {
            //noinspection unchecked
            Triple<T0, T1, T2> pair = (Triple) obj;
            return Util.equals(this.v0, pair.v0)
                && Util.equals(this.v1, pair.v1)
                && Util.equals(this.v2, pair.v2);
        }
        return false;
    }

    public int hashCode() {
        int k0 = (v0 == null) ? 0 : v0.hashCode();
        int k1 = (v1 == null) ? 0 : v1.hashCode();
        int k2 = (v2 == null) ? 0 : v2.hashCode();
        return ((k0 << 8) | k0) ^ (k1 << 4 | k1) ^ k2;
    }


    public int compareTo(Triple<T0, T1, T2> that) {
        int c = compare((Comparable) this.v0, (Comparable)that.v0);
        if (c == 0) {
            c = compare((Comparable) this.v1, (Comparable)that.v1);
        }
        if (c == 0) {
            c = compare((Comparable) this.v2, (Comparable)that.v2);
        }
        return c;
    }

    public String toString() {
        return "<" + v0 + ", " + v1 + ", " + v2 + ">";
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

    /**
     * Returns an iterable over the slice #0 of an iterable.
     *
     * @param iterable Iterable over triples
     * @param <T0> Type #0
     * @param <T1> Type #1
     * @param <T2> Type #2
     * @return Iterable over the 0'th elements of each triple
     */
    public static <T0, T1, T2> Iterable<T0> iter0(
        final Iterable<Triple<T0, T1, T2>> iterable)
    {
        return new Iterable<T0>() {
            public Iterator<T0> iterator() {
                final Iterator<Triple<T0, T1, T2>> iterator =
                    iterable.iterator();
                return new Iterator<T0>() {
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    public T0 next() {
                        return iterator.next().v0;
                    }

                    public void remove() {
                        iterator.remove();
                    }
                };
            }
        };
    }

    /**
     * Returns an iterable over the slice #1 of an iterable.
     *
     * @param iterable Iterable over triples
     * @param <T0> Type #0
     * @param <T1> Type #1
     * @param <T2> Type #2
     * @return Iterable over the 1'th elements of each triple
     */
    public static <T0, T1, T2> Iterable<T1> iter1(
        final Iterable<Triple<T0, T1, T2>> iterable)
    {
        return new Iterable<T1>() {
            public Iterator<T1> iterator() {
                final Iterator<Triple<T0, T1, T2>> iterator =
                    iterable.iterator();
                return new Iterator<T1>() {
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    public T1 next() {
                        return iterator.next().v1;
                    }

                    public void remove() {
                        iterator.remove();
                    }
                };
            }
        };
    }
    /**
     * Returns an iterable over the slice #2 of an iterable.
     *
     * @param iterable Iterable over triples
     * @param <T0> Type #0
     * @param <T1> Type #1
     * @param <T2> Type #2
     * @return Iterable over the 2'th elements of each triple
     */
    public static <T0, T1, T2> Iterable<T2> iter2(
        final Iterable<Triple<T0, T1, T2>> iterable)
    {
        return new Iterable<T2>() {
            public Iterator<T2> iterator() {
                final Iterator<Triple<T0, T1, T2>> iterator =
                    iterable.iterator();
                return new Iterator<T2>() {
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    public T2 next() {
                        return iterator.next().v2;
                    }

                    public void remove() {
                        iterator.remove();
                    }
                };
            }
        };
    }
}

// End Triple.java
