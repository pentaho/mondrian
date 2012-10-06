/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.Util;

import java.util.*;

/**
 * Tuple containing three values.
 *
 * @author jhyde
 */
public class Tuple3<T0, T1, T2>
    implements Comparable<Tuple3<T0, T1, T2>>
{
    public T0 v0;
    public T1 v1;
    public T2 v2;

    /**
     * Creates a Tuple3.
     *
     * @param v0 First value
     * @param v1 Second value
     * @param v2 Third value
     */
    public Tuple3(T0 v0, T1 v1, T2 v2) {
        this.v0 = v0;
        this.v1 = v1;
        this.v2 = v2;
    }

    /**
     * Creates a Tuple3, inferring type parameters from the arguments.
     *
     * @param v0 First value
     * @param v1 Second value
     * @param v2 Third value
     * @return Tuple3
     */
    public static <T0, T1, T2> Tuple3<T0, T1, T2> of(T0 v0, T1 v1, T2 v2) {
        return new Tuple3<T0, T1, T2>(v0, v1, v2);
    }

    public boolean equals(Object obj) {
        if (obj instanceof Tuple3) {
            //noinspection unchecked
            Tuple3<T0, T1, T2> pair = (Tuple3) obj;
            return Util.equals(this.v0, pair.v0)
                && Util.equals(this.v1, pair.v1)
                && Util.equals(this.v2, pair.v2);
        }
        return false;
    }

    public int hashCode() {
        int k = (v0 == null) ? 0 : v0.hashCode();
        int k1 = (v1 == null) ? 0 : v1.hashCode();
        int k2 = (v2 == null) ? 0 : v2.hashCode();
        return ((k << 4) | k)
            ^ ((k1 << 8) | k1)
            ^ k2;
    }


    public int compareTo(Tuple3<T0, T1, T2> that) {
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
     * Returns an iterable over the first slice of an iterable.
     *
     * @param iterable Iterable over 3-tuples
     * @param <T0> First type
     * @param <T1> Second type
     * @param <T2> Third type
     * @return Iterable over the first elements
     */
    public static <T0, T1, T2> Iterable<T0> slice0(
        final Iterable<Tuple3<T0, T1, T2>> iterable)
    {
        return new Iterable<T0>() {
            public Iterator<T0> iterator() {
                final Iterator<Tuple3<T0, T1, T2>> iterator =
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
     * Returns an iterable over the second slice of an iterable.
     *
     * @param iterable Iterable over 3-tuples
     * @param <T0> First type
     * @param <T1> Second type
     * @param <T2> Third type
     * @return Iterable over the second elements
     */
    public static <T0, T1, T2> Iterable<T1> slice1(
        final Iterable<Tuple3<T0, T1, T2>> iterable)
    {
        return new Iterable<T1>() {
            public Iterator<T1> iterator() {
                final Iterator<Tuple3<T0, T1, T2>> iterator =
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
     * Returns an iterable over the third slice of an iterable.
     *
     * @param iterable Iterable over 3-tuples
     * @param <T0> First type
     * @param <T1> Second type
     * @param <T2> Third type
     * @return Iterable over the third elements
     */
    public static <T0, T1, T2> Iterable<T2> slice2(
        final Iterable<Tuple3<T0, T1, T2>> iterable)
    {
        return new Iterable<T2>() {
            public Iterator<T2> iterator() {
                final Iterator<Tuple3<T0, T1, T2>> iterator =
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

    /**
     * Creates an iterable that iterates in parallel over three iterables.
     *
     * @param i0 First iterable
     * @param i1 Second iterable
     * @param i2 Third iterable
     * @param <T0> Element type of first iterable
     * @param <T1> Element type of second iterable
     * @param <T2> Element type of third iterable
     * @return Iterable over all iterables in parallel
     */
    public static <T0, T1, T2> Iterable<Tuple3<T0, T1, T2>> iterate(
        final Iterable<T0> i0,
        final Iterable<T1> i1,
        final Iterable<T2> i2)
    {
        return new Iterable<Tuple3<T0, T1, T2>>()
        {
            public Iterator<Tuple3<T0, T1, T2>> iterator()
            {
                final Iterator<T0> iterator0 = i0.iterator();
                final Iterator<T1> iterator1 = i1.iterator();
                final Iterator<T2> iterator2 = i2.iterator();

                return new Iterator<Tuple3<T0, T1, T2>>()
                {
                    public boolean hasNext()
                    {
                        return iterator0.hasNext()
                               && iterator1.hasNext()
                               && iterator2.hasNext();
                    }

                    public Tuple3<T0, T1, T2> next()
                    {
                        return Tuple3.of(
                            iterator0.next(),
                            iterator1.next(),
                            iterator2.next());
                    }

                    public void remove()
                    {
                        iterator0.remove();
                        iterator1.remove();
                        iterator2.remove();
                    }
                };
            }
        };
    }
}

// End Tuple3.java
