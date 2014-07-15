/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.Util;

import java.util.*;

/**
 * Pair of values.
 *
 * <p>Because a pair implements {@link #equals(Object)}, {@link #hashCode()} and
 * {@link #compareTo(Pair)}, it can be used in any kind of
 * {@link java.util.Collection}.
 *
 * @author jhyde
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

    /**
     * Creates a pair, inferring type parameters from the arguments.
     *
     * @param left Left value
     * @param right Right value
     * @return Pair
     */
    public static <L, R> Pair<L, R> of(L left, R right) {
        return new Pair<L, R>(left, right);
    }

    public boolean equals(Object obj) {
        if (obj instanceof Pair) {
            //noinspection unchecked
            Pair<L, R> pair = (Pair) obj;
            return Util.equals(this.left, pair.left)
                && Util.equals(this.right, pair.right);
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

    /**
     * Converts two lists into a list of {@link Pair}s.
     *
     * <p>The length of the combined list is the lesser of the lengths of the
     * source lists. But typically the source lists will be the same length.</p>
     *
     * @param ks Left list
     * @param vs Right list
     * @return List of pairs
     */
    public static <K, V> List<Pair<K, V>> zip(
        final List<K> ks,
        final List<V> vs)
    {
        return new AbstractList<Pair<K, V>>() {
            public Pair<K, V> get(int index) {
                return Pair.of(ks.get(index), vs.get(index));
            }

            public int size() {
                return Math.min(ks.size(), vs.size());
            }
        };
    }

    /**
     * Converts two arrays into a list of {@link Pair}s.
     *
     * <p>The length of the combined list is the lesser of the lengths of the
     * source arrays. But typically the source arrays will be the same
     * length.</p>
     *
     * @param ks Left array
     * @param vs Right array
     * @return List of pairs
     */
    public static <K, V> List<Pair<K, V>> zip(
        final K[] ks,
        final V[] vs)
    {
        return new AbstractList<Pair<K, V>>() {
            public Pair<K, V> get(int index) {
                return Pair.of(ks[index], vs[index]);
            }

            public int size() {
                return Math.min(ks.length, vs.length);
            }
        };
    }

    /**
     * Returns an iterable over the left slice of an iterable.
     *
     * @param iterable Iterable over pairs
     * @param <L> Left type
     * @param <R> Right type
     * @return Iterable over the left elements
     */
    public static <L, R> Iterable<L> leftIter(
        final Iterable<Pair<L, R>> iterable)
    {
        return new Iterable<L>() {
            public Iterator<L> iterator() {
                final Iterator<Pair<L, R>> iterator = iterable.iterator();
                return new Iterator<L>() {
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    public L next() {
                        return iterator.next().left;
                    }

                    public void remove() {
                        iterator.remove();
                    }
                };
            }
        };
    }

    /**
     * Returns an iterable over the right slice of an iterable.
     *
     * @param iterable Iterable over pairs
     * @param <L> right type
     * @param <R> Right type
     * @return Iterable over the right elements
     */
    public static <L, R> Iterable<R> rightIter(
        final Iterable<Pair<L, R>> iterable)
    {
        return new Iterable<R>() {
            public Iterator<R> iterator() {
                final Iterator<Pair<L, R>> iterator = iterable.iterator();
                return new Iterator<R>() {
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    public R next() {
                        return iterator.next().right;
                    }

                    public void remove() {
                        iterator.remove();
                    }
                };
            }
        };
    }

    /**
     * Creates an iterable that iterates in parallel over a pair of iterables.
     *
     * @param i0 First iterable
     * @param i1 Second iterable
     * @param <K> Key type (element type of first iterable)
     * @param <V> Value type (element type of second iterable)
     * @return Iterable in over both iterables in parallel
     */
    public static <K, V> java.lang.Iterable<Pair<K, V>> iterate(
        final Iterable<K> i0,
        final Iterable<V> i1)
    {
        return new Iterable<Pair<K, V>>()
        {
            public Iterator<Pair<K, V>> iterator()
            {
                assert !(i0 instanceof Collection
                         && i1 instanceof Collection
                         && ((Collection) i0).size()
                            != ((Collection) i1).size())
                    : "size mismatch: i0=" + i0 + ", i1=" + i1;
                final Iterator<K> iterator0 = i0.iterator();
                final Iterator<V> iterator1 = i1.iterator();

                return new Iterator<Pair<K, V>>()
                {
                    public boolean hasNext()
                    {
                        final boolean hasNext0 = iterator0.hasNext();
                        final boolean hasNext1 = iterator1.hasNext();
                        assert hasNext0 == hasNext1;
                        return hasNext0 && hasNext1;
                    }

                    public Pair<K, V> next()
                    {
                        return Pair.of(iterator0.next(), iterator1.next());
                    }

                    public void remove()
                    {
                        iterator0.remove();
                        iterator1.remove();
                    }
                };
            }
        };
    }

    /**
     * Returns a list of the left elements of a list of pairs.
     */
    public static <L, R> List<L> left(final List<Pair<L, R>> list) {
        return new AbstractList<L>() {
            public L get(int index) {
                return list.get(index).left;
            }

            public int size() {
                return list.size();
            }

            public L remove(int index) {
                Pair<L, R> pair = list.remove(index);
                return pair == null ? null : pair.left;
            }
        };
    }

    /**
     * Returns a list of the right elements of a list of pairs.
     */
    public static <L, R> List<R> right(final List<Pair<L, R>> list) {
        return new AbstractList<R>() {
            public R get(int index) {
                return list.get(index).right;
            }

            public int size() {
                return list.size();
            }

            public R remove(int index) {
                Pair<L, R> pair = list.remove(index);
                return pair == null ? null : pair.right;
            }
        };
    }
}

// End Pair.java
