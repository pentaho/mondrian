/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.Util;

import java.io.Serializable;
import java.util.*;

/**
 * Implementation of {@link java.util.SortedSet} based on an array. The array
 * must already be sorted in natural order.
 *
 * @param <E>
 *
 * @author Julian Hyde
 */
public class ArraySortedSet<E extends Comparable<E>>
    extends AbstractSet<E>
    implements SortedSet<E>, Serializable
{
    private static final long serialVersionUID = -7613058579094914399L;
    private final E[] values;
    private final int start;
    private final int end;

    /**
     * Creates a set backed by an array. The array must be sorted, and is
     * not copied.
     *
     * @param values Array of values
     */
    public ArraySortedSet(E[] values) {
        this(values, 0, values.length);
    }

    /**
     * Creates a set backed by a region of an array. The array must be
     * sorted, and is not copied.
     *
     * @param values Array of values
     * @param start Index of start of region
     * @param end Index of first element after end of region
     */
    public ArraySortedSet(E[] values, int start, int end) {
        super();
        this.values = values;
        this.start = start;
        this.end = end;
    }

    public Iterator<E> iterator() {
        return asList().iterator();
    }

    public int size() {
        return end - start;
    }

    public Comparator<? super E> comparator() {
        return null;
    }

    public SortedSet<E> subSet(E fromElement, E toElement) {
        int from = Util.binarySearch(values, start, end, fromElement);
        if (from < 0) {
            from = - (from + 1);
        }
        int to = Util.binarySearch(values, from, end, toElement);
        if (to < 0) {
            to = - (to + 1);
        }
        return subset(from, to);
    }

    public SortedSet<E> headSet(E toElement) {
        int to = Util.binarySearch(values, start, end, toElement);
        if (to < 0) {
            to = - (to + 1);
        }
        return subset(start, to);
    }

    public SortedSet<E> tailSet(E fromElement) {
        int from = Util.binarySearch(values, start, end, fromElement);
        if (from < 0) {
            from = - (from + 1);
        }
        return subset(from, end);
    }

    private SortedSet<E> subset(int from, int to) {
        if (from == start && to == end) {
            return this;
        }
        return new ArraySortedSet<E>(values, from, to);
    }

    private List<E> asList() {
        //noinspection unchecked
        List<E> list = Arrays.asList(values);
        if (start > 0 || end < values.length) {
            list = list.subList(start, end);
        }
        return list;
    }

    public E first() {
        try {
            return values[start];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException();
        }
    }

    public E last() {
        try {
            return values[end - 1];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException();
        }
    }

    @Override
    public Object[] toArray() {
        if (start == 0 && end == values.length) {
            return values.clone();
        } else {
            final Object[] os = new Object[end - start];
            System.arraycopy(values, start, os, 0, end - start);
            return os;
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public <T> T[] toArray(T[] a) {
        int size = size();
        T[] r = a.length >= size
            ? a
            : (T[]) java.lang.reflect.Array.newInstance(
                a.getClass().getComponentType(), size);
        //noinspection SuspiciousSystemArraycopy
        System.arraycopy(values, start, r, 0, end - start);
        if (r.length > size) {
            r[size] = null;
        }
        return r;
    }

    /**
     * Performs a merge between two {@link ArraySortedSet} instances
     * in O(n) time, returning a third instance that doesn't include
     * duplicates.
     *
     * <p>For example,
     * <tt>ArraySortedSet("a", "b", "c").merge(ArraySortedSet("a", "c",
     * "e"))</tt> returns
     * <tt>ArraySortedSet("a", "b", "c", "e")}</tt>.</p>
     *
     * @param arrayToMerge Other set to combine with this
     * @return Set containing union of the elements of inputs
     *
     * @see Util#intersect(java.util.SortedSet, java.util.SortedSet)
     */
    public ArraySortedSet<E> merge(
        ArraySortedSet<E> arrayToMerge)
    {
        assert arrayToMerge != null;

        // No need to merge when one array is empty.
        if (this.size() == 0) {
            return arrayToMerge;
        }
        if (arrayToMerge.size() == 0) {
            return this;
        }

        int p1 = 0, p2 = 0, m = 0, k = this.size() + arrayToMerge.size();

        final E[] data1 = this.values;
        final E[] data2 = arrayToMerge.values;
        @SuppressWarnings({"unchecked"})
        E[] merged =
            Util.genericArray(
                (Class<E>) this.values[0].getClass(),
                k);

        while (p1 < this.size() && p2 < arrayToMerge.size()) {
            final int compare =
                data1[p1].compareTo(data2[p2]);
            if (compare == 0) {
                merged[m++] = data1[p1++];
                p2++;
            } else if (compare < 0) {
                merged[m++] = data1[p1++];
            } else {
                merged[m++] = data2[p2++];
            }
        }

        while (p1 < this.size()) {
            merged[m++] = data1[p1++];
        }

        while (p2 < arrayToMerge.size()) {
            merged[m++] = data2[p2++];
        }

        // Note that m < k if there were duplicates. Result has fewer elements
        // than sum of inputs. But it's not worth truncating the array.
        return new ArraySortedSet<E>(merged, 0, m);
    }

    @Override
    public boolean contains(Object o) {
        //noinspection unchecked
        return o != null
            && Util.binarySearch(values, start, end, (E) o) >= 0;
    }
}

// End ArraySortedSet.java
