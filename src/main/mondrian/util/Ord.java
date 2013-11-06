/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import java.util.*;

/**
 * Pair of an element and an ordinal.
 *
 * @see Pair
 */
public class Ord<E> implements Map.Entry<Integer, E> {
    public final int i;
    public final E e;

    /**
     * Creates an Ord.
     */
    public Ord(int i, E e) {
        this.i = i;
        this.e = e;
    }

    /**
     * Creates an Ord.
     */
    public static <E> Ord<E> of(int n, E e) {
        return new Ord<E>(n, e);
    }

    /**
     * Creates an iterable of {@code Ord}s over an iterable.
     */
    public static <E> Iterable<Ord<E>> zip(final Iterable<E> iterable) {
        return new Iterable<Ord<E>>() {
            public Iterator<Ord<E>> iterator() {
                return zip(iterable.iterator());
            }
        };
    }

    /**
     * Creates an iterator of {@code Ord}s over an iterator.
     */
    public static <E> Iterator<Ord<E>> zip(final Iterator<E> iterator) {
        return new Iterator<Ord<E>>() {
            int n = 0;

            public boolean hasNext() {
                return iterator.hasNext();
            }

            public Ord<E> next() {
                return Ord.of(n++, iterator.next());
            }

            public void remove() {
                iterator.remove();
            }
        };
    }

    /**
     * Returns a numbered list based on an array.
     */
    public static <E> List<Ord<E>> zip(final E[] elements) {
        return zip(Arrays.asList(elements));
    }

    /**
     * Returns a numbered list.
     */
    public static <E> List<Ord<E>> zip(final List<E> elements) {
        return new AbstractList<Ord<E>>() {
            public Ord<E> get(int index) {
                return of(index, elements.get(index));
            }

            public int size() {
                return elements.size();
            }
        };
    }

    public Integer getKey() {
        return i;
    }

    public E getValue() {
        return e;
    }

    public E setValue(E value) {
        throw new UnsupportedOperationException();
    }
}

// End Ord.java
