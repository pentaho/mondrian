/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import java.util.*;

/**
 * Iterable over an iterator.
 *
 * <p>It can be restarted. As you iterate, it stores elements in a backing
 * array. If you call {@link #iterator()} again, it will first replay elements
 * from that array.</p>
 */
public class IteratorIterable<E> implements Iterable<E> {
    private final List<E> list = new ArrayList<E>();
    private final Iterator<E> recordingIterator;

    /** Creates an IteratorIterable. */
    public IteratorIterable(final Iterator<E> iterator) {
        this.recordingIterator =
            new Iterator<E>() {
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                public E next() {
                    final E e = iterator.next();
                    list.add(e);
                    return e;
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
    }

    public Iterator<E> iterator() {
        // Return an iterator over the union of (1) the list, (2) the rest
        // of the iterator. The second part writes elements to the list as
        // it returns them.
        //noinspection unchecked
        return Composite.of(
            // Can't use ArrayList.iterator(). It throws
            // ConcurrentModificationException, because the list is growing
            // under its feet.
            new Iterator<E>() {
                int i = 0;

                public boolean hasNext() {
                    return i < list.size();
                }

                public E next() {
                    return list.get(i++);
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            },
            recordingIterator);
    }
}

// End IteratorIterable.java