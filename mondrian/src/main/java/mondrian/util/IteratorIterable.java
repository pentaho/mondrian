/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

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
