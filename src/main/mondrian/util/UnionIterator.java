/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2008-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import mondrian.olap.Util;

import java.util.*;

/**
 * Iterator over union of several {@link Iterable} collections.
 *
 * @see mondrian.olap.Util#union(Iterable[])
 *
 * @author jhyde
 * @version $Id$
 * @since Apr 28, 2008
 */
public class UnionIterator<T> implements Iterator<T> {
    private final Iterator<Iterable<? extends T>> iterableIterator;
    private Iterator<? extends T> iterator;

    /**
     * Creates a UnionIterator.
     *
     * @param iterables Array of iterables
     */
    public UnionIterator(Iterable<? extends T>... iterables) {
        List<Iterable<? extends T>> list;
        if (Util.PreJdk15) {
            // Retroweaver has its own version of Iterable, but
            // Collection doesn't implement it. Solve the problem by
            // creating an explicit Iterable wrapper.
            list = new ArrayList<Iterable<? extends T>>(iterables.length);
            for (Iterable<? extends T> iterable : iterables) {
                //noinspection unchecked
                list.add(new MyIterable(iterable));
            }
        } else {
            list = Arrays.asList(iterables);
        }
        this.iterableIterator = list.iterator();
        moveToNext();
    }

    public UnionIterator(Collection<? extends T>... iterables) {
        List<Iterable<? extends T>> list =
            new ArrayList<Iterable<? extends T>>(iterables.length);
        for (Iterable<? extends T> iterable : iterables) {
            //noinspection unchecked
            list.add(new MyIterable(iterable));
        }
        this.iterableIterator = list.iterator();
        moveToNext();
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    public T next() {
        final T t = iterator.next();
        if (!iterator.hasNext()) {
            moveToNext();
        }
        return t;
    }

    /**
     * Moves to the next iterator that has at least one element.
     * Called after finishing an iterator, or at the start.
     */
    private void moveToNext() {
        do {
            if (iterableIterator.hasNext()) {
                iterator = iterableIterator.next().iterator();
            } else {
                iterator = Collections.<T>emptyList().iterator();
                break;
            }
        } while (!iterator.hasNext());
    }

    public void remove() {
        iterator.remove();
    }

    private static class MyIterable<T> implements Iterable {
        private final Iterable<T> iterable;

        /**
         * Creates a MyIterable.
         *
         * @param iterable Iterable
         */
        public MyIterable(Iterable<T> iterable) {
            this.iterable = iterable;
        }

        public Iterator<T> iterator() {
            return iterable.iterator();
        }
    }
}

// End UnionIterator.java
