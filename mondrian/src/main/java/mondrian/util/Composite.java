/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.util;

import java.util.*;

/**
 * Composite collections.
 *
 * @author jhyde
 */
public abstract class Composite {

    /**
     * Creates a composite list, inferring the element type from the arguments.
     *
     * @param lists One or more lists
     * @param <T> element type
     * @return composite list
     */
    public static <T> List<T> of(
        List<? extends T>... lists)
    {
        return CompositeList.<T>of(lists);
    }

    /**
     * Creates a composite iterable, inferring the element type from the
     * arguments.
     *
     * @param iterables One or more iterables
     * @param <T> element type
     * @return composite iterable
     */
    public static <T> Iterable<T> of(
        Iterable<? extends T>... iterables)
    {
        return new CompositeIterable<T>(iterables);
    }

    /**
     * Creates a composite list, inferring the element type from the arguments.
     *
     * @param iterators One or more iterators
     * @param <T> element type
     * @return composite list
     */
    public static <T> Iterator<T> of(
        Iterator<? extends T>... iterators)
    {
        final Iterator[] iterators1 = (Iterator[]) iterators;
        return new CompositeIterator<T>(iterators1);
    }

    private static class CompositeIterable<T> implements Iterable<T> {
        private final Iterable<? extends T>[] iterables;

        public CompositeIterable(Iterable<? extends T>[] iterables) {
            this.iterables = iterables;
        }

        public Iterator<T> iterator() {
            return new CompositeIterator(iterables);
        }
    }

    private static class CompositeIterator<T> implements Iterator<T> {
        private final Iterator<Iterator<T>> iteratorIterator;
        private boolean hasNext;
        private T next;
        private Iterator<T> iterator;

        public CompositeIterator(Iterator<T>[] iterables) {
            this.iteratorIterator = Arrays.asList(iterables).iterator();
            this.iterator = Collections.<T>emptyList().iterator();
            this.hasNext = true;
            advance();
        }

        public CompositeIterator(final Iterable<T>[] iterables) {
            this.iteratorIterator =
                new IterableIterator<T>(iterables);
                Arrays.asList(iterables).iterator();
            this.iterator = Collections.<T>emptyList().iterator();
            this.hasNext = true;
            advance();
        }

        private void advance() {
            for (;;) {
                if (iterator.hasNext()) {
                    next = iterator.next();
                    return;
                }
                if (!iteratorIterator.hasNext()) {
                    hasNext = false;
                    break;
                }
                iterator = iteratorIterator.next();
            }
        }

        public boolean hasNext() {
            return hasNext;
        }

        public T next() {
            final T next1 = next;
            advance();
            return next1;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class IterableIterator<T>
        implements Iterator<Iterator<T>>
    {
        private int i;
        private final Iterable<T>[] iterables;

        public IterableIterator(Iterable<T>[] iterables) {
            this.iterables = iterables;
            i = 0;
        }

        public boolean hasNext() {
            return i < iterables.length;
        }

        public Iterator<T> next() {
            return iterables[i++].iterator();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

// End Composite.java
