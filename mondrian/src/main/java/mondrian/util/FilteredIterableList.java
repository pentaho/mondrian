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
 * Iterable list which filters undesirable elements.
 * To be used instead of removing elements from an iterable list.
 *
 * @author Luis F. Canals
 * @since december, 2007
 */
public class FilteredIterableList<T> extends AbstractSequentialList<T> {
    private List<T> plainList;
    private int size;
    private boolean isEmpty;
    private int lastIndex = 0;
    private int lastGetIndex = -1;
    private T lastGet = null;
    private ListIterator<T> lastListIterator;

    private final List<? extends T> internal;
    private final Filter<T> filter;

    private final Map<Integer, T> cached;

    public FilteredIterableList(
        final List<? extends T> list,
        final Filter filter)
    {
        super();
        this.plainList = null;
        this.filter = filter;
        this.internal = list;
        this.isEmpty = ! this.listIterator(0).hasNext();
        this.size = -1;
        this.cached = new CacheMap<Integer, T>(4);
    }

    public T get(final int index) {
        if (this.plainList != null) {
            return this.plainList.get(index);
        }

        final T t = cached.get(index);
        if (t != null) {
            return cached.get(index);
        } else {
            if (index != this.lastGetIndex || index < 0) {
                this.lastGet = super.get(index);
                if (this.lastGet == null) {
                    throw new IndexOutOfBoundsException();
                }
                this.lastGetIndex = index;
            }
            cached.put(index, this.lastGet);
            return this.lastGet;
        }
    }

    public ListIterator<T> listIterator(final int index) {
        if (this.plainList == null) {
            if (index == this.lastIndex + 1 && this.lastListIterator != null) {
                if (this.lastListIterator.hasNext()) {
                    this.lastIndex = index;
                    return this.lastListIterator;
                } else {
                    throw new IndexOutOfBoundsException();
                }
            } else {
                final Iterator<? extends T> it = internal.iterator();
                T nextTmp = null;
                while (it.hasNext()) {
                    final T n = it.next();
                    if (filter.accept(n)) {
                        nextTmp = n;
                        break;
                    }
                }
                final T first = nextTmp;
                this.lastListIterator = new ListIterator<T>() {
                    private int idx = 0;
                    private T nxt = first;
                    private void postNext() {
                        while (it.hasNext()) {
                            final T n = it.next();
                            if (filter.accept(n)) {
                                nxt = n;
                                return;
                            }
                        }
                        nxt = null;
                    }
                    public boolean hasNext() {
                        return nxt != null;
                    }
                    public T next() {
                        idx++;
                        final T n = nxt;
                        cached.put(idx - 1, n);
                        postNext();
                        return n;
                    }
                    public int nextIndex() {
                        return idx;
                    }
                    public void add(final T t) {
                        throw new UnsupportedOperationException();
                    }
                    public void set(final T t) {
                        throw new UnsupportedOperationException();
                    }
                    public boolean hasPrevious() {
                        throw new UnsupportedOperationException();
                    }
                    public T previous() {
                        throw new UnsupportedOperationException();
                    }
                    public int previousIndex() {
                        throw new UnsupportedOperationException();
                    }
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
                this.lastIndex = 0;
            }

            for (int i = this.lastIndex; i < index; i++) {
                if (!this.lastListIterator.hasNext()) {
                    throw new IndexOutOfBoundsException();
                }
                this.lastListIterator.next();
            }
            this.lastIndex = index;
            return this.lastListIterator;
        } else {
            return plainList.listIterator(index);
        }
    }

    public boolean isEmpty() {
        return this.plainList != null ? this.plainList.isEmpty() : this.isEmpty;
    }

    public int size() {
        if (this.size == -1) {
            int s = this.lastIndex;
            try {
                final ListIterator<T> it = this.listIterator(this.lastIndex);
                while (it.hasNext()) {
                    s++;
                    it.next();
                }
            } catch (IndexOutOfBoundsException ioobe) {
                // Subyacent list is no more present...
            }
            this.size = s;
        }
        this.lastListIterator = null;
        return this.size;
    }

    public Object[] toArray() {
        ensurePlainList();
        return this.plainList.toArray();
    }

    @Override
    public <T> T[] toArray(T[] contents) {
        ensurePlainList();
        return this.plainList.toArray(contents);
    }

    private void ensurePlainList() {
        if (this.plainList == null) {
            final List<T> tmpPlainList = new ArrayList<T>();
            for (final Iterator<T> it = this.listIterator(0); it.hasNext();) {
                tmpPlainList.add(it.next());
            }
            this.plainList = tmpPlainList;
        }
    }

    public int hashCode() {
        return this.filter.hashCode();
    }

/*
    public List<T> subList(final int start, final int end) {
        return new AbstractList<T>() {
            public T get(final int index) {
                return FilteredIterableList.this.get(index-start);
            }
            public int size() {
                return FilteredIterableList.this.size() - start;
            }
        };
    }
*/


    //
    // Inner classes ---------------------------------
    //

    /**
     * Filter to determine which elements should be shown.
     */
    public static interface Filter<T> {
        public boolean accept(final T element);
    }
}

// End FilteredIterableList.java
