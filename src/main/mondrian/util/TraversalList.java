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

import java.lang.reflect.Array;
import java.util.*;

/**
 * Implementation of {@link java.util.List} for transposing an array of
 * lists.
 *
 * @author Luis F. Canals
 * @since Dec, 2007
 */
public class TraversalList<T> extends UnsupportedList<List<T>> {
    private boolean asInternalArray = false;
    private List<T>[] internalArray = null;
    private final List<T>[] lists;
    private final Class<T> clazz;
    private final T[] tmpArray; // work space; not threadsafe even for reads

    public TraversalList(
        final List<T>[] lists,
        Class<T> clazz)
    {
        this.lists = lists;
        this.clazz = clazz;
        //noinspection unchecked
        this.tmpArray = (T[]) Array.newInstance(clazz, lists.length);
    }

    public List<T> get(int index) {
        if (this.asInternalArray) {
            return internalArray[index];
        } else {
            for (int i = 0; i < lists.length; i++) {
                tmpArray[i] = lists[i].get(index);
            }
            return Util.flatList(tmpArray.clone());
        }
    }

    public Iterator<List<T>> iterator() {
        return new Iterator<List<T>>() {
            private int currentIndex = 0;
            private List<T> precalculated;

            public List<T> next() {
                if (precalculated != null) {
                    final List<T> t = precalculated;
                    precalculated = null;
                    currentIndex++;
                    return t;
                } else {
                    return get(currentIndex++);
                }
            }

            public boolean hasNext() {
                try {
                    precalculated = get(currentIndex);
                    return true;
                } catch (IndexOutOfBoundsException e) {
                    return false;
                }
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    // Used by Collections.sort
    public ListIterator<List<T>> listIterator(final int index) {
        return new ListItr(index) {
            public void set(final List<T> l) {
                TraversalList.this.set(cursor - 1, l);
            }
        };
    }

    // Used by Collections.sort
    public ListIterator<List<T>> listIterator() {
        return new ListItr(0) {
            public void set(final List<T> l) {
                TraversalList.this.set(cursor - 1, l);
            }
        };
    }

    public int size() {
        return lists[0].size();
    }

    public List<List<T>> subList(final int first, final int last) {
        return new AbstractList<List<T>>() {
            public List<T> get(int index) {
                return TraversalList.this.get(index + first);
            }
            public int size() {
                return last - first;
            }
        };
    }

    private List<T>[] materialize(List<T>[] a) {
        final List<T>[] array;
        if (a != null
            && a.length == size()
            && a.getClass().getComponentType() == clazz)
        {
            array = a;
        } else {
            //noinspection unchecked
            array = (List<T>[]) new List[this.size()];
        }
        int k = 0;
        for (List<T> x : this) {
            array[k++] = x;
        }
        this.asInternalArray = true;
        this.internalArray = array;
        return array;
    }

    @Override
    public <S> S[] toArray(S[] a) {
        // Our requirements are stronger than the general toArray(T[] a)
        // contract. We will use the user's array 'a' only if it is PRECISELY
        // the right type and size; otherwise we will allocate our own array.
        //noinspection unchecked
        return (S[]) materialize((List<T>[]) a);
    }

    public Object[] toArray() {
        return materialize(null);
    }

    // Used by Collections.sort
    public List<T> set(final int index, List<T> l) {
        if (this.asInternalArray) {
            final List<T> previous = this.internalArray[index];
            this.internalArray[index] = l;
            return previous;
        } else {
            throw new UnsupportedOperationException();
        }
    }
}

// End TraversalList.java
