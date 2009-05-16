/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import java.util.*;
import java.lang.reflect.Array;

/**
 * Implementation of {@link java.util.List} for transposing an array of
 * lists.
 *
 * @author Luis F. Canals
 * @version $Id$
 * @since Dec, 2007
 */
public class TraversalList<T> extends UnsupportedList<T[]> {
    private boolean asInternalArray = false;
    private T[][] internalArray = null;
    private final List<T>[] lists;
    private final Class<T> clazz;

    public TraversalList(
        final List<T>[] lists,
        Class<T> clazz)
    {
        this.lists = lists;
        this.clazz = clazz;
    }

    public T[] get(int index) {
        if (this.asInternalArray) {
            return internalArray[index];
        } else {
            final T[] tuples = (T[]) Array.newInstance(clazz, lists.length);
            for (int i = 0; i < lists.length; i++) {
                tuples[i] = lists[i].get(index);
            }
            return tuples;
        }
    }

    public Iterator<T[]> iterator() {
        return new Iterator<T[]>() {
            private int currentIndex = 0;
            private T[] precalculated;

            public T[] next() {
                if (precalculated != null) {
                    final T[] t = precalculated;
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
    public ListIterator<T[]> listIterator(final int index) {
        return new ListItr(index) {
            public void set(final T[] l) {
                TraversalList.this.set(cursor - 1, l);
            }
        };
    }

    // Used by Collections.sort
    public ListIterator<T[]> listIterator() {
        return new ListItr(0) {
            public void set(final T[] l) {
                TraversalList.this.set(cursor - 1, l);
            }
        };
    }

    public int size() {
        return lists[0].size();
    }

    public List<T[]> subList(final int first, final int last) {
        return new AbstractList<T[]>() {
            public T[] get(int index) {
                return TraversalList.this.get(index + first);
            }
            public int size() {
                return last - first;
            }
        };
    }

    private T[][] materialize(Object[][] a) {
        final T[][] array;
        if (a != null
            && a.length == size()
            && a.getClass().getComponentType() == clazz) {
            array = (T[][]) a;
        } else {
            // TODO: use reflection to create a real T[][]
            array = (T[][]) new Object[this.size()][];
        }
        int k = 0;
        for (T[] x : this) {
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
        return (S[]) materialize((Object[][]) a);
    }

    public Object[] toArray() {
        return materialize(null);
    }

    // Used by Collections.sort
    public T[] set(final int index, T[] l) {
        if (this.asInternalArray) {
            final T[] previous = this.internalArray[index];
            this.internalArray[index] = l;
            return previous;
        } else {
            throw new UnsupportedOperationException();
        }
    }
}

// End TraversalList.java
