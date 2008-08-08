/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import java.util.*;

/**
 * @author Luis F. Canals
 * @version $Id$
 * @since december, 2007
 */
public class ConcatenableList<T> extends AbstractList<T> {
    private static int nextHashCode = 1000;
    private final List<List<T>> lists;
    private List<T> plainList;
    private final int hashCode = nextHashCode++;
    private Iterator<T> getIterator = null;
    private int previousIndex = -200;
    private T previousElement = null;
    private T prePreviousElement = null;

    public ConcatenableList() {
        this.lists = new ArrayList<List<T>>();
        this.plainList = null;
    }


    /**
     * Performs a load of all elements into memory, removing sequential
     * access advantages.
     */
    public Object[] toArray() {
        if (this.plainList == null) {
            this.plainList = new ArrayList<T>();
            for (final List<T> list : lists) {
                for (final T t : list) {
                    this.plainList.add(t);
                }
            }
        }
        return this.plainList.toArray();
    }

    public boolean addAll(final Collection<? extends T> collection) {
        if (this.plainList == null) {
            final List<T> list = (List<T>) collection;
            return this.lists.add(list);
        } else {
            for (final T e : collection) {
                this.plainList.add(e);
            }
            return true;
        }
    }

    public T get(final int index) {
        if (this.plainList == null) {
            if (index == 0) {
                this.getIterator = this.iterator();
                this.previousIndex = index;
                if (this.getIterator.hasNext()) {
                    this.previousElement = this.getIterator.next();
                    return this.previousElement;
                } else {
                    this.getIterator = null;
                    this.previousIndex = -200;
                    throw new IndexOutOfBoundsException("Index " + index
                            + " out of concatenable list range");
                }
            } else if (this.previousIndex + 1 == index && this.getIterator != null) {
                this.previousIndex = index;
                if (this.getIterator.hasNext()) {
                    this.prePreviousElement = this.previousElement;
                    this.previousElement = this.getIterator.next();
                    return this.previousElement;
                } else {
                    this.getIterator = null;
                    this.previousIndex = -200;
                    throw new IndexOutOfBoundsException("Index " + index
                            + " out of concatenable list range");
                }
            } else if (this.previousIndex == index) {
                return this.previousElement;
            } else if (this.previousIndex - 1 == index) {
                return this.prePreviousElement;
            } else {
                this.previousIndex = -200;
                this.getIterator = null;
                final Iterator<T> it = this.iterator();
                if (!it.hasNext()) {
                    throw new IndexOutOfBoundsException("Index " + index
                            + " out of concatenable list range");
                }
                for (int i = 0; i < index; i++) {
                    if (!it.hasNext()) {
                        throw new IndexOutOfBoundsException("Index " + index
                            + " out of concatenable list range");
                    }
                    this.prePreviousElement = it.next();
                }
                this.previousElement = it.next();
                this.previousIndex = index;
                this.getIterator = it;
                return this.previousElement;
            }
        } else {
            this.previousElement = this.plainList.get(index);
            return this.previousElement;
        }
    }


    /**
     * Adds elements at the end of the list.
     */
    public boolean add(final T t) {
        if (this.plainList == null) {
            return this.addAll(Collections.singletonList(t));
        } else {
            return this.plainList.add(t);
        }
    }

    /**
     * Adds elements at the middle of the list.
     */
    public void add(final int index, final T t) {
        if (this.plainList == null) {
            throw new UnsupportedOperationException();
        } else {
            this.plainList.add(index, t);
        }
    }



    /**
     * Burnt method!
     */
    public T set(final int index, final T t) {
        if (this.plainList == null) {
            throw new UnsupportedOperationException();
        } else {
            return this.plainList.set(index, t);
        }
    }

    public int size() {
        if (this.plainList == null) {
            int size = 0;
            for (final List<T> list : lists) {
                size += list.size();
            }
            return size;
        } else {
            return this.plainList.size();
        }
    }

    public Iterator<T> iterator() {
        if (this.plainList == null) {
            return new Iterator<T>() {
                private final Iterator<List<T>> listsIt=lists.iterator();
                private Iterator<T> currentListIt;

                public boolean hasNext() {
                    if (currentListIt == null) {
                        if (listsIt.hasNext()) {
                            currentListIt = listsIt.next().iterator();
                        } else {
                            return false;
                        }
                    }

                    if (currentListIt.hasNext()) {
                        return true;
                    } else {
                        if (listsIt.hasNext()) {
                            currentListIt = listsIt.next().iterator();
                            return currentListIt.hasNext();
                        } else {
                            return false;
                        }
                    }
                }

                public T next() {
                    if (currentListIt.hasNext()) {
                        return currentListIt.next();
                    } else {
                        currentListIt = listsIt.next().iterator();
                        return currentListIt.next();
                    }
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        } else {
            return this.plainList.iterator();
        }
    }

    public boolean isEmpty() {
        if (this.plainList != null) {
            return this.plainList.isEmpty();
        }
        if (this.lists.isEmpty()) {
            return true;
        } else {
            for (final List<T> l : lists) {
                if (!l.isEmpty()) {
                    return false;
                }
            }
            return true;
        }
    }

    public void clear() {
        this.plainList = null;
        this.lists.clear();
    }

    public int hashCode() {
        return this.hashCode;
    }
}

// End ConcatenableList.java
