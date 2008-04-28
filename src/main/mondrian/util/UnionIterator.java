/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2008-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

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

    public UnionIterator(Iterable<? extends T>... iterables) {
        this.iterableIterator = Arrays.asList(iterables).iterator();
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
}

// End UnionIterator.java
