/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.calc.impl;

import mondrian.calc.TupleIterator;
import mondrian.olap.Member;

import java.util.List;

/**
* Abstract implementation of {@link TupleIterator}.
 *
 * <p>Derived classes need to implement only {@link #forward()}.
 * {@code forward} must set the {@link #current}
 * field, and derived classes can use it.
 *
 * @author jhyde
 */
public abstract class AbstractTupleIterator
    extends AbstractTupleCursor
    implements TupleIterator
{
    protected boolean hasNext;

    public AbstractTupleIterator(int arity) {
        super(arity);
    }

    public boolean hasNext() {
        return hasNext;
    }

    public List<Member> next() {
        List<Member> o = current();
        hasNext = forward();
        return o;
    }

    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

}

// End AbstractTupleIterator.java
