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
