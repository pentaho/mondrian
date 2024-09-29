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

import mondrian.calc.*;
import mondrian.olap.Member;

import java.util.Iterator;
import java.util.List;

/**
* Abstract implementation of {@link mondrian.calc.TupleIterable}.
 *
 * <p>Derived classes need to implement only {@link #tupleCursor()},
 * and this implementation will implement {@link #tupleIterator()} and
 * {@link #iterator()} by creating a wrapper around that cursor. (The cursor
 * interface is easier to implement efficiently than the wider iterator
 * interface.) If you have a more efficient implementation of cursor, override
 * the {@code tupleIterator} method.
 *
 * @author jhyde
 */
public abstract class AbstractTupleIterable
    implements TupleIterable
{
    protected final int arity;

    /**
     * Creates an AbstractTupleIterable.
     *
     * @param arity Arity (number of members per tuple)
     */
    public AbstractTupleIterable(int arity) {
        this.arity = arity;
    }

    public int getArity() {
        return arity;
    }

    public Iterable<Member> slice(int column) {
        return TupleCollections.slice(this, column);
    }

    public final Iterator<List<Member>> iterator() {
        return tupleIterator();
    }

    public TupleIterator tupleIterator() {
        return TupleCollections.iterator(tupleCursor());
    }
}

// End AbstractTupleIterable.java
