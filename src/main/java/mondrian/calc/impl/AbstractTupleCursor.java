/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.calc.impl;

import mondrian.calc.TupleCursor;
import mondrian.olap.Evaluator;
import mondrian.olap.Member;

/**
 * Abstract implementation of {@link mondrian.calc.TupleIterator}.
 *
 * <p>Derived classes need to implement only {@link #forward()}.
 *
 * @author jhyde
 */
public abstract class AbstractTupleCursor implements TupleCursor {
    protected final int arity;

    public AbstractTupleCursor(int arity) {
        super();
        this.arity = arity;
    }

    public void setContext(Evaluator evaluator) {
        evaluator.setContext(current());
    }

    public void currentToArray(Member[] members, int offset) {
        if (offset == 0) {
            current().toArray(members);
        } else {
            //noinspection SuspiciousSystemArraycopy
            System.arraycopy(current().toArray(), 0, members, offset, arity);
        }
    }

    public int getArity() {
        return arity;
    }

    public Member member(int column) {
        return current().get(column);
    }
}

// End AbstractTupleCursor.java
