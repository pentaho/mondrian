/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.calc.impl;

import mondrian.calc.*;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.type.SetType;

/**
 * Adapter which computes a set expression and converts it to any list or
 * iterable type.
 *
 * @author jhyde
 * @since Nov 7, 2008
 */
public abstract class GenericIterCalc
    extends AbstractCalc
    implements ListCalc, IterCalc
{
    /**
     * Creates a GenericIterCalc without specifying child calculated
     * expressions.
     *
     * <p>Subclass should override {@link #getCalcs()}.
     *
     * @param exp Source expression
     */
    protected GenericIterCalc(Exp exp) {
        super(exp, null);
    }

    /**
     * Creates an GenericIterCalc.
     *
     * @param exp Source expression
     * @param calcs Child compiled expressions
     */
    protected GenericIterCalc(Exp exp, Calc[] calcs) {
        super(exp, calcs);
    }

    public SetType getType() {
        return (SetType) type;
    }

    public TupleList evaluateList(Evaluator evaluator) {
        Object o = evaluate(evaluator);
        if (o instanceof TupleList) {
            return (TupleList) o;
        } else {
            // Iterable
            final TupleIterable iterable = (TupleIterable) o;
            TupleList tupleList =
                TupleCollections.createList(iterable.getArity());
            TupleCursor cursor = iterable.tupleCursor();
            while (cursor.forward()) {
                tupleList.addCurrent(cursor);
            }
            return tupleList;
        }
    }

    public TupleIterable evaluateIterable(Evaluator evaluator) {
        Object o = evaluate(evaluator);
        return (TupleIterable) o;
    }
}

// End GenericIterCalc.java
