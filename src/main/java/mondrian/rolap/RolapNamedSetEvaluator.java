/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.*;
import mondrian.olap.*;

import java.util.List;

/**
 * Evaluation context for a particular named set.
 *
 * @author jhyde
 * @since November 11, 2008
 */
class RolapNamedSetEvaluator
    implements Evaluator.NamedSetEvaluator, TupleList.PositionCallback
{
    private final RolapResult.RolapResultEvaluatorRoot rrer;
    private final NamedSet namedSet;

    /** Value of this named set; set on first use. */
    private TupleList list;

    /**
     * Dummy list used as a marker to detect re-entrant calls to
     * {@link #ensureList}.
     */
    private static final TupleList DUMMY_LIST =
        TupleCollections.createList(1);

    /**
     * Ordinal of current iteration through the named set. Used to implement
     * the &lt;Named Set&gt;.CurrentOrdinal and &lt;Named Set&gt;.Current
     * functions.
     */
    private int currentOrdinal;

    /**
     * Creates a RolapNamedSetEvaluator.
     *
     * @param rrer Evaluation root context
     * @param namedSet Named set
     */
    public RolapNamedSetEvaluator(
        RolapResult.RolapResultEvaluatorRoot rrer,
        NamedSet namedSet)
    {
        this.rrer = rrer;
        this.namedSet = namedSet;
    }

    public TupleIterable evaluateTupleIterable() {
        ensureList();
        return list;
    }

    /**
     * Evaluates and saves the value of this named set, if it has not been
     * evaluated already.
     */
    private void ensureList() {
        if (list != null) {
            if (list == DUMMY_LIST) {
                throw rrer.result.slicerEvaluator.newEvalException(
                    null,
                    "Illegal attempt to reference value of named set '"
                    + namedSet.getName() + "' while evaluating itself");
            }
            return;
        }
        if (RolapResult.LOGGER.isDebugEnabled()) {
            RolapResult.LOGGER.debug(
                "Named set " + namedSet.getName() + ": starting evaluation");
        }
        list = DUMMY_LIST; // recursion detection
        try {
            final Calc calc =
                rrer.getCompiled(
                    namedSet.getExp(), false, ResultStyle.ITERABLE);
            TupleIterable iterable =
                (TupleIterable)
                    rrer.result.evaluateExp(
                        calc,
                        rrer.result.slicerEvaluator);

            // Axes can be in two forms: list or iterable. If iterable, we
            // need to materialize it, to ensure that all cell values are in
            // cache.
            final TupleList rawList;
            if (iterable instanceof TupleList) {
                rawList = (TupleList) iterable;
            } else {
                rawList = TupleCollections.createList(iterable.getArity());
                TupleCursor cursor = iterable.tupleCursor();
                while (cursor.forward()) {
                    rawList.addCurrent(cursor);
                }
            }
            if (RolapResult.LOGGER.isDebugEnabled()) {
                RolapResult.LOGGER.debug(generateDebugMessage(calc, rawList));
            }
            // Wrap list so that currentOrdinal is updated whenever the list
            // is accessed. The list is immutable, because we don't override
            // AbstractList.set(int, Object).
            this.list = rawList.withPositionCallback(this);
        } finally {
            if (this.list == DUMMY_LIST) {
                this.list = null;
            }
        }
    }

    private String generateDebugMessage(Calc calc, TupleList rawList) {
        final StringBuilder buf = new StringBuilder();
        buf.append(this);
        buf.append(": ");
        buf.append("Named set ");
        buf.append(namedSet.getName());
        buf.append(" evaluated to:");
        buf.append(Util.nl);
        int arity = calc.getType().getArity();
        int rowCount = 0;
        final int maxRowCount = 100;
        if (arity == 1) {
            for (Member t : rawList.slice(0)) {
                if (rowCount++ > maxRowCount) {
                    buf.append("...");
                    buf.append(Util.nl);
                    break;
                }
                buf.append(t);
                buf.append(Util.nl);
            }
        } else {
            for (List<Member> t : rawList) {
                if (rowCount++ > maxRowCount) {
                    buf.append("...");
                    buf.append(Util.nl);
                    break;
                }
                int k = 0;
                for (Member member : t) {
                    if (k++ > 0) {
                        buf.append(", ");
                    }
                    buf.append(member);
                }
                buf.append(Util.nl);
            }
        }
        return buf.toString();
    }

    public int currentOrdinal() {
        return currentOrdinal;
    }

    public void onPosition(int index) {
        this.currentOrdinal = index;
    }

    public Member[] currentTuple() {
        final List<Member> tuple = list.get(currentOrdinal);
        return tuple.toArray(new Member[tuple.size()]);
    }

    public Member currentMember() {
        return list.get(0, currentOrdinal);
    }
}

// End RolapNamedSetEvaluator.java
