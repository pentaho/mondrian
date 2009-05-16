/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.type.SetType;
import mondrian.calc.Calc;
import mondrian.calc.ResultStyle;

import java.util.*;

/**
 * Evaluation context for a particular named set.
 *
 * @author jhyde
 * @since November 11, 2008
 * @version $Id$
 */
class RolapNamedSetEvaluator
    implements Evaluator.NamedSetEvaluator
{
    private final RolapResult.RolapResultEvaluatorRoot rrer;
    private final String name;
    private final Exp exp;

    /** Value of this named set; set on first use. */
    private List list;

    /**
     * Dummy list used as a marker to detect re-entrant calls to
     * {@link #ensureList}.
     */
    private static final List DUMMY_LIST =
        Collections.unmodifiableList(Arrays.asList(new Object()));

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
     * @param name Name of named set
     * @param exp Expression to evaluate named set
     */
    public RolapNamedSetEvaluator(
        RolapResult.RolapResultEvaluatorRoot rrer,
        String name,
        Exp exp)
    {
        this.rrer = rrer;
        this.name = name;
        this.exp = exp;
    }

    public Iterable<Member> evaluateMemberIterable() {
        ensureList();
        return new Iterable<Member>() {
            public Iterator<Member> iterator() {
                return new Iterator<Member>() {
                    int i = -1;

                    public boolean hasNext() {
                        return i < list.size() - 1;
                    }

                    public Member next() {
                        currentOrdinal = ++i;
                        return (Member) list.get(i);
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public Iterable<Member[]> evaluateTupleIterable() {
        ensureList();
        return new Iterable<Member[]>() {
            public Iterator<Member[]> iterator() {
                return new Iterator<Member[]>() {
                    int i = -1;

                    public boolean hasNext() {
                        return i < list.size() - 1;
                    }

                    public Member[] next() {
                        currentOrdinal = ++i;
                        return (Member[]) list.get(i);
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Evaluates and saves the value of this named set, if it has not been
     * evaluated already.
     *
     * @param <T> Type of member of this set: Member or Member[].
     */
    private <T> void ensureList() {
        if (list != null) {
            if (list == DUMMY_LIST) {
                throw rrer.slicerEvaluator.newEvalException(
                    null,
                    "Illegal attempt to reference value of named set '" +
                        name + "' while evaluating itself");
            }
            return;
        }
        if (RolapResult.LOGGER.isDebugEnabled()) {
            RolapResult.LOGGER.debug(
                "Named set " + name + ": starting evaluation");
        }
        list = DUMMY_LIST; // recursion detection
        final RolapEvaluatorRoot root =
            rrer.slicerEvaluator.root;
        final Calc calc =
            root.getCompiled(exp, false, ResultStyle.ITERABLE);
        Object o =
            rrer.result.evaluateExp(
                calc,
                rrer.slicerEvaluator.push());
        final List<T> rawList;

        // Axes can be in two forms: list or iterable. If iterable, we
        // need to materialize it, to ensure that all cell values are in
        // cache.
        if (o instanceof List) {
            rawList = (List<T>) o;
        } else {
            Iterable<T> iter = (Iterable<T>) o;
            rawList = new ArrayList<T>();
            for (T e : iter) {
                rawList.add(e);
            }
        }
        if (RolapResult.LOGGER.isDebugEnabled()) {
            final StringBuilder buf =
                new StringBuilder(
                    this + ": " +
                    "Named set " + name + " evaluated to:" + Util.nl);
            int arity = ((SetType) calc.getType()).getArity();
            int rowCount = 0;
            final int maxRowCount = 100;
            if (arity == 1) {
                for (Member t : Util.<Member>cast(rawList)) {
                    if (rowCount++ > maxRowCount) {
                        buf.append("...");
                        buf.append(Util.nl);
                        break;
                    }
                    buf.append(t);
                    buf.append(Util.nl);
                }
            } else {
                for (Member[] t : Util.<Member[]>cast(rawList)) {
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
            RolapResult.LOGGER.debug(buf);
        }
        // Wrap list so that currentOrdinal is updated whenever the list
        // is accessed. The list is immutable, because we don't override
        // AbstractList.set(int, Object).
        this.list = new AbstractList<T>() {
            public T get(int index) {
                currentOrdinal = index;
                return rawList.get(index);
            }

            public int size() {
                return rawList.size();
            }
        };
    }

    public int currentOrdinal() {
        return currentOrdinal;
    }

    public Member[] currentTuple() {
        return (Member[]) list.get(currentOrdinal);
    }

    public Member currentMember() {
        return (Member) list.get(currentOrdinal);
    }
}

// End RolapNamedSetEvaluator.java
