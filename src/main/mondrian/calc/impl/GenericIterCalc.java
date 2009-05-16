/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.olap.type.SetType;
import mondrian.calc.*;

import java.util.*;

/**
 * Adapter which computes a set expression and converts it to any list or
 * iterable type.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 7, 2008
 */
public abstract class GenericIterCalc
    extends AbstractCalc
    implements ListCalc, MemberListCalc, TupleListCalc,
    IterCalc, TupleIterCalc, MemberIterCalc
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

    public List evaluateList(Evaluator evaluator) {
        Object o = evaluate(evaluator);
        if (o instanceof List) {
            return (List) o;
        } else {
            // Iterable
            final Iterable iter = (Iterable) o;
            Iterator it = iter.iterator();
            List<Object> list = new ArrayList<Object>();
            while (it.hasNext()) {
                list.add(it.next());
            }
            return list;
        }
    }
    @SuppressWarnings({"unchecked"})
    public final List<Member> evaluateMemberList(Evaluator evaluator) {
        return (List<Member>) evaluateList(evaluator);
    }

    @SuppressWarnings({"unchecked"})
    public final List<Member[]> evaluateTupleList(Evaluator evaluator) {
        return (List<Member[]>) evaluateList(evaluator);
    }

    public Iterable evaluateIterable(Evaluator evaluator) {
        Object o = evaluate(evaluator);
        if (o instanceof Iterable) {
            return (Iterable) o;
        } else {
            final List list = (List) o;
            // for java4 must convert List into an Iterable
            return new Iterable() {
                public Iterator iterator() {
                    return list.iterator();
                }
            };
        }
    }

    @SuppressWarnings({"unchecked"})
    public Iterable<Member> evaluateMemberIterable(Evaluator evaluator) {
        return (Iterable<Member>) evaluateIterable(evaluator);
    }

    @SuppressWarnings({"unchecked"})
    public Iterable<Member[]> evaluateTupleIterable(Evaluator evaluator) {
        return (Iterable<Member[]>) evaluateIterable(evaluator);
    }
}

// End GenericIterCalc.java
