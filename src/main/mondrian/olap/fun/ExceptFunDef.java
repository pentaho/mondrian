/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.util.FilteredIterableList;

import java.util.*;

/**
 * Definition of the <code>Except</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class ExceptFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
            "Except",
            "Except(<Set1>, <Set2>[, ALL])",
            "Finds the difference between two sets, optionally retaining duplicates.",
            new String[]{"fxxx", "fxxxy"},
            ExceptFunDef.class);

    public ExceptFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        // todo: implement ALL
        final ListCalc listCalc0 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc1 = compiler.compileList(call.getArg(1));
        final Type elementType = ((SetType) listCalc0.getType()).getElementType();
        if (elementType instanceof TupleType) {
            final TupleListCalc tupleListCalc0 = (TupleListCalc) listCalc0;
            final TupleListCalc tupleListCalc1 = (TupleListCalc) listCalc1;
            return new AbstractListCalc(call, new Calc[] {listCalc0, listCalc1}) {
                public List evaluateList(Evaluator evaluator) {
                    List<Member[]> list0 =
                        tupleListCalc0.evaluateTupleList(evaluator);
                    if (list0.isEmpty()) {
                        return list0;
                    }
                    List<Member[]> list1 = tupleListCalc1.evaluateTupleList(evaluator);
                    return exceptTuples(list0, list1);
                }
            };
        } else {
            final MemberListCalc memberListCalc0 = (MemberListCalc) listCalc0;
            final MemberListCalc memberListCalc1 = (MemberListCalc) listCalc1;
            return new AbstractListCalc(call, new Calc[] {listCalc0, listCalc1}) {
                public List evaluateList(Evaluator evaluator) {
                    List<Member> list0 = memberListCalc0.evaluateMemberList(evaluator);
                    if (list0.isEmpty()) {
                        return list0;
                    }
                    List<Member> list1 = memberListCalc1.evaluateMemberList(evaluator);
                    return except(list0, list1);
                }
            };
        }
    }

    <T> List<T> except(final List<T> list0, final List<T> list1) {
        if (list0.size() == 0) {
            return list0;
        }
        final Set<T> set = new HashSet<T>(list1);
        return new FilteredIterableList<T>(
                list0,
                new FilteredIterableList.Filter<T>() {
                    public boolean accept(final T o) {
                        return !set.contains(o);
                    }
                });
    }

    List exceptTuples(final List<Member[]> list0, final List<Member[]> list1) {
        if (list0.size() == 0) {
            return list0;
        }
        // Because the .equals and .hashCode methods of
        // Member[] use identity, wrap each tuple in a list.
        final Set<List<Member>> set = new HashSet<List<Member>>();
        for (Member[] members : list1) {
            set.add(Arrays.asList(members));
        }
        return new FilteredIterableList<Member[]>(
                list0,
                new FilteredIterableList.Filter<Member[]>() {
                    public boolean accept(final Member[] o) {
                        return !set.contains(Arrays.asList(o));
                    }
                });
    }
}

// End ExceptFunDef.java
