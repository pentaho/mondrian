/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.*;

/**
 * Definition of the <code>DrilldownMember</code> MDX function.
 *
 * @author Grzegorz Lojek
 * @since 6 December, 2004
 */
class DrilldownMemberFunDef extends FunDefBase {
    static final String[] reservedWords = new String[] {"RECURSIVE"};
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "DrilldownMember",
            "DrilldownMember(<Set1>, <Set2>[, RECURSIVE])",
            "Drills down the members in a set that are present in a second specified set.",
            new String[]{"fxxx", "fxxxy"},
            DrilldownMemberFunDef.class,
            reservedWords);

    public DrilldownMemberFunDef(FunDef funDef) {
        super(funDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc2 = compiler.compileList(call.getArg(1));
        final String literalArg = getLiteralArg(call, 2, "", reservedWords);
        final boolean recursive = literalArg.equals("RECURSIVE");

        return new AbstractListCalc(
            call,
            new Calc[] {listCalc1, listCalc2})
        {
            public TupleList evaluateList(Evaluator evaluator) {
                final TupleList list1 = listCalc1.evaluateList(evaluator);
                final TupleList list2 = listCalc2.evaluateList(evaluator);
                return drilldownMember(list1, list2, evaluator);
            }

            /**
             * Drills down an element.
             *
             * <p>Algorithm: If object is present in {@code memberSet} adds to
             * result children of the object. If flag {@code recursive} is set
             * then this method is called recursively for the children.
             *
             * @param evaluator Evaluator
             * @param tuple Tuple (may have arity 1)
             * @param memberSet Set of members
             * @param resultList Result
             */
            protected void drillDownObj(
                Evaluator evaluator,
                Member[] tuple,
                Set<Member> memberSet,
                TupleList resultList)
            {
                for (int k = 0; k < tuple.length; k++) {
                    Member member = tuple[k];
                    if (memberSet.contains(member)) {
                        List<Member> children =
                            evaluator.getSchemaReader().getMemberChildren(
                                member);
                        final Member[] tuple2 = tuple.clone();
                        for (Member childMember : children) {
                            tuple2[k] = childMember;
                            resultList.addTuple(tuple2);
                            if (recursive) {
                                drillDownObj(
                                    evaluator, tuple2, memberSet, resultList);
                            }
                        }
                        break;
                    }
                }
            }

            private TupleList drilldownMember(
                TupleList v0,
                TupleList v1,
                Evaluator evaluator)
            {
                assert v1.getArity() == 1;
                if (v0.isEmpty() || v1.isEmpty()) {
                    return v0;
                }

                Set<Member> set1 = new HashSet<Member>(v1.slice(0));

                TupleList result = TupleCollections.createList(v0.getArity());
                int i = 0, n = v0.size();
                final Member[] members = new Member[v0.getArity()];
                while (i < n) {
                    List<Member> o = v0.get(i++);
                    o.toArray(members);
                    result.add(o);
                    drillDownObj(evaluator, members, set1, result);
                }
                return result;
            }
        };
    }
}

// End DrilldownMemberFunDef.java
