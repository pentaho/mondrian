/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

/**
 * Definition of the <code>DrilldownMember</code> MDX function.
 *
 * @author Grzegorz Lojek
 * @since 6 December, 2004
 * @version $Id$
 */
class DrilldownMemberFunDef extends FunDefBase {
    static final String[] reservedWords = new String[] {"RECURSIVE"};
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
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

        return new AbstractListCalc(call, new Calc[] {listCalc1, listCalc2}) {
            public List evaluateList(Evaluator evaluator) {
                final List<?> list1 = listCalc1.evaluateList(evaluator);
                final List<Member> list2 = listCalc2.evaluateList(evaluator);
                return drilldownMember(list1, list2, evaluator);
            }

            /**
             * Drills down an element.
             *
             * Algorithm: If object is present in a_hsSet1 then adds to result children
             * of the object. If flag a_bRecursive is set then this method is called
             * recursively for the children.
             *
             * @param element Element of a set, can be either {@link Member} or
             *   {@link Member}[]
             *
             *
             */
            protected void drillDownObj(
                    Evaluator evaluator,
                    Object element,
                    Set memberSet,
                    List<Object> resultList) {
                if (null == element) {
                    return;
                }

                Member m = null;
                Member[] tuple;
                int k = -1;
                if (element instanceof Member) {
                    m = (Member) element;
                    if (!memberSet.contains(m)) {
                        return;
                    }
                    tuple = null;
                } else {
                    Util.assertTrue(element instanceof Member[]);
                    tuple = (Member[]) element;
                    m = null;
                    for (Member member : tuple) {
                        ++k;
                        if (memberSet.contains(member)) {
                            m = member;
                            break;
                        }
                    }
                    if (m == null) {
                        //not found
                        return;
                    }
                }

                List<Member> children = evaluator.getSchemaReader().getMemberChildren(m);
                for (Member member : children) {
                    Object objNew;
                    if (tuple == null) {
                        objNew = member;
                    } else {
                        Member[] members = tuple.clone();
                        members[k] = member;
                        objNew = members;
                    }

                    resultList.add(objNew);
                    if (recursive) {
                        drillDownObj(evaluator, objNew, memberSet, resultList);
                    }
                }
            }

            private List drilldownMember(List<?> v0, List<Member> v1, Evaluator evaluator) {
                if (null == v0 ||
                    v0.isEmpty() ||
                    null == v1 ||
                    v1.isEmpty()) {
                    return v0;
                }

                Set<Member> set1 = new HashSet<Member>();
                set1.addAll(v1);

                List<Object> result = new ArrayList<Object>();
                int i = 0, n = v0.size();
                while (i < n) {
                    Object o = v0.get(i++);
                    result.add(o);
                    drillDownObj(evaluator, o, set1, result);
                }
                return result;
            }
        };
    }
}

// End DrilldownMemberFunDef.java
