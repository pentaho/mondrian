/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.TypeUtil;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Definition of the <code>Generate</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class GenerateFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
            "Generate",
            "Generate(<Set1>, <Set2>[, ALL])",
            "Applies a set to each member of another set and joins the resulting sets by union.",
            new String[] {"fxxx", "fxxxy"},
            GenerateFunDef.class);
    private static final String[] ReservedWords = new String[] {"ALL"};

    public GenerateFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        final Type type = args[1].getType();
        final Type memberType = TypeUtil.toMemberOrTupleType(type);
        return new SetType(memberType);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc2 = compiler.compileList(call.getArg(1));
        final String literalArg = getLiteralArg(call, 2, "", ReservedWords);
        final boolean all = literalArg.equalsIgnoreCase("ALL");
        final boolean tuple = ((SetType) listCalc1.getType()).getElementType()
                instanceof TupleType;

        return new AbstractListCalc(call, new Calc[] {listCalc1, listCalc2}) {
            public List evaluateList(Evaluator evaluator) {
                final List list1 = listCalc1.evaluateList(evaluator);
                final Evaluator evaluator2 = evaluator.push();
                List result = new ArrayList();
                if (tuple) {
                    if (all) {
                        for (Object aList1 : list1) {
                            final Member[] members = (Member[]) aList1;
                            evaluator2.setContext(members);
                            final List result2 = listCalc2.evaluateList(
                                evaluator2);
                            result.addAll(result2);
                        }
                    } else {
                        final Set<Object> emitted = new HashSet<Object>();
                        for (Object aList1 : list1) {
                            final Member[] members = (Member[]) aList1;
                            evaluator2.setContext(members);
                            final List result2 = listCalc2.evaluateList(
                                evaluator2);
                            for (Object row : result2) {
                                if (emitted.add(row)) {
                                    result.add(row);
                                }
                            }
                        }
                    }
                } else {
                    if (all) {
                        for (Object aList1 : list1) {
                            final Member member = (Member) aList1;
                            evaluator2.setContext(member);
                            final List result2 = listCalc2.evaluateList(
                                evaluator2);
                            result.addAll(result2);
                        }
                    } else {
                        Set<Object> emitted = new HashSet<Object>();
                        for (Object aList1 : list1) {
                            final Member member = (Member) aList1;
                            evaluator2.setContext(member);
                            final List result2 = listCalc2.evaluateList(
                                evaluator2);
                            for (Object row : result2) {
                                if (emitted.add(row)) {
                                    result.add(row);
                                }
                            }
                        }
                    }
                }
                return result;
            }

            public boolean dependsOn(Dimension dimension) {
                return anyDependsButFirst(getCalcs(), dimension);
            }
        };
    }
}

// End GenerateFunDef.java
