/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.ResultStyle;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.TupleType;

import java.util.*;

/**
 * Definition of the <code>TopCount</code> and <code>BottomCount</code>
 * MDX builtin functions.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class TopBottomCountFunDef extends FunDefBase {
    boolean top;

    static final MultiResolver TopCountResolver = new MultiResolver(
            "TopCount",
            "TopCount(<Set>, <Count>[, <Numeric Expression>])",
            "Returns a specified number of items from the top of a set, optionally ordering the set first.",
            new String[]{"fxxnn", "fxxn"}) {
        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            return new TopBottomCountFunDef(dummyFunDef, true);
        }
    };

    static final MultiResolver BottomCountResolver = new MultiResolver(
            "BottomCount",
            "BottomCount(<Set>, <Count>[, <Numeric Expression>])",
            "Returns a specified number of items from the bottom of a set, optionally ordering the set first.",
            new String[]{"fxxnn", "fxxn"}) {
        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            return new TopBottomCountFunDef(dummyFunDef, false);
        }
    };

    public TopBottomCountFunDef(FunDef dummyFunDef, final boolean top) {
        super(dummyFunDef);
        this.top = top;
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        // Compile the member list expression. Ask for a mutable list, because
        // we're going to sortMembers it later.
        final ListCalc listCalc =
                compiler.compileList(call.getArg(0), true);
        final IntegerCalc integerCalc =
                compiler.compileInteger(call.getArg(1));
        final Calc orderCalc =
                call.getArgCount() > 2 ?
                compiler.compileScalar(call.getArg(2), true) :
                null;
        final int arity = call.getType() instanceof TupleType ?
            ((TupleType) call.getType()).elementTypes.length :
            1;
        return new AbstractListCalc(call, new Calc[] {listCalc, integerCalc, orderCalc}) {
            public List evaluateList(Evaluator evaluator) {
                // Use a native evaluator, if more efficient.
                // TODO: Figure this out at compile time.
                SchemaReader schemaReader = evaluator.getSchemaReader();
                NativeEvaluator nativeEvaluator =
                        schemaReader.getNativeSetEvaluator(
                                call.getFunDef(), call.getArgs(), evaluator, this);
                if (nativeEvaluator != null) {
                    return (List) nativeEvaluator.execute(ResultStyle.LIST);
                }

                List list = listCalc.evaluateList(evaluator);
                int n = integerCalc.evaluateInteger(evaluator);

                if (n == mondrian.olap.fun.FunUtil.IntegerNull) {
                    return new java.util.ArrayList();
                }

                if(orderCalc != null) {
                    if(list.isEmpty()) return list;
                    final Object trial = list.get(0);
                    boolean highCard = false;
                    if(trial instanceof Member) {
                        if(((Member) trial).getHierarchy().getDimension()
                                .isHighCardinality()) {
                            highCard = true;
                        }
                    } else {
                        final Member[] trials = (Member[]) trial;
                        for(int i=0; i<trials.length; i++) {
                            if(trials[i].getHierarchy().getDimension()
                                    .isHighCardinality()) {
                                break;
                            }
                        }
                    }

                    if(highCard) {
                        final List l2 = new ArrayList();
                        final Iterator iterator = list.iterator();
                        for(int i=0; i<n+1 && iterator.hasNext(); i++) {
                            final Object o = iterator.next();
                            l2.add(o);
                        }
                        final Evaluator eval = evaluator.push();
                        if(arity == 1) {
                            for(;;) {
                                sortMembers(
                                    eval,
                                    (List<Member>) l2,
                                    orderCalc, top, true);
                                l2.remove(l2.size()-1);
                                if(!iterator.hasNext()) break;
                                l2.add(iterator.next());
                            }
                            return l2;
                        } else {
                            for(;;) {
                                sortTuples(
                                    eval,
                                    (List<mondrian.olap.Member[]>) l2,
                                    orderCalc, top, true, arity);
                                l2.remove(l2.size()-1);
                                if(!iterator.hasNext()) break;
                                l2.add(iterator.next());
                            }
                            return l2;
                        }
                    } else {
                        if (arity == 1) {
                            sortMembers(
                                evaluator.push(),
                                (List<Member>) list,
                                orderCalc, top, true);
                        } else {
                            sortTuples(
                                evaluator.push(),
                                (List<mondrian.olap.Member[]>) list,
                                orderCalc, top, true, arity);
                        }
                        if (n < list.size()) {
                            list = list.subList(0, n);
                        }
                        return list;
                    }
                } else {
                    if(list instanceof AbstractList && list.size()<n) {
                        return list;
                    } else {
                        return list.subList(0, n);
                    }
                }
            }

            public boolean dependsOn(Dimension dimension) {
                return anyDependsButFirst(getCalcs(), dimension);
            }
        };
    }
}

// End TopBottomCountFunDef.java
