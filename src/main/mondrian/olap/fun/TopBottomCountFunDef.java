/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.ResultStyle;
import mondrian.calc.impl.DelegatingTupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

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

    static final MultiResolver TopCountResolver =
        new MultiResolver(
            "TopCount",
            "TopCount(<Set>, <Count>[, <Numeric Expression>])",
            "Returns a specified number of items from the top of a set, optionally ordering the set first.",
            new String[]{"fxxnn", "fxxn"})
        {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new TopBottomCountFunDef(dummyFunDef, true);
            }
        };

    static final MultiResolver BottomCountResolver =
        new MultiResolver(
            "BottomCount",
            "BottomCount(<Set>, <Count>[, <Numeric Expression>])",
            "Returns a specified number of items from the bottom of a set, optionally ordering the set first.",
            new String[]{"fxxnn", "fxxn"})
        {
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
        // we're going to sort it later.
        final ListCalc listCalc =
                compiler.compileList(call.getArg(0), true);
        final IntegerCalc integerCalc =
                compiler.compileInteger(call.getArg(1));
        final Calc orderCalc =
            call.getArgCount() > 2
            ? compiler.compileScalar(call.getArg(2), true)
            : null;
        final int arity = call.getType().getArity();
        return new AbstractListCalc(
            call,
            new Calc[]{listCalc, integerCalc, orderCalc})
        {
            public TupleList evaluateList(Evaluator evaluator) {
                // Use a native evaluator, if more efficient.
                // TODO: Figure this out at compile time.
                SchemaReader schemaReader = evaluator.getSchemaReader();
                NativeEvaluator nativeEvaluator =
                    schemaReader.getNativeSetEvaluator(
                        call.getFunDef(), call.getArgs(), evaluator, this);
                if (nativeEvaluator != null) {
                    return
                        (TupleList) nativeEvaluator.execute(ResultStyle.LIST);
                }

                int n = integerCalc.evaluateInteger(evaluator);
                if (n == 0 || n == mondrian.olap.fun.FunUtil.IntegerNull) {
                    return TupleCollections.emptyList(arity);
                }

                TupleList list = listCalc.evaluateList(evaluator);
                if (list.isEmpty()) {
                    return list;
                }

                if (orderCalc == null) {
                    if (list instanceof AbstractList && list.size() < n) {
                        return list;
                    } else {
                        return list.subList(0, n);
                    }
                }

                return partiallySortList(
                    evaluator, list, hasHighCardDimension(list), n, arity);
            }

            private TupleList partiallySortList(
                Evaluator evaluator,
                TupleList list,
                boolean highCard,
                int n,
                int arity)
            {
                if (highCard) {
                    // sort list in chunks, collect the results
                    final int chunkSize = 6400; // what is this really?
                    TupleList allChunkResults = TupleCollections.createList(
                        arity);
                    for (int i = 0, next; i < list.size(); i = next) {
                        next = Math.min(i + chunkSize, list.size());
                        final TupleList chunk = list.subList(i, next);
                        TupleList chunkResult =
                            partiallySortList(
                                evaluator, chunk, false, n, arity);
                        allChunkResults.addAll(chunkResult);
                    }
                    // one last sort, to merge and cull
                    return partiallySortList(
                        evaluator, allChunkResults, false, n, arity);
                }

                // normal case: no need for chunks
                return new DelegatingTupleList(
                    list.getArity(),
                    partiallySortTuples(
                        evaluator.push(),
                        list,
                        orderCalc, n, top, arity));
            }


            public boolean dependsOn(Hierarchy hierarchy) {
                return anyDependsButFirst(getCalcs(), hierarchy);
            }

            private boolean hasHighCardDimension(TupleList l) {
                final List<Member> trial = l.get(0);
                for (Member m : trial) {
                    if (m.getHierarchy().getDimension().isHighCardinality()) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}

// End TopBottomCountFunDef.java
