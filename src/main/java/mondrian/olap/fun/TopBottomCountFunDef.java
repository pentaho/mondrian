/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.AbstractList;
import java.util.List;

/**
 * Definition of the <code>TopCount</code> and <code>BottomCount</code>
 * MDX builtin functions.
 *
 * @author jhyde
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
                assert list.getArity() == arity;
                if (list.isEmpty()) {
                    return list;
                }

                if (orderCalc == null) {
                    // REVIEW: Why require "instanceof AbstractList"?
                    if (list instanceof AbstractList && list.size() <= n) {
                        return list;
                    } else {
                        return list.subList(0, n);
                    }
                }

                return partiallySortList(
                    evaluator, list,
                    Math.min(n, list.size()));
            }

            private TupleList partiallySortList(
                Evaluator evaluator,
                TupleList list,
                int n)
            {
                assert list.size() > 0;
                assert n <= list.size();

                // normal case: no need for chunks
                final int savepoint = evaluator.savepoint();
                try {
                    switch (list.getArity()) {
                    case 1:
                        final List<Member> members =
                        partiallySortMembers(
                            evaluator.push(),
                            list.slice(0),
                            orderCalc, n, top);
                        return new UnaryTupleList(members);
                    default:
                        final List<List<Member>> tuples =
                        partiallySortTuples(
                            evaluator.push(),
                            list,
                            orderCalc, n, top);
                        return new DelegatingTupleList(
                            list.getArity(),
                            tuples);
                    }
                } finally {
                    evaluator.restore(savepoint);
                }
            }

            public boolean dependsOn(Hierarchy hierarchy) {
                return anyDependsButFirst(getCalcs(), hierarchy);
            }
        };
    }
}

// End TopBottomCountFunDef.java
