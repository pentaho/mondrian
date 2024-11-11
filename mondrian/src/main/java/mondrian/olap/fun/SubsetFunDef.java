/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.FunDef;

/**
 * Definition of the <code>Subset</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class SubsetFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Subset",
            "Subset(<Set>, <Start>[, <Count>])",
            "Returns a subset of elements from a set.",
            new String[] {"fxxn", "fxxnn"},
            SubsetFunDef.class);

    public SubsetFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
            compiler.compileList(call.getArg(0));
        final IntegerCalc startCalc =
            compiler.compileInteger(call.getArg(1));
        final IntegerCalc countCalc =
            call.getArgCount() > 2
            ? compiler.compileInteger(call.getArg(2))
            : null;
        return new AbstractListCalc(
            call, new Calc[] {listCalc, startCalc, countCalc})
        {
            public TupleList evaluateList(Evaluator evaluator) {
                final int savepoint = evaluator.savepoint();
                try {
                    evaluator.setNonEmpty(false);
                    final TupleList list = listCalc.evaluateList(evaluator);
                    final int start = startCalc.evaluateInteger(evaluator);
                    int end;
                    if (countCalc != null) {
                        final int count = countCalc.evaluateInteger(evaluator);
                        end = start + count;
                    } else {
                        end = list.size();
                    }
                    if (end > list.size()) {
                        end = list.size();
                    }
                    if (start >= end || start < 0) {
                        return TupleCollections.emptyList(list.getArity());
                    }
                    if (start == 0 && end == list.size()) {
                        return list;
                    }
                    assert 0 <= start;
                    assert start < end;
                    assert end <= list.size();
                    return list.subList(start, end);
                } finally {
                    evaluator.restore(savepoint);
                }
            }
        };
    }
}

// End SubsetFunDef.java
