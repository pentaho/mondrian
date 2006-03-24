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

import mondrian.olap.FunDef;
import mondrian.olap.Evaluator;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.IntegerCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;
import java.util.Collections;

/**
 * Definition of the <code>Subset</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class SubsetFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
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
                call.getArgCount() > 2 ?
                compiler.compileInteger(call.getArg(2)) :
                null;
        return new AbstractListCalc(call, new Calc[] {listCalc, startCalc, countCalc}) {
            public List evaluateList(Evaluator evaluator) {
                final List list =
                        listCalc.evaluateList(evaluator);
                final int start =
                        startCalc.evaluateInteger(evaluator);
                int end;
                if (countCalc != null) {
                    final int count =
                        countCalc.evaluateInteger(evaluator);
                    end = start + count;
                } else {
                    end = list.size();
                }
                if (end > list.size()) {
                    end = list.size();
                }
                if (start >= end || start < 0) {
                    return Collections.EMPTY_LIST;
                }
                assert 0 <= start;
                assert start < end;
                assert end <= list.size();
                if (start == 0 && end == list.size()) {
                    return list;
                } else {
                    return list.subList(start, end);
                }
            }
        };
    }
}

// End SubsetFunDef.java
