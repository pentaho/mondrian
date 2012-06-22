/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Definition of the <code>Percentile</code> MDX function.
 *
 * @author jhyde
 * @since Jan 16, 2008
 */
class PercentileFunDef extends AbstractAggregateFunDef {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Percentile",
            "Percentile(<Set>, <Numeric Expression>, <Percent>)",
            "Returns the value of the tuple that is at a given percentile of a set.",
            new String[] {"fnxnn"},
            PercentileFunDef.class);

    public PercentileFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
            compiler.compileList(call.getArg(0));
        final Calc calc =
            compiler.compileScalar(call.getArg(1), true);
        final DoubleCalc percentCalc =
            compiler.compileDouble(call.getArg(2));
        return new AbstractDoubleCalc(
            call, new Calc[] {listCalc, calc, percentCalc})
        {
            public double evaluateDouble(Evaluator evaluator) {
                TupleList list = evaluateCurrentList(listCalc, evaluator);
                double percent = percentCalc.evaluateDouble(evaluator) * 0.01;
                final int savepoint = evaluator.savepoint();
                try {
                    evaluator.setNonEmpty(false);
                    final double percentile =
                        percentile(evaluator, list, calc, percent);
                    return percentile;
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

// End PercentileFunDef.java
