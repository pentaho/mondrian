/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.calc.impl.ValueCalc;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.mdx.ResolvedFunCall;

/**
 * Definition of the <code>Median</code> MDX functions.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class MedianFunDef extends AbstractAggregateFunDef {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
        "Median",
            "Median(<Set>[, <Numeric Expression>])",
            "Returns the median value of a numeric expression evaluated over a set.",
            new String[]{"fnx", "fnxn"},
            MedianFunDef.class);

    public MedianFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
                compiler.compileList(call.getArg(0));
        final Calc calc = call.getArgCount() > 1
            ? compiler.compileScalar(call.getArg(1), true)
            : new ValueCalc(call);
        return new AbstractDoubleCalc(call, new Calc[] {listCalc, calc}) {
            public double evaluateDouble(Evaluator evaluator) {
                final int savepoint = evaluator.savepoint();
                evaluator.setNonEmpty(false);
                TupleList list = evaluateCurrentList(listCalc, evaluator);
                final double percentile =
                    percentile(
                        evaluator, list, calc, 0.5);
                evaluator.restore(savepoint);
                return percentile;
            }

            public boolean dependsOn(Hierarchy hierarchy) {
                return anyDependsButFirst(getCalcs(), hierarchy);
            }
        };
    }
}

// End MedianFunDef.java
