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
import mondrian.calc.impl.ValueCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Definition of the <code>Stdev</code> builtin MDX function, and its alias
 * <code>Stddev</code>.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class StdevFunDef extends AbstractAggregateFunDef {
    static final ReflectiveMultiResolver StdevResolver =
        new ReflectiveMultiResolver(
            "Stdev",
            "Stdev(<Set>[, <Numeric Expression>])",
            "Returns the standard deviation of a numeric expression evaluated over a set (unbiased).",
            new String[]{"fnx", "fnxn"},
            StdevFunDef.class);

    static final ReflectiveMultiResolver StddevResolver =
        new ReflectiveMultiResolver(
            "Stddev",
            "Stddev(<Set>[, <Numeric Expression>])",
            "Alias for Stdev.",
            new String[]{"fnx", "fnxn"},
            StdevFunDef.class);

    public StdevFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
            compiler.compileList(call.getArg(0));
        final Calc calc =
            call.getArgCount() > 1
            ? compiler.compileScalar(call.getArg(1), true)
            : new ValueCalc(call);
        return new AbstractDoubleCalc(call, new Calc[] {listCalc, calc}) {
            public double evaluateDouble(Evaluator evaluator) {
                TupleList memberList = evaluateCurrentList(listCalc, evaluator);
                final int savepoint = evaluator.savepoint();
                try {
                    evaluator.setNonEmpty(false);
                    final double stdev =
                        (Double) stdev(
                            evaluator, memberList, calc, false);
                    return stdev;
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

// End StdevFunDef.java
