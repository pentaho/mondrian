/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.calc.impl.ValueCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Definition of the <code>Avg</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class AvgFunDef extends AbstractAggregateFunDef {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
        "Avg",
            "Avg(<Set>[, <Numeric Expression>])",
            "Returns the average value of a numeric expression evaluated over a set.",
            new String[]{"fnx", "fnxn"},
            AvgFunDef.class);

    public AvgFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        final Calc calc = call.getArgCount() > 1
            ? compiler.compileScalar(call.getArg(1), true)
            : new ValueCalc(call);
        return new AbstractDoubleCalc(call, new Calc[]{listCalc, calc}) {
            public double evaluateDouble(Evaluator evaluator) {
                TupleList memberList = evaluateCurrentList(listCalc, evaluator);
                final int savepoint = evaluator.savepoint();
                evaluator.setNonEmpty(false);
                try {
                    final double avg =
                        (Double) avg(
                            evaluator, memberList, calc);
                    return avg;
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

// End AvgFunDef.java
