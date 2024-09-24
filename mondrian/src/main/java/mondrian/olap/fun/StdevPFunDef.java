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
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.calc.impl.ValueCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Definition of the <code>StdevP</code> builtin MDX function, and its alias
 * <code>StddevP</code>.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class StdevPFunDef extends AbstractAggregateFunDef {

    static final Resolver StdevpResolver =
        new ReflectiveMultiResolver(
            "StdevP",
            "StdevP(<Set>[, <Numeric Expression>])",
            "Returns the standard deviation of a numeric expression evaluated over a set (biased).",
            new String[]{"fnx", "fnxn"},
            StdevPFunDef.class);

    static final Resolver StddevpResolver =
        new ReflectiveMultiResolver(
            "StddevP",
            "StddevP(<Set>[, <Numeric Expression>])",
            "Alias for StdevP.",
            new String[]{"fnx", "fnxn"},
            StdevPFunDef.class);

    public StdevPFunDef(FunDef dummyFunDef) {
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
                final int savepoint = evaluator.savepoint();
                try {
                    evaluator.setNonEmpty(false);
                    TupleList list = evaluateCurrentList(listCalc, evaluator);
                    final double stdev =
                        (Double) stdev(
                            evaluator, list, calc, true);
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

// End StdevPFunDef.java
