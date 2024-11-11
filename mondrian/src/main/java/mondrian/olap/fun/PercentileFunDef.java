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
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Definition of the <code>Percentile</code> MDX function.
 * <p>There is some discussion about what the "right" percentile function is.
 * Here is a <a href="http://cnx.org/content/m10805/latest/">good overview</a>.
 * Wikipedia also lists
 * <a href="http://en.wikipedia.org/wiki/Percentile">
 * methods of calculating percentile</a>.
 * </p>
 * <p>
 * This class based on MS Excel formulae:
 * </p>
 * <blockquote>rank = P / 100 * (N - 1) + 1</blockquote>
 * <blockquote>percentile = A[n]+d*(A[n+1]-A[n])</blockquote>
 * <p>Definition can also be found on
 * <a href="http://en.wikipedia.org/wiki/Percentile">Wikipedia</a></p>
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
