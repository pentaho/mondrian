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
 * Definition of the <code>VarP</code> MDX builtin function
 * (and its synonym <code>VarianceP</code>).
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class VarPFunDef extends AbstractAggregateFunDef {
    static final Resolver VariancePResolver =
        new ReflectiveMultiResolver(
            "VarianceP",
            "VarianceP(<Set>[, <Numeric Expression>])",
            "Alias for VarP.",
            new String[]{"fnx", "fnxn"},
            VarPFunDef.class);

    static final Resolver VarPResolver =
        new ReflectiveMultiResolver(
            "VarP",
            "VarP(<Set>[, <Numeric Expression>])",
            "Returns the variance of a numeric expression evaluated over a set (biased).",
            new String[]{"fnx", "fnxn"},
            VarPFunDef.class);

    public VarPFunDef(FunDef dummyFunDef) {
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
                    return
                        (Double) var(evaluator, memberList, calc, true);
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

// End VarPFunDef.java
