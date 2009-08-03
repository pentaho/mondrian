/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.FunDef;
import mondrian.olap.Evaluator;
import mondrian.olap.Dimension;
import mondrian.calc.*;
import mondrian.calc.impl.ValueCalc;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;

/**
 * Definition of the <code>Percentile</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
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
                List memberList = evaluateCurrentList(listCalc, evaluator);
                double percent = percentCalc.evaluateDouble(evaluator) * 0.01;
                return percentile(evaluator.push(false), memberList, calc, percent);
            }

            public boolean dependsOn(Dimension dimension) {
                return anyDependsButFirst(getCalcs(), dimension);
            }
        };
    }
}

// End PercentileFunDef.java
