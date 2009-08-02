/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.impl.ValueCalc;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;

/**
 * Definition of the <code>VarP</code> MDX builtin function
 * (and its synonym <code>VarianceP</code>).
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class VarPFunDef extends AbstractAggregateFunDef {
    static final Resolver VariancePResolver = new ReflectiveMultiResolver(
            "VarianceP",
            "VarianceP(<Set>[, <Numeric Expression>])",
            "Alias for VarP.",
            new String[]{"fnx", "fnxn"},
            VarPFunDef.class);

    static final Resolver VarPResolver = new ReflectiveMultiResolver(
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
                List memberList = evaluateCurrentList(listCalc, evaluator);
                return (Double) var(
                    evaluator.push(false), memberList, calc, true);
            }

            public boolean dependsOn(Hierarchy hierarchy) {
                return anyDependsButFirst(getCalcs(), hierarchy);
            }
        };
    }
}

// End VarPFunDef.java
