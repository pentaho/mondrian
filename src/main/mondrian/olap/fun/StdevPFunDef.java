/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.FunDef;
import mondrian.olap.Evaluator;
import mondrian.olap.Dimension;
import mondrian.olap.Exp;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.impl.ValueCalc;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;

/**
 * Definition of the <code>StdevP</code> builtin MDX function, and its alias
 * <code>StddevP</code>.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class StdevPFunDef extends AbstractAggregateFunDef {

    static final Resolver StdevpResolver = new ReflectiveMultiResolver(
            "StdevP",
            "StdevP(<Set>[, <Numeric Expression>])",
            "Returns the standard deviation of a numeric expression evaluated over a set (biased).",
            new String[]{"fnx", "fnxn"},
            StdevPFunDef.class);

    static final Resolver StddevpResolver = new ReflectiveMultiResolver(
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
        final Calc calc = call.getArgCount() > 1 ?
                compiler.compileScalar(call.getArg(1), true) :
                new ValueCalc(call);
        return new AbstractDoubleCalc(call, new Calc[] {listCalc, calc}) {
            public double evaluateDouble(Evaluator evaluator) {
                List memberList = evaluateCurrentList(listCalc, evaluator);
                return (Double)stdev(evaluator.push(), memberList, calc, true);
            }

            public boolean dependsOn(Dimension dimension) {
                return anyDependsButFirst(getCalcs(), dimension);
            }
        };
    }
}

// End StdevpFunDef.java
