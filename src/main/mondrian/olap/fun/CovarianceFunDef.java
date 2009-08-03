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
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.impl.ValueCalc;
import mondrian.calc.impl.AbstractCalc;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;

/**
 * Definition of the <code>Covariance</code> and <code>CovarianceN</code> MDX functions.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class CovarianceFunDef extends FunDefBase {
    static final ReflectiveMultiResolver CovarianceResolver = new ReflectiveMultiResolver(
            "Covariance",
            "Covariance(<Set>, <Numeric Expression>[, <Numeric Expression>])",
            "Returns the covariance of two series evaluated over a set (biased).",
            new String[]{"fnxn","fnxnn"},
            CovarianceFunDef.class);

    static final MultiResolver CovarianceNResolver = new ReflectiveMultiResolver(
            "CovarianceN",
            "CovarianceN(<Set>, <Numeric Expression>[, <Numeric Expression>])",
            "Returns the covariance of two series evaluated over a set (unbiased).",
            new String[]{"fnxn","fnxnn"},
            CovarianceFunDef.class);

    private final boolean biased;

    public CovarianceFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
        this.biased = dummyFunDef.getName().equals("Covariance");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
                compiler.compileList(call.getArg(0));
        final Calc calc1 =
                compiler.compileScalar(call.getArg(1), true);
        final Calc calc2 = call.getArgCount() > 2 ?
                compiler.compileScalar(call.getArg(2), true) :
                new ValueCalc(call);
        return new AbstractDoubleCalc(call, new Calc[] {listCalc, calc1, calc2}) {
            public double evaluateDouble(Evaluator evaluator) {
                List memberList = listCalc.evaluateList(evaluator);
                return (Double)covariance(
                        evaluator.push(false), memberList,
                        calc1, calc2, biased);
            }

            public boolean dependsOn(Dimension dimension) {
                return anyDependsButFirst(getCalcs(), dimension);
            }
        };
    }
}

// End CovarianceFunDef.java
