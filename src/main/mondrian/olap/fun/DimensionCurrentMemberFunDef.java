/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.DimensionCalc;
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.Member;
import mondrian.olap.Evaluator;
import mondrian.olap.Dimension;

/**
 * Definition of the <code>&lt;Dimension&gt;.CurrentMember</code> MDX builtin function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
public class DimensionCurrentMemberFunDef extends FunDefBase {
    static final DimensionCurrentMemberFunDef instance =
            new DimensionCurrentMemberFunDef();

    private DimensionCurrentMemberFunDef() {
        super("CurrentMember", "<Dimension>.CurrentMember",
                "Returns the current member along a dimension during an iteration.",
                "pmd");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final DimensionCalc dimensionCalc =
                compiler.compileDimension(call.getArg(0));
        return new CalcImpl(call, dimensionCalc);
    }

    public static class CalcImpl extends AbstractMemberCalc {
        private final DimensionCalc dimensionCalc;

        public CalcImpl(Exp exp, DimensionCalc dimensionCalc) {
            super(exp, new Calc[] {dimensionCalc});
            this.dimensionCalc = dimensionCalc;
        }

        protected String getName() {
            return "CurrentMember";
        }

        public Member evaluateMember(Evaluator evaluator) {
            Dimension dimension =
                    dimensionCalc.evaluateDimension(evaluator);
            return evaluator.getContext(dimension);
        }

        public boolean dependsOn(Dimension dimension) {
            return dimensionCalc.getType().usesDimension(dimension, true) ;
        }
    }
}

// End DimensionCurrentMemberFunDef.java
