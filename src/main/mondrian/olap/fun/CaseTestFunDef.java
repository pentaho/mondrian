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

import mondrian.olap.*;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.BooleanCalc;
import mondrian.calc.impl.ConstantCalc;
import mondrian.calc.impl.AbstractCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;
import java.util.ArrayList;

/**
 * Definition of the tested <code>CASE</code> MDX operator.
 *
 * Syntax is:
 * <blockquote><pre><code>Case
 * When &lt;Logical Expression&gt; Then &lt;Expression&gt;
 * [...]
 * [Else &lt;Expression&gt;]
 * End</code></blockquote>.
 *
 * @see CaseMatchFunDef
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class CaseTestFunDef extends FunDefBase {
    static final ResolverImpl Resolver = new ResolverImpl();

    public CaseTestFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Exp[] args = call.getArgs();
        final BooleanCalc[] conditionCalcs =
                new BooleanCalc[args.length / 2];
        final Calc[] exprCalcs =
                new Calc[args.length / 2];
        final List calcList = new ArrayList();
        for (int i = 0, j = 0; i < exprCalcs.length; i++) {
            conditionCalcs[i] =
                    compiler.compileBoolean(args[j++]);
            calcList.add(conditionCalcs[i]);
            exprCalcs[i] =
                    compiler.compileScalar(args[j++], true);
            calcList.add(exprCalcs[i]);
        }
        final Calc defaultCalc =
                args.length % 2 == 1 ?
                compiler.compileScalar(args[args.length - 1], true) :
                ConstantCalc.constantNull(call.getType());
        calcList.add(defaultCalc);
        final Calc[] calcs = (Calc[])
                calcList.toArray(new Calc[calcList.size()]);

        return new AbstractCalc(call) {
            public Object evaluate(Evaluator evaluator) {
                for (int i = 0; i < conditionCalcs.length; i++) {
                    if (conditionCalcs[i].evaluateBoolean(evaluator)) {
                        return exprCalcs[i].evaluate(evaluator);
                    }
                }
                return defaultCalc.evaluate(evaluator);
            }

            public Calc[] getCalcs() {
                return calcs;
            }
        };
    }

    private static class ResolverImpl extends ResolverBase {
        public ResolverImpl() {
            super(
                    "_CaseTest",
                    "Case When <Logical Expression> Then <Expression> [...] [Else <Expression>] End",
                    "Evaluates various conditions, and returns the corresponding expression for the first which evaluates to true.",
                    Syntax.Case);
        }

        public FunDef resolve(
                Exp[] args, Validator validator, int[] conversionCount) {
            if (args.length < 1) {
                return null;
            }
            int j = 0;
            int clauseCount = args.length / 2;
            int mismatchingArgs = 0;
            int returnType = args[1].getCategory();
            for (int i = 0; i < clauseCount; i++) {
                if (!validator.canConvert(args[j++], Category.Logical, conversionCount)) {
                    mismatchingArgs++;
                }
                if (!validator.canConvert(args[j++], returnType, conversionCount)) {
                    mismatchingArgs++;
                }
            }
            if (j < args.length) {
                if (!validator.canConvert(args[j++], returnType, conversionCount)) {
                    mismatchingArgs++;
                }
            }
            Util.assertTrue(j == args.length);
            if (mismatchingArgs != 0) {
                return null;
            }
            FunDef dummy = createDummyFunDef(this, returnType, args);
            return new CaseTestFunDef(dummy);
        }

        public boolean requiresExpression(int k) {
            return true;
        }
    }
}

// End CaseTestFunDef.java
