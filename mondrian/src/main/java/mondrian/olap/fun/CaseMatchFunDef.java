/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.ConstantCalc;
import mondrian.calc.impl.GenericCalc;
import mondrian.mdx.ResolvedFunCall;
// PATCH: Additional imports
import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.ScalarType;

import java.util.ArrayList;
import java.util.List;

/**
 * Definition of the matched <code>CASE</code> MDX operator.
 *
 * Syntax is:
 * <blockquote><pre><code>Case &lt;Expression&gt;
 * When &lt;Expression&gt; Then &lt;Expression&gt;
 * [...]
 * [Else &lt;Expression&gt;]
 * End</code></blockquote>.
 *
 * @see CaseTestFunDef
 * @author jhyde
 * @since Mar 23, 2006
 */
class CaseMatchFunDef extends FunDefBase {
    static final ResolverImpl Resolver = new ResolverImpl();

    private CaseMatchFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Exp[] args = call.getArgs();
        final List<Calc> calcList = new ArrayList<Calc>();
        final Calc valueCalc =
                compiler.compileScalar(args[0], true);
        calcList.add(valueCalc);
        final int matchCount = (args.length - 1) / 2;
        final Calc[] matchCalcs = new Calc[matchCount];
        final Calc[] exprCalcs = new Calc[matchCount];
        // PATCH: Check if return type is ScalarType
        boolean returnScalar = call.getType() instanceof ScalarType;
        for (int i = 0, j = 1; i < exprCalcs.length; i++) {
            matchCalcs[i] = compiler.compileScalar(args[j++], true);
            calcList.add(matchCalcs[i]);
            // PATCH: Compile scalar value for Member and Tuple types
            exprCalcs[i] = returnScalar ? compiler.compileScalar(args[j++], true) : compiler.compile(args[j++]);
            calcList.add(exprCalcs[i]);
        }
        final Calc defaultCalc =
            args.length % 2 == 0
            // PATCH: Compile scalar value for Member and Tuple types
            ? (returnScalar ? compiler.compileScalar(args[args.length - 1], true) :
                compiler.compile(args[args.length - 1]))
            : ConstantCalc.constantNull(call.getType());
        calcList.add(defaultCalc);
        final Calc[] calcs = calcList.toArray(new Calc[calcList.size()]);

        return new GenericCalc(call) {
            public Object evaluate(Evaluator evaluator) {
                Object value = valueCalc.evaluate(evaluator);
                // PATCH: Get double value for BigDecimal and Double comparison
                double doubleValue = 0;
                if (value instanceof Number) {
                    doubleValue = ((Number) value).doubleValue();
                }
                for (int i = 0; i < matchCalcs.length; i++) {
                    Object match = matchCalcs[i].evaluate(evaluator);
                    // PATCH: Compare also double values for BigDecimal and Double comparison
                    if (match.equals(value) || match instanceof Number && value instanceof Number &&
                            ((Number) match).doubleValue() == doubleValue) {
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

    // PATCH: Override getResultType to use first return type
    public Type getResultType(Validator validator, Exp[] args) {
        Type firstReturnType = args.length > 2 ? args[2].getType() : null;
        Type type = castType(firstReturnType, getReturnCategory());
        if (type != null) {
            return type;
        }
        throw Util.newInternal(
            "Cannot deduce type of call to function '_CaseMatch'");
    }

    private static class ResolverImpl extends ResolverBase {
        private ResolverImpl() {
            super(
                "_CaseMatch",
                "Case <Expression> When <Expression> Then <Expression> [...] [Else <Expression>] End",
                "Evaluates various expressions, and returns the corresponding expression for the first which matches a particular value.",
                Syntax.Case);
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            if (args.length < 3) {
                return null;
            }
            int valueType = args[0].getCategory();
            // PATCH: Change value type from Member or Tuple to generic Value type
            if (valueType == Category.Member || valueType == Category.Tuple) {
                valueType = Category.Value;
            }
            int returnType = args[2].getCategory();
            int j = 0;
            int clauseCount = (args.length - 1) / 2;
            int mismatchingArgs = 0;
            if (!validator.canConvert(j, args[j++], valueType, conversions)) {
                mismatchingArgs++;
            }
            for (int i = 0; i < clauseCount; i++) {
                if (!validator.canConvert(j, args[j++], valueType, conversions))
                {
                    mismatchingArgs++;
                }
                if (!validator.canConvert(
                        j, args[j++], returnType, conversions))
                {
                    // PATCH: Change return type to generic Value type
                    returnType = Category.Value;
                    if (!validator.canConvert(j - 1, args[j - 1], returnType, conversions)) {
                        mismatchingArgs++;
                    }
                }
            }
            if (j < args.length) {
                if (!validator.canConvert(
                        j, args[j++], returnType, conversions))
                {
                    // PATCH: Change return type to generic Value type
                    returnType = Category.Value;
                    if (!validator.canConvert(j - 1, args[j - 1], returnType, conversions)) {
                        mismatchingArgs++;
                    }
                }
            }
            Util.assertTrue(j == args.length);
            if (mismatchingArgs != 0) {
                return null;
            }

            FunDef dummy = createDummyFunDef(this, returnType, args);
            return new CaseMatchFunDef(dummy);
        }

        public boolean requiresExpression(int k) {
            return true;
        }
    }
}

// End CaseMatchFunDef.java
