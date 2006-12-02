/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.calc.*;
import mondrian.calc.impl.GenericCalc;
import mondrian.mdx.ResolvedFunCall;

/**
 * Definition of the <code>CoalesceEmpty</code> MDX function.
 *
 * <p>It evaluates each of the arguments to the function, returning the
 * first such argument that does not return a null value.
 *
 * @author gjohnson
 * @version $Id$
 */
public class CoalesceEmptyFunDef extends FunDefBase {
    static final ResolverBase Resolver = new ResolverImpl();

    public CoalesceEmptyFunDef(ResolverBase resolverBase, int type, int[] types) {
        super(resolverBase,  type, types);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Exp[] args = call.getArgs();
        final Calc[] calcs = new Calc[args.length];
        for (int i = 0; i < args.length; i++) {
            calcs[i] = compiler.compileScalar(args[i], true);
        }
        return new GenericCalc(call) {
            public Object evaluate(Evaluator evaluator) {
                for (Calc calc : calcs) {
                    final Object o = calc.evaluate(evaluator);
                    if (o != null) {
                        return o;
                    }
                }
                return null;
            }

            public Calc[] getCalcs() {
                return calcs;
            }
        };
    }

    private static class ResolverImpl extends ResolverBase {
        public ResolverImpl() {
            super(
                    "CoalesceEmpty",
                    "CoalesceEmpty(<Value Expression>[, <Value Expression>...])",
                    "Coalesces an empty cell value to a different value. All of the expressions must be of the same type (number or string).",
                    Syntax.Function);
        }

        public FunDef resolve(
                Exp[] args, Validator validator, int[] conversionCount) {
            if (args.length < 1) {
                return null;
            }
            final int[] types = {Category.Numeric, Category.String};
            final int[] argTypes = new int[args.length];
            for (int type : types) {
                int matchingArgs = 0;
                conversionCount[0] = 0;
                for (int i = 0; i < args.length; i++) {
                    if (validator.canConvert(args[i], type, conversionCount)) {
                        matchingArgs++;
                    }
                    argTypes[i] = type;
                }
                if (matchingArgs == args.length) {
                    return new CoalesceEmptyFunDef(this, type, argTypes);
                }
            }
            return null;
        }

        public boolean requiresExpression(int k) {
            return true;
        }
    }
}

// End CoalesceEmptyFunDef.java
