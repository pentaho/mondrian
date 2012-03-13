/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.GenericCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.List;

/**
 * Definition of the <code>CoalesceEmpty</code> MDX function.
 *
 * <p>It evaluates each of the arguments to the function, returning the
 * first such argument that does not return a null value.
 *
 * @author gjohnson
 */
public class CoalesceEmptyFunDef extends FunDefBase {
    static final ResolverBase Resolver = new ResolverImpl();

    public CoalesceEmptyFunDef(ResolverBase resolverBase, int type, int[] types)
    {
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
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            if (args.length < 1) {
                return null;
            }
            final int[] types = {Category.Numeric, Category.String};
            final int[] argTypes = new int[args.length];
            for (int type : types) {
                int matchingArgs = 0;
                conversions.clear();
                for (int i = 0; i < args.length; i++) {
                    if (validator.canConvert(i, args[i], type, conversions)) {
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
