/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.GenericCalc;
import mondrian.calc.impl.GenericIterCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.SetType;
import mondrian.olap.type.Type;

import java.io.PrintWriter;
import java.util.List;

/**
 * Definition of the <code>Cache</code> system function, which is smart enough
 * to evaluate its argument only once.
 *
 * @author jhyde
 * @since 2005/8/14
 */
public class CacheFunDef extends FunDefBase {
    static final String NAME = "Cache";
    private static final String SIGNATURE = "Cache(<<Exp>>)";
    private static final String DESCRIPTION =
        "Evaluates and returns its sole argument, applying statement-level caching";
    private static final Syntax SYNTAX = Syntax.Function;
    static final CacheFunResolver Resolver = new CacheFunResolver();

    private CacheFunDef(
        String name,
        String signature,
        String description,
        Syntax syntax,
        int category,
        Type type)
    {
        super(
            name, signature, description, syntax,
            category, new int[] {category});
        Util.discard(type);
    }

    public void unparse(Exp[] args, PrintWriter pw) {
        args[0].unparse(pw);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Exp exp = call.getArg(0);
        final ExpCacheDescriptor cacheDescriptor =
                new ExpCacheDescriptor(exp, compiler);
        if (call.getType() instanceof SetType) {
            return new GenericIterCalc(call) {
                public Object evaluate(Evaluator evaluator) {
                    return evaluator.getCachedResult(cacheDescriptor);
                }

                public Calc[] getCalcs() {
                    return new Calc[] {cacheDescriptor.getCalc()};
                }

                public ResultStyle getResultStyle() {
                    // cached lists are immutable
                    return ResultStyle.LIST;
                }
            };
        } else {
            return new GenericCalc(call) {
                public Object evaluate(Evaluator evaluator) {
                    return evaluator.getCachedResult(cacheDescriptor);
                }

                public Calc[] getCalcs() {
                    return new Calc[] {cacheDescriptor.getCalc()};
                }

                public ResultStyle getResultStyle() {
                    return ResultStyle.VALUE;
                }
            };
        }
    }

    private static class CacheFunResolver extends ResolverBase {
        private CacheFunResolver() {
            super(NAME, SIGNATURE, DESCRIPTION, SYNTAX);
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            if (args.length != 1) {
                return null;
            }
            final Exp exp = args[0];
            final int category = exp.getCategory();
            final Type type = exp.getType();
            return new CacheFunDef(
                NAME, SIGNATURE, DESCRIPTION, SYNTAX,
                category, type);
        }

        public boolean requiresExpression(int k) {
            return false;
        }
    }
}

// End CacheFunDef.java
