/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 February, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.calc.*;
import mondrian.calc.impl.GenericCalc;
import mondrian.mdx.ResolvedFunCall;

import java.io.PrintWriter;

/**
 * Definition of the <code>$Cache</code> system function, which is smart enough
 * to evaluate its argument only once.
 *
 * @author jhyde
 * @since 2005/8/14
 * @version $Id$
 */
public class CacheFunDef extends FunDefBase {
    private final Type type;
    private static final String NAME = "$Cache";
    private static final String SIGNATURE = "$Cache(<<Exp>>)";
    private static final String DESCRIPTION = "Evaluates and returns its sole argument, applying statement-level caching";
    private static final Syntax SYNTAX = Syntax.Internal;
    private ExpCacheDescriptor cacheDescriptor;

    CacheFunDef(
            String name,
            String signature,
            String description,
            Syntax syntax,
            int category,
            Type type) {
        super(name, signature, description, syntax,
                category, new int[] {category});
        this.type = type;
    }

    public void unparse(Exp[] args, PrintWriter pw) {
        args[0].unparse(pw);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Exp exp = call.getArg(0);
        final ExpCacheDescriptor cacheDescriptor =
                new ExpCacheDescriptor(exp, compiler);
        return new GenericCalc(call) {
            public Object evaluate(Evaluator evaluator) {
                return evaluator.getCachedResult(cacheDescriptor);
            }

            public Calc[] getCalcs() {
                return new Calc[] {cacheDescriptor.getCalc()};
            }
        };
    }

    public static class CacheFunResolver extends ResolverBase {
        CacheFunResolver() {
            super(NAME, SIGNATURE, DESCRIPTION, SYNTAX);
        }

        public FunDef resolve(
                Exp[] args, Validator validator, int[] conversionCount) {
            if (args.length != 1) {
                return null;
            }
            final Exp exp = args[0];
            final int category = exp.getCategory();
            final Type type = exp.getType();
            return new CacheFunDef(NAME, SIGNATURE, DESCRIPTION, SYNTAX,
                    category, type);
        }

        public boolean requiresExpression(int k) {
            return false;
        }
    }
}

// End CacheFunDef.java
