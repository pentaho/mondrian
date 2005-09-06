/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 February, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;

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

    // implement FunDef
    public Object evaluate(Evaluator evaluator, Exp[] args) {
        if (cacheDescriptor == null) {
            // First call, setup the cache descriptor. (An expensive process,
            // so we only want to do it once.)
            cacheDescriptor = new ExpCacheDescriptor(args[0], evaluator);
        } else {
            assert args[0] == cacheDescriptor.getExp();
        }
        Object o = evaluator.getCachedResult(cacheDescriptor);
        return o;
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
            final Type type = exp.getTypeX();
            return new CacheFunDef(NAME, SIGNATURE, DESCRIPTION, SYNTAX,
                    category, type);
        }

        public boolean requiresExpression(int k) {
            return false;
        }
    }
}

// End CacheFunDef.java
