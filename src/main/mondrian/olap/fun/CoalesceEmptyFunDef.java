/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2005 Julian Hyde and others.
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
                for (int i = 0; i < calcs.length; i++) {
                    Calc calc = calcs[i];
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

}

// End CoalesceEmptyFunDef.java
