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

import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Util;

/**
 * The <code>CoalesceEmptyFunDef</code> class implements the CoalesceEmpty(...)
 * MDX function. It evaluates each of the arguments to the function, returning the
 * first such argument that does not return a null value.
 *
 * @author gjohnson
 */
public class CoalesceEmptyFunDef extends FunDefBase {
    public CoalesceEmptyFunDef(ResolverBase resolverBase, int type, int[] types) {
        super(resolverBase,  type, types);
    }

    /**
     * @param evaluator The evaluation context.
     * @param args The arguments to <code>CoalesceEmpty</code>
     * @return The first non-null argument, or null if all arguments are null.
     */
    public Object evaluate(Evaluator evaluator, Exp[] args) {
        for (int idx = 0; idx < args.length; idx++) {
            Object argument = getScalarArg(evaluator, args, idx);
//            Evaluator current = evaluator.push();
//            Object argument = getScalarArg(current, args, idx);

            if (argument != null && argument != Util.nullValue) {
                return argument;
            }
        }

        return null;
    }
}
