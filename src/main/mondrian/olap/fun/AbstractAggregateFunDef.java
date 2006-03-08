/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.mdx.UnresolvedFunCall;

/**
 * Abstract base class for all aggregate functions (<code>Aggregate</code>,
 * <code>Sum</code>, <code>Avg</code>, et cetera).
 *
 * @author jhyde
 * @since 2005/8/14
 * @version $Id$
 */
class AbstractAggregateFunDef extends FunDefBase {
    public AbstractAggregateFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    protected Exp validateArg(
            Validator validator, Exp[] args, int i, int category) {
        // If expression cache is enabled, wrap first expression (the set)
        // in a function which will use the expression cache.
        if (i == 0) {
            if (MondrianProperties.instance().EnableExpCache.get()) {
                Exp arg = args[0];
                final Exp cacheCall = new UnresolvedFunCall(
                        "$Cache",
                        Syntax.Internal,
                        new Exp[] {arg});
                return validator.validate(cacheCall, false);
            }
        }
        return super.validateArg(validator, args, i, category);
    }
}

// End AbstractAggregateFunDef.java
