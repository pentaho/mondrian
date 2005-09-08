/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;

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

    public boolean callDependsOn(FunCall call, Dimension dimension) {
        // Aggregate(<Set>, <Value Expression>) depends upon everything
        // <Value Expression> depends upon, except the dimensions of <Set>.
        return callDependsOnSet(call, dimension);
    }

    protected Exp validateArg(
            Validator validator, FunCall call, int i, int type) {
        // If expression cache is enabled, wrap first expression (the set)
        // in a function which will use the expression cache.
        if (i == 0) {
            if (MondrianProperties.instance().EnableExpCache.get()) {
                Exp arg = call.getArgs()[0];
                final Exp cacheCall = new FunCall("$Cache",
                        Syntax.Internal,
                        new Exp[] {arg});
                return validator.validate(cacheCall, false);
            }
        }
        return super.validateArg(validator, call, i, type);
    }

    final Exp valueFunCall = BuiltinFunTable.instance().createValueFunCall();
}

// End AbstractAggregateFunDef.java
