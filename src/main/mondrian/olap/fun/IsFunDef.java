/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.FunDef;
import mondrian.olap.Evaluator;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.AbstractBooleanCalc;
import mondrian.mdx.ResolvedFunCall;

/**
 * Definition of the <code>Is</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class IsFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
            "IS",
            "<Expression> IS <Expression>",
            "Returns whether two objects are the same (idempotent)",
            new String[] {"ibmm", "ibll", "ibhh", "ibdd"},
            IsFunDef.class);

    public IsFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Calc calc0 = compiler.compile(call.getArg(0));
        final Calc calc1 = compiler.compile(call.getArg(1));
        return new AbstractBooleanCalc(call, new Calc[] {calc0, calc1}) {
            public boolean evaluateBoolean(Evaluator evaluator) {
                Object o0 = calc0.evaluate(evaluator);
                Object o1 = calc1.evaluate(evaluator);
                return o0 == o1;
            }
        };
    }
}

// End IsFunDef.java
