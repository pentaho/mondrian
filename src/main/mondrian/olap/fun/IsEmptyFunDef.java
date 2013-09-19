/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.AbstractBooleanCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.FunDef;

/**
 * Definition of the <code>IsEmpty</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class IsEmptyFunDef extends FunDefBase {
    static final ReflectiveMultiResolver FunctionResolver =
        new ReflectiveMultiResolver(
            "IsEmpty",
            "IsEmpty(<Value Expression>)",
            "Determines if an expression evaluates to the empty cell value.",
            new String[] {"fbS", "fbn"},
            IsEmptyFunDef.class);

    static final ReflectiveMultiResolver PostfixResolver =
        new ReflectiveMultiResolver(
            "IS EMPTY",
            "<Value Expression> IS EMPTY",
            "Determines if an expression evaluates to the empty cell value.",
            new String[] {"Qbm", "Qbt"},
            IsEmptyFunDef.class);

    public IsEmptyFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Calc calc = compiler.compileScalar(call.getArg(0), true);
        return new AbstractBooleanCalc(call, new Calc[] {calc}) {
            public boolean evaluateBoolean(Evaluator evaluator) {
                Object o = calc.evaluate(evaluator);
                return o == null;
            }
        };
    }
}

// End IsEmptyFunDef.java
