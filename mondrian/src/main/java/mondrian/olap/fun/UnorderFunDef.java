/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.FunDef;

/**
 * Definition of the <code>Unorder</code> MDX function.
 *
 * @author jhyde
 * @since Sep 06, 2008
 */
class UnorderFunDef extends FunDefBase {

    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Unorder",
            "Unorder(<Set>)",
            "Removes any enforced ordering from a specified set.",
            new String[]{"fxx"},
            UnorderFunDef.class);

    public UnorderFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        // Currently Unorder has no effect. In future, we may use the function
        // as a marker to weaken the ordering required from an expression and
        // therefore allow the compiler to use a more efficient implementation
        // that does not return a strict order.
        return compiler.compile(call.getArg(0));
    }
}

// End UnorderFunDef.java
