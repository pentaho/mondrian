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
import mondrian.calc.impl.AbstractIntegerCalc;
import mondrian.mdx.NamedSetExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;

/**
 * Definition of the <code>&lt;Named Set&gt;.CurrentOrdinal</code> MDX builtin
 * function.
 *
 * @author jhyde
 * @since Oct 19, 2008
 */
public class NamedSetCurrentOrdinalFunDef extends FunDefBase {
    static final NamedSetCurrentOrdinalFunDef instance =
        new NamedSetCurrentOrdinalFunDef();

    private NamedSetCurrentOrdinalFunDef() {
        super(
            "CurrentOrdinal",
            "Returns the ordinal of the current iteration through a named set.",
            "pix");
    }

    public Exp createCall(Validator validator, Exp[] args) {
        assert args.length == 1;
        final Exp arg0 = args[0];
        if (!(arg0 instanceof NamedSetExpr)) {
            throw MondrianResource.instance().NotANamedSet.ex();
        }
        return super.createCall(validator, args);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Exp arg0 = call.getArg(0);
        assert arg0 instanceof NamedSetExpr : "checked this in createCall";
        final NamedSetExpr namedSetExpr = (NamedSetExpr) arg0;
        return new AbstractIntegerCalc(call, new Calc[0]) {
            public int evaluateInteger(Evaluator evaluator) {
                return namedSetExpr.getEval(evaluator).currentOrdinal();
            }
        };
    }
}

// End NamedSetCurrentOrdinalFunDef.java
