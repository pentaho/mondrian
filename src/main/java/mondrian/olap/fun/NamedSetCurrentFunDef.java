/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.calc.impl.AbstractTupleCalc;
import mondrian.mdx.NamedSetExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;

/**
 * Definition of the <code>&lt;Named Set&gt;.Current</code> MDX
 * builtin function.
 *
 * @author jhyde
 * @since Oct 19, 2008
 */
public class NamedSetCurrentFunDef extends FunDefBase {
    static final NamedSetCurrentFunDef instance =
        new NamedSetCurrentFunDef();

    private NamedSetCurrentFunDef() {
        super(
            "Current",
            "Returns the current member or tuple of a named set.",
            "ptx");
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
        if (arg0.getType().getArity() == 1) {
            return new AbstractMemberCalc(call, new Calc[0]) {
                public Member evaluateMember(Evaluator evaluator) {
                    return namedSetExpr.getEval(evaluator).currentMember();
                }
            };
        } else {
            return new AbstractTupleCalc(call, new Calc[0]) {
                public Member[] evaluateTuple(Evaluator evaluator) {
                    return namedSetExpr.getEval(evaluator).currentTuple();
                }
            };
        }
    }
}

// End NamedSetCurrentFunDef.java
