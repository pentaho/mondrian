/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.MemberCalc;
import mondrian.calc.impl.AbstractCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.Evaluator;

/**
 * Definition of the <code>&lt;Member&gt;.OrderKey</code> MDX builtin
 * function.
 *
 * <p>Syntax:
 * <blockquote><code>&lt;Member&gt;.OrderKey</code></blockquote>
 *
 * @author kvu
 * @version $Id$
 * @since Nov 10, 2008
 */
public class MemberOrderKeyFunDef extends FunDefBase {
    static final MemberOrderKeyFunDef instance =
            new MemberOrderKeyFunDef();

    private MemberOrderKeyFunDef() {
        super("OrderKey",
                "Returns the member order key.",
                "pvm");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc memberCalc =
                compiler.compileMember(call.getArg(0));
        return new CalcImpl(call, memberCalc);
    }

    public static class CalcImpl extends AbstractCalc {
        private final MemberCalc memberCalc;

        public CalcImpl(Exp exp, MemberCalc memberCalc) {
            super(exp);
            this.memberCalc = memberCalc;
        }

        public OrderKey evaluate(Evaluator evaluator) {
            return new OrderKey(memberCalc.evaluateMember(evaluator));
        }

        protected String getName() {
            return "OrderKey";
        }

        public Calc[] getCalcs() {
            return new Calc[] {memberCalc};
        }
    }
}

// End MemberOrderKeyFunDef.java
