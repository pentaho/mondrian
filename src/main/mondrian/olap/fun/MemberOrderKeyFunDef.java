/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
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
public final class MemberOrderKeyFunDef extends FunDefBase {
    static final MemberOrderKeyFunDef instance =
        new MemberOrderKeyFunDef();

    /**
     * Creates the singleton MemberOrderKeyFunDef.
     */
    private MemberOrderKeyFunDef() {
        super(
            "OrderKey", "Returns the member order key.", "pvm");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc memberCalc =
                compiler.compileMember(call.getArg(0));
        return new CalcImpl(call, memberCalc);
    }

    public static class CalcImpl extends AbstractCalc {
        private final MemberCalc memberCalc;

        /**
         * Creates a CalcImpl.
         *
         * @param exp Source expression
         * @param memberCalc Compiled expression to calculate member
         */
        public CalcImpl(Exp exp, MemberCalc memberCalc) {
            super(exp, new Calc[] {memberCalc});
            this.memberCalc = memberCalc;
        }

        public OrderKey evaluate(Evaluator evaluator) {
            return new OrderKey(memberCalc.evaluateMember(evaluator));
        }

        protected String getName() {
            return "OrderKey";
        }
    }
}

// End MemberOrderKeyFunDef.java
