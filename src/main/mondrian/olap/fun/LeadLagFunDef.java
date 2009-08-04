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

import mondrian.olap.FunDef;
import mondrian.olap.Member;
import mondrian.olap.Evaluator;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.MemberCalc;
import mondrian.calc.IntegerCalc;
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.mdx.ResolvedFunCall;

/**
 * Definition of the <code>Lead</code> and <code>Lag</code> MDX functions.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class LeadLagFunDef extends FunDefBase {
    static final ReflectiveMultiResolver LagResolver =
        new ReflectiveMultiResolver(
            "Lag",
            "<Member>.Lag(<Numeric Expression>)",
            "Returns a member further along the specified member's dimension.",
            new String[]{"mmmn"},
            LeadLagFunDef.class);

    static final ReflectiveMultiResolver LeadResolver =
        new ReflectiveMultiResolver(
            "Lead",
            "<Member>.Lead(<Numeric Expression>)",
            "Returns a member further along the specified member's dimension.",
            new String[]{"mmmn"},
            LeadLagFunDef.class);

    public LeadLagFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc memberCalc =
                compiler.compileMember(call.getArg(0));
        final IntegerCalc integerCalc =
                compiler.compileInteger(call.getArg(1));
        final boolean lag = call.getFunName().equals("Lag");
        return new AbstractMemberCalc(
            call,
            new Calc[] {memberCalc, integerCalc})
        {
            public Member evaluateMember(Evaluator evaluator) {
                Member member = memberCalc.evaluateMember(evaluator);
                int n = integerCalc.evaluateInteger(evaluator);
                if (lag) {
                    if (n == Integer.MIN_VALUE) {
                        // Bump up lagValue by one, otherwise -n (used
                        // in the getLeadMember call below) is out of
                        // range because Integer.MAX_VALUE ==
                        // -(Integer.MIN_VALUE + 1).
                        n += 1;
                    }

                    n = -n;
                }
                return evaluator.getSchemaReader().getLeadMember(member, n);
            }
        };
    }
}

// End LeadLagFunDef.java
