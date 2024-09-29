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

import mondrian.calc.*;
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Definition of the <code>Lead</code> and <code>Lag</code> MDX functions.
 *
 * @author jhyde
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
