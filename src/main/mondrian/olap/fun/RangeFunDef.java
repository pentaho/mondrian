/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.MemberCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Member;

import java.util.Collections;
import java.util.List;

/**
 * Definition of the MDX <code>&lt;Member&gt : &lt;Member&gt;</code> operator,
 * which returns the set of members between a given pair of members.
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 */
class RangeFunDef extends FunDefBase {
    static final RangeFunDef instance = new RangeFunDef();

    private RangeFunDef() {
        super(":", "<Member> : <Member>",
                "Infix colon operator returns the set of members between a given pair of members.",
                "ixmm");
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc memberCalc0 =
                compiler.compileMember(call.getArg(0));
        final MemberCalc memberCalc1 =
                compiler.compileMember(call.getArg(1));
        return new AbstractListCalc(call, new Calc[] {memberCalc0, memberCalc1}) {
            public List evaluateList(Evaluator evaluator) {
                final Member member0 = memberCalc0.evaluateMember(evaluator);
                final Member member1 = memberCalc1.evaluateMember(evaluator);
                if (member0.isNull() || member1.isNull()) {
                    return Collections.EMPTY_LIST;
                }
                if (member0.getLevel() != member1.getLevel()) {
                    throw evaluator.newEvalException(
                            call.getFunDef(),
                            "Members must belong to the same level");
                }
                return FunUtil.memberRange(evaluator, member0, member1);
            }
        };
    }
}

// End RangeFunDef.java
