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

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.impl.AbstractStringCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Member;

import java.util.List;

/**
 * Definition of the <code>SetToStr</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Aug 3, 2006
 */
class SetToStrFunDef extends FunDefBase {
    public static final FunDefBase instance = new SetToStrFunDef();

    private SetToStrFunDef() {
        super("SetToStr", "SetToStr(<Set>)", "Constructs a string from a set.", "fSx");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
                compiler.compileList(call.getArg(0));
        return new AbstractStringCalc(call, new Calc[] {listCalc}) {
            public String evaluateString(Evaluator evaluator) {
                final List list = listCalc.evaluateList(evaluator);
                return strToSet(list);
            }
        };
    }

    static String strToSet(List list) {
        StringBuffer buf = new StringBuffer();
        buf.append("{");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
                buf.append(", ");
            }
            final Object o = list.get(i);
            appendMemberOrTuple(buf, o);
        }
        buf.append("}");
        return buf.toString();
    }

    static void appendMemberOrTuple(
            StringBuffer buf,
            Object memberOrTuple) {
        if (memberOrTuple instanceof Member) {
            Member member = (Member) memberOrTuple;
            buf.append(member.getUniqueName());
        } else {
            Member[] members = (Member[]) memberOrTuple;
            appendTuple(buf, members);
        }
    }
}

// End SetToStrFunDef.java
