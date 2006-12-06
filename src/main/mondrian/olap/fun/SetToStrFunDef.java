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
import mondrian.olap.Exp;
import mondrian.olap.type.SetType;

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
        Exp arg = call.getArg(0);
        final ListCalc listCalc = compiler.compileList(arg);
        if (((SetType) arg.getType()).getArity() == 1) {
            return new AbstractStringCalc(call, new Calc[] {listCalc}) {
                public String evaluateString(Evaluator evaluator) {
                    final List<Member> list =
                        (List<Member>) listCalc.evaluateList(evaluator);
                    return memberSetToStr(list);
                }
            };
        } else {
            return new AbstractStringCalc(call, new Calc[] {listCalc}) {
                public String evaluateString(Evaluator evaluator) {
                    final List<Member[]> list =
                        (List<Member[]>) listCalc.evaluateList(evaluator);
                    return tupleSetToStr(list);
                }
            };
        }
    }

    static String memberSetToStr(List<Member> list) {
        StringBuilder buf = new StringBuilder();
        buf.append("{");
        int k = 0;
        for (Member member : list) {
            if (k++ > 0) {
                buf.append(", ");
            }
            buf.append(member.getUniqueName());
        }
        buf.append("}");
        return buf.toString();
    }

    static String tupleSetToStr(List<Member[]> list) {
        StringBuilder buf = new StringBuilder();
        buf.append("{");
        int k = 0;
        for (Member[] members : list) {
            if (k++ > 0) {
                buf.append(", ");
            }
            appendTuple(buf, members);
        }
        buf.append("}");
        return buf.toString();
    }
}

// End SetToStrFunDef.java
