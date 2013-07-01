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

import mondrian.calc.*;
import mondrian.calc.impl.AbstractStringCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.List;

/**
 * Definition of the <code>SetToStr</code> MDX function.
 *
 * @author jhyde
 * @since Aug 3, 2006
 */
class SetToStrFunDef extends FunDefBase {
    public static final FunDefBase instance = new SetToStrFunDef();

    private SetToStrFunDef() {
        super("SetToStr", "Constructs a string from a set.", "fSx");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        Exp arg = call.getArg(0);
        final ListCalc listCalc = compiler.compileList(arg);
        return new AbstractStringCalc(call, new Calc[]{listCalc}) {
            public String evaluateString(Evaluator evaluator) {
                final TupleList list = listCalc.evaluateList(evaluator);
                if (list.getArity() == 1) {
                    return memberSetToStr(list.slice(0));
                } else {
                    return tupleSetToStr(list);
                }
            }
        };
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

    static String tupleSetToStr(TupleList list) {
        StringBuilder buf = new StringBuilder();
        buf.append("{");
        int k = 0;
        Member[] members = new Member[list.getArity()];
        final TupleCursor cursor = list.tupleCursor();
        while (cursor.forward()) {
            if (k++ > 0) {
                buf.append(", ");
            }
            cursor.currentToArray(members, 0);
            appendTuple(buf, members);
        }
        buf.append("}");
        return buf.toString();
    }
}

// End SetToStrFunDef.java
