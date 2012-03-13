/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2009 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractStringCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.olap.type.TypeUtil;

/**
 * Definition of the <code>TupleToStr</code> MDX function.
 *
 * <p>Syntax:
 * <blockquote><code>
 * TupleToStr(&lt;Tuple&gt;)
 * </code></blockquote>
 *
 * @author jhyde
 * @since Aug 3, 2006
 */
class TupleToStrFunDef extends FunDefBase {
    static final TupleToStrFunDef instance = new TupleToStrFunDef();

    private TupleToStrFunDef() {
        super("TupleToStr", "Constructs a string from a tuple.", "fSt");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        if (TypeUtil.couldBeMember(call.getArg(0).getType())) {
            final MemberCalc memberCalc =
                    compiler.compileMember(call.getArg(0));
            return new AbstractStringCalc(call, new Calc[] {memberCalc}) {
                public String evaluateString(Evaluator evaluator) {
                    final Member member =
                            memberCalc.evaluateMember(evaluator);
                    if (member.isNull()) {
                        return "";
                    }
                    StringBuilder buf = new StringBuilder();
                    buf.append(member.getUniqueName());
                    return buf.toString();
                }
            };
        } else {
            final TupleCalc tupleCalc =
                    compiler.compileTuple(call.getArg(0));
            return new AbstractStringCalc(call, new Calc[] {tupleCalc}) {
                public String evaluateString(Evaluator evaluator) {
                    final Member[] members =
                            tupleCalc.evaluateTuple(evaluator);
                    if (members == null) {
                        return "";
                    }
                    StringBuilder buf = new StringBuilder();
                    SetToStrFunDef.appendTuple(buf, members);
                    return buf.toString();
                }
            };
        }
    }

}

// End TupleToStrFunDef.java
