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
import mondrian.calc.TupleCalc;
import mondrian.calc.impl.AbstractStringCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.type.TypeUtil;
import mondrian.olap.Evaluator;
import mondrian.olap.Member;

/**
 * Definition of the <code>TupleToStr</code> MDX function.
 *
 * <p>Syntax:
 * <blockquote><code>
 * TupleToStr(&lt;Tuple&gt;)
 * </code></blockquote>
 *
 * @author jhyde
 * @version $Id$
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
