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
