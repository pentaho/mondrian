/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.MemberCalc;
import mondrian.calc.TupleList;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Member;

/**
 * The {@code &lt;Member&gt;.CHILDREN} operator.
 */
class ChildrenFunDef extends FunDefBase {
    public ChildrenFunDef() {
        super("Children", "Returns the children of a member.", "pxm");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler)
    {
        final MemberCalc memberCalc =
            compiler.compileMember(call.getArg(0));
        return new ChildrenCalc(call, memberCalc);
    }

    private static class ChildrenCalc extends AbstractListCalc {
        private final MemberCalc memberCalc;

        public ChildrenCalc(ResolvedFunCall call, MemberCalc memberCalc) {
            super(call, new Calc[]{memberCalc}, false);
            this.memberCalc = memberCalc;
        }

        public TupleList evaluateList(Evaluator evaluator) {
            // Return the list of children. The list is immutable,
            // hence 'false' above.
            Member member = memberCalc.evaluateMember(evaluator);
            return new UnaryTupleList(
                getNonEmptyMemberChildren(evaluator, member));
        }
    }
}

// End ChildrenFunDef.java
