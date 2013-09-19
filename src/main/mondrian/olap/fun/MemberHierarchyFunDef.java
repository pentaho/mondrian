/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractHierarchyCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Definition of the <code>&lt;Member&gt;.Hierarchy</code> MDX builtin function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
public class MemberHierarchyFunDef extends FunDefBase {
    static final MemberHierarchyFunDef instance = new MemberHierarchyFunDef();

    private MemberHierarchyFunDef() {
        super("Hierarchy", "Returns a member's hierarchy.", "phm");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc memberCalc =
                compiler.compileMember(call.getArg(0));
        return new CalcImpl(call, memberCalc);
    }

    public static class CalcImpl extends AbstractHierarchyCalc {
        private final MemberCalc memberCalc;

        public CalcImpl(Exp exp, MemberCalc memberCalc) {
            super(exp, new Calc[] {memberCalc});
            this.memberCalc = memberCalc;
        }

        public Hierarchy evaluateHierarchy(Evaluator evaluator) {
            Member member = memberCalc.evaluateMember(evaluator);
            return member.getHierarchy();
        }
    }
}

// End MemberHierarchyFunDef.java
