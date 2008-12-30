/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractMemberListCalc;
import mondrian.calc.impl.AbstractTupleListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.FunDef;
import mondrian.olap.Member;
import mondrian.olap.type.SetType;

import java.util.List;

/**
 * Definition of the <code>Hierarchize</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class HierarchizeFunDef extends FunDefBase {
    static final String[] prePost = {"PRE","POST"};
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
            "Hierarchize",
            "Hierarchize(<Set>[, POST])",
            "Orders the members of a set in a hierarchy.",
            new String[] {"fxx", "fxxy"},
            HierarchizeFunDef.class,
            prePost);

    public HierarchizeFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
            compiler.compileList(call.getArg(0), true);
        String order = getLiteralArg(call, 1, "PRE", prePost);
        final boolean post = order.equals("POST");
        final int arity = ((SetType) listCalc.getType()).getArity();
        if (arity == 1) {
            final MemberListCalc memberListCalc = (MemberListCalc) listCalc;
            return new AbstractMemberListCalc(call, new Calc[] {listCalc}) {
                public List<Member> evaluateMemberList(Evaluator evaluator) {
                    List<Member> list =
                        memberListCalc.evaluateMemberList(evaluator);
                    hierarchizeMemberList(list, post);
                    return list;
                }
            };
        } else {
            final TupleListCalc tupleListCalc = (TupleListCalc) listCalc;
            return new AbstractTupleListCalc(call, new Calc[] {listCalc}) {
                public List<Member[]> evaluateTupleList(Evaluator evaluator) {
                    List<Member[]> list = tupleListCalc.evaluateTupleList(evaluator);
                    hierarchizeTupleList(list, post, arity);
                    return list;
                }
            };
        }
    }
}

// End HierarchizeFunDef.java
