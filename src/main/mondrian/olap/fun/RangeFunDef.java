/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2007 Julian Hyde and others
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
import mondrian.calc.impl.ConstantCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Member;
import mondrian.olap.type.NullType;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapMember;

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

    
    /**
     * return two membercalc objects, substituting null's with the hierarchy
     * null member of the other expression.
     * 
     * @param exp0 first expression
     * @param exp1 second expression
     * 
     * @return two member calcs
     */
    private MemberCalc[] compileMembers(Exp exp0, Exp exp1, ExpCompiler compiler) {
        MemberCalc[] members = new MemberCalc[2];
        
        if (exp0.getType() instanceof NullType) {
            members[0] = null;
        } else {
            members[0] = compiler.compileMember(exp0);
        }
        
        if (exp1.getType() instanceof NullType) {
            members[1] = null;
        } else {
            members[1] = compiler.compileMember(exp1);
        }
        
        // replace any null types with hierachy null member
        // if both objects are null, throw exception
        
        if (members[0]== null && members[1] == null) {
            throw MondrianResource.instance().TwoNullsNotSupported.ex();
        } else if (members[0] == null) {
            Member nullMember = ((RolapMember)members[1].evaluate(null)).getHierarchy().getNullMember();
            members[0] = (MemberCalc)ConstantCalc.constantMember(nullMember);
        } else if (members[1] == null) {
            Member nullMember = ((RolapMember)members[0].evaluate(null)).getHierarchy().getNullMember();
            members[1] = (MemberCalc)ConstantCalc.constantMember(nullMember);
        }
        
        return members;
    }
    
    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc[] memberCalcs = compileMembers(call.getArg(0), call.getArg(1), compiler);
        return new AbstractListCalc(call, new Calc[] {memberCalcs[0], memberCalcs[1]}) {
            public List evaluateList(Evaluator evaluator) {
                final Member member0 = memberCalcs[0].evaluateMember(evaluator);
                final Member member1 = memberCalcs[1].evaluateMember(evaluator);
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
