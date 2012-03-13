/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.NullType;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapMember;

/**
 * Definition of the MDX <code>&lt;Member&gt : &lt;Member&gt;</code> operator,
 * which returns the set of members between a given pair of members.
 *
 * @author jhyde
 * @since 3 March, 2002
 */
class RangeFunDef extends FunDefBase {
    static final RangeFunDef instance = new RangeFunDef();

    private RangeFunDef() {
        super(
            ":",
            "<Member> : <Member>",
            "Infix colon operator returns the set of members between a given pair of members.",
            "ixmm");
    }


    /**
     * Returns two membercalc objects, substituting nulls with the hierarchy
     * null member of the other expression.
     *
     * @param exp0 first expression
     * @param exp1 second expression
     *
     * @return two member calcs
     */
    private MemberCalc[] compileMembers(
        Exp exp0, Exp exp1, ExpCompiler compiler)
    {
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

        if (members[0] == null && members[1] == null) {
            throw MondrianResource.instance().TwoNullsNotSupported.ex();
        } else if (members[0] == null) {
            Member nullMember =
                ((RolapMember) members[1].evaluate(null)).getHierarchy()
                .getNullMember();
            members[0] = (MemberCalc)ConstantCalc.constantMember(nullMember);
        } else if (members[1] == null) {
            Member nullMember =
                ((RolapMember) members[0].evaluate(null)).getHierarchy()
                .getNullMember();
            members[1] = (MemberCalc)ConstantCalc.constantMember(nullMember);
        }

        return members;
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc[] memberCalcs =
            compileMembers(call.getArg(0), call.getArg(1), compiler);
        return new AbstractListCalc(
            call, new Calc[] {memberCalcs[0], memberCalcs[1]})
        {
            public TupleList evaluateList(Evaluator evaluator) {
                final Member member0 = memberCalcs[0].evaluateMember(evaluator);
                final Member member1 = memberCalcs[1].evaluateMember(evaluator);
                if (member0.isNull() || member1.isNull()) {
                    return TupleCollections.emptyList(1);
                }
                if (member0.getLevel() != member1.getLevel()) {
                    throw evaluator.newEvalException(
                        call.getFunDef(),
                        "Members must belong to the same level");
                }
                return new UnaryTupleList(
                    FunUtil.memberRange(evaluator, member0, member1));
            }
        };
    }
}

// End RangeFunDef.java
