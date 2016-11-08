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
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapHierarchy;

import java.util.*;

/**
 * Definition of the <code>LastPeriods</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class LastPeriodsFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "LastPeriods",
            "LastPeriods(<Index> [, <Member>])",
            "Returns a set of members prior to and including a specified member.",
            new String[] {"fxn", "fxnm"},
            LastPeriodsFunDef.class);

    public LastPeriodsFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        if (args.length == 1) {
            // If Member is not specified,
            // it is Time.CurrentMember.
            RolapHierarchy defaultTimeHierarchy =
                ((RolapCube) validator.getQuery().getCube()).getTimeHierarchy(
                    getName());
            return new SetType(MemberType.forHierarchy(defaultTimeHierarchy));
        } else {
            Type type = args[1].getType();
            Type memberType =
            TypeUtil.toMemberOrTupleType(type);
            return new SetType(memberType);
        }
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        // Member defaults to [Time].currentmember
        Exp[] args = call.getArgs();
        final MemberCalc memberCalc;
        if (args.length == 1) {
            final RolapHierarchy timeHierarchy =
                ((RolapCube) compiler.getEvaluator().getCube())
                    .getTimeHierarchy(getName());
            memberCalc =
                new HierarchyCurrentMemberFunDef.FixedCalcImpl(
                    call, timeHierarchy);
        } else {
            memberCalc = compiler.compileMember(args[1]);
        }

        // Numeric Expression.
        final IntegerCalc indexValueCalc =
                compiler.compileInteger(args[0]);

        return new AbstractListCalc(
            call, new Calc[] {memberCalc, indexValueCalc})
        {
            public TupleList evaluateList(Evaluator evaluator) {
                Member member = memberCalc.evaluateMember(evaluator);
                int indexValue = indexValueCalc.evaluateInteger(evaluator);

                return new UnaryTupleList(
                    lastPeriods(member, evaluator, indexValue));
            }
        };
    }

    /**
     * If Index is positive, returns the set of Index
     * members ending with Member and starting with the
     * member lagging Index - 1 from Member.
     *
     * <p>If Index is negative, returns the set of (- Index)
     * members starting with Member and ending with the
     * member leading (- Index - 1) from Member.
     *
     * <p>If Index is zero, the empty set is returned.
    */
    List<Member> lastPeriods(
        Member member,
        Evaluator evaluator,
        int indexValue)
    {
        // empty set
        if ((indexValue == 0) || member.isNull()) {
            return Collections.emptyList();
        }
        List<Member> list = new ArrayList<Member>();

        // set with just member
        if ((indexValue == 1) || (indexValue == -1)) {
            list.add(member);
            return list;
        }

        // When null is found, getting the first/last
        // member at a given level is not particularly
        // fast.
        Member startMember;
        Member endMember;
        if (indexValue > 0) {
            startMember = evaluator.getSchemaReader()
                .getLeadMember(member, - (indexValue - 1));
            endMember = member;
            if (startMember.isNull()) {
                List<Member> members = evaluator.getSchemaReader()
                    .getLevelMembers(member.getLevel(), false);
                startMember = members.get(0);
            }
        } else {
            startMember = member;
            endMember = evaluator.getSchemaReader()
                .getLeadMember(member, -(indexValue + 1));
            if (endMember.isNull()) {
                List<Member> members = evaluator.getSchemaReader()
                    .getLevelMembers(member.getLevel(), false);
                endMember = members.get(members.size() - 1);
            }
        }

        evaluator.getSchemaReader().getMemberRange(
            member.getLevel(),
            startMember,
            endMember,
            list);
        return list;
    }
}

// End LastPeriodsFunDef.java
