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
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.calc.impl.ConstantCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapHierarchy;

/**
 * Definition of the <code>ParallelPeriod</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class ParallelPeriodFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "ParallelPeriod",
            "ParallelPeriod([<Level>[, <Numeric Expression>[, <Member>]]])",
            "Returns a member from a prior period in the same relative position as a specified member.",
            new String[] {"fm", "fml", "fmln", "fmlnm"},
            ParallelPeriodFunDef.class);

    public ParallelPeriodFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        if (args.length == 0) {
            // With no args, the default implementation cannot
            // guess the hierarchy, so we supply the Time
            // dimension.
            RolapHierarchy defaultTimeHierarchy =
                ((RolapCube) validator.getQuery().getCube()).getTimeHierarchy(
                    getName());
            return MemberType.forHierarchy(defaultTimeHierarchy);
        }
        return super.getResultType(validator, args);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        // Member defaults to [Time].currentmember
        Exp[] args = call.getArgs();

        // Numeric Expression defaults to 1.
        final IntegerCalc lagValueCalc =
            (args.length >= 2)
            ? compiler.compileInteger(args[1])
            : ConstantCalc.constantInteger(1);

        // If level is not specified, we compute it from
        // member at runtime.
        final LevelCalc ancestorLevelCalc =
            args.length >= 1
            ? compiler.compileLevel(args[0])
            : null;

        final MemberCalc memberCalc;
        switch (args.length) {
        case 3:
            memberCalc = compiler.compileMember(args[2]);
            break;
        case 1:
            final Hierarchy hierarchy = args[0].getType().getHierarchy();
            if (hierarchy != null) {
                // For some functions, such as Levels(<string expression>),
                // the dimension cannot be determined at compile time.
                memberCalc =
                    new HierarchyCurrentMemberFunDef.FixedCalcImpl(
                        call, hierarchy);
            } else {
                memberCalc = null;
            }
            break;
        default:
            final RolapHierarchy timeHierarchy =
                ((RolapCube) compiler.getEvaluator().getCube())
                    .getTimeHierarchy(getName());
            memberCalc =
                new HierarchyCurrentMemberFunDef.FixedCalcImpl(
                    call, timeHierarchy);
            break;
        }

        return new AbstractMemberCalc(
            call,
            new Calc[] {memberCalc, lagValueCalc, ancestorLevelCalc})
        {
            public Member evaluateMember(Evaluator evaluator) {
                Member member;
                int lagValue = lagValueCalc.evaluateInteger(evaluator);
                Level ancestorLevel;
                if (ancestorLevelCalc != null) {
                    ancestorLevel = ancestorLevelCalc.evaluateLevel(evaluator);
                    if (memberCalc == null) {
                        member =
                            evaluator.getContext(ancestorLevel.getHierarchy());
                    } else {
                        member = memberCalc.evaluateMember(evaluator);
                    }
                } else {
                    member = memberCalc.evaluateMember(evaluator);
                    Member parent = member.getParentMember();
                    if (parent == null) {
                        // This is a root member,
                        // so there is no parallelperiod.
                        return member.getHierarchy().getNullMember();
                    }
                    ancestorLevel = parent.getLevel();
                }
                return parallelPeriod(
                    member, ancestorLevel, evaluator, lagValue);
            }
        };
    }

    Member parallelPeriod(
        Member member,
        Level ancestorLevel,
        Evaluator evaluator,
        int lagValue)
    {
        // Now do some error checking.
        // The ancestorLevel and the member must be from the
        // same hierarchy.
        if (member.getHierarchy() != ancestorLevel.getHierarchy()) {
            MondrianResource.instance().FunctionMbrAndLevelHierarchyMismatch.ex(
                "ParallelPeriod",
                ancestorLevel.getHierarchy().getUniqueName(),
                member.getHierarchy().getUniqueName());
        }

        if (lagValue == Integer.MIN_VALUE) {
            // Bump up lagValue by one; otherwise -lagValue (used in
            // the getleadMember call below) is out of range because
            // Integer.MAX_VALUE == -(Integer.MIN_VALUE + 1)
            lagValue +=  1;
        }

        int distance =
            member.getLevel().getDepth()
            - ancestorLevel.getDepth();
        Member ancestor = FunUtil.ancestor(
            evaluator, member, distance, ancestorLevel);
        Member inLaw = evaluator.getSchemaReader()
            .getLeadMember(ancestor, -lagValue);
        return FunUtil.cousin(
            evaluator.getSchemaReader(), member, inLaw);
    }
}

// End ParallelPeriodFunDef.java
