/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.MemberType;
import mondrian.calc.*;
import mondrian.calc.impl.DimensionCurrentMemberCalc;
import mondrian.calc.impl.ConstantCalc;
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.resource.MondrianResource;

/**
 * Definition of the <code>ParallelPeriod</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class ParallelPeriodFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
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
            Dimension defaultTimeDimension = 
                validator.getQuery().getCube().getTimeDimension();
            if (defaultTimeDimension == null) {
                throw MondrianResource.instance().
                            NoTimeDimensionInCube.ex(getName());
            }
            Hierarchy hierarchy = defaultTimeDimension.getHierarchy();
            return MemberType.forHierarchy(hierarchy);
        }
        return super.getResultType(validator, args);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        // Member defaults to [Time].currentmember
        Exp[] args = call.getArgs();
        final MemberCalc memberCalc;
        switch (args.length) {
        case 3:
            memberCalc = compiler.compileMember(args[2]);
            break;
        case 1:
            final Dimension dimension =
                    args[0].getType().getHierarchy().getDimension();
            memberCalc = new DimensionCurrentMemberCalc(dimension);
            break;
        default:
            final Dimension timeDimension =
                    compiler.getEvaluator().getCube()
                    .getTimeDimension();
            if (timeDimension == null) {
                throw MondrianResource.instance().
                            NoTimeDimensionInCube.ex(getName());
            }
            memberCalc = new DimensionCurrentMemberCalc(
                    timeDimension);
            break;
        }

        // Numeric Expression defaults to 1.
        final IntegerCalc lagValueCalc = (args.length >= 2) ?
                compiler.compileInteger(args[1]) :
                ConstantCalc.constantInteger(1);

        // If level is not specified, we compute it from
        // member at runtime.
        final LevelCalc ancestorLevelCalc =
                args.length >= 1 ?
                compiler.compileLevel(args[0]) :
                null;

        return new AbstractMemberCalc(call, new Calc[] {memberCalc, lagValueCalc, ancestorLevelCalc}) {
            public Member evaluateMember(Evaluator evaluator) {
                Member member = memberCalc.evaluateMember(
                        evaluator);
                int lagValue = lagValueCalc.evaluateInteger(
                        evaluator);
                Level ancestorLevel;
                if (ancestorLevelCalc != null) {
                    ancestorLevel = ancestorLevelCalc
                            .evaluateLevel(evaluator);
                } else {
                    Member parent = member.getParentMember();
                    if (parent == null) {
                        // This is a root member,
                        // so there is no parallelperiod.
                        return member.getHierarchy()
                                .getNullMember();
                    }
                    ancestorLevel = parent.getLevel();
                }
                return parallelPeriod(member, ancestorLevel,
                        evaluator, lagValue);
            }
        };
    }

    Member parallelPeriod(
            Member member,
            Level ancestorLevel,
            Evaluator evaluator,
            int lagValue) {
        // Now do some error checking.
        // The ancestorLevel and the member must be from the
        // same hierarchy.
        if (member.getHierarchy() != ancestorLevel.getHierarchy()) {
            MondrianResource.instance().FunctionMbrAndLevelHierarchyMismatch.ex(
                    "ParallelPeriod", ancestorLevel.getHierarchy().getUniqueName(),
                    member.getHierarchy().getUniqueName()
            );
        }

        if (lagValue == Integer.MIN_VALUE) {
            // bump up lagValue by one 
            // otherwise -lagValue(used in the getleadMember call below)is out of range
            // because Integer.MAX_VALUE == -(Integer.MIN_VALUE + 1)
            lagValue +=  1;
        }

        int distance = member.getLevel().getDepth() -
                ancestorLevel.getDepth();
        Member ancestor = FunUtil.ancestor(
                evaluator, member, distance, ancestorLevel);
        Member inLaw = evaluator.getSchemaReader()
                .getLeadMember(ancestor, -lagValue);
        return FunUtil.cousin(
                evaluator.getSchemaReader(), member, inLaw);
    }
}

// End ParallelPeriodFunDef.java
