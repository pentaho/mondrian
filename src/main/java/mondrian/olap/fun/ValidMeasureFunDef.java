/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.GenericCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.TypeUtil;
import mondrian.resource.MondrianResource;
import mondrian.rolap.*;

import java.util.*;

/**
 * Definition of the <code>ValidMeasure</code> MDX function.
 *
 * <p>Returns a valid measure in a virtual cube by forcing inapplicable
 * dimensions to their top level.
 *
 * <p>Syntax:
 * <blockquote><code>
 * ValidMeasure(&lt;Tuple&gt;)
 * </code></blockquote>
 *
 * @author kwalker, mpflug
 */
public class ValidMeasureFunDef extends FunDefBase
{
    static final ValidMeasureFunDef instance = new ValidMeasureFunDef();

    private ValidMeasureFunDef() {
        super(
            "ValidMeasure",
                "Returns a valid measure in a virtual cube by forcing inapplicable dimensions to their top level.",
                "fnt");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Calc calc;
        final Exp arg = call.getArg(0);
        if (TypeUtil.couldBeMember(arg.getType())) {
            calc = compiler.compileMember(arg);
        } else {
            calc = compiler.compileTuple(arg);
        }
        return new CalcImpl(call, calc);
    }

    private static class CalcImpl
        extends GenericCalc
    {
        private final Calc calc;

        public CalcImpl(ResolvedFunCall call, Calc calc) {
            super(call);
            this.calc = calc;
        }

        public Object evaluate(Evaluator evaluator) {
            final List<Member> memberList;
            if (calc.isWrapperFor(MemberCalc.class)) {
                memberList =
                    Collections.singletonList(
                        calc.unwrap(MemberCalc.class)
                            .evaluateMember(evaluator));
            } else {
                final Member[] tupleMembers =
                    calc.unwrap((TupleCalc.class)).evaluateTuple(evaluator);
                memberList = Arrays.asList(tupleMembers);
            }
            RolapCube virtualCube = (RolapCube) evaluator.getCube();
            // find the measure in the tuple
            int measurePosition = -1;
            for (int i = 0; i < memberList.size(); i++) {
                if (memberList.get(i).getDimension().isMeasures()) {
                    measurePosition = i;
                    break;
                }
            }
            // problem: if measure is in two base cubes
            RolapMeasureGroup measureGroup =
                getMeasureGroup(memberList.get(measurePosition));
            // declare members array and fill in with all needed members
            final List<Member> validMeasureMembers =
                forceMembersToAll(memberList, virtualCube, measureGroup);
            // this needs to be done before validMeasureMembers are set on the
            // context since calculated members defined on a non joining
            // dimension might have been pulled to default member
            List<Member> calculatedMembers =
                getCalculatedMembersFromContext(evaluator);

            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setContext(validMeasureMembers);
                evaluator.setContext(calculatedMembers);
                return evaluator.evaluateCurrent();
            } finally {
                evaluator.restore(savepoint);
            }
        }

        private List<Member> getCalculatedMembersFromContext(
            Evaluator evaluator)
        {
            Member[] currentMembers = evaluator.getMembers();
            List<Member> calculatedMembers = new ArrayList<Member>();
            for (Member currentMember : currentMembers) {
                if (currentMember.isCalculated()) {
                    calculatedMembers.add(currentMember);
                }
            }
            return calculatedMembers;
        }

        public Calc[] getCalcs() {
            return new Calc[]{calc};
        }

        private RolapMeasureGroup getMeasureGroup(Member member) {
            if (!RolapStoredMeasure.class
                .isAssignableFrom(member.getClass()))
            {
                // Cannot use calculated members in ValidMeasure.
                throw MondrianResource.instance()
                    .ValidMeasureUsingCalculatedMember
                    .ex(member.getUniqueName());
            }
            return ((RolapStoredMeasure) member).getMeasureGroup();
        }

        // REVIEW: We could compute some of this information at prepare time.
        private List<Member> forceMembersToAll(
            List<Member> memberList,
            RolapCube virtualCube,
            RolapMeasureGroup measureGroup)
        {
            final List<Member> validMeasureMembers =
                new ArrayList<Member>(memberList);

            final Set<Hierarchy> hierarchies = new HashSet<Hierarchy>();
            for (Member member : memberList) {
                hierarchies.add(member.getHierarchy());
            }

            // start adding to validMeasureMembers at right place
            for (RolapCubeDimension nonJoiningDim
                : measureGroup.nonJoiningDimensions(
                    virtualCube.getDimensionList()))
            {
                for (Hierarchy hierarchy : nonJoiningDim.getHierarchyList()) {
                    if (!hierarchies.contains(hierarchy)) {
                        validMeasureMembers.add(
                            hierarchy.hasAll()
                                ? hierarchy.getAllMember()
                                : hierarchy.getDefaultMember());
                    }
                }
            }
            return validMeasureMembers;
        }

        public boolean dependsOn(Hierarchy hierarchy) {
            // depends on all hierarchies
            return butDepends(getCalcs(), hierarchy);
        }
    }
}

// End ValidMeasureFunDef.java
