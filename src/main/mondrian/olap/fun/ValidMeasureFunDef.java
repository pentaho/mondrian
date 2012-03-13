/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.GenericCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.TypeUtil;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapMember;

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
                memberList = new ArrayList<Member>(1);
                memberList.add(
                    calc.unwrap(MemberCalc.class).evaluateMember(evaluator));
            } else {
                final Member[] tupleMembers =
                    calc.unwrap((TupleCalc.class)).evaluateTuple(evaluator);
                memberList = Arrays.asList(tupleMembers);
            }
            RolapCube baseCube = null;
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
            baseCube =
                getBaseCubeofMeasure(
                    evaluator, memberList.get(measurePosition), baseCube);
            List<Dimension> vMinusBDimensions =
                getDimensionsToForceToAllLevel(
                    virtualCube, baseCube, memberList);
            // declare members array and fill in with all needed members
            final List<Member> validMeasureMembers =
                new ArrayList<Member>(memberList);
            // start adding to validMeasureMembers at right place
            for (Dimension vMinusBDimension : vMinusBDimensions) {
                final Hierarchy hierarchy = vMinusBDimension.getHierarchy();
                if (hierarchy.hasAll()) {
                    validMeasureMembers.add(hierarchy.getAllMember());
                } else {
                    validMeasureMembers.add(hierarchy.getDefaultMember());
                }
            }
            // this needs to be done before validmeasuremembers are set on the
            // context since calculated members defined on a non joining
            // dimension might have been pulled to default member
            List<Member> calculatedMembers =
                getCalculatedMembersFromContext(evaluator);

            evaluator.setContext(validMeasureMembers);
            evaluator.setContext(calculatedMembers);

            return evaluator.evaluateCurrent();
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

        private RolapCube getBaseCubeofMeasure(
            Evaluator evaluator, Member member, RolapCube baseCube)
        {
            final Cube[] cubes = evaluator.getSchemaReader().getCubes();
            for (Cube cube1 : cubes) {
                RolapCube cube = (RolapCube) cube1;
                if (!cube.isVirtual()) {
                    for (RolapMember measure : cube.getMeasuresMembers()) {
                        if (measure.getName().equals(member.getName())) {
                            baseCube = cube;
                        }
                    }
                }
                if (baseCube != null) {
                    break;
                }
            }
            return baseCube;
        }

        private List<Dimension> getDimensionsToForceToAllLevel(
            RolapCube virtualCube,
            RolapCube baseCube,
            List<Member> memberList)
        {
            List<Dimension> vMinusBDimensions = new ArrayList<Dimension>();
            Set<Dimension> virtualCubeDims = new HashSet<Dimension>();
            virtualCubeDims.addAll(Arrays.asList(virtualCube.getDimensions()));

            Set<Dimension> nonJoiningDims =
                baseCube.nonJoiningDimensions(virtualCubeDims);

            for (Dimension nonJoiningDim : nonJoiningDims) {
                if (!isDimInMembersList(memberList, nonJoiningDim)) {
                    vMinusBDimensions.add(nonJoiningDim);
                }
            }
            return vMinusBDimensions;
        }

        private boolean isDimInMembersList(
            List<Member> members,
            Dimension dimension)
        {
            for (Member member : members) {
                if (member.getName().equalsIgnoreCase(dimension.getName())) {
                    return true;
                }
            }
            return false;
        }

        public boolean dependsOn(Hierarchy hierarchy) {
            // depends on all hierarchies
            return butDepends(getCalcs(), hierarchy);
        }
    }
}

// End ValidMeasureFunDef.java
