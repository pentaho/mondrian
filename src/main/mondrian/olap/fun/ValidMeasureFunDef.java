/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.MemberCalc;
import mondrian.calc.TupleCalc;
import mondrian.calc.impl.GenericCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.type.TypeUtil;
import mondrian.olap.*;
import mondrian.rolap.RolapCube;

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
 * @version $Id$
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
            extends GenericCalc {
        private final Calc calc;

        public CalcImpl(ResolvedFunCall call, Calc calc) {
            super(call);
            this.calc = calc;
        }

        public Object evaluate(Evaluator evaluator) {
            final Member[] members;
            if (calc instanceof MemberCalc) {
                members = new Member[1];
                members[0] = ((MemberCalc) calc).evaluateMember(evaluator);
            } else {
                members = ((TupleCalc)calc).evaluateTuple(evaluator);
            }
            RolapCube baseCube = null;
            RolapCube virtualCube = (RolapCube) evaluator.getCube();
            // find the measure in the tuple
            int measurePosition = -1;
            for (int i = 0; i < members.length; i++) {
                if (members[i].getDimension().isMeasures()) {
                    measurePosition = i;
                    break;
                }
            }
            // problem: if measure is in two base cubes
            baseCube =
                    getBaseCubeofMeasure(
                            evaluator, members[measurePosition], baseCube);
            List<Dimension> vMinusBDimensions =
                    getDimensionsToForceToAllLevel(virtualCube, baseCube, members);
            // declare members array and fill in with all needed members
            final Member[] validMeasureMembers =
                    new Member[vMinusBDimensions.size() + members.length];
            System.arraycopy(members, 0, validMeasureMembers, 0, members.length);
            // start adding to validMeasureMembers at right place
            for (int i = 0; i < vMinusBDimensions.size(); i++) {
                final Hierarchy hierarchy = vMinusBDimensions.get(i).getHierarchy();
                if (hierarchy.hasAll()) {
                    validMeasureMembers[members.length + i] =
                            hierarchy.getAllMember();
                } else {
                    validMeasureMembers[members.length + i] =
                            hierarchy.getDefaultMember();
                }
            }
            // this needs to be done before validmeasuremembers are set on the
            // context since calculated members defined on a non joining
            // dimension might have been pulled to default member
            List<Member> calculatedMembers = getCalculatedMembersFromContext(evaluator);

            evaluator.setContext(validMeasureMembers);

            for (Member member : calculatedMembers) {
                evaluator.setContext(member);
            }

            return evaluator.evaluateCurrent();
        }

        private List<Member> getCalculatedMembersFromContext(Evaluator evaluator) {
            Member[] currentMembers = evaluator.getMembers();
            List<Member> calculatedMembers = new ArrayList<Member>();
            for (Member currentMember : currentMembers) {
                if(currentMember.isCalculated()){
                    calculatedMembers.add(currentMember);
                }
            }
            return calculatedMembers;
        }


        public Calc[] getCalcs() {
            return new Calc[]{calc};
        }

        private RolapCube getBaseCubeofMeasure(
                Evaluator evaluator, Member member, RolapCube baseCube) {
            final Cube[] cubes = evaluator.getSchemaReader().getCubes();
            for (Cube cube1 : cubes) {
                RolapCube cube = (RolapCube) cube1;
                if (!cube.isVirtual()) {
                    for (int j = 0; j < cube.getMeasuresMembers().length; j++) {
                        if (cube.getMeasuresMembers()[j].getName().equals(
                                member.getName())) {
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
            Member[] memberArray)
        {
            List<Dimension> vMinusBDimensions = new ArrayList<Dimension>();
            Set<Dimension> virtualCubeDims = new HashSet<Dimension>();
            virtualCubeDims.addAll(Arrays.asList(virtualCube.getDimensions()));

            Set<Dimension> nonJoiningDims =
                baseCube.nonJoiningDimensions(virtualCubeDims);

            for (Dimension nonJoiningDim : nonJoiningDims) {
                if (!isDimInMembersArray(memberArray, nonJoiningDim)) {
                    vMinusBDimensions.add(nonJoiningDim);
                }
            }
            return vMinusBDimensions;
        }

        private boolean isDimInMembersArray(
                Member[] members,
            Dimension dimension)
        {
            for (Member member : members) {
                if (member.getName().equalsIgnoreCase(dimension.getName())) {
                    return true;
                }
            }
            return false;
        }

        public boolean dependsOn(Dimension dimension) {
            // depends on all dimensions
            return butDepends(getCalcs(), dimension);
        }
    }
}

// End ValidMeasureFunDef.java
