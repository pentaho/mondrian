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
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Definition of the <code>EXISTS</code> MDX function.
 *
 * @author kvu
 * @since Mar 23, 2008
 */
class ExistsFunDef extends FunDefBase
{
    static final Resolver resolver =
        new ReflectiveMultiResolver(
            "Exists",
            "Exists(<Set1>, <Set2>])",
            "Returns the the set of tuples of the first set that exist with one or more tuples of the second set.",
            new String[] {"fxxx"},
            ExistsFunDef.class);

    public ExistsFunDef(FunDef dummyFunDef)
    {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc2 = compiler.compileList(call.getArg(1));

        return new AbstractListCalc(call, new Calc[] {listCalc1, listCalc2}) {
            public TupleList evaluateList(Evaluator evaluator) {
                TupleList leftTuples = listCalc1.evaluateList(evaluator);
                if (leftTuples.isEmpty()) {
                    return TupleCollections.emptyList(leftTuples.getArity());
                }
                TupleList rightTuples = listCalc2.evaluateList(evaluator);
                if (rightTuples.isEmpty()) {
                    return TupleCollections.emptyList(leftTuples.getArity());
                }
                TupleList result =
                    TupleCollections.createList(leftTuples.getArity());

                List<Hierarchy> leftDims = getHierarchies(leftTuples.get(0));
                List<Hierarchy> rightDims = getHierarchies(rightTuples.get(0));

                leftLoop:
                for (List<Member> leftTuple : leftTuples) {
                    for (List<Member> rightTuple : rightTuples) {
                        if (existsInTuple(leftTuple, rightTuple,
                            leftDims, rightDims))
                        {
                            result.add(leftTuple);
                            continue leftLoop;
                        }
                    }
                }
                return result;
            }
        };
    }

    private static boolean isOnSameHierarchyChain(Member mA, Member mB)
    {
        return (FunUtil.isAncestorOf(mA, mB, false))||
            (FunUtil.isAncestorOf(mB, mA, false));
    }


    /**
     * Returns true if leftTuple Exists w/in rightTuple
     *
     *
     *
     * @param leftTuple tuple from arg one of EXISTS()
     * @param rightTuple tuple from arg two of EXISTS()
     * @param leftHierarchies list of hierarchies from leftTuple, in the same
     *                        order
     * @param rightHierarchies list of the hiearchies from rightTuple,
     *                         in the same order
     * @return true if each member from leftTuple is somewhere in the
     *         hierarchy chain of the corresponding member from rightTuple,
     *         false otherwise.
     *         If there is no explicit corresponding member from either
     *         right or left, then the default member is used.
     */
    private boolean existsInTuple(
        final List<Member> leftTuple, final List<Member> rightTuple,
        final List<Hierarchy> leftHierarchies,
        final List<Hierarchy> rightHierarchies)
    {
        List<Member> checkedMembers = new ArrayList<Member>();

        for (Member leftMember : leftTuple) {
            Member rightMember = getCorrespondingMember(
                leftMember, rightTuple, rightHierarchies);
            checkedMembers.add(rightMember);
            if (!isOnSameHierarchyChain(leftMember, rightMember)) {
                return false;
            }
        }
        // this loop handles members in the right tuple not present in left
        // Such a member could only impact the resulting tuple list if the
        // default member of the hierarchy is not the all member.
        for (Member rightMember : rightTuple) {
            if (checkedMembers.contains(rightMember)) {
                // already checked in the previous loop
                continue;
            }
            Member leftMember = getCorrespondingMember(
                rightMember, leftTuple, leftHierarchies);
            if (!isOnSameHierarchyChain(leftMember, rightMember)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the corresponding member from tuple, or the default member
     * for the hierarchy if member is not explicitly contained in the tuple.
     *
     *
     * @param member source member
     * @param tuple tuple containing the target member
     * @param tupleHierarchies list of the hierarchies explicitly contained
     *                         in the tuple, in the same order.
     * @return target member
     */
    private Member getCorrespondingMember(
        final Member member, final List<Member> tuple,
        final List<Hierarchy> tupleHierarchies)
    {
        assert tuple.size() == tupleHierarchies.size();
        int dimPos = tupleHierarchies.indexOf(member.getHierarchy());
        if (dimPos >= 0) {
            return tuple.get(dimPos);
        } else {
            return member.getHierarchy().getDefaultMember();
        }
    }

    private static List<Hierarchy> getHierarchies(final List<Member> members)
    {
        List<Hierarchy> hierarchies = new ArrayList<Hierarchy>();
        for (Member member : members) {
            hierarchies.add(member.getHierarchy());
        }
        return hierarchies;
    }

}

// End ExistsFunDef.java
