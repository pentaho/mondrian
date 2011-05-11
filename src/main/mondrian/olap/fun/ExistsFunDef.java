/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2002 Kana Software, Inc.
// Copyright (C) 2004-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.*;

/**
 * Definition of the <code>EXISTS</code> MDX function.
 *
 * @author kvu
 * @version $Id$
 * @since Mar 23, 2008
 */
class ExistsFunDef extends FunDefBase
{
    static final Resolver resolver = new ReflectiveMultiResolver(
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

                List<Dimension> leftDims = getDimensions(leftTuples.get(0));
                List<Dimension> rightDims = getDimensions(rightTuples.get(0));

                // map dimensions of right object to those in left object
                // return empty list if not all of right dims can be mapped
                int rightSize = rightDims.size();
                int [] idxmap = new int[rightSize];
                for (int i = 0; i < rightSize; i++) {
                    Dimension d = rightDims.get(i);
                    if (leftDims.contains(d)) {
                        idxmap[i] = leftDims.indexOf(d);
                    } else {
                        return TupleCollections.emptyList(
                           leftTuples.getArity());
                    }
                }

                leftLoop:
                for (List<Member> leftTuple : leftTuples) {
                    rightLoop:
                    for (List<Member> rightTuple : rightTuples) {
                        for (int i = 0; i < rightSize; i++) {
                            Member leftMem = leftTuple.get(idxmap[i]);
                            Member rightMem = rightTuple.get(i);
                            if (!isOnSameHierarchyChain(leftMem, rightMem)) {
                                // Right tuple does not match left tuple. Try
                                // next right tuple.
                                continue rightLoop;
                            }
                        }
                        // Left tuple matches one of the right tuples. Add it
                        // to the result.
                        result.add(leftTuple);
                        continue leftLoop;
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

    private static List<Dimension> getDimensions(List<Member> members)
    {
        List<Dimension> dimensions = new ArrayList<Dimension>();
        for (Member member : members) {
            dimensions.add(member.getDimension());
        }
        return dimensions;
    }
}

// End ExistsFunDef.java
