/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
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
                            leftDims, rightDims, null))
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
