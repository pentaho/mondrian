/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2002 Kana Software, Inc.
// Copyright (C) 2004-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.impl.*;
import mondrian.olap.*;
import mondrian.calc.*;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.type.SetType;

import java.util.*;

/**
 * Definition of the <code>INTERSECT</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class IntersectFunDef extends FunDefBase
{
    private static final String[] ReservedWords = new String[] {"ALL"};

    static final Resolver resolver = new ReflectiveMultiResolver(
            "Intersect",
            "Intersect(<Set1>, <Set2>[, ALL])",
            "Returns the intersection of two input sets, optionally retaining duplicates.",
            new String[] {"fxxxy", "fxxx"},
            IntersectFunDef.class,
            ReservedWords);

    public IntersectFunDef(FunDef dummyFunDef)
    {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final String literalArg = getLiteralArg(call, 2, "", ReservedWords);
        final boolean all = literalArg.equalsIgnoreCase("ALL");
        final int arity = ((SetType) call.getType()).getArity();

        if (arity == 1) {
            final MemberListCalc listCalc1 =
                (MemberListCalc) compiler.compileList(call.getArg(0));
            final MemberListCalc listCalc2 =
                (MemberListCalc) compiler.compileList(call.getArg(1));
            return new AbstractMemberListCalc(
                call, new Calc[] {listCalc1, listCalc2})
            {
                public List<Member> evaluateMemberList(Evaluator evaluator) {
                    List<Member> leftList =
                        listCalc1.evaluateMemberList(evaluator);
                    if (leftList.isEmpty()) {
                        return Collections.emptyList();
                    }
                    List<Member> rightList =
                        listCalc2.evaluateMemberList(evaluator);
                    if (rightList.isEmpty()) {
                        return Collections.emptyList();
                    }
                    final Set<Member> rightSet = new HashSet<Member>(rightList);
                    final List<Member> result = new ArrayList<Member>();
                    final Set<Member> resultSet =
                        all ? null : new HashSet<Member>();
                    for (Member leftMember : leftList) {
                        if (rightSet.contains(leftMember)) {
                            if (resultSet == null
                                || resultSet.add(leftMember))
                            {
                                result.add(leftMember);
                            }
                        }
                    }
                    return result;
                }
            };
        } else {
            final TupleListCalc listCalc1 =
                (TupleListCalc) compiler.compileList(call.getArg(0));
            final TupleListCalc listCalc2 =
                (TupleListCalc) compiler.compileList(call.getArg(1));
            return new AbstractTupleListCalc(
                call, new Calc[] {listCalc1, listCalc2})
            {
                public List<Member[]> evaluateTupleList(Evaluator evaluator) {
                    List<Member[]> leftList =
                        listCalc1.evaluateTupleList(evaluator);
                    if (leftList.isEmpty()) {
                        return Collections.emptyList();
                    }
                    final List<Member[]> rightList =
                        listCalc2.evaluateTupleList(evaluator);
                    if (rightList.isEmpty()) {
                        return Collections.emptyList();
                    }
                    Set<List<Member>> rightSet =
                        buildSearchableCollection(rightList);
                    final List<Member[]> result = new ArrayList<Member[]>();
                    final Set<List<Member>> resultSet =
                        all ? null : new HashSet<List<Member>>();
                    for (Member[] leftTuple : leftList) {
                        List<Member> leftKey = Arrays.asList(leftTuple);
                        if (rightSet.contains(leftKey)) {
                            if (resultSet == null
                                || resultSet.add(leftKey))
                            {
                                result.add(leftTuple);
                            }
                        }
                    }
                    return result;
                }

                private Set<List<Member>> buildSearchableCollection(
                    List<Member[]> tuples)
                {
                    Set<List<Member>> result =
                        new HashSet<List<Member>>(tuples.size(), 1);
                    for (Member[] tuple : tuples) {
                        result.add(Arrays.asList(tuple));
                    }
                    return result;
                }
            };
        }
    }
}

// End IntersectFunDef.java
