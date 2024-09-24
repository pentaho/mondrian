/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Definition of the <code>DrilldownLevel</code> MDX function.
 *
 * <p>Syntax:
 *
 * <blockquote><pre>
 * DrilldownLevel(Set_Expression[, Level_Expression])
 * DrilldownLevel(Set_Expression, , Numeric_Expression)
 * </pre></blockquote>
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class DrilldownLevelFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "DrilldownLevel",
            "DrilldownLevel(<Set>[, <Level>]) or DrilldownLevel(<Set>, , <Index>)",
            "Drills down the members of a set, at a specified level, to one level below. Alternatively, drills down on a specified dimension in the set.",
            new String[]{"fxx", "fxxl", "fxxen"},
            DrilldownLevelFunDef.class);

    public DrilldownLevelFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
            compiler.compileList(call.getArg(0));
        final LevelCalc levelCalc =
            call.getArgCount() > 1
                && call.getArg(1).getType()
                instanceof mondrian.olap.type.LevelType
                ? compiler.compileLevel(call.getArg(1))
                : null;
        final IntegerCalc indexCalc =
            call.getArgCount() > 2
                ? compiler.compileInteger(call.getArg(2))
                : null;
        final int arity = listCalc.getType().getArity();
        if (indexCalc == null) {
            return new AbstractListCalc(call, new Calc[] {listCalc, levelCalc})
            {
                public TupleList evaluateList(Evaluator evaluator) {
                    TupleList list = listCalc.evaluateList(evaluator);
                    if (list.size() == 0) {
                        return list;
                    }
                    int searchDepth = -1;
                    if (levelCalc != null) {
                        Level level = levelCalc.evaluateLevel(evaluator);
                        searchDepth = level.getDepth();
                    }
                    return new UnaryTupleList(
                        drill(searchDepth, list.slice(0), evaluator));
                }
            };
        } else {
            return new AbstractListCalc(call, new Calc[] {listCalc, indexCalc})
            {
                public TupleList evaluateList(Evaluator evaluator) {
                    TupleList list = listCalc.evaluateList(evaluator);
                    if (list.isEmpty()) {
                        return list;
                    }
                    final int index = indexCalc.evaluateInteger(evaluator);
                    if (index < 0 || index >= arity) {
                        return list;
                    }
                    TupleList result = TupleCollections.createList(arity);
                    final SchemaReader schemaReader =
                        evaluator.getSchemaReader();
                    final Member[] tupleClone = new Member[arity];
                    for (List<Member> tuple : list) {
                        result.add(tuple);
                        final List<Member> children =
                            schemaReader.getMemberChildren(tuple.get(index));
                        for (Member child : children) {
                            tuple.toArray(tupleClone);
                            tupleClone[index] = child;
                            result.addTuple(tupleClone);
                        }
                    }
                    return result;
                }
            };
        }
    }

    List<Member> drill(int searchDepth, List<Member> list, Evaluator evaluator)
    {
        if (searchDepth == -1) {
            searchDepth = list.get(0).getLevel().getDepth();

            for (int i = 1, m = list.size(); i < m; i++) {
                Member member = list.get(i);
                int memberDepth = member.getLevel().getDepth();

                if (memberDepth > searchDepth) {
                    searchDepth = memberDepth;
                }
            }
        }

        List<Member> drilledSet = new ArrayList<Member>();

        for (int i = 0, m = list.size(); i < m; i++) {
            Member member = list.get(i);
            drilledSet.add(member);

            Member nextMember =
                i == (m - 1)
                ? null
                : list.get(i + 1);

            //
            // This member is drilled if it's at the correct depth
            // and if it isn't drilled yet. A member is considered
            // to be "drilled" if it is immediately followed by
            // at least one descendant
            //
            if (member.getLevel().getDepth() == searchDepth
                && !FunUtil.isAncestorOf(member, nextMember, true))
            {
                final List<Member> childMembers =
                    evaluator.getSchemaReader().getMemberChildren(member);
                for (Member childMember : childMembers) {
                    drilledSet.add(childMember);
                }
            }
        }
        return drilledSet;
    }
}

// End DrilldownLevelFunDef.java
