/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.SetType;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.*;

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
 * @version $Id$
 * @since Mar 23, 2006
 */
class DrilldownLevelFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
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
        final int arity = ((SetType) listCalc.getType()).getArity();
        if (indexCalc == null) {
            return new AbstractListCalc(call, new Calc[] {listCalc, levelCalc}) {
                public List evaluateList(Evaluator evaluator) {
                    List<Member> list = listCalc.evaluateList(evaluator);
                    if (list.size() == 0) {
                        return list;
                    }
                    int searchDepth = -1;
                    if (levelCalc != null) {
                        Level level = levelCalc.evaluateLevel(evaluator);
                        searchDepth = level.getDepth();
                    }
                    return drill(searchDepth, list, evaluator);
                }
            };
        } else if (arity == 1) {
            return new AbstractListCalc(call, new Calc[] {listCalc, indexCalc}) {
                public List evaluateList(Evaluator evaluator) {
                    List<Member> list = listCalc.evaluateList(evaluator);
                    if (list.size() == 0) {
                        return list;
                    }
                    final int index = indexCalc.evaluateInteger(evaluator);
                    List<Member> result = new ArrayList<Member>();
                    final SchemaReader schemaReader =
                        evaluator.getSchemaReader();
                    for (Member member : list) {
                        result.add(member);
                        if (index == 0) {
                            final List<Member> children =
                                schemaReader.getMemberChildren(member);
                            result.addAll(children);
                        }
                    }
                    return result;
                }
            };
        } else {
            return new AbstractListCalc(call, new Calc[] {listCalc, indexCalc}) {
                public List evaluateList(Evaluator evaluator) {
                    List<Member[]> list = listCalc.evaluateList(evaluator);
                    if (list.size() == 0) {
                        return list;
                    }
                    final int index = indexCalc.evaluateInteger(evaluator);
                    List<Member[]> result = new ArrayList<Member[]>();
                    final SchemaReader schemaReader =
                        evaluator.getSchemaReader();
                    for (Member[] tuple : list) {
                        result.add(tuple);
                        if (index >= 0 && index < tuple.length) {
                            final List<Member> children =
                                schemaReader.getMemberChildren(tuple[index]);
                            for (Member child : children) {
                                final Member[] tupleClone = tuple.clone();
                                tupleClone[index] = child;
                                result.add(tupleClone);
                            }
                        }
                    }
                    return result;
                }
            };
        }
    }

    List<Member> drill(int searchDepth, List<Member> list, Evaluator evaluator) {
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

            Member nextMember = i == (m - 1) ?
                null :
                list.get(i + 1);

            //
            // This member is drilled if it's at the correct depth
            // and if it isn't drilled yet. A member is considered
            // to be "drilled" if it is immediately followed by
            // at least one descendant
            //
            if (member.getLevel().getDepth() == searchDepth
                    && !FunUtil.isAncestorOf(member, nextMember, true)) {
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
