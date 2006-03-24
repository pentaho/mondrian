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

import mondrian.olap.FunDef;
import mondrian.olap.Evaluator;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.LevelCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;
import java.util.ArrayList;

/**
 * Definition of the <code>DrilldownLevel</code> MDX function.
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
            new String[]{"fxx", "fxxl"},
            DrilldownLevelFunDef.class);

    public DrilldownLevelFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
                compiler.compileList(call.getArg(0));
        final LevelCalc levelCalc =
                call.getArgCount() > 1 ?
                compiler.compileLevel(call.getArg(1)) :
                null;
        return new AbstractListCalc(call, new Calc[] {listCalc, levelCalc}) {
            public List evaluateList(Evaluator evaluator) {
                List list = listCalc.evaluateList(evaluator);
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
    }

    List drill(int searchDepth, List list, Evaluator evaluator) {
        if (searchDepth == -1) {
            searchDepth = ((Member)list.get(0)).getLevel().getDepth();

            for (int i = 1, m = list.size(); i < m; i++) {
                Member member = (Member) list.get(i);
                int memberDepth = member.getLevel().getDepth();

                if (memberDepth > searchDepth) {
                    searchDepth = memberDepth;
                }
            }
        }

        List drilledSet = new ArrayList();

        for (int i = 0, m = list.size(); i < m; i++) {
            Member member = (Member) list.get(i);
            drilledSet.add(member);

            Member nextMember = i == (m - 1) ?
                    null :
                    (Member) list.get(i + 1);

            //
            // This member is drilled if it's at the correct depth
            // and if it isn't drilled yet. A member is considered
            // to be "drilled" if it is immediately followed by
            // at least one descendant
            //
            if (member.getLevel().getDepth() == searchDepth
                    && !FunUtil.isAncestorOf(member, nextMember, true)) {
                Member[] childMembers = evaluator.getSchemaReader().getMemberChildren(member);
                for (int j = 0; j < childMembers.length; j++) {
                    drilledSet.add(childMembers[j]);
                }
            }
        }
        return drilledSet;
    }
}

// End DrilldownLevelFunDef.java
