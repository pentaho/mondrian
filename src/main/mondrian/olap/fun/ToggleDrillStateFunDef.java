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

import mondrian.olap.*;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.resource.MondrianResource;

import java.util.List;
import java.util.HashSet;
import java.util.ArrayList;

/**
 * Definition of the <code>ToggleDrillState</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class ToggleDrillStateFunDef extends FunDefBase {
    static final String[] ReservedWords = new String[] {"RECURSIVE"};
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
            "ToggleDrillState",
            "ToggleDrillState(<Set1>, <Set2>[, RECURSIVE])",
            "Toggles the drill state of members. This function is a combination of DrillupMember and DrilldownMember.",
            new String[]{"fxxx", "fxxxy"},
            ToggleDrillStateFunDef.class,
            ReservedWords);

    public ToggleDrillStateFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        if (call.getArgCount() > 2) {
            throw MondrianResource.instance().ToggleDrillStateRecursiveNotSupported.ex();
        }
        final ListCalc listCalc0 =
                compiler.compileList(call.getArg(0));
        final ListCalc listCalc1 =
                compiler.compileList(call.getArg(1));
        return new AbstractListCalc(call, new Calc[] {listCalc0, listCalc1}) {
            public List evaluateList(Evaluator evaluator) {
                final List list0 = listCalc0.evaluateList(evaluator);
                final List list1 = listCalc1.evaluateList(evaluator);
                return toggleDrillState(evaluator, list0, list1);
            }
        };
    }

    List toggleDrillState(Evaluator evaluator, List v0, List list1) {
        if (list1.isEmpty()) {
            return v0;
        }
        if (v0.isEmpty()) {
            return v0;
        }
        HashSet set = new HashSet();
        set.addAll(list1);
        HashSet set1 = set;
        List result = new ArrayList();
        int i = 0, n = v0.size();
        while (i < n) {
            Object o = v0.get(i++);
            result.add(o);
            Member m = null;
            int k = -1;
            if (o instanceof Member) {
                if (!set1.contains(o)) {
                    continue;
                }
                m = (Member) o;
                k = -1;
            } else {
                Util.assertTrue(o instanceof Member[]);
                Member[] members = (Member[]) o;
                for (int j = 0; j < members.length; j++) {
                    Member member = members[j];
                    if (set1.contains(member)) {
                        k = j;
                        m = member;
                        break;
                    }
                }
                if (k == -1) {
                    continue;
                }
            }
            boolean isDrilledDown = false;
            if (i < n) {
                Object next = v0.get(i);
                Member nextMember = (k < 0) ? (Member) next :
                        ((Member[]) next)[k];
                boolean strict = true;
                if (FunUtil.isAncestorOf(m, nextMember, strict)) {
                    isDrilledDown = true;
                }
            }
            if (isDrilledDown) {
                // skip descendants of this member
                do {
                    Object next = v0.get(i);
                    Member nextMember = (k < 0) ? (Member) next :
                            ((Member[]) next)[k];
                    boolean strict = true;
                    if (FunUtil.isAncestorOf(m, nextMember, strict)) {
                        i++;
                    } else {
                        break;
                    }
                } while (i < n);
            } else {
                Member[] children = evaluator.getSchemaReader().getMemberChildren(m);
                for (int j = 0; j < children.length; j++) {
                    if (k < 0) {
                        result.add(children[j]);
                    } else {
                        Member[] members = (Member[]) ((Member[]) o).clone();
                        members[k] = children[j];
                        result.add(members);
                    }
                }
            }
        }
        return result;
    }

}

// End ToggleDrillStateFunDef.java
