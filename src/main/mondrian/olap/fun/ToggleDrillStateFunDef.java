/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.SetType;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.resource.MondrianResource;

import java.util.List;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Set;

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
        if (((SetType) call.getType()).getArity() == 1) {
            return new AbstractListCalc(call, new Calc[] {listCalc0, listCalc1}) {
                public List evaluateList(Evaluator evaluator) {
                    final List<Member> list0 = listCalc0.evaluateList(evaluator);
                    final List<Member> list1 = listCalc1.evaluateList(evaluator);
                    return toggleDrillStateMembers(evaluator, list0, list1);
                }
            };
        } else {
            return new AbstractListCalc(call, new Calc[] {listCalc0, listCalc1}) {
                public List evaluateList(Evaluator evaluator) {
                    final List<Member[]> list0 = listCalc0.evaluateList(evaluator);
                    final List<Member> list1 = listCalc1.evaluateList(evaluator);
                    return toggleDrillStateTuples(evaluator, list0, list1);
                }
            };
        }
    }

    List<Member> toggleDrillStateMembers(
        Evaluator evaluator, List<Member> v0, List<Member> list1)
    {
        if (list1.isEmpty()) {
            return v0;
        }
        if (v0.isEmpty()) {
            return v0;
        }
        Set<Member> set = new HashSet<Member>();
        set.addAll(list1);
        List<Member> result = new ArrayList<Member>();
        int i = 0, n = v0.size();
        while (i < n) {
            Member m = v0.get(i++);
            result.add(m);
            if (!set.contains(m)) {
                continue;
            }
            boolean isDrilledDown = false;
            if (i < n) {
                Member nextMember = v0.get(i);
                boolean strict = true;
                if (FunUtil.isAncestorOf(m, nextMember, strict)) {
                    isDrilledDown = true;
                }
            }
            if (isDrilledDown) {
                // skip descendants of this member
                do {
                    Member nextMember = v0.get(i);
                    boolean strict = true;
                    if (FunUtil.isAncestorOf(m, nextMember, strict)) {
                        i++;
                    } else {
                        break;
                    }
                } while (i < n);
            } else {
                List<Member> children =
                    evaluator.getSchemaReader().getMemberChildren(m);
                for (Member child : children) {
                    result.add(child);
                }
            }
        }
        return result;
    }

    List<Member[]> toggleDrillStateTuples(
        Evaluator evaluator, List<Member[]> v0, List<Member> list1)
    {
        if (list1.isEmpty()) {
            return v0;
        }
        if (v0.isEmpty()) {
            return v0;
        }
        Set<Member> set = new HashSet<Member>();
        set.addAll(list1);
        List<Member[]> result = new ArrayList<Member[]>();
        int i = 0, n = v0.size();
        while (i < n) {
            Member[] o = v0.get(i++);
            result.add(o);
            Member m = null;
            int k = -1;
            for (int j = 0; j < o.length; j++) {
                Member member = o[j];
                if (set.contains(member)) {
                    k = j;
                    m = member;
                    break;
                }
            }
            if (k == -1) {
                continue;
            }
            boolean isDrilledDown = false;
            if (i < n) {
                Member[] next = v0.get(i);
                Member nextMember = next[k];
                boolean strict = true;
                if (FunUtil.isAncestorOf(m, nextMember, strict)) {
                    isDrilledDown = true;
                }
            }
            if (isDrilledDown) {
                // skip descendants of this member
                do {
                    Member[] next = v0.get(i);
                    Member nextMember = next[k];
                    boolean strict = true;
                    if (FunUtil.isAncestorOf(m, nextMember, strict)) {
                        i++;
                    } else {
                        break;
                    }
                } while (i < n);
            } else {
                List<Member> children = evaluator.getSchemaReader().getMemberChildren(m);
                for (Member child : children) {
                    Member[] members = o.clone();
                    members[k] = child;
                    result.add(members);
                }
            }
        }
        return result;
    }
}

// End ToggleDrillStateFunDef.java
