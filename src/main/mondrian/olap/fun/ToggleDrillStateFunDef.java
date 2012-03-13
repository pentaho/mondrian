/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;

import java.util.*;

/**
 * Definition of the <code>ToggleDrillState</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class ToggleDrillStateFunDef extends FunDefBase {
    static final String[] ReservedWords = new String[] {"RECURSIVE"};
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
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
            throw MondrianResource.instance()
                .ToggleDrillStateRecursiveNotSupported.ex();
        }
        final ListCalc listCalc0 =
            compiler.compileList(call.getArg(0));
        final ListCalc listCalc1 =
            compiler.compileList(call.getArg(1));
        return new AbstractListCalc(call, new Calc[]{listCalc0, listCalc1}) {
            public TupleList evaluateList(Evaluator evaluator) {
                final TupleList list0 = listCalc0.evaluateList(evaluator);
                final TupleList list1 = listCalc1.evaluateList(evaluator);
                return toggleDrillStateTuples(evaluator, list0, list1);
            }
        };
    }

    TupleList toggleDrillStateTuples(
        Evaluator evaluator, TupleList v0, TupleList list1)
    {
        assert list1.getArity() == 1;
        if (list1.isEmpty()) {
            return v0;
        }
        if (v0.isEmpty()) {
            return v0;
        }
        final Member[] members = new Member[v0.getArity()]; // tuple workspace
        final Set<Member> set = new HashSet<Member>(list1.slice(0));
        TupleList result = v0.cloneList((v0.size() * 3) / 2 + 1); // allow 50%
        int i = 0, n = v0.size();
        while (i < n) {
            List<Member> o = v0.get(i++);
            result.add(o);
            Member m = null;
            int k = -1;
            for (int j = 0; j < o.size(); j++) {
                Member member = o.get(j);
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
                List<Member> next = v0.get(i);
                Member nextMember = next.get(k);
                boolean strict = true;
                if (FunUtil.isAncestorOf(m, nextMember, strict)) {
                    isDrilledDown = true;
                }
            }
            if (isDrilledDown) {
                // skip descendants of this member
                do {
                    List<Member> next = v0.get(i);
                    Member nextMember = next.get(k);
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
                    o.toArray(members);
                    members[k] = child;
                    result.addTuple(members);
                }
            }
        }
        return result;
    }
}

// End ToggleDrillStateFunDef.java
