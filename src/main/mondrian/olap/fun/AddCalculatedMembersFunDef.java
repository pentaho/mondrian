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

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.*;

/**
 * Definition of the <code>AddCalculatedMembers</code> MDX function.
 *
 * AddCalculatedMembers adds calculated members that are siblings
 * of the members in the set. The set is limited to one dimension.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class AddCalculatedMembersFunDef extends FunDefBase {
    static final AddCalculatedMembersFunDef instance = new AddCalculatedMembersFunDef();

    private AddCalculatedMembersFunDef() {
        super(
                "AddCalculatedMembers",
                "AddCalculatedMembers(<Set>)",
                "Adds calculated members to a set.",
                "fxx");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        return new AbstractListCalc(call, new Calc[] {listCalc}) {
            public List evaluateList(Evaluator evaluator) {
                final List list = listCalc.evaluateList(evaluator);
                return addCalculatedMembers(list, evaluator);
            }
        };
    }

    private List addCalculatedMembers(List memberList, Evaluator evaluator) {
        // Determine unique levels in the set
        Map levelMap = new HashMap();
        Dimension dim = null;

        Iterator it = memberList.iterator();
        while (it.hasNext()) {
            Object obj = it.next();
            if (!(obj instanceof Member)) {
                throw newEvalException(this, "Only single dimension members allowed in set for AddCalculatedMembers");
            }
            Member member = (Member) obj;
            if (dim == null) {
                dim = member.getDimension();
            } else if (dim != member.getDimension()) {
                throw newEvalException(this, "Only members from the same dimension are allowed in the AddCalculatedMembers set: "
                        + dim.toString() + " vs " + member.getDimension().toString());
            }
            if (!levelMap.containsKey(member.getLevel())) {
                levelMap.put(member.getLevel(), null);
            }
        }

        // For each level, add the calculated members from both
        // the schema and the query
        List workingList = new ArrayList(memberList);
        final SchemaReader schemaReader =
                evaluator.getQuery().getSchemaReader(true);
        it = levelMap.keySet().iterator();
        while (it.hasNext()) {
            Level level = (Level) it.next();
            List calcMemberList =
                    schemaReader.getCalculatedMembers(level);
            workingList.addAll(calcMemberList);
        }
        memberList = workingList;
        return memberList;
    }
}

// End AddCalculatedMembersFunDef.java
