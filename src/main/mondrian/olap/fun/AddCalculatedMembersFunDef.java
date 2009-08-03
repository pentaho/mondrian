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

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.SetType;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.Type;

import java.util.*;

/**
 * Definition of the <code>AddCalculatedMembers</code> MDX function.
 *
 * <p>AddCalculatedMembers adds calculated members that are siblings
 * of the members in the set. The set is limited to one dimension.
 *
 * <p>Syntax:
 *
 * <blockquote><pre>AddCalculatedMembers(&lt;Set&gt;)</pre></blockquote>

 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class AddCalculatedMembersFunDef extends FunDefBase {
    private static final AddCalculatedMembersFunDef instance =
        new AddCalculatedMembersFunDef();

    public static final Resolver resolver = new ResolverImpl();
    private static final String FLAG = "fxx";

    private AddCalculatedMembersFunDef() {
        super(
            "AddCalculatedMembers",
            "Adds calculated members to a set.",
            FLAG);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final MemberListCalc listCalc =
            (MemberListCalc) compiler.compileList(call.getArg(0));
        return new AbstractListCalc(call, new Calc[] {listCalc}) {
            public List evaluateList(Evaluator evaluator) {
                final List<Member> list =
                    listCalc.evaluateMemberList(evaluator);
                return addCalculatedMembers(list, evaluator);
            }
        };
    }

    private List<Member> addCalculatedMembers(
        List<Member> memberList,
        Evaluator evaluator)
    {
        // Determine unique levels in the set
        Map<Level, Object> levelMap = new HashMap<Level, Object>();
        Dimension dim = null;

        for (Member member : memberList) {
            if (dim == null) {
                dim = member.getDimension();
            } else if (dim != member.getDimension()) {
                throw newEvalException(this,
                    "Only members from the same dimension are allowed in the AddCalculatedMembers set: "
                        + dim.toString() + " vs " +
                        member.getDimension().toString());
            }
            if (!levelMap.containsKey(member.getLevel())) {
                levelMap.put(member.getLevel(), null);
            }
        }

        // For each level, add the calculated members from both
        // the schema and the query
        List<Member> workingList = new ArrayList<Member>(memberList);
        final SchemaReader schemaReader =
                evaluator.getQuery().getSchemaReader(true);
        for (Level level : levelMap.keySet()) {
            List<Member> calcMemberList =
                schemaReader.getCalculatedMembers(level);
            workingList.addAll(calcMemberList);
        }
        memberList = workingList;
        return memberList;
    }

    private static class ResolverImpl extends MultiResolver {
        public ResolverImpl() {
            super(
                instance.getName(),
                instance.getSignature(),
                instance.getDescription(),
                new String[] {FLAG});
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            if (args.length == 1) {
                Exp arg = args[0];
                final Type type1 = arg.getType();
                if (type1 instanceof SetType) {
                    SetType type = (SetType) type1;
                    if (type.getElementType() instanceof MemberType) {
                        return instance;
                    } else {
                        throw newEvalException(
                            instance,
                            "Only single dimension members allowed in set for AddCalculatedMembers");
                    }
                }
            }
            return null;
        }
    }
}

// End AddCalculatedMembersFunDef.java
