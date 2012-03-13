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
import mondrian.calc.impl.UnaryTupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.*;

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
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        return new AbstractListCalc(call, new Calc[] {listCalc}) {
            public TupleList evaluateList(Evaluator evaluator) {
                final TupleList list =
                    listCalc.evaluateList(evaluator);
                return new UnaryTupleList(
                    addCalculatedMembers(list.slice(0), evaluator));
            }
        };
    }

    private List<Member> addCalculatedMembers(
        List<Member> memberList,
        Evaluator evaluator)
    {
        // Determine unique levels in the set
        final Set<Level> levels = new LinkedHashSet<Level>();
        Hierarchy hierarchy = null;

        for (Member member : memberList) {
            if (hierarchy == null) {
                hierarchy = member.getHierarchy();
            } else if (hierarchy != member.getHierarchy()) {
                throw newEvalException(
                    this,
                    "Only members from the same hierarchy are allowed in the "
                    + "AddCalculatedMembers set: " + hierarchy
                    + " vs " + member.getHierarchy());
            }
            levels.add(member.getLevel());
        }

        // For each level, add the calculated members from both
        // the schema and the query
        List<Member> workingList = new ArrayList<Member>(memberList);
        final SchemaReader schemaReader =
                evaluator.getQuery().getSchemaReader(true);
        for (Level level : levels) {
            List<Member> calcMemberList =
                schemaReader.getCalculatedMembers(level);
            workingList.addAll(calcMemberList);
        }
        return workingList;
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
