/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.LevelType;
import mondrian.olap.type.Type;

import java.util.ArrayList;
import java.util.List;


/**
 * Definition of the <code>Ancestors</code> MDX function.
 *
 * @author lboudreau
 * @since Nov 27 2012
 */
class AncestorsFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Ancestors",
            "Ancestors(<Member>, {<Level>|<Numeric Expression>})",
            "Returns the set of all ancestors of a specified member at a specified level or at a specified distance from the member",
            new String[] {"fxml", "fxmn"},
            AncestorsFunDef.class);

    public AncestorsFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public int getReturnCategory() {
        return Category.Set;
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc memberCalc =
            compiler.compileMember(call.getArg(0));
        final Type type1 = call.getArg(1).getType();
        if (type1 instanceof LevelType) {
            final LevelCalc levelCalc =
                compiler.compileLevel(call.getArg(1));
            return new AbstractListCalc(
                call, new Calc[] {memberCalc, levelCalc})
            {
                public TupleList evaluateList(Evaluator evaluator) {
                    Level level = levelCalc.evaluateLevel(evaluator);
                    Member member = memberCalc.evaluateMember(evaluator);
                    int distance =
                        member.getDepth() - level.getDepth();
                    List<Member> ancestors = new ArrayList<Member>();
                    for (int curDist = 1; curDist <= distance; curDist++) {
                        ancestors.add(
                            ancestor(evaluator, member, curDist, null));
                    }
                    return TupleCollections.asTupleList(ancestors);
                }
            };
        } else {
            final IntegerCalc distanceCalc =
                compiler.compileInteger(call.getArg(1));
            return new AbstractListCalc(
                call, new Calc[] {memberCalc, distanceCalc})
            {
                public TupleList evaluateList(Evaluator evaluator) {
                    Member member = memberCalc.evaluateMember(evaluator);
                    int distance = distanceCalc.evaluateInteger(evaluator);
                    List<Member> ancestors = new ArrayList<Member>();
                    for (int curDist = 1; curDist <= distance; curDist++) {
                        ancestors.add(
                            ancestor(evaluator, member, curDist, null));
                    }
                    return TupleCollections.asTupleList(ancestors);
                }
            };
        }
    }
}

// End AncestorsFunDef.java
