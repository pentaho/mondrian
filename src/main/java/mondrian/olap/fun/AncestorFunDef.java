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
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.LevelType;
import mondrian.olap.type.Type;

/**
 * Definition of the <code>Ancestor</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class AncestorFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Ancestor",
            "Ancestor(<Member>, {<Level>|<Numeric Expression>})",
            "Returns the ancestor of a member at a specified level.",
            new String[] {"fmml", "fmmn"},
            AncestorFunDef.class);

    public AncestorFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc memberCalc =
            compiler.compileMember(call.getArg(0));
        final Type type1 = call.getArg(1).getType();
        if (type1 instanceof LevelType) {
            final LevelCalc levelCalc =
                compiler.compileLevel(call.getArg(1));
            return new AbstractMemberCalc(
                call, new Calc[] {memberCalc, levelCalc})
            {
                public Member evaluateMember(Evaluator evaluator) {
                    Level level = levelCalc.evaluateLevel(evaluator);
                    Member member = memberCalc.evaluateMember(evaluator);
                    int distance =
                        member.getLevel().getDepth() - level.getDepth();
                    return ancestor(evaluator, member, distance, level);
                }
            };
        } else {
            final IntegerCalc distanceCalc =
                compiler.compileInteger(call.getArg(1));
            return new AbstractMemberCalc(
                call, new Calc[] {memberCalc, distanceCalc})
            {
                public Member evaluateMember(Evaluator evaluator) {
                    int distance = distanceCalc.evaluateInteger(evaluator);
                    Member member = memberCalc.evaluateMember(evaluator);
                    return ancestor(evaluator, member, distance, null);
                }
            };
        }
    }
}

// End AncestorFunDef.java
