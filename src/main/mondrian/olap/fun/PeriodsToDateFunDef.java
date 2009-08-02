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
import mondrian.olap.type.*;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.LevelCalc;
import mondrian.calc.MemberCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapHierarchy;

import java.util.List;

/**
 * Definition of the <code>PeriodsToDate</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class PeriodsToDateFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
            "PeriodsToDate",
            "PeriodsToDate([<Level>[, <Member>]])",
            "Returns a set of periods (members) from a specified level starting with the first period and ending with a specified member.",
            new String[]{"fx", "fxl", "fxlm"},
            PeriodsToDateFunDef.class);

    public PeriodsToDateFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        if (args.length == 0) {
            // With no args, the default implementation cannot
            // guess the hierarchy.
            RolapHierarchy defaultTimeHierarchy =
                ((RolapCube) validator.getQuery().getCube()).getTimeHierarchy(
                    getName());
            return new SetType(MemberType.forHierarchy(defaultTimeHierarchy));
        }

        if (args.length >= 2) {
            Type hierarchyType = args[0].getType();
            MemberType memberType = (MemberType) args[1].getType();
            if (memberType.getHierarchy() != null
                && hierarchyType.getHierarchy() != null
                && memberType.getHierarchy() != hierarchyType.getHierarchy())
            {
                throw Util.newError(
                    "Type mismatch: member must belong to hierarchy "
                    + hierarchyType.getHierarchy().getUniqueName());
            }
        }

        // If we have at least one arg, it's a level which will
        // tell us the type.
        return super.getResultType(validator, args);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final LevelCalc levelCalc =
            call.getArgCount() > 0
            ? compiler.compileLevel(call.getArg(0))
            : null;
        final MemberCalc memberCalc =
            call.getArgCount() > 1
            ? compiler.compileMember(call.getArg(1))
            : null;
        final RolapHierarchy timeHierarchy =
            levelCalc == null
            ? ((RolapCube) compiler.getEvaluator().getCube()).getTimeHierarchy(
                getName())
            : null;

        return new AbstractListCalc(call, new Calc[] {levelCalc, memberCalc}) {
            public List evaluateList(Evaluator evaluator) {
                final Member member;
                final Level level;
                if (levelCalc == null) {
                    member = evaluator.getContext(timeHierarchy);
                    level = member.getLevel().getParentLevel();
                } else {
                    level = levelCalc.evaluateLevel(evaluator);
                    if (memberCalc == null) {
                        member = evaluator.getContext(level.getHierarchy());
                    } else {
                        member = memberCalc.evaluateMember(evaluator);
                    }
                }
                return periodsToDate(evaluator, level, member);
            }

            public boolean dependsOn(Hierarchy hierarchy) {
                if (super.dependsOn(hierarchy)) {
                    return true;
                }
                if (memberCalc != null) {
                    return false;
                } else if (levelCalc != null) {
                    return levelCalc.getType().usesHierarchy(hierarchy, true);
                } else {
                    return hierarchy == timeHierarchy;
                }
            }
        };
    }
}

// End PeriodsToDateFunDef.java
