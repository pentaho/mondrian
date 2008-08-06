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

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.SetType;
import mondrian.olap.type.MemberType;
import mondrian.resource.MondrianResource;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.LevelCalc;
import mondrian.calc.MemberCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

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
            Dimension defaultTimeDimension =
                validator.getQuery().getCube().getTimeDimension();
            if (defaultTimeDimension == null) {
                throw MondrianResource.instance().
                            NoTimeDimensionInCube.ex(getName());
            }
            Hierarchy hierarchy = defaultTimeDimension.getHierarchy();
            return new SetType(
                    MemberType.forHierarchy(hierarchy));
        }
        final Type type = args[0].getType();
        if (type.getDimension() == null ||
            type.getDimension().getDimensionType() !=
                mondrian.olap.DimensionType.TimeDimension) {
            throw MondrianResource.instance().TimeArgNeeded.ex(getName());
        }
        return super.getResultType(validator, args);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final LevelCalc levelCalc =
                call.getArgCount() > 0 ?
                compiler.compileLevel(call.getArg(0)) :
                null;
        final MemberCalc memberCalc =
                call.getArgCount() > 1 ?
                compiler.compileMember(call.getArg(1)) :
                null;
        final Dimension timeDimension = compiler
                .getEvaluator().getCube().getTimeDimension();

        return new AbstractListCalc(call, new Calc[] {levelCalc, memberCalc}) {
            public List evaluateList(Evaluator evaluator) {
                final Member member;
                final Level level;
                if (levelCalc == null) {
                    if (timeDimension == null) {
                        throw MondrianResource.instance().
                                    NoTimeDimensionInCube.ex(getName());
                    }
                    member = evaluator.getContext(timeDimension);
                    level = member.getLevel().getParentLevel();
                } else {
                    level = levelCalc.evaluateLevel(evaluator);
                    if (memberCalc == null) {
                        member = evaluator.getContext(
                                level.getHierarchy().getDimension());
                    } else {
                        member = memberCalc.evaluateMember(evaluator);
                    }
                }
                return periodsToDate(evaluator, level, member);
            }

            public boolean dependsOn(Dimension dimension) {
                if (super.dependsOn(dimension)) {
                    return true;
                }
                if (memberCalc != null) {
                    return false;
                } else if (levelCalc != null) {
                    return levelCalc.getType().usesDimension(dimension, true) ;
                } else {
                    if (timeDimension == null) {
                        throw MondrianResource.instance().
                                    NoTimeDimensionInCube.ex(getName());
                    }
                    return dimension == timeDimension;
                }
            }
        };
    }
}

// End PeriodsToDateFunDef.java
