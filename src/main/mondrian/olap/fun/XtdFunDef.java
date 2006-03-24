/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.SetType;
import mondrian.olap.type.MemberType;
import mondrian.resource.MondrianResource;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;

/**
 * Definition of <code>Ytd</code>, <code>Qtd</code>, <code>Mtd</code>,
 * and <code>Wtd</code> MDX builtin functions.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class XtdFunDef extends FunDefBase {
    private final LevelType levelType;

    static final ResolverImpl MtdResolver = new ResolverImpl(
            "Mtd",
            "Mtd([<Member>])",
            "A shortcut function for the PeriodsToDate function that specifies the level to be Month.",
            new String[]{"fx", "fxm"},
            LevelType.TimeMonths);

    static final ResolverImpl QtdResolver = new ResolverImpl(
            "Qtd",
            "Qtd([<Member>])",
            "A shortcut function for the PeriodsToDate function that specifies the level to be Quarter.",
            new String[]{"fx", "fxm"},
            LevelType.TimeQuarters);

    static final ResolverImpl WtdResolver = new ResolverImpl(
            "Wtd",
            "Wtd([<Member>])",
            "A shortcut function for the PeriodsToDate function that specifies the level to be Week.",
            new String[]{"fx", "fxm"},
            LevelType.TimeWeeks);

    static final ResolverImpl YtdResolver = new ResolverImpl(
            "Ytd",
            "Ytd([<Member>])",
            "A shortcut function for the PeriodsToDate function that specifies the level to be Year.",
            new String[]{"fx", "fxm"},
            LevelType.TimeYears);

    public XtdFunDef(FunDef dummyFunDef, LevelType levelType) {
        super(dummyFunDef);
        this.levelType = levelType;
    }

    public Type getResultType(Validator validator, Exp[] args) {
        if (args.length == 0) {
            // With no args, the default implementation cannot
            // guess the hierarchy.
            Hierarchy hierarchy = validator.getQuery()
                    .getCube().getTimeDimension()
                    .getHierarchy();
            return new SetType(MemberType.forHierarchy(hierarchy));
        }
        final Type type = args[0].getType();
        if (type.getHierarchy().getDimension()
                .getDimensionType() !=
                DimensionType.TimeDimension) {
            throw MondrianResource.instance().TimeArgNeeded.ex(getName());
        }
        return super.getResultType(validator, args);
    }

    private Level getLevel(Evaluator evaluator) {
        switch (levelType.ordinal) {
        case LevelType.TimeYearsORDINAL:
            return evaluator.getCube().getYearLevel();
        case LevelType.TimeQuartersORDINAL:
            return evaluator.getCube().getQuarterLevel();
        case LevelType.TimeMonthsORDINAL:
            return evaluator.getCube().getMonthLevel();
        case LevelType.TimeWeeksORDINAL:
            return evaluator.getCube().getWeekLevel();
        case LevelType.TimeDaysORDINAL:
            return evaluator.getCube().getWeekLevel();
        default:
            throw levelType.unexpected();
        }
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Level level = getLevel(compiler.getEvaluator());
        switch (call.getArgCount()) {
        case 0:
            return new AbstractListCalc(call, new Calc[0]) {
                public List evaluateList(Evaluator evaluator) {
                    return periodsToDate(evaluator, level, null);
                }

                public boolean dependsOn(Dimension dimension) {
                    return dimension.getDimensionType() ==
                            mondrian.olap.DimensionType.TimeDimension;
                }
            };
        default:
            final MemberCalc memberCalc =
                    compiler.compileMember(call.getArg(0));
            return new AbstractListCalc(call, new Calc[] {memberCalc}) {
                public List evaluateList(Evaluator evaluator) {
                    return periodsToDate(evaluator, level,
                            memberCalc.evaluateMember(evaluator));
                }
            };
        }
    }

    private static class ResolverImpl extends MultiResolver {
        private final LevelType levelType;

        public ResolverImpl(
                String name,
                String signature,
                String description,
                String[] signatures,
                LevelType levelType) {
            super(name, signature, description, signatures);
            this.levelType = levelType;
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            return new XtdFunDef(dummyFunDef, levelType);
        }
    };
}

// End XtdFunDef.java
