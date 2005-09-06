/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.SetType;
import mondrian.olap.type.MemberType;

/**
 * Definition of Ytd, Qtd, Mtd, Wtd functions.
 */
class XtdFunDef extends FunDefBase {
    private final LevelType levelType;

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
            return new SetType(
                    new MemberType(hierarchy, null, null));
        }
        final Type type = args[0].getTypeX();
        if (type.getHierarchy().getDimension()
                .getDimensionType() !=
                DimensionType.TimeDimension) {
            throw MondrianResource.instance()
                    .newTimeArgNeeded(getName());
        }
        return super.getResultType(validator, args);
    }

    public boolean callDependsOn(FunCall call, Dimension dimension) {
        // The zero argument form (e.g. Ytd()) depends on the time dimension.
        // The one argument form depends upon what its arg depends on.
        if (call.getArgCount() == 0) {
            final Type type = call.getTypeX();
            return type.usesDimension(dimension);
        } else {
            return super.callDependsOn(call, dimension);
        }
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

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        return periodsToDate(
                evaluator,
                getLevel(evaluator),
                getMemberArg(evaluator, args, 0, false));
    }

    public static class Resolver extends MultiResolver {
        private final LevelType levelType;

        public Resolver(
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
