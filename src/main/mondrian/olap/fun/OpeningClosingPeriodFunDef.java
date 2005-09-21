/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 February, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;

/**
 * Definition of the <code>OpeningPeriod</code> and <code>ClosingPeriod</code>
 * builtin functions.
 *
 * @author jhyde
 * @since 2005/8/14
 * @version $Id$
 */
class OpeningClosingPeriodFunDef extends FunDefBase {
    private final boolean opening;

    public OpeningClosingPeriodFunDef(
            FunDef dummyFunDef,
            boolean opening) {
        super(dummyFunDef);
        this.opening = opening;
    }

    public Type getResultType(Validator validator, Exp[] args) {
        if (args.length == 0) {
            // With no args, the default implementation cannot
            // guess the hierarchy, so we supply the Time
            // dimension.
            Hierarchy hierarchy = validator.getQuery()
                    .getCube().getTimeDimension()
                    .getHierarchy();
            return new MemberType(hierarchy, null, null);
        }
        return super.getResultType(validator, args);
    }

    public boolean callDependsOn(FunCall call, Dimension dimension) {
        switch (call.getArgCount()) {
        case 0:
            // OpeningPeriod() depends on [Time]
            return dimension.getDimensionType() == DimensionType.TimeDimension;
        case 1:
            // OpeningPeriod(<Level>)
            // depends on Level's dimension even the expression does not.
            if (super.callDependsOn(call, dimension)) {
                return true;
            }
            return call.getArg(0).getTypeX().usesDimension(dimension);
        case 2:
            // OpeningPeriod(<Level>, <Member>)
            // depends upon whatever its args depend on.
            return super.callDependsOn(call, dimension);
        default:
            throw Util.newInternal("bad arg count " + call.getArgCount());
        }
    }

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        Member member;
        Level level;

        //
        // If the member argument is present, use it. Otherwise default
        // to the time dimension's current member.
        //
        if (args.length == 2) {
            member = getMemberArg(evaluator, args, 1, false);
        } else {
            member = evaluator.getContext(evaluator.getCube().getTimeDimension());
        }

        //
        // If the level argument is present, use it. Otherwise use the level
        // immediately after that of the member argument.
        //
        if (args.length >= 1) {
            level = getLevelArg(evaluator,  args, 0, false);
        } else {
            int targetDepth = member.getLevel().getDepth() + 1;
            Level[] levels = member.getHierarchy().getLevels();

            if (levels.length <= targetDepth) {
                return member.getHierarchy().getNullMember();
            }
            level = levels[targetDepth];
        }

        //
        // Make sure the member and the level come from the same hierarchy.
        //
        if (!member.getHierarchy().equals(level.getHierarchy())) {
            throw MondrianResource.instance().FunctionMbrAndLevelHierarchyMismatch.ex(
                    opening ? "OpeningPeriod" : "ClosingPeriod",
                    level.getHierarchy().getUniqueName(),
                    member.getHierarchy().getUniqueName());
        }

        //
        // Shortcut if the level is above the member.
        //
        if (level.getDepth() < member.getLevel().getDepth()) {
            return member.getHierarchy().getNullMember();
        }

        //
        // Shortcut if the level is the same as the member
        //
        if (level == member.getLevel()) {
            return member;
        }

        return getDescendant(evaluator.getSchemaReader(), member, level,
            opening);
    }

    /**
     * Returns the first or last descendant of the member at the target level.
     * This method is the implementation of both OpeningPeriod and ClosingPeriod.
     * @param schemaReader The schema reader to use to evaluate the function.
     * @param member The member from which the descendant is to be found.
     * @param targetLevel The level to stop at.
     * @param returnFirstDescendant Flag indicating whether to return the first
     * or last descendant of the member.
     * @return A member.
     * @pre member.getLevel().getDepth() < level.getDepth();
     */
    static Member getDescendant(
            SchemaReader schemaReader,
            Member member,
            Level targetLevel,
            boolean returnFirstDescendant) {
        Member[] children;

        final int targetLevelDepth = targetLevel.getDepth();
        assertPrecondition(member.getLevel().getDepth() < targetLevelDepth,
                "member.getLevel().getDepth() < targetLevel.getDepth()");

        for (;;) {
            children = schemaReader.getMemberChildren(member);

            if (children.length == 0) {
                return targetLevel.getHierarchy().getNullMember();
            }

            member = children[returnFirstDescendant ? 0 : (children.length - 1)];

            if (member.getLevel().getDepth() == targetLevelDepth) {
                if (member.isHidden()) {
                    return member.getHierarchy().getNullMember();
                } else {
                    return member;
                }
            }
        }
    }

    public static Resolver createResolver(final boolean opening) {
        return opening ?
                (Resolver) new MultiResolver(
                        "OpeningPeriod",
                        "OpeningPeriod([<Level>[, <Member>]])",
                        "Returns the first descendant of a member at a level.",
                        new String[] {"fm", "fml", "fmlm"}) {
                    protected FunDef createFunDef(
                            Exp[] args, FunDef dummyFunDef) {
                        return new OpeningClosingPeriodFunDef(
                                dummyFunDef, opening);
                    }
                } :
                new MultiResolver(
                        "ClosingPeriod",
                        "ClosingPeriod([<Level>[, <Member>]])",
                        "Returns the last descendant of a member at a level.",
                        new String[] {"fm", "fml", "fmlm", "fmm"}) {
                    protected FunDef createFunDef(
                            Exp[] args, FunDef dummyFunDef) {
                        return new OpeningClosingPeriodFunDef(
                                dummyFunDef, opening);
                    }
                };
    }
}

// End OpeningClosingPeriodFunDef.java
