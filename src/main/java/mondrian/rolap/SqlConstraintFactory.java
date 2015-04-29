/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2015 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.sql.*;

import java.util.*;

/**
 * Creates the right constraint for common tasks.
 *
 * @author av
 * @since Nov 21, 2005
 */
public final class SqlConstraintFactory {

    static boolean enabled;

    private static final SqlConstraintFactory instance =
        new SqlConstraintFactory();

    /**
     * singleton
     */
    private SqlConstraintFactory() {
    }

    private boolean enabled(final Evaluator context) {
        if (context != null) {
            return enabled && context.nativeEnabled();
        }
        return enabled;
    }

    public static SqlConstraintFactory instance() {
        setNativeNonEmptyValue();
        return instance;
    }

    public static void setNativeNonEmptyValue() {
        enabled = MondrianProperties.instance().EnableNativeNonEmpty.get();
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(
        RolapEvaluator context)
    {
        if (!enabled) {
            return DefaultMemberChildrenConstraint.instance();
        }
        final List<RolapMeasureGroup> measureGroupList =
            new ArrayList<RolapMeasureGroup>();
        if (!SqlContextConstraint.checkValidContext(
                context,
                true,
                Collections.<RolapCubeLevel>emptyList(),
                false,
                measureGroupList))
        {
            return DefaultMemberChildrenConstraint.instance();
        }
        return new SqlContextConstraint(context, measureGroupList, false);
    }

    public TupleConstraint getLevelMembersConstraint(RolapEvaluator context) {
        // NOTE: Always seems to be called with context == null, except tests.
        return getLevelMembersConstraint(
            context,
            Collections.<RolapCubeLevel>emptyList());
    }

    /**
     * Returns a constraint that restricts the members of a level to those that
     * are non-empty in the given context. If the constraint cannot be
     * implemented (say if native constraints are disabled) returns null.
     *
     * @param context Context within which members must be non-empty
     * @param levels  levels being referenced in the current context
     * @return Constraint
     */
    public TupleConstraint getLevelMembersConstraint(
        RolapEvaluator context,
        List<RolapCubeLevel> levels)
    {
        assert levels != null;
        if (context == null || !enabled) {
            return DefaultTupleConstraint.instance();
        }
        final List<RolapMeasureGroup> measureGroupList =
            new ArrayList<RolapMeasureGroup>();
        if (useDefaultTupleConstraint(context, levels, measureGroupList)) {
            return DefaultTupleConstraint.instance();
        }
        if (context.isNonEmpty()) {
            Set<CrossJoinArg> joinArgs =
                new CrossJoinArgFactory(false).buildConstraintFromAllAxes(
                    context);
            if (joinArgs.size() > 0) {
                return new RolapNativeCrossJoin.NonEmptyCrossJoinConstraint(
                    joinArgs.toArray(
                        new CrossJoinArg[joinArgs.size()]),
                    context,
                    measureGroupList);
            }
        }
        return new SqlContextConstraint(
            context, measureGroupList, false);
    }

    private boolean useDefaultTupleConstraint(
        Evaluator context,  List<RolapCubeLevel> levels,
        List<RolapMeasureGroup> measureGroupList)
    {
        if (!SqlContextConstraint.checkValidContext(
                (RolapEvaluator) context, false, levels,
                false, measureGroupList))
        {
            return true;
        }
        final int threshold = MondrianProperties.instance()
            .LevelPreCacheThreshold.get();
        if (threshold <= 0) {
            return false;
        }
        if (levels != null) {
            long totalCard = 1;
            for (Level level : levels) {
                totalCard *=
                    getLevelCardinality((RolapLevel) level);
                if (totalCard > threshold) {
                    return false;
                }
            }
        }
        return true;
    }


    public MemberChildrenConstraint getChildByNameConstraint(
        RolapMember parent,
        Id.NameSegment childName)
    {
        // Ragged hierarchies span multiple levels, so SQL WHERE does not work
        // there
        if (useDefaultMemberChildrenConstraint(parent)) {
            return DefaultMemberChildrenConstraint.instance();
        }
        return new ChildByNameConstraint(childName);
    }

    public MemberChildrenConstraint getChildrenByNamesConstraint(
        RolapMember parent,
        List<Id.NameSegment> childNames)
    {
        if (useDefaultMemberChildrenConstraint(parent)) {
            return DefaultMemberChildrenConstraint.instance();
        }
        return new ChildByNameConstraint(childNames);
    }

    private boolean useDefaultMemberChildrenConstraint(RolapMember parent) {
        int threshold = MondrianProperties.instance()
            .LevelPreCacheThreshold.get();
        return !enabled
            || parent.getHierarchy().isRagged()
            || (!isDegenerate(parent.getLevel())
            && threshold > 0
            && getChildLevelCardinality(parent) < threshold);
    }

    private boolean isDegenerate(Level level) {
        if (level instanceof RolapCubeLevel) {
            RolapCubeHierarchy hier = (RolapCubeHierarchy)level
                .getHierarchy();
            for (RolapMeasureGroup measureGroup
                : hier.getCube().getMeasureGroups())
            {
                RolapSchema.PhysRelation relation =
                    measureGroup.getFactRelation();
                final RolapSchema.PhysPath path = measureGroup.getPath(
                    (RolapDimension) level.getDimension());
                if (measureGroup.aggregate || path == null) {
                    continue;
                }
                List<RolapSchema.PhysHop> hops = path.hopList;
                if (hops.size() > 1
                    || !relation.equals(hops.get(0).relation))
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private int getChildLevelCardinality(RolapMember parent) {
        RolapLevel level = parent.getLevel().getChildLevel();
        if (level == null) {
            // couldn't determine child level, give most pessimistic answer
            return Integer.MAX_VALUE;
        }
        return getLevelCardinality(level);
    }

    private int getLevelCardinality(RolapLevel level) {
        if (level.isParentChild()) {
            // .getLevelCardinality returns cardinality of the level's key,
            // which can be inflated for P-C.  Return a conservative value
            // instead.
            return Integer.MAX_VALUE;
        } else {
            return getSchemaReader(level)
                .getLevelCardinality(level, true, true);
        }
    }

    private SchemaReader getSchemaReader(RolapLevel level) {
        return level.getHierarchy().getRolapSchema().getSchemaReader();
    }

    /**
     * Returns a constraint that allows to read all children of multiple parents
     * at once using a LevelMember query style. This does not work
     * for parent/child hierarchies.
     *
     * @param parentMembers List of parents (all must belong to same level)
     * @param mcc           The constraint that would return the children for
     *                      each single parent
     * @return constraint
     */
    public TupleConstraint getDescendantsConstraint(
        List<RolapMember> parentMembers,
        MemberChildrenConstraint mcc)
    {
        return new DescendantsConstraint(parentMembers, mcc);
    }
}

// End SqlConstraintFactory.java
