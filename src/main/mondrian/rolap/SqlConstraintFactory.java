/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2013 Pentaho and others
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
        if (!SqlContextConstraint.checkValidContext(
                context,
                false,
                levels,
                false,
                measureGroupList))
        {
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

    public MemberChildrenConstraint getChildByNameConstraint(
        RolapMember parent,
        Id.NameSegment childName)
    {
        // Ragged hierarchies span multiple levels, so SQL WHERE does not work
        // there
        if (!enabled || parent.getHierarchy().isRagged()) {
            return DefaultMemberChildrenConstraint.instance();
        }
        return new ChildByNameConstraint(childName);
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
