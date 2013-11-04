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

import mondrian.olap.Evaluator;
import mondrian.rolap.sql.*;

import java.util.*;

/**
 * TupleConstaint which restricts the result of a tuple sqlQuery to a
 * set of parents.  All parents must belong to the same level.
 *
 * @author av
 * @since Nov 10, 2005
 */
class DescendantsConstraint implements TupleConstraint {
    List<RolapMember> parentMembers;
    MemberChildrenConstraint mcc;

    /**
     * Creates a DescendantsConstraint.
     *
     * @param parentMembers list of parents all from the same level
     *
     * @param mcc the constraint that would return the children for each single
     * parent
     */
    public DescendantsConstraint(
        List<RolapMember> parentMembers,
        MemberChildrenConstraint mcc)
    {
        this.parentMembers = parentMembers;
        this.mcc = mcc;
    }

    public void addConstraint(
        SqlQueryBuilder queryBuilder,
        RolapStarSet starSet)
    {
        mcc.addMemberConstraint(queryBuilder, starSet, parentMembers);
    }

    public void addLevelConstraint(
        SqlQuery sqlQuery,
        RolapStarSet starSet,
        RolapCubeLevel level)
    {
        mcc.addLevelConstraint(sqlQuery, starSet, level);
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(
        RolapMember parent)
    {
        return mcc;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation returns null, because descendants is not cached.
     */
    public Object getCacheKey() {
        return null;
    }

    public Evaluator getEvaluator() {
        return null;
    }

    public List<RolapMeasureGroup> getMeasureGroupList() {
        return mcc instanceof TupleConstraint
            ? ((TupleConstraint) mcc).getMeasureGroupList()
            : Collections.<RolapMeasureGroup>emptyList();
    }

    public boolean isJoinRequired() {
        return mcc instanceof TupleConstraint
            && ((TupleConstraint) mcc).isJoinRequired();
    }
}

// End DescendantsConstraint.java
