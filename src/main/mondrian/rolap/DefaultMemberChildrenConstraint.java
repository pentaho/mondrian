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

import mondrian.rolap.sql.*;

import java.util.List;

/**
 * Restricts the SQL result set to the parent member of a
 * MemberChildren query.  If called with a calculated member an
 * exception will be thrown.
 */
public class DefaultMemberChildrenConstraint
    implements MemberChildrenConstraint
{
    private static final MemberChildrenConstraint instance =
        new DefaultMemberChildrenConstraint();

    protected DefaultMemberChildrenConstraint() {
    }

    public RolapStarSet createStarSet(RolapMeasureGroup aggMeasureGroup) {
        return new RolapStarSet(null, null, aggMeasureGroup);
    }

    public void addMemberConstraint(
        SqlQueryBuilder queryBuilder,
        RolapStarSet starSet,
        RolapMember parent)
    {
        SqlConstraintUtils.addMemberConstraint(
            queryBuilder, starSet, parent, true);
    }

    public void addMemberConstraint(
        SqlQueryBuilder queryBuilder,
        RolapStarSet starSet,
        List<RolapMember> parents)
    {
        boolean exclude = false;
        SqlConstraintUtils.addMemberConstraint(
            queryBuilder, starSet, parents, true, false, exclude);
    }

    public void addLevelConstraint(
        SqlQuery query,
        RolapStarSet starSet,
        RolapCubeLevel level)
    {
    }

    public String toString() {
        return "DefaultMemberChildrenConstraint";
    }

    public Object getCacheKey() {
        // we have no state, so all instances are equal
        return this;
    }

    public static MemberChildrenConstraint instance() {
        return instance;
    }
}

// End DefaultMemberChildrenConstraint.java

