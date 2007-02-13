/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.List;
import java.util.Map;

import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.aggmatcher.AggStar;

/**
 * Restricts the SQL result set to the parent member of a
 * MemberChildren query.  If called with a calculated member an
 * exception will be thrown.
 *
 * @version $Id$
 */
public class DefaultMemberChildrenConstraint
    implements MemberChildrenConstraint {

    private static final MemberChildrenConstraint instance
        = new DefaultMemberChildrenConstraint();

    /** we have no state, so all instances are equal */
    private static final Object cacheKey = new Object();

    protected DefaultMemberChildrenConstraint() {
    }

    public void addMemberConstraint(
        SqlQuery sqlQuery,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap,
        AggStar aggStar,
        RolapMember parent)
    {
        SqlConstraintUtils.addMemberConstraint(
            sqlQuery, levelToColumnMap, aggStar, parent, true);
    }

    public void addMemberConstraint(
        SqlQuery sqlQuery,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap,
        AggStar aggStar,
        List<RolapMember> parents)
    {
        SqlConstraintUtils.addMemberConstraint(
            sqlQuery,
            levelToColumnMap,
            aggStar,
            parents,
            true,
            false);
    }

    public void addLevelConstraint(
        SqlQuery query,
        AggStar aggStar,
        RolapLevel level,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap) {
    }

    public String toString() {
        return "DefaultMemberChildrenConstraint";
    }

    public Object getCacheKey() {
        return cacheKey;
    }

    public static MemberChildrenConstraint instance() {
        return instance;
    }

    public int getMaxRows() {
        return 0;
    }
}

// End DefaultMemberChildrenConstraint.java

