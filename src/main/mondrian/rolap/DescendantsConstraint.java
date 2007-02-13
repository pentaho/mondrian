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

import mondrian.olap.Evaluator;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.aggmatcher.AggStar;

/**
 * TupleConstaint which restricts the result of a tuple sqlQuery to a
 * set of parents.  All parents must belong to the same level.
 *
 * @author av
 * @since Nov 10, 2005
 * @version $Id$
 */
class DescendantsConstraint implements TupleConstraint {
    List<RolapMember> parentMembers;
    MemberChildrenConstraint mcc;

    /**
     * Creates a DescendantsConstraint.
     *
     * @param parentMembers list of parents all from the same level
     * @param mcc the constraint that would return the children for each single parent
     */
    public DescendantsConstraint(
        List<RolapMember> parentMembers,
        MemberChildrenConstraint mcc)
    {
        this.parentMembers = parentMembers;
        this.mcc = mcc;
    }

    public void addConstraint(
        SqlQuery sqlQuery,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap)
    {
        mcc.addMemberConstraint(
            sqlQuery, levelToColumnMap, null, parentMembers);
    }

    public void addLevelConstraint(
        SqlQuery sqlQuery, AggStar aggStar,
        RolapLevel level, Map<RolapLevel, RolapStar.Column> levelToColumnMap)
    {
        mcc.addLevelConstraint(sqlQuery, aggStar, level, levelToColumnMap);
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(RolapMember parent) {
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
}

// End DescendantsConstraint.java
