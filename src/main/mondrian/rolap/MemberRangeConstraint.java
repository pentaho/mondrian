/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;

import java.util.Arrays;

class MemberRangeConstraint extends DefaultMemberChildrenConstraint {
    private final Object cacheKey;
    private Object startKey;
    private Object endKey;

    public MemberRangeConstraint(
        Object startKey,
        Object endKey)
    {
        this.startKey = startKey;
        this.endKey = endKey;
        this.cacheKey =
            Arrays.asList(
                MemberRangeConstraint.class,
                startKey,
                endKey);
    }

    @Override
    public int hashCode() {
        return getCacheKey().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof MemberRangeConstraint
            && getCacheKey().equals(
                ((MemberRangeConstraint) obj).getCacheKey());
    }

    public void addLevelConstraint(
        SqlQuery query,
        RolapCube baseCube,
        AggStar aggStar,
        RolapLevel level)
    {
        super.addLevelConstraint(query, baseCube, aggStar, level);
        query.addWhere(
            SqlConstraintUtils.constrainMemberRange(
                query,
                level.keyExp,
                level.getDatatype(),
                startKey,
                endKey));
    }

    public String toString() {
        return "MemberRangeConstraint(" + startKey + ", " + endKey + ")";
    }

    public Object getCacheKey() {
        return cacheKey;
    }

}
// End MemberRangeConstraint.java