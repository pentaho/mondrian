/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.util.Pair;

import java.util.*;

/**
 * Restricts the SQL result set to members where particular columns have
 * particular values.
 */
public class MemberKeyConstraint
    implements TupleConstraint
{
    private final Pair<List<RolapSchema.PhysColumn>, List<Comparable>> cacheKey;
    private final List<RolapSchema.PhysColumn> columnList;
    private final List<Comparable> valueList;

    public MemberKeyConstraint(
        List<RolapSchema.PhysColumn> columnList,
        List<Comparable> valueList)
    {
        this.columnList = columnList;
        this.valueList = valueList;
        cacheKey = Pair.of(columnList, valueList);
    }

    public boolean isJoinRequired() {
        return false;
    }

    public List<RolapMeasureGroup> getMeasureGroupList() {
        return Collections.emptyList();
    }

    public void addConstraint(
        SqlQueryBuilder queryBuilder, RolapStarSet baseCube)
    {
        for (int i = 0; i < columnList.size(); i++) {
            RolapSchema.PhysColumn expression = columnList.get(i);
            final Comparable value = valueList.get(i);
                SqlConstraintUtils.constrainLevel2(
                    queryBuilder,
                    expression,
                    null,
                    value);
        }
    }

    public void addLevelConstraint(
        SqlQuery sqlQuery,
        RolapStarSet baseCube,
        RolapCubeLevel level)
    {
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(
        RolapMember parent)
    {
        return null;
    }

    public String toString() {
        return "MemberKeyConstraint";
    }

    public Object getCacheKey() {
        return cacheKey;
    }

    public Evaluator getEvaluator() {
        return null;
    }
}

// End MemberKeyConstraint.java
