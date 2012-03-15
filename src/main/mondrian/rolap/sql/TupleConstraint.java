/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import mondrian.olap.Evaluator;
import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.AggStar;

/**
 * Restricts the SQL result of {@link mondrian.rolap.TupleReader}. This is also
 * used by
 * {@link SqlMemberSource#getMembersInLevel(RolapLevel, TupleConstraint)}.
 *
 * @see mondrian.rolap.TupleReader
 * @see mondrian.rolap.SqlMemberSource
 *
 * @author av
 */
public interface TupleConstraint extends SqlConstraint {
    /**
     * Modifies a Level.Members query.
     *
     * @param sqlQuery the query to modify
     * @param aggStar aggregate star to use
     * @param baseCube base cube for virtual cube constraints
     */
    public void addConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar);

    /**
     * Will be called multiple times for every "group by" level in
     * Level.Members query, i.e. the level that contains the members and all
     * parent levels except All.
     * If the condition requires so,
     * it may join the levels table to the fact table.
     *
     * @param sqlQuery the query to modify
     * @param baseCube base cube for virtual cube constraints
     * @param aggStar Aggregate table, or null if query is against fact table
     * @param level the level which is accessed in the Level.Members query
     */
    public void addLevelConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        RolapLevel level);

    /**
     * When the members of a level are fetched, the result is grouped
     * by into parents and their children. These parent/children are
     * stored in the parent/children cache, whose key consists of the parent
     * and the MemberChildrenConstraint#hashKey(). So we need a matching
     * MemberChildrenConstraint to store the parent with its children into
     * the parent/children cache.
     *
     * <p>The returned MemberChildrenConstraint must be one that would have
     * returned the same children for the given parent as the MemberLevel query
     * has found for that parent.
     *
     * <p>If null is returned, the parent/children will not be cached (but the
     * level/members still will be).
     */
    MemberChildrenConstraint getMemberChildrenConstraint(RolapMember parent);

    /**
     * @return the evaluator currently associated with the constraint; null
     * if there is no associated evaluator
     */
    public Evaluator getEvaluator();
}

// End TupleConstraint.java
