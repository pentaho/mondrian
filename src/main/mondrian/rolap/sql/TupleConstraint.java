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
package mondrian.rolap.sql;

import mondrian.olap.Evaluator;
import mondrian.rolap.*;

import java.util.List;

/**
 * Restricts the SQL result of {@link mondrian.rolap.TupleReader}. This is also
 * used by
 * {@link mondrian.rolap.MemberReader#getMembersInLevel(mondrian.rolap.RolapCubeLevel, TupleConstraint)}.
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
     * @param queryBuilder Query builder
     * @param starSet Star set
     */
    public void addConstraint(
        SqlQueryBuilder queryBuilder,
        RolapStarSet starSet);

    /**
     * Will be called multiple times for every "group by" level in
     * Level.Members query, i.e. the level that contains the members and all
     * parent levels except All.
     * If the condition requires so,
     * it may join the levels table to the fact table.
     *
     * @param sqlQuery the query to modify
     * @param starSet Star set
     * @param level the level which is accessed in the Level.Members query
     */
    public void addLevelConstraint(
        SqlQuery sqlQuery,
        RolapStarSet starSet,
        RolapCubeLevel level);

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

    /**
     * Returns the list of measure groups that this constraint joins to.
     *
     * @return list of measure groups, never null
     */
    List<RolapMeasureGroup> getMeasureGroupList();

    /**
     * Returns whether a join with the fact table is required. A join is
     * required if the context contains members from dimensions other than
     * level. If we are interested in the members of a level or a members
     * children then it does not make sense to join only one dimension (the one
     * that contains the requested members) with the fact table for NON EMPTY
     * optimization.
     */
    boolean isJoinRequired();
}

// End TupleConstraint.java
