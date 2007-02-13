/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.sql;

import mondrian.olap.Evaluator;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;
import mondrian.rolap.RolapStar;
import mondrian.rolap.aggmatcher.AggStar;

import java.util.Map;

/**
 * Restricts the SQL result of {@link mondrian.rolap.TupleReader}. This is also
 * used by
 * {@link mondrian.rolap.SqlMemberSource#getMembersInLevel(RolapLevel, int, int, TupleConstraint)}.
 *
 * @see mondrian.rolap.TupleReader
 * @see mondrian.rolap.SqlMemberSource
 *
 * @author av
 * @version $Id$
 */
public interface TupleConstraint extends SqlConstraint {
    /**
     * Modifies a Level.Members query.
     *
     * @param sqlQuery the query to modify
     * @param levelToColumnMap maps a level to the column which holds its key
     *   in the current RolapStar 
     */
    public void addConstraint(
        SqlQuery sqlQuery,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap);

    /**
     * Will be called multiple times for every "group by" level in
     * Level.Members query, i.e. the level that contains the members and all
     * parent levels except All.
     * If the condition requires so,
     * it may join the levels table to the fact table.
     *
     * @param query the query to modify
     * @param aggStar Aggregate table, or null if query is against fact table
     * @param level the level which is accessed in the Level.Members query
     * @param levelToColumnMap set in the case of a virtual cube; use this
     */
    public void addLevelConstraint(
        SqlQuery query,
        AggStar aggStar,
        RolapLevel level,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap);

    /**
     * When the members of a level are fetched, the result is grouped
     * by into parents and their children. These parent/children are
     * stored in the parent/children cache, whose key consists of the parent
     * and the MemberChildrenConstraint#hashKey(). So we need a matching
     * MemberChildrenConstraint to store the parent with its children into
     * the parent/children cache.
     * <p>
     * The returned MemberChildrenConstraint must be one that would have returned
     * the same children for the given parent as the MemberLevel query has found
     * for that parent.
     * <p>
     * If null is returned, the parent/children will not be cached (but
     * the level/members still will be).
     */
    MemberChildrenConstraint getMemberChildrenConstraint(RolapMember parent);

    /**
     * @return the evaluator currently associated with the constraint; null
     * if there is no associated evaluator
     */
    public Evaluator getEvaluator();
}

// End TupleConstraint.java
