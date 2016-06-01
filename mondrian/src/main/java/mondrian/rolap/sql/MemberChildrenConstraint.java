/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2009 Pentaho and others
// All Rights Reserved.
*/

package mondrian.rolap.sql;

import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.AggStar;

import java.util.List;

/**
 * Restricts the SQL result of a MembersChildren query in SqlMemberSource.
 *
 * @see mondrian.rolap.SqlMemberSource
 *
 * @author av
 * @since Nov 2, 2005
 */
public interface MemberChildrenConstraint extends SqlConstraint {

    /**
     * Modifies a <code>Member.Children</code> query so that only the children
     * of <code>parent</code> will be returned in the result set.
     *
     * @param sqlQuery the query to modify
     * @param baseCube base cube for virtual members
     * @param aggStar Aggregate star, if we are reading from an aggregate table,
     * @param parent the parent member that restricts the returned children
     */
    public void addMemberConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        RolapMember parent);

    /**
     * Modifies a <code>Member.Children</code> query so that (all or some)
     * children of <em>all</em> parent members contained in <code>parents</code>
     * will be returned in the result set.
     *
     * @param sqlQuery Query to modify
     * @param baseCube Base cube for virtual members
     * @param aggStar Aggregate table, or null if query is against fact table
     * @param parents List of parent members that restrict the returned
     *        children
     */
    public void addMemberConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        List<RolapMember> parents);

    /**
     * Will be called once for the level that contains the
     * children of a Member.Children query. If the condition requires so,
     * it may join the levels table to the fact table.
     *
     * @param query the query to modify
     * @param baseCube base cube for virtual members
     * @param aggStar Aggregate table, or null if query is against fact table
     * @param level the level that contains the children
     */
    public void addLevelConstraint(
        SqlQuery query,
        RolapCube baseCube,
        AggStar aggStar,
        RolapLevel level);

}

// End MemberChildrenConstraint.java
