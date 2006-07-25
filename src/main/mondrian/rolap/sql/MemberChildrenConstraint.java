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

import java.util.List;
import java.util.Map;

import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;

/**
 * Restricts the SQL result of a MembersChildren query in SqlMemberSource.
 *
 * @see mondrian.rolap.SqlMemberSource
 *
 * @author av
 * @since Nov 2, 2005
 * @version $Id$
 */
public interface MemberChildrenConstraint extends SqlConstraint {
    
    /**
     * modifies a Member.Children sqlQuery so that only the children
     * of <code>parent</code> will be returned in the resultset
     * @param sqlQuery the query to modify
     * @param parent the parent member that restricts the returned children
     */
    public void addMemberConstraint(SqlQuery sqlQuery, RolapMember parent);

    /**
     * modifies a Member.Children sqlQuery so that (all or some) children
     * of <em>all</em> parent members contained in <code>parents</code> will 
     * be returned in the resultset.
     * @param sqlQuery the query to modify
     * @param parents list of parent members that restrict the returned children.
     * all parents will belong to the same level and there at least two in the list.
     */
    public void addMemberConstraint(SqlQuery sqlQuery, List parents);
    
    /**
     * Will be called once for the level that contains the
     * children of a Member.Children query. If the condition requires so,
     * it may join the levels table to the fact table.
     *
     * @param query the query to modify
     * @param level the level that contains the children
     * @param levelToColumnMap set in the case of a virtual cube; use this
     * to map a level to the columns from the base cube
     */
    public void addLevelConstraint(
        SqlQuery query, RolapLevel level, Map levelToColumnMap);

}

// End MemberChildrenConstraint.java
