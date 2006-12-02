/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.Arrays;
import java.util.Map;

import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.aggmatcher.AggStar;

/**
 * Constraint which optimizes the search for a child by name. This is used
 * whenever the string representation of a member is parsed, e.g.
 * [Customers].[All Customers].[USA].[CA]. Restricts the result to
 * the member we are searching for.
 *
 * @author avix
 * @version $Id$
 */
class ChildByNameConstraint extends DefaultMemberChildrenConstraint {
    String childName;
    Object cacheKey;

    /**
     * Creates a <code>ChildByNameConstraint</code>.
     *
     * @param childName Name of child
     */
    public ChildByNameConstraint(String childName) {
        this.childName = childName;
        this.cacheKey = Arrays.asList(
                new Object[] {
                    super.getCacheKey(),
                    ChildByNameConstraint.class, childName});
    }

    public void addLevelConstraint(
        SqlQuery query,
        AggStar aggStar,
        RolapLevel level, 
        Map<RolapLevel, RolapStar.Column> levelToColumnMap)
    {
        super.addLevelConstraint(query, aggStar, level, levelToColumnMap);
        query.addWhere(
            SqlConstraintUtils.constrainLevel(
                level,
                query,
                childName,
                true));
    }

    public String toString() {
        return "ChildByNameConstraint(" + childName + ")";
    }

    public Object getCacheKey() {
        return cacheKey;
    }

}

// End ChildByNameConstraint.java
