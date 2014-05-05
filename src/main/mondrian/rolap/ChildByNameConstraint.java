/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Id;
import mondrian.rolap.sql.SqlQuery;

import java.util.Arrays;

/**
 * Constraint which optimizes the search for a child by name. This is used
 * whenever the string representation of a member is parsed, e.g.
 * [Customers].[USA].[CA]. Restricts the result to
 * the member we are searching for.
 *
 * @author avix
 */
class ChildByNameConstraint extends DefaultMemberChildrenConstraint {
    final String childName;
    private final Object cacheKey;

    /**
     * Creates a <code>ChildByNameConstraint</code>.
     *
     * @param childName Name of child
     */
    public ChildByNameConstraint(Id.NameSegment childName) {
        this.childName = childName.name;
        this.cacheKey = Arrays.asList(ChildByNameConstraint.class, childName);
    }

    @Override
    public int hashCode() {
        return getCacheKey().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ChildByNameConstraint
            && getCacheKey().equals(
                ((ChildByNameConstraint) obj).getCacheKey());
    }

    public void addLevelConstraint(
        SqlQuery query,
        RolapStarSet starSet,
        RolapCubeLevel level)
    {
        super.addLevelConstraint(query, starSet, level);
        query.addWhere(
            SqlConstraintUtils.constrainLevel(
                level.attribute.getNameExp(), query.getDialect(), childName,
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
