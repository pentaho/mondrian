/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2017 Hitachi Vantara
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Id;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;

import java.util.*;

/**
 * Constraint which optimizes the search for a child by name. This is used
 * whenever the string representation of a member is parsed, e.g.
 * [Customers].[USA].[CA]. Restricts the result to
 * the member we are searching for.
 *
 * @author avix
 */
class ChildByNameConstraint extends DefaultMemberChildrenConstraint {
    private final String[] childNames;
    private final Object cacheKey;

    /**
     * Creates a <code>ChildByNameConstraint</code>.
     *
     * @param childName Name of child
     */
    public ChildByNameConstraint(Id.NameSegment childName) {
        this.childNames = new String[]{childName.name};
        this.cacheKey = Arrays.asList(ChildByNameConstraint.class, childName);
    }

    public ChildByNameConstraint(List<Id.NameSegment> childNames) {
        this.childNames = new String[childNames.size()];
        int i = 0;
        for (Id.NameSegment name : childNames) {
            this.childNames[i++] = name.name;
        }
        this.cacheKey = Arrays.asList(
            ChildByNameConstraint.class, this.childNames);
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
        RolapCube baseCube,
        AggStar aggStar,
        RolapLevel level)
    {
        super.addLevelConstraint(query, baseCube, aggStar, level);
        query.addWhere(
            SqlConstraintUtils.constrainLevel(
                level, query, baseCube, aggStar, childNames, true));
    }

    public String toString() {
        return "ChildByNameConstraint(" + Arrays.toString(childNames) + ")";
    }

    public Object getCacheKey() {
        return cacheKey;
    }

    public List<String> getChildNames() {
        return Arrays.asList(childNames);
    }

}

// End ChildByNameConstraint.java
