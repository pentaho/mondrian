/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

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
