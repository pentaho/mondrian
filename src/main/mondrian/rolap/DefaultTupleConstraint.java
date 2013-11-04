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
package mondrian.rolap;

import mondrian.olap.Evaluator;
import mondrian.rolap.sql.*;

import java.util.Collections;
import java.util.List;

/**
 * TupleConstraint which does not restrict the result.
 */
public class DefaultTupleConstraint implements TupleConstraint {

    private static final TupleConstraint instance =
        new DefaultTupleConstraint();

    /**
     * Creates the singleton instance.
     */
    private DefaultTupleConstraint() {
    }

    public List<RolapMeasureGroup> getMeasureGroupList() {
        return Collections.emptyList();
    }

    public boolean isJoinRequired() {
        return false;
    }

    public void addConstraint(
        SqlQueryBuilder queryBuilder,
        RolapStarSet starSet)
    {
    }

    public void addLevelConstraint(
        SqlQuery sqlQuery,
        RolapStarSet starSet,
        RolapCubeLevel level)
    {
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(
        RolapMember parent)
    {
        return DefaultMemberChildrenConstraint.instance();
    }

    public String toString() {
        return "DefaultTupleConstraint";
    }

    public Object getCacheKey() {
        // we have no state, so all instances are equal
        return this;
    }

    /**
     * Returns the singleton instance.
     *
     * @return the instance
     */
    public static TupleConstraint instance() {
        return instance;
    }

    public Evaluator getEvaluator() {
        return null;
    }
}

// End DefaultTupleConstraint.java
