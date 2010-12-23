/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.Evaluator;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.aggmatcher.AggStar;

import java.util.List;
import java.util.Collections;

/**
 * TupleConstraint which does not restrict the result.
 *
 * @version $Id$
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
        SqlQuery sqlQuery,
        RolapStarSet starSet,
        AggStar aggStar)
    {
    }

    public void addLevelConstraint(
        SqlQuery query,
        RolapStarSet starSet,
        AggStar aggStar,
        RolapLevel level)
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
