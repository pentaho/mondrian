/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2015-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Evaluator;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * Constraint which excludes the members in the list received in constructor.
 *
 * @author Pedro Vale
 */
class MemberExcludeConstraint implements TupleConstraint {
    private final List<RolapMember> excludes;
    private final Object cacheKey;
    private final RolapLevel level;
    private final RolapNativeSet.SetConstraint csc;
    private final Map<RolapLevel, List<RolapMember>> roles;

    /**
     * Creates a <code>MemberExcludeConstraint</code>.
     *
     */
    public MemberExcludeConstraint(
        List<RolapMember> excludes,
        RolapLevel level,
        RolapNativeSet.SetConstraint csc)
    {
        this.excludes = excludes;
        this.cacheKey = asList(MemberExcludeConstraint.class, excludes, csc);
        this.level = level;
        this.csc = csc;

        roles = (csc == null)
            ? Collections.<RolapLevel, List<RolapMember>>emptyMap()
            : SqlConstraintUtils.getRolesConstraints(csc.getEvaluator());
    }


    @Override
    public int hashCode() {
        return getCacheKey().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof MemberExcludeConstraint
            && getCacheKey()
                .equals(((MemberExcludeConstraint) obj).getCacheKey());
    }

    @Override
    public void addLevelConstraint(
        SqlQuery query,
        RolapCube baseCube,
        AggStar aggStar,
        RolapLevel level)
    {
        if (level.equals(this.level)) {
            SqlConstraintUtils.addMemberConstraint(
                query, baseCube, aggStar, excludes, true, false, true);
        }
        if (csc != null) {
            for (CrossJoinArg cja : csc.args) {
                if (cja.getLevel().equals(level)) {
                    cja.addConstraint(query, baseCube, aggStar);
                }
            }
        }

        if (roles.containsKey(level)) {
            List<RolapMember> members = roles.get(level);
            SqlConstraintUtils.addMemberConstraint(
                query, baseCube, aggStar, members, true, false, false);
        }
    }

    public String toString() {
        return "MemberExcludeConstraint(" + excludes + ")";
    }

    @Override
    public Object getCacheKey() {
        return cacheKey;
    }


    public MemberChildrenConstraint getMemberChildrenConstraint(
        RolapMember parent)
    {
        return DefaultMemberChildrenConstraint.instance();
    }

    @Override
    public void addConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar)
    {
    }

    @Override
    public Evaluator getEvaluator() {
        if (csc != null) {
            return csc.getEvaluator();
        } else {
            return null;
        }
    }

    @Override
    public boolean supportsAggTables() {
        return true;
    }
}

// End MemberExcludeConstraint.java