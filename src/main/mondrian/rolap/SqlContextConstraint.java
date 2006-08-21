/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.*;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.olap.Util;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.rolap.aggmatcher.AggStar;

/**
 * limits the result of a Member SQL query to the current evaluation context.
 * All Members of the current context are joined against the fact table and only
 * those rows are returned, that have an entry in the fact table.
 * <p>
 * For example, if you have two dimensions, "invoice" and "time", and the current
 * context (e.g. the slicer) contains a day from the "time" dimension, then
 * only the invoices of that day are found. Used to optimize NON EMPTY.
 *
 * <p> The {@link TupleConstraint} methods may silently ignore calculated
 * members (depends on the <code>strict</code> c'tor argument), so these may
 * return more members than the current context restricts to. The
 * MemberChildren methods will never accept calculated members as parents,
 * these will cause an exception.
 *
 * @author av
 * @since Nov 2, 2005
 */
public class SqlContextConstraint implements MemberChildrenConstraint,
        TupleConstraint {
    List cacheKey;
    private Evaluator evaluator;
    private boolean strict;

    /**
     * @return false if this contstraint will not work for the current context
     */
    public static boolean isValidContext(Evaluator context) {
        return isValidContext(context, true);
    }

    /**
     * @param context evaluation context
     * @param disallowVirtualCube if true, check for virtual cubes
     *
     * @return false if constraint will not work for current context
     */
    public static boolean isValidContext(
        Evaluator context, boolean disallowVirtualCube) {

        if (context == null) {
            return false;
        }
        if (disallowVirtualCube) {
            RolapCube cube = (RolapCube) context.getCube();
            if (cube.isVirtual()) {
                return false;
            }
        }
        return true;
    }

   /**
    * @param strict defines the behaviour if the evaluator context
    * contains calculated members. If true, an exception is thrown,
    * otherwise calculated members are silently ignored. The
    * methods {@link mondrian.rolap.sql.MemberChildrenConstraint#addMemberConstraint(mondrian.rolap.sql.SqlQuery,mondrian.rolap.aggmatcher.AggStar,java.util.List)} and
    * {@link mondrian.rolap.sql.MemberChildrenConstraint#addMemberConstraint(mondrian.rolap.sql.SqlQuery,mondrian.rolap.aggmatcher.AggStar,RolapMember)} will
    * never accept a calculated member as parent.
    */
    SqlContextConstraint(RolapEvaluator evaluator, boolean strict) {
        this.evaluator = evaluator;
        this.strict = strict;
        cacheKey = new ArrayList();
        cacheKey.add(getClass());
        cacheKey.add(Boolean.valueOf(strict));
        cacheKey.addAll(Arrays.asList(evaluator.getMembers()));
    }

    /**
     * Called from MemberChildren: adds <code>parent</code> to the current
     * context and restricts the SQL resultset to that new context.
     */
    public void addMemberConstraint(
        SqlQuery sqlQuery, AggStar aggStar, RolapMember parent)
    {
        if (parent.isCalculated()) {
            throw Util.newInternal("cannot restrict SQL to calculated member");
        }
        Evaluator e = evaluator.push(parent);
        SqlConstraintUtils.addContextConstraint(sqlQuery, aggStar, e, strict);
        SqlConstraintUtils.addMemberConstraint(sqlQuery, aggStar, parent, true);
    }

    public void addMemberConstraint(
        SqlQuery sqlQuery, AggStar aggStar, List parents) {
        SqlConstraintUtils.addContextConstraint(
            sqlQuery, aggStar, evaluator, strict);
        SqlConstraintUtils.addMemberConstraint(
            sqlQuery, aggStar, parents, true);
    }

    /**
     * Called from LevelMembers: restricts the SQL resultset to the current
     * context.
     */
    public void addConstraint(SqlQuery sqlQuery) {
        SqlConstraintUtils.addContextConstraint(
            sqlQuery, null, evaluator, strict);
    }

    /**
     * Returns whether a join with the fact table is required. A join is
     * required if the context contains members from dimensions other than
     * level. If we are interested in the members of a level or a members
     * children then it does not make sense to join only one dimension (the one
     * that contains the requested members) with the fact table for NON EMPTY
     * optimization.
     */
    protected boolean isJoinRequired() {
        Member[] members = evaluator.getMembers();
        // members[0] is the Measure, so loop starts at 1
        for (int i = 1; i < members.length; i++) {
            if (!members[i].isAll()) {
                return true;
            }
        }
        return false;
    }

    public void addLevelConstraint(
        SqlQuery sqlQuery, AggStar aggStar,
        RolapLevel level, Map levelToColumnMap) {
        if (!isJoinRequired()) {
            return;
        }
        SqlConstraintUtils.joinLevelTableToFactTable(
            sqlQuery, aggStar, evaluator, level, levelToColumnMap);
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(RolapMember parent) {
        return this;
    }

    public Object getCacheKey() {
        return cacheKey;
    }

    public Evaluator getEvaluator() {
        return evaluator;
    }
}

// End SqlContextConstraint.java
