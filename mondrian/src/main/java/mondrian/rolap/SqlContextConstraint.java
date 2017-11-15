/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.rolap.RestrictedMemberReader.MultiCardinalityDefaultMember;
import mondrian.rolap.RolapHierarchy.LimitedRollupMember;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.*;

import java.util.*;

/**
 * limits the result of a Member SQL query to the current evaluation context.
 * All Members of the current context are joined against the fact table and only
 * those rows are returned, that have an entry in the fact table.
 *
 * <p>For example, if you have two dimensions, "invoice" and "time", and the
 * current context (e.g. the slicer) contains a day from the "time" dimension,
 * then only the invoices of that day are found. Used to optimize NON EMPTY.
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
public class SqlContextConstraint
    implements MemberChildrenConstraint, TupleConstraint
{
    private final List<Object> cacheKey;
    private Evaluator evaluator;
    private boolean strict;

    /**
     * @param context evaluation context
     * @param strict false if more rows than requested may be returned
     * (i.e. the constraint is incomplete)
     *
     * @return false if this contstraint will not work for the current context
     */
    public static boolean isValidContext(Evaluator context, boolean strict) {
        return isValidContext(context, true, null, strict);
    }

    /**
     * @param context evaluation context
     * @param disallowVirtualCube if true, check for virtual cubes
     * @param levels levels being referenced in the current context
     * @param strict false if more rows than requested may be returned
     * (i.e. the constraint is incomplete)
     *
     * @return false if constraint will not work for current context
     */
    public static boolean isValidContext(
        Evaluator context,
        boolean disallowVirtualCube,
        Level [] levels,
        boolean strict)
    {
        if (context == null) {
            return false;
        }
        RolapCube cube = (RolapCube) context.getCube();
        if (disallowVirtualCube) {
            if (cube.isVirtual()) {
                return false;
            }
        }
        if (cube.isVirtual()) {
            Query query = context.getQuery();
            Set<RolapCube> baseCubes = new HashSet<RolapCube>();
            List<RolapCube> baseCubeList = new ArrayList<RolapCube>();
            if (!findVirtualCubeBaseCubes(query, baseCubes, baseCubeList)) {
                return false;
            }
            assert levels != null;
            query.setBaseCubes(baseCubeList);
        }

        if (SqlConstraintUtils.measuresConflictWithMembers(
                context.getQuery().getMeasuresMembers(), context.getMembers()))
        {
            // one or more dimension members referenced within measure calcs
            // conflict with the context members.  Not safe to apply
            // SqlContextConstraint.
            return false;
        }

        // may return more rows than requested?
        if (!strict) {
            return true;
        }

        // we can not handle all calc members in slicer. Calc measure and some
        // like aggregates are exceptions
        Member[] members = context.getMembers();
        for (int i = 1; i < members.length; i++) {
            if (members[i].isCalculated()
                && !SqlConstraintUtils.isSupportedCalculatedMember(members[i]))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Locates base cubes related to the measures referenced in the query.
     *
     * @param query query referencing the virtual cube
     * @param baseCubes set of base cubes
     *
     * @return true if valid measures exist
     */
    private static boolean findVirtualCubeBaseCubes(
        Query query,
        Set<RolapCube> baseCubes,
        List<RolapCube> baseCubeList)
    {
        // Gather the unique set of level-to-column maps corresponding
        // to the underlying star/cube where the measure column
        // originates from.
        Set<Member> measureMembers = query.getMeasuresMembers();
        // if no measures are explicitly referenced, just use the default
        // measure
        if (measureMembers.isEmpty()) {
            Cube cube = query.getCube();
            Dimension dimension = cube.getDimensions()[0];
            query.addMeasuresMembers(
                dimension.getHierarchy().getDefaultMember());
        }
        for (Member member : query.getMeasuresMembers()) {
            if (member instanceof RolapStoredMeasure) {
                addMeasure(
                    (RolapStoredMeasure) member, baseCubes, baseCubeList);
            } else if (member instanceof RolapCalculatedMember) {
                findMeasures(member.getExpression(), baseCubes, baseCubeList);
            }
        }
        if (baseCubes.isEmpty()) {
            return false;
        }
        return true;
    }

    /**
     * Adds information regarding a stored measure to maps
     *
     * @param measure the stored measure
     * @param baseCubes set of base cubes
     */
    private static void addMeasure(
        RolapStoredMeasure measure,
        Set<RolapCube> baseCubes,
        List<RolapCube> baseCubeList)
    {
        RolapCube baseCube = measure.getCube();
        if (baseCubes.add(baseCube)) {
            baseCubeList.add(baseCube);
        }
    }

    /**
     * Extracts the stored measures referenced in an expression
     *
     * @param exp expression
     * @param baseCubes set of base cubes
     */
    private static void findMeasures(
        Exp exp,
        Set<RolapCube> baseCubes,
        List<RolapCube> baseCubeList)
    {
        if (exp instanceof MemberExpr) {
            MemberExpr memberExpr = (MemberExpr) exp;
            Member member = memberExpr.getMember();
            if (member instanceof RolapStoredMeasure) {
                addMeasure(
                    (RolapStoredMeasure) member, baseCubes, baseCubeList);
            } else if (member instanceof RolapCalculatedMember) {
                findMeasures(member.getExpression(), baseCubes, baseCubeList);
            }
        } else if (exp instanceof ResolvedFunCall) {
            ResolvedFunCall funCall = (ResolvedFunCall) exp;
            Exp [] args = funCall.getArgs();
            for (Exp arg : args) {
                findMeasures(arg, baseCubes, baseCubeList);
            }
        }
    }

    /**
    * Creates a SqlContextConstraint.
    *
    * @param evaluator Evaluator
    * @param strict defines the behaviour if the evaluator context
    * contains calculated members. If true, an exception is thrown,
    * otherwise calculated members are silently ignored. The
    * methods {@link mondrian.rolap.sql.MemberChildrenConstraint#addMemberConstraint(mondrian.rolap.sql.SqlQuery, mondrian.rolap.RolapCube, mondrian.rolap.aggmatcher.AggStar, RolapMember)} and
    * {@link mondrian.rolap.sql.MemberChildrenConstraint#addMemberConstraint(mondrian.rolap.sql.SqlQuery, mondrian.rolap.RolapCube, mondrian.rolap.aggmatcher.AggStar, java.util.List)} will
    * never accept a calculated member as parent.
    */
    SqlContextConstraint(RolapEvaluator evaluator, boolean strict) {
        this.evaluator = evaluator.push();
        this.strict = strict;
        cacheKey = new ArrayList<Object>();
        cacheKey.add(getClass());
        cacheKey.add(strict);

        List<Member> members = new ArrayList<Member>();
        List<Member> expandedMembers = new ArrayList<Member>();

        members.addAll(
            Arrays.asList(
                SqlConstraintUtils.expandMultiPositionSlicerMembers(
                    evaluator.getMembers(), evaluator)));

        // Now we'll need to expand the aggregated members
        expandedMembers.addAll(
            SqlConstraintUtils.expandSupportedCalculatedMembers(
                members,
                evaluator).getMembers());
        cacheKey.add(expandedMembers);
        cacheKey.add(evaluator.getSlicerTuples());

        // Add restrictions imposed by Role based access filtering
        Map<Level, List<RolapMember>> roleMembers =
            SqlConstraintUtils.getRoleConstraintMembers(
                this.getEvaluator().getSchemaReader(),
                this.getEvaluator().getMembers());
        for (List<RolapMember> list : roleMembers.values()) {
            cacheKey.addAll(list);
        }

        // MONDRIAN-2597
        //For virtual cube we add all base cubes
        //associated with this virtual cube to the key
        if (evaluator.getCube().isVirtual()) {
            cacheKey.addAll(evaluator.getCube().getBaseCubes());
        }
    }

    /**
     * Called from MemberChildren: adds <code>parent</code> to the current
     * context and restricts the SQL resultset to that new context.
     */
    public void addMemberConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        RolapMember parent)
    {
        if (parent.isCalculated()) {
            throw Util.newInternal("cannot restrict SQL to calculated member");
        }
        final int savepoint = evaluator.savepoint();
        try {
            evaluator.setContext(parent);
            SqlConstraintUtils.addContextConstraint(
                sqlQuery, aggStar, evaluator, baseCube, strict);
        } finally {
            evaluator.restore(savepoint);
        }

         SqlConstraintUtils.addMemberConstraint(
                sqlQuery, baseCube, aggStar, parent, true);
    }

    /**
     * Adds <code>parents</code> to the current
     * context and restricts the SQL resultset to that new context.
     */
    public void addMemberConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        List<RolapMember> parents)
    {
        SqlConstraintUtils.addContextConstraint(
            sqlQuery, aggStar, evaluator, baseCube, strict);
        boolean exclude = false;
        SqlConstraintUtils.addMemberConstraint(
            sqlQuery, baseCube, aggStar, parents, true, false, exclude);
    }

    /**
     * Called from LevelMembers: restricts the SQL resultset to the current
     * context.
     */
    public void addConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar)
    {
        SqlConstraintUtils.addContextConstraint(
            sqlQuery, aggStar, evaluator, baseCube, strict);
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
            if (!members[i].isAll()
                || members[i] instanceof LimitedRollupMember
                || members[i] instanceof MultiCardinalityDefaultMember)
            {
                return true;
            }
        }
        return false;
    }

    public void addLevelConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        RolapLevel level)
    {
        if (!isJoinRequired()) {
            return;
        }
        SqlConstraintUtils.joinLevelTableToFactTable(
            sqlQuery, baseCube, aggStar, evaluator, (RolapCubeLevel)level);
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(
        RolapMember parent)
    {
        return this;
    }

    public Object getCacheKey() {
        return cacheKey;
    }

    public Evaluator getEvaluator() {
        return evaluator;
    }

    @Override
    public boolean supportsAggTables() {
        return true;
    }
}

// End SqlContextConstraint.java

