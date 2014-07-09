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

import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.rolap.sql.*;

import java.util.*;

/**
 * Constraint that
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
    private final RolapEvaluator evaluator;
    private final boolean strict;
    private final List<RolapMeasureGroup> measureGroupList;

    /**
     * Tests whether this is a valid context, and populates a list of stars.
     *
     * @param context evaluation context
     * @param disallowVirtualCube if true, check for virtual cubes
     * @param levels levels being referenced in the current context
     * @param strict false if more rows than requested may be returned
     * (i.e. the constraint is incomplete)
     *
     * @param measureGroupList List of measure groups
     *
     * @return false if constraint will not work for current context
     */
    public static boolean checkValidContext(
        RolapEvaluator context,
        boolean disallowVirtualCube,
        List<RolapCubeLevel> levels,
        boolean strict,
        List<RolapMeasureGroup> measureGroupList)
    {
        if (context == null) {
            return false;
        }
        RolapCube cube = context.getCube();
        if (cube.isVirtual()) {
            if (disallowVirtualCube) {
                return false;
            }

            Query query = context.getQuery();
            Set<RolapCube> baseCubes = new LinkedHashSet<RolapCube>();
            assert measureGroupList.isEmpty();
            Set<RolapMeasureGroup> measureGroupSet =
                new LinkedHashSet<RolapMeasureGroup>();
            if (!findVirtualCubeStars(query, baseCubes, measureGroupSet)) {
                return false;
            }
            assert levels != null;
            measureGroupList.addAll(measureGroupSet);
            // REVIEW: Remove the following line. The setMeasureGroups method
            // may be called several times while processing a query. Probably
            // with the same argument, but it's a sign that the information
            // should be stored somewhere else. Or at least derived at a
            // different time in the query life cycle.
            query.setMeasureGroups(measureGroupList);
        } else {
            Util.deprecated("review following code, pasted from above", false);
            Query query = context.getQuery();
            Set<RolapCube> baseCubes = new LinkedHashSet<RolapCube>();
            assert measureGroupList.isEmpty();
            Set<RolapMeasureGroup> measureGroupSet =
                new LinkedHashSet<RolapMeasureGroup>();
            if (!findVirtualCubeStars(query, baseCubes, measureGroupSet)) {
                return false;
            }
            assert levels != null;
            measureGroupList.addAll(measureGroupSet);
        }

        // may return more rows than requested?
        if (!strict) {
            return true;
        }

        // Although it is technically possible to build a native SQL predicate
        // to represent a multi-position compound slicer (see
        // http://jira.pentaho.com/browse/MONDRIAN-791), this trick
        // requires that we have access to the slicer axis (so we can iterate
        // over its positions). Alas, the evaluator does not give us access to
        // the slicer axis, but only the members on it
        if (SqlConstraintUtils.hasMultiPositionSlicer(context)) {
            return false;
        }

        // we can not handle calc members in slicer except calc measure
        Member[] members = context.getMembers();
        for (int i = 1; i < members.length; i++) {
            if (members[i].isCalculated()) {
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
    private static boolean findVirtualCubeStars(
        Query query,
        Collection<RolapCube> baseCubes,
        Set<RolapMeasureGroup> measureGroupSet)
    {
        // Gather the unique set of level-to-column maps corresponding
        // to the underlying star/cube where the measure column
        // originates from.
        Set<Member> measureMembers = query.getMeasuresMembers();
        // if no measures are explicitly referenced, just use the default
        // measure
        if (measureMembers.isEmpty()) {
            Cube cube = query.getCube();
            Dimension dimension = cube.getDimensionList().get(0);
            query.addMeasuresMembers(
                dimension.getHierarchy().getDefaultMember());
        }
        for (Member member : query.getMeasuresMembers()) {
            if (member instanceof RolapStoredMeasure) {
                addMeasure(
                    (RolapStoredMeasure) member, baseCubes, measureGroupSet);
            } else if (member instanceof CalculatedMember) {
                findMeasures(
                    member.getExpression(), baseCubes, measureGroupSet);
            }
        }
        if (baseCubes.isEmpty()) {
            return false;
        }
        return true;
    }

    /**
     * Adds information regarding a stored measure to maps.
     *
     * @param measure The stored measure
     * @param baseCubes Collection of base cubes
     * @param stars Collection of stars
     */
    private static void addMeasure(
        RolapStoredMeasure measure,
        Collection<RolapCube> baseCubes,
        Set<RolapMeasureGroup> stars)
    {
        RolapCube baseCube = measure.getCube();
        baseCubes.add(baseCube);
        stars.add(measure.getMeasureGroup());
    }

    /**
     * Extracts the stored measures referenced in an expression
     *
     * @param exp expression
     * @param baseCubes set of base cubes
     * @param measureGroupSet Measure group set
     */
    private static void findMeasures(
        Exp exp,
        Collection<RolapCube> baseCubes,
        Set<RolapMeasureGroup> measureGroupSet)
    {
        if (exp instanceof MemberExpr) {
            MemberExpr memberExpr = (MemberExpr) exp;
            Member member = memberExpr.getMember();
            if (member instanceof RolapStoredMeasure) {
                addMeasure(
                    (RolapStoredMeasure) member, baseCubes, measureGroupSet);
            } else if (member instanceof CalculatedMember) {
                findMeasures(
                    member.getExpression(), baseCubes, measureGroupSet);
            }
        } else if (exp instanceof ResolvedFunCall) {
            ResolvedFunCall funCall = (ResolvedFunCall) exp;
            Exp [] args = funCall.getArgs();
            for (Exp arg : args) {
                findMeasures(arg, baseCubes, measureGroupSet);
            }
        }
    }

    /**
     * Creates a SqlContextConstraint.
     *
     * @param evaluator Evaluator
     *
     * @param measureGroupList List of stars to join to
     *
     * @param strict defines the behaviour if the evaluator context
     * contains calculated members. If true, an exception is thrown,
     * otherwise calculated members are silently ignored. The methods
     * {@link mondrian.rolap.sql.MemberChildrenConstraint#addMemberConstraint(mondrian.rolap.sql.SqlQueryBuilder, RolapStarSet, RolapMember)}
     * and
     * {@link mondrian.rolap.sql.MemberChildrenConstraint#addMemberConstraint(mondrian.rolap.sql.SqlQueryBuilder, RolapStarSet, java.util.List)}
     * will
     * {@link Util#deprecated ... what text was removed here?}
     */
    SqlContextConstraint(
        RolapEvaluator evaluator,
        List<RolapMeasureGroup> measureGroupList,
        boolean strict)
    {
        this.evaluator = evaluator.push();
        this.strict = strict;
        cacheKey = new ArrayList<Object>();
        cacheKey.add(getClass());
        cacheKey.add(strict);
        final List<RolapMember> memberList =
            new ArrayList<RolapMember>(Arrays.asList(evaluator.getMembers()));
        SqlConstraintUtils.removeMultiPositionSlicerMembers(
            memberList, evaluator);
        cacheKey.addAll(memberList);

        // Add restrictions imposed by Role based access filtering
        Map<Level, List<RolapMember>> roleMembers =
            SqlConstraintUtils.getRoleConstraintMembers(
                this.getEvaluator().getSchemaReader(),
                this.getEvaluator().getMembers());
        for (List<RolapMember> list : roleMembers.values()) {
            cacheKey.addAll(list);
        }

        // For virtual cubes, context constraint should be evaluated in the
        // query's context, because the query might reference different base
        // cubes.
        //
        // Note: we could avoid adding base cubes to the key if the evaluator
        // contains measure members referenced in the query, rather than
        // just the default measure for the entire virtual cube. The commented
        // code in RolapResult() that replaces the default measure seems to
        // do that.
        assert measureGroupList != null;
        cacheKey.addAll(measureGroupList);
        assert Util.isDistinct(measureGroupList) : measureGroupList;
        this.measureGroupList = measureGroupList;
    }

    /**
     * Called from MemberChildren: adds <code>parent</code> to the current
     * context and restricts the SQL result set to that new context.
     */
    public void addMemberConstraint(
        SqlQueryBuilder queryBuilder,
        RolapStarSet starSet,
        RolapMember parent)
    {
        if (parent.isCalculated()) {
            throw Util.newInternal("cannot restrict SQL to calculated member");
        }
        final int savepoint = evaluator.savepoint();
        try {
            evaluator.setContext(parent);
            SqlConstraintUtils.addContextConstraint(
                queryBuilder, starSet, evaluator, strict);
        } finally {
            evaluator.restore(savepoint);
        }
    }

    /**
     * {@inheritDoc}
     *
     * Adds <code>parents</code> to the current
     * context and restricts the SQL result set to that new context.
     */
    public void addMemberConstraint(
        SqlQueryBuilder queryBuilder,
        RolapStarSet starSet,
        List<RolapMember> parents)
    {
        SqlConstraintUtils.addContextConstraint(
            queryBuilder, starSet, evaluator, strict);
        boolean exclude = false;
        SqlConstraintUtils.addMemberConstraint(
            queryBuilder, starSet, parents, true, false, exclude);
    }

    /**
     * {@inheritDoc}
     *
     * Called from LevelMembers: restricts the SQL result set to the current
     * context.
     */
    public void addConstraint(
        SqlQueryBuilder queryBuilder,
        RolapStarSet starSet)
    {
        SqlConstraintUtils.addContextConstraint(
            queryBuilder, starSet, evaluator, strict);
    }

    public boolean isJoinRequired() {
        Member[] members = evaluator.getMembers();
        // members[0] is the Measure, so loop starts at 1
        for (int i = 1; i < members.length; i++) {
            if (!members[i].isAll()) {
                return true;
            }
        }
        return false;
    }

    public RolapStarSet createStarSet(RolapMeasureGroup aggMeasureGroup) {
        final Member measure = this.evaluator.getMembers()[0];
        final RolapStar star;
        final RolapMeasureGroup measureGroup;
        if (measure instanceof RolapStoredMeasure) {
            RolapStoredMeasure storedMeasure = (RolapStoredMeasure) measure;
            star = storedMeasure.getStarMeasure().getStar();
            measureGroup = storedMeasure.getMeasureGroup();
        } else {
            star = null;
            measureGroup = null;
        }
        return new RolapStarSet(star, measureGroup, aggMeasureGroup);
    }

    public void addLevelConstraint(
        SqlQuery sqlQuery,
        RolapStarSet starSet,
        RolapCubeLevel level)
    {
        if (!isJoinRequired()) {
            return;
        }
        SqlConstraintUtils.joinLevelTableToFactTable(
            sqlQuery, starSet, evaluator, level);
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

    public List<RolapMeasureGroup> getMeasureGroupList() {
        return measureGroupList;
    }
}

// End SqlContextConstraint.java

