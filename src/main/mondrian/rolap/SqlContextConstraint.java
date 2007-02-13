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

import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
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
    List<Object> cacheKey;
    private Evaluator evaluator;
    private boolean strict;

    /**
     * @return false if this contstraint will not work for the current context
     */
    public static boolean isValidContext(Evaluator context) {
        return isValidContext(context, true, null);
    }

    /**
     * @param context evaluation context
     * @param disallowVirtualCube if true, check for virtual cubes
     * @param levels levels being referenced in the current context
     *
     * @return false if constraint will not work for current context
     */
    public static boolean isValidContext(
        Evaluator context,
        boolean disallowVirtualCube,
        Level [] levels)
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
            Set<Map<RolapLevel, RolapStar.Column>> baseCubesLevelToColumnMaps =
                new HashSet<Map<RolapLevel, RolapStar.Column>>();
            Map<Map<RolapLevel, RolapStar.Column>, RolapMember> measureMap =
                new HashMap<Map<RolapLevel, RolapStar.Column>, RolapMember>();
            if (!findVirtualCubeJoinLevels(
                query,
                baseCubesLevelToColumnMaps,
                measureMap))
            {
                return false;
            }
            assert(levels != null);
            // we need to make sure all the levels join with each fact table;
            // otherwise, it doesn't make sense to do the processing
            // natively, as you'll end up with cartesian product joins!
            for (Map<RolapLevel, RolapStar.Column> map : baseCubesLevelToColumnMaps) {
                for (Level level : levels) {
                    if (map.get((RolapLevel) level) == null) {
                        return false;
                    }
                }
            }
            query.setVirtualCubeBaseCubeMaps(baseCubesLevelToColumnMaps);
            query.setLevelMapToMeasureMap(measureMap);
        }
        return true;
    }

    /**
     * Locates joins required with the underlying fact tables that make up a
     * virtual cube by validating the measures referenced in the query.
     *
     * @param query query referencing the virtual cube
     * @param baseCubesLevelToColumnMaps level to column maps corresponding
     * to the base cubes referenced from the virtual cube
     * @param measureMap mapping between a measure and the level to column
     * maps
     *
     * @return true if valid measures exist
     */
    private static boolean findVirtualCubeJoinLevels(
        Query query,
        Set<Map<RolapLevel, RolapStar.Column>> baseCubesLevelToColumnMaps,
        Map<Map<RolapLevel, RolapStar.Column>, RolapMember> measureMap)
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
                    (RolapStoredMeasure) member,
                    baseCubesLevelToColumnMaps,
                    measureMap);
            } else if (member instanceof RolapCalculatedMember) {
                findMeasures(
                    member.getExpression(),
                    baseCubesLevelToColumnMaps,
                    measureMap);
            }
        }
        if (measureMap.isEmpty()) {
            return false;
        }

        return true;
    }

    /**
     * Adds information regarding a stored measure to maps
     *
     * @param measure the stored measure
     * @param baseCubesLevelToColumnMaps level to column maps for the
     * underlying cubes that make up the virtual cube referenced in a query
     * @param measureMap maps a level-to-column map to a measure
     */
    private static void addMeasure(
        RolapStoredMeasure measure,
        Set<Map<RolapLevel, RolapStar.Column>> baseCubesLevelToColumnMaps,
        Map<Map<RolapLevel, RolapStar.Column>, RolapMember> measureMap)
    {
        RolapStar.Measure starMeasure =
            (RolapStar.Measure) measure.getStarMeasure();
        RolapStar star = starMeasure.getStar();
        RolapCube baseCube = measure.getCube();
        Map<RolapLevel, RolapStar.Column> levelToColumnMap =
            star.getLevelToColumnMap(baseCube);
        if (baseCubesLevelToColumnMaps.add(levelToColumnMap)) {
            measureMap.put(levelToColumnMap, (RolapMember) measure);
        }
    }

    /**
     * Extracts the stored measures referenced in an expression
     *
     * @param exp expression
     * @param baseCubesLevelToColumnMaps
     * @param baseCubesLevelToColumnMaps level to column maps for the
     * underlying cubes that make up the virtual cube referenced in a query
     * @param measureMap maps a level-to-column map to a measure
     */
    private static void findMeasures(
        Exp exp,
        Set<Map<RolapLevel, RolapStar.Column>> baseCubesLevelToColumnMaps,
        Map<Map<RolapLevel, RolapStar.Column>, RolapMember> measureMap)
    {
        if (exp instanceof MemberExpr) {
            MemberExpr memberExpr = (MemberExpr) exp;
            Member member = memberExpr.getMember();
            if (member instanceof RolapStoredMeasure) {
                addMeasure(
                    (RolapStoredMeasure) member,
                    baseCubesLevelToColumnMaps,
                    measureMap);
            } else if (member instanceof RolapCalculatedMember) {
                findMeasures(
                    member.getExpression(),
                    baseCubesLevelToColumnMaps,
                    measureMap);
            }
        } else if (exp instanceof ResolvedFunCall) {
            ResolvedFunCall funCall = (ResolvedFunCall) exp;
            Exp [] args = funCall.getArgs();
            for (Exp arg : args) {
                findMeasures(
                    arg,
                    baseCubesLevelToColumnMaps,
                    measureMap);
            }
        }
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
        cacheKey = new ArrayList<Object>();
        cacheKey.add(getClass());
        cacheKey.add(strict);
        cacheKey.addAll(Arrays.asList(evaluator.getMembers()));
    }

    /**
     * Called from MemberChildren: adds <code>parent</code> to the current
     * context and restricts the SQL resultset to that new context.
     */
    public void addMemberConstraint(
        SqlQuery sqlQuery,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap,
        AggStar aggStar,
        RolapMember parent)
    {
        if (parent.isCalculated()) {
            throw Util.newInternal("cannot restrict SQL to calculated member");
        }
        Evaluator e = evaluator.push(parent);
        SqlConstraintUtils.addContextConstraint(sqlQuery, aggStar, e, strict);
        SqlConstraintUtils.addMemberConstraint(
            sqlQuery, levelToColumnMap, aggStar, parent, true);
    }

    public void addMemberConstraint(
        SqlQuery sqlQuery,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap,
        AggStar aggStar,
        List<RolapMember> parents)
    {
        SqlConstraintUtils.addContextConstraint(
            sqlQuery, aggStar, evaluator, strict);
        SqlConstraintUtils.addMemberConstraint(
            sqlQuery, levelToColumnMap, aggStar, parents, true, false);
    }

    /**
     * Called from LevelMembers: restricts the SQL resultset to the current
     * context.
     */
    public void addConstraint(
        SqlQuery sqlQuery, Map<RolapLevel, RolapStar.Column> levelToColumnMap) {
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
        SqlQuery sqlQuery,
        AggStar aggStar,
        RolapLevel level,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap) {
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
