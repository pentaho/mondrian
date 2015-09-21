/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2015 Pentaho and others
// All Rights Reserved.
//
*/
package mondrian.rolap;

import mondrian.olap.Member;
import mondrian.olap.fun.VisualTotalsFunDef;
import mondrian.rolap.agg.OrPredicate;
import mondrian.rolap.agg.ValueColumnPredicate;
import mondrian.rolap.sql.SqlQuery;
import mondrian.util.Pair;

import java.util.*;


/**
 * Constructs a Pair<BitKey, StarPredicate> based on an tuple list and
 * measure, along with the string representation of the predicate.
 * Also sets the isSatisfiable flag based on whether a predicate
 * is compatible with the measure.
 *
 * This logic was extracted from RolapAggregationManager and AggregationKey.
 */
public class CompoundPredicateInfo {

    private final Pair<BitKey, StarPredicate> predicate;
    private final String predicateString;
    private final RolapMeasure measure;
    private boolean satisfiable = true;

    public CompoundPredicateInfo(
        List<List<Member>> tupleList, RolapMeasure measure)
    {
        this.predicate = predicateFromTupleList(tupleList, measure);
        this.predicateString = getPredicateString(
            getStar(measure), getPredicate());
        this.measure = measure;
    }

    public StarPredicate getPredicate() {
        return predicate == null ? null : predicate.right;
    }

    public BitKey getBitKey() {
        return predicate == null ? null : predicate.left;
    }

    public String getPredicateString() {
        return predicateString;
    }

    public boolean isSatisfiable() {
        return satisfiable;
    }

    public RolapCube getCube() {
        return measure.isCalculated() ? null
            : ((RolapStoredMeasure)measure).getCube();
    }
    /**
     * Returns a string representation of the predicate
     */
    public static String getPredicateString(
        RolapStar star, StarPredicate predicate)
    {
        if (star == null || predicate == null) {
            return null;
        }
        final StringBuilder buf = new StringBuilder();
        SqlQuery query =
            new SqlQuery(
                star.getSqlQueryDialect());
        buf.setLength(0);
        predicate.toSql(query, buf);
        return buf.toString();
    }

    private static RolapStar getStar(RolapMeasure measure) {
        if (measure.isCalculated()) {
            return null;
        }
        final RolapStoredMeasure storedMeasure =
            (RolapStoredMeasure) measure;
        final RolapStar.Measure starMeasure =
            (RolapStar.Measure) storedMeasure.getStarMeasure();
        assert starMeasure != null;
        return starMeasure.getStar();
    }

    private Pair<BitKey, StarPredicate> predicateFromTupleList(
        List<List<Member>> tupleList, RolapMeasure measure)
    {
        if (measure.isCalculated()) {
            // need a base measure to build predicates
            return null;
        }
        RolapCube cube = ((RolapStoredMeasure)measure).getCube();

        BitKey compoundBitKey;
        StarPredicate compoundPredicate;
        Map<BitKey, List<RolapCubeMember[]>> compoundGroupMap;
        boolean unsatisfiable;
        int starColumnCount = getStar(measure).getColumnCount();

        compoundBitKey = BitKey.Factory.makeBitKey(starColumnCount);
        compoundBitKey.clear();
        compoundGroupMap =
            new LinkedHashMap<BitKey, List<RolapCubeMember[]>>();
        unsatisfiable =
            makeCompoundGroup(
                starColumnCount,
                cube,
                tupleList,
                compoundGroupMap);

        if (unsatisfiable) {
            satisfiable = false;
            return null;
        }
        compoundPredicate =
            makeCompoundPredicate(compoundGroupMap, cube);
        if (compoundPredicate != null) {
            for (BitKey bitKey : compoundGroupMap.keySet()) {
                compoundBitKey = compoundBitKey.or(bitKey);
            }
        }
        return  Pair.of(compoundBitKey, compoundPredicate);
    }

    /**
     * Groups members (or tuples) from the same compound (i.e. hierarchy) into
     * groups that are constrained by the same set of columns.
     *
     * <p>E.g.
     *
     * <pre>Members
     *     [USA].[CA],
     *     [Canada].[BC],
     *     [USA].[CA].[San Francisco],
     *     [USA].[OR].[Portland]</pre>
     *
     * will be grouped into
     *
     * <pre>Group 1:
     *     {[USA].[CA], [Canada].[BC]}
     * Group 2:
     *     {[USA].[CA].[San Francisco], [USA].[OR].[Portland]}</pre>
     *
     * <p>This helps with generating optimal form of sql.
     *
     * <p>In case of aggregating over a list of tuples, similar logic also
     * applies.
     *
     * <p>For example:
     *
     * <pre>Tuples:
     *     ([Gender].[M], [Store].[USA].[CA])
     *     ([Gender].[F], [Store].[USA].[CA])
     *     ([Gender].[M], [Store].[USA])
     *     ([Gender].[F], [Store].[Canada])</pre>
     *
     * will be grouped into
     *
     * <pre>Group 1:
     *     {([Gender].[M], [Store].[USA].[CA]),
     *      ([Gender].[F], [Store].[USA].[CA])}
     * Group 2:
     *     {([Gender].[M], [Store].[USA]),
     *      ([Gender].[F], [Store].[Canada])}</pre>
     *
     * <p>This function returns a boolean value indicating if any constraint
     * can be created from the aggregationList. It is possible that only part
     * of the aggregationList can be applied, which still leads to a (partial)
     * constraint that is represented by the compoundGroupMap.
     */
    private boolean makeCompoundGroup(
        int starColumnCount,
        RolapCube baseCube,
        List<List<Member>> aggregationList,
        Map<BitKey, List<RolapCubeMember[]>> compoundGroupMap)
    {
        // The more generalized aggregation as aggregating over tuples.
        // The special case is a tuple defined by only one member.
        int unsatisfiableTupleCount = 0;
        for (List<Member> aggregation : aggregationList) {
            if (!(aggregation.size() > 0
                && (aggregation.get(0) instanceof RolapCubeMember
                || aggregation.get(0) instanceof
                VisualTotalsFunDef.VisualTotalMember)))
            {
                ++unsatisfiableTupleCount;
                continue;
            }

            BitKey bitKey = BitKey.Factory.makeBitKey(starColumnCount);
            RolapCubeMember[] tuple;

            tuple = new RolapCubeMember[aggregation.size()];
            int i = 0;
            for (Member member : aggregation) {
                if (member instanceof VisualTotalsFunDef.VisualTotalMember) {
                    tuple[i] = (RolapCubeMember)
                        ((VisualTotalsFunDef.VisualTotalMember) member)
                            .getMember();
                } else {
                    tuple[i] = (RolapCubeMember)member;
                }
                i++;
            }

            boolean tupleUnsatisfiable = false;
            for (RolapCubeMember member : tuple) {
                // Tuple cannot be constrained if any of the member cannot be.
                tupleUnsatisfiable =
                    makeCompoundGroupForMember(member, baseCube, bitKey);
                if (tupleUnsatisfiable) {
                    // If this tuple is unsatisfiable, skip it and try to
                    // constrain the next tuple.
                    unsatisfiableTupleCount ++;
                    break;
                }
            }

            if (!tupleUnsatisfiable && !bitKey.isEmpty()) {
                // Found tuple(columns) to constrain,
                // now add it to the compoundGroupMap
                addTupleToCompoundGroupMap(tuple, bitKey, compoundGroupMap);
            }
        }
        return (unsatisfiableTupleCount == aggregationList.size());
    }

    private void addTupleToCompoundGroupMap(
        RolapCubeMember[] tuple,
        BitKey bitKey,
        Map<BitKey, List<RolapCubeMember[]>> compoundGroupMap)
    {
        List<RolapCubeMember[]> compoundGroup = compoundGroupMap.get(bitKey);
        if (compoundGroup == null) {
            compoundGroup = new ArrayList<RolapCubeMember[]>();
            compoundGroupMap.put(bitKey, compoundGroup);
        }
        compoundGroup.add(tuple);
    }

    private boolean makeCompoundGroupForMember(
        RolapCubeMember member,
        RolapCube baseCube,
        BitKey bitKey)
    {
        RolapCubeMember levelMember = member;
        boolean memberUnsatisfiable = false;
        while (levelMember != null) {
            RolapCubeLevel level = levelMember.getLevel();
            // Only need to constrain the nonAll levels
            if (!level.isAll()) {
                RolapStar.Column column = level.getBaseStarKeyColumn(baseCube);
                if (column != null) {
                    bitKey.set(column.getBitPosition());
                } else {
                    // One level in a member causes the member to be
                    // unsatisfiable.
                    memberUnsatisfiable = true;
                    break;
                }
            }

            levelMember = levelMember.getParentMember();
        }
        return memberUnsatisfiable;
    }

    private StarPredicate makeCompoundPredicate(
        Map<BitKey, List<RolapCubeMember[]>> compoundGroupMap,
        RolapCube baseCube)
    {
        List<StarPredicate> compoundPredicateList =
            new ArrayList<StarPredicate> ();
        for (List<RolapCubeMember[]> group : compoundGroupMap.values()) {
            // e.g {[USA].[CA], [Canada].[BC]}
            StarPredicate compoundGroupPredicate = null;
            for (RolapCubeMember[] tuple : group) {
                // [USA].[CA]
                StarPredicate tuplePredicate = null;

                for (RolapCubeMember member : tuple) {
                    tuplePredicate = makeCompoundPredicateForMember(
                        member, baseCube, tuplePredicate);
                }
                if (tuplePredicate != null) {
                    if (compoundGroupPredicate == null) {
                        compoundGroupPredicate = tuplePredicate;
                    } else {
                        compoundGroupPredicate =
                            compoundGroupPredicate.or(tuplePredicate);
                    }
                }
            }

            if (compoundGroupPredicate != null) {
                // Sometimes the compound member list does not constrain any
                // columns; for example, if only AllLevel is present.
                compoundPredicateList.add(compoundGroupPredicate);
            }
        }

        StarPredicate compoundPredicate = null;

        if (compoundPredicateList.size() > 1) {
            compoundPredicate = new OrPredicate(compoundPredicateList);
        } else if (compoundPredicateList.size() == 1) {
            compoundPredicate = compoundPredicateList.get(0);
        }

        return compoundPredicate;
    }

    private StarPredicate makeCompoundPredicateForMember(
        RolapCubeMember member,
        RolapCube baseCube,
        StarPredicate memberPredicate)
    {
        while (member != null) {
            RolapCubeLevel level = member.getLevel();
            if (!level.isAll()) {
                RolapStar.Column column = level.getBaseStarKeyColumn(baseCube);
                if (memberPredicate == null) {
                    memberPredicate =
                        new ValueColumnPredicate(column, member.getKey());
                } else {
                    memberPredicate =
                        memberPredicate.and(
                            new ValueColumnPredicate(column, member.getKey()));
                }
            }
            // Don't need to constrain USA if CA is unique
            if (member.getLevel().isUnique()) {
                break;
            }
            member = member.getParentMember();
        }
        return memberPredicate;
    }
}

// End CompoundPredicateInfo.java
