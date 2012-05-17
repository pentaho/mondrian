/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.rolap.sql.SqlQuery;

import java.util.*;

/**
 * Column context that an Aggregation is computed for.
 *
 * <p>Column context has two components:</p>
 * <ul>
 * <li>The column constraints which define the dimentionality of an
 *   Aggregation</li>
 * <li>An orthogonal context for which the measures are defined. This context
 *   is sometimes referred to as the compound member predicates, and usually of
 *   the shape:
 *      <blockquote>OR(AND(column predicates))</blockquote></li>
 * </ul>
 *
 * <p>Any column is only used in either column context or compound context, not
 * both.</p>
 *
 * @author Rushan Chen
 */
public class AggregationKey
{
    /**
     * This is needed because for a Virtual Cube: two CellRequests
     * could have the same BitKey but have different underlying
     * base cubes. Without this, one get the result in the
     * SegmentArrayQuerySpec addMeasure Util.assertTrue being
     * triggered (which is what happened).
     */
    private final RolapStar star;

    private final BitKey constrainedColumnsBitKey;

    /**
     * List of StarPredicate (representing the predicate
     * defining the compound member).
     *
     * <p>In sorted order of BitKey. This ensures that the map is deterministic
     * (otherwise different runs generate SQL statements in different orders),
     * and speeds up comparison.
     */
    final List<StarPredicate> compoundPredicateList;

    private int hashCode;

    /**
     * Creates an AggregationKey.
     */
    public AggregationKey(
        BitKey constrainedColumnsBitKey,
        RolapStar star,
        List<StarPredicate> compoundPredicateList)
    {
        this.constrainedColumnsBitKey = constrainedColumnsBitKey;
        this.star = star;
        this.compoundPredicateList = compoundPredicateList;
    }

    /**
     * Creates an AggregationKey from a cell request.
     *
     * @param request Cell request
     */
    public static AggregationKey create(CellRequest request) {
        Map<BitKey, StarPredicate> compoundPredicateMap =
            request.getCompoundPredicateMap();
        return new AggregationKey(
            request.getConstrainedColumnsBitKey(),
            request.getMeasure().getStar(),
            compoundPredicateMap == null
                ? Collections.<StarPredicate>emptyList()
                : new ArrayList<StarPredicate>(compoundPredicateMap.values()));
    }

    public final int computeHashCode() {
        return computeHashCode(
            constrainedColumnsBitKey,
            star,
            compoundPredicateList == null
                ? null
                : new AbstractList<BitKey>() {
                    public BitKey get(int index) {
                        return compoundPredicateList.get(index)
                            .getConstrainedColumnBitKey();
                    }

                    public int size() {
                        return compoundPredicateList.size();
                    }
                });
    }

    public static int computeHashCode(
        BitKey constrainedColumnsBitKey,
        RolapStar star,
        Collection<BitKey> compoundPredicateBitKeys)
    {
        int retCode = constrainedColumnsBitKey.hashCode();
        retCode = Util.hash(retCode, star);
        return Util.hash(retCode, compoundPredicateBitKeys);
    }

    public int hashCode() {
        if (hashCode == 0) {
            // Compute hash code on first use. It is expensive to compute, and
            // not always required.
            hashCode = computeHashCode();
        }
        return hashCode;
    }

    public boolean equals(Object other) {
        if (!(other instanceof AggregationKey)) {
            return false;
        }
        final AggregationKey that = (AggregationKey) other;
        return constrainedColumnsBitKey.equals(that.constrainedColumnsBitKey)
            && star.equals(that.star)
            && equal(compoundPredicateList, that.compoundPredicateList);
    }

    /**
     * Returns whether two lists of compound predicates are equal.
     *
     * @param list1 First compound predicate map
     * @param list2 Second compound predicate map
     * @return Whether compound predicate maps are equal
     */
    static boolean equal(
        final List<StarPredicate> list1,
        final List<StarPredicate> list2)
    {
        if (list1 == null) {
            return list2 == null;
        }
        if (list2 == null) {
            return false;
        }
        final int size = list1.size();
        if (size != list2.size()) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            StarPredicate pred1 = list1.get(i);
            StarPredicate pred2 = list2.get(i);
            if (!pred1.equalConstraint(pred2)) {
                return false;
            }
        }
        return true;
    }

    public String toString() {
        return
            star.getFactTable().getTableName()
            + " " + constrainedColumnsBitKey.toString()
            + "\n"
            + (compoundPredicateList == null
                ? "{}"
                : compoundPredicateList.toString());
    }

    /**
     * Returns the bitkey of columns that constrain this aggregation.
     *
     * @return Bitkey of contraining columns
     */
    public final BitKey getConstrainedColumnsBitKey() {
        return constrainedColumnsBitKey;
    }

    /**
     * Returns the star.
     *
     * @return Star
     */
    public final RolapStar getStar() {
        return star;
    }

    /**
     * Returns the list of compound predicates.
     *
     * @return list of predicates
     */
    public List<StarPredicate> getCompoundPredicateList() {
        return compoundPredicateList;
    }

    /**
     * Returns a list of compound predicates, expressed as SQL strings.
     *
     * @param star Star
     * @param compoundPredicateList Predicate list
     * @return list of predicate strings
     */
    public static List<String> getCompoundPredicateStringList(
        RolapStar star,
        List<StarPredicate> compoundPredicateList)
    {
        if (compoundPredicateList.isEmpty()) {
            return Collections.emptyList();
        }
        final List<String> cp = new ArrayList<String>();
        final StringBuilder buf = new StringBuilder();
        for (StarPredicate compoundPredicate : compoundPredicateList) {
            buf.setLength(0);
            compoundPredicate.toSql(star.getSqlQueryDialect(), buf);
            cp.add(buf.toString());
        }
        return cp;
    }
}

// End AggregationKey.java
