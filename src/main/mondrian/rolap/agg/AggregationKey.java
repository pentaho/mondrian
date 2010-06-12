/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import java.util.*;

import mondrian.olap.Util;
import mondrian.rolap.*;

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
 * @version $Id$
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

    /*
    * This map must be deternimistic; otherwise different runs generate SQL
    * statements in different orders.
    *
     * TODO: change this to SortedMap to speed up comparison.
     */
    private final Map<BitKey, StarPredicate> compoundPredicateMap;

    private int hashCode;

    /**
     * Creates an AggregationKey.
     *
     * @param request Cell request
     */
    public AggregationKey(CellRequest request) {
        this.constrainedColumnsBitKey = request.getConstrainedColumnsBitKey();
        this.star = request.getMeasure().getStar();
        this.compoundPredicateMap = request.getCompoundPredicateMap();
    }

    public int computeHashCode() {
        int retCode = constrainedColumnsBitKey.hashCode();
        retCode = Util.hash(retCode, star);
        if (compoundPredicateMap != null) {
            for (BitKey bitKey : compoundPredicateMap.keySet()) {
                retCode = Util.hash(retCode, bitKey.hashCode());
            }
        }
        return retCode;
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
            && equal(compoundPredicateMap, that.compoundPredicateMap);
    }

    /**
     * Two compound predicates are equal.
     *
     * @param map1 First compound predicate map
     * @param map2 Second compound predicate map
     * @return Whether compound predicate maps are equal
     */
    private static boolean equal(
        final Map<BitKey, StarPredicate> map1,
        final Map<BitKey, StarPredicate> map2)
    {
        if (map1 == null) {
            return map2 == null;
        }
        if (map2 == null) {
            return false;
        }
        if (map1.size() != map2.size()) {
            return false;
        }
        for (BitKey bitKey : map1.keySet()) {
            StarPredicate thisPred = map1.get(bitKey);
            StarPredicate otherPred = map2.get(bitKey);
            if (thisPred == null
                || otherPred == null
                || !thisPred.equalConstraint(otherPred))
            {
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
            + (compoundPredicateMap == null
                ? "{}"
                : compoundPredicateMap.toString());
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
     * @return list of predicates
     */
    public List<StarPredicate> getCompoundPredicateList() {
        if (compoundPredicateMap == null) {
            return Collections.emptyList();
        }
        return new ArrayList<StarPredicate>(compoundPredicateMap.values());
    }

}

// End AggregationKey.java
