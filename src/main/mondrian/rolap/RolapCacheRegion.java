/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Util;
import mondrian.rolap.agg.Predicates;

import java.util.*;

/**
 * A <code>RolapCacheRegion</code> represents a region of multidimensional space
 * in the cache.
 *
 * <p>The region is represented in terms of the columns of a given
 * {@link mondrian.rolap.RolapStar}, and constraints on those columns.
 *
 * <p>Compare with {@link mondrian.olap.CacheControl.CellRegion}: a
 * <code>CellRegion</code> is in terms of {@link mondrian.olap.Member} objects
 * (logical); whereas a <code>RolapCacheRegion</code> is in terms of columns
 * (physical).
 */
public class RolapCacheRegion {
    private final RolapStar star;
    private final BitKey bitKey;
    private Map<BitKey, StarPredicate> predicates =
        new HashMap<BitKey, StarPredicate>();

    public RolapCacheRegion(
        RolapStar star,
        List<RolapStar.Measure> starMeasureList)
    {
        this.star = star;
        bitKey = BitKey.Factory.makeBitKey(star.getColumnCount());
        for (RolapStar.Measure measure : starMeasureList) {
            bitKey.set(measure.getBitPosition());
        }
    }

    public BitKey getConstrainedColumnsBitKey() {
        return bitKey;
    }

    /**
     * Returns the predicate associated with the
     * <code>columnOrdinal</code>th column.
     *
     * @param columnOrdinal Column ordinal
     * @return Predicate, or null if not constrained
     */
    public StarColumnPredicate getPredicate(int columnOrdinal) {
        Util.deprecated(
            "review: what if there's more than one? what if there"
            + " are predicates that cross other columns?",
            false);
        int count = 0;
        StarColumnPredicate columnPredicate = null;
        for (Map.Entry<BitKey, StarPredicate> entry : predicates.entrySet()) {
            if (entry.getKey().get(columnOrdinal)) {
                ++count;
                if (entry.getValue() instanceof StarColumnPredicate) {
                    columnPredicate =
                        (StarColumnPredicate) entry.getValue();
                }
            }
        }
        assert count <= 1;
        return columnPredicate;
    }

    /**
     * Adds a predicate which applies to multiple columns.
     *
     * <p>The typical example of a multi-column predicate is a member
     * constraint. For example, the constraint "m between 1997.Q3 and
     * 1998.Q2" translates into "year = 1997 and quarter >= Q3 or year =
     * 1998 and quarter <= Q2".
     *
     * @param predicate Predicate
     */
    public void addPredicate(StarPredicate predicate)
    {
        BitKey predicateBitKey =
            Predicates.getBitKey(predicate, star);
        predicates.put(
            predicateBitKey,
            predicate);
        bitKey.or(predicateBitKey);
    }

    /**
     * Returns a collection of all multi-column predicates.
     *
     * @return Collection of all multi-column constraints
     */
    public Collection<StarPredicate> getPredicates() {
        return predicates.values();
    }

    /**
     * Returns the list of all column predicates.
     *
     * @return List of all column predicates
     */
    public Collection<StarColumnPredicate> getColumnPredicates() {
        final List<StarColumnPredicate> list =
                new ArrayList<StarColumnPredicate>();
        for (StarPredicate predicate : predicates.values()) {
            if (predicate instanceof StarColumnPredicate) {
                list.add((StarColumnPredicate) predicate);
            }
        }
        return list;
    }
}

// End RolapCacheRegion.java
