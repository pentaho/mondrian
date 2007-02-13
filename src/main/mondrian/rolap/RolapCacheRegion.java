/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

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
    private final BitKey bitKey;
    private final Map<Integer, StarColumnPredicate> columnPredicates =
        new HashMap<Integer, StarColumnPredicate>();
    private Map<List<RolapStar.Column>, StarPredicate> predicates =
        new HashMap<List<RolapStar.Column>, StarPredicate>();

    public RolapCacheRegion(RolapStar star, List<RolapStar.Measure> starMeasureList) {
        bitKey = BitKey.Factory.makeBitKey(star.getColumnCount());
        for (RolapStar.Measure measure : starMeasureList) {
            bitKey.set(measure.getBitPosition());
        }
    }

    public BitKey getConstrainedColumnsBitKey() {
        return bitKey;
    }

    /**
     * Adds a predicate which applies to a single column.
     *
     * @param column Constrained column
     * @param predicate Predicate
     */
    public void addPredicate(
        RolapStar.Column column,
        StarColumnPredicate predicate)
    {
        int bitPosition = column.getBitPosition();
        assert !bitKey.get(bitPosition);
        bitKey.set(bitPosition);
        columnPredicates.put(bitPosition, predicate);
    }

    /**
     * Returns the predicate associated with the
     * <code>columnOrdinal</code>th column.
     *
     * @param columnOrdinal Column ordinal
     * @return Predicate, or null if not constrained
     */
    public StarColumnPredicate getPredicate(int columnOrdinal) {
        return columnPredicates.get(columnOrdinal);
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
        final List<RolapStar.Column> columnList =
            predicate.getConstrainedColumnList();
        predicates.put(
            new ArrayList<RolapStar.Column>(columnList),
            predicate);
        for (RolapStar.Column column : columnList) {
            bitKey.set(column.getBitPosition());
        }
    }

    /**
     * Returns a collection of all multi-column predicates.
     *
     * @return Collection of all multi-column constraints
     */
    public Collection<StarPredicate> getPredicates() {
        return predicates.values();
    }
}

// End RolapCacheRegion.java
