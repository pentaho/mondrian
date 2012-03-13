/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2009 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import java.util.Collection;

/**
 * Refinement of {@link StarPredicate} which constrains precisely one column.
 *
 * @author jhyde
 * @since Jan 15, 2007
 */
public interface StarColumnPredicate extends StarPredicate {
    /**
     * Adds the values in this constraint to a collection.
     *
     * @param collection Collection to add values to
     */
    void values(Collection<Object> collection);

    /**
     * Returns whether this constraint would return <code>true</code> for a
     * given value.
     *
     * @param value Value
     * @return Whether predicate is true
     */
    boolean evaluate(Object value);

    /**
     * Returns the column constrained by this predicate.
     *
     * @return Column constrained by this predicate.
     */
    RolapStar.Column getConstrainedColumn();

    /**
     * Applies this predicate to a predicate from the axis of
     * a segment, and tests for overlap. The result might be that there
     * is no overlap, full overlap (so the constraint can be removed),
     * or partial overlap (so the constraint will need to be replaced with
     * a stronger constraint, say 'x > 10' is replaced with 'x > 20').
     *
     * @param predicate Predicate
     * @return description of overlap between predicates, if any
     */
    Overlap intersect(StarColumnPredicate predicate);

    /**
     * Returns whether this predicate might intersect another predicate.
     * That is, whether there might be a value which holds true for both
     * constraints.
     *
     * @param other Other constraint
     * @return Whether constraints intersect
     */
    boolean mightIntersect(StarPredicate other);

    // override with stricter return type
    StarColumnPredicate minus(StarPredicate predicate);

    /**
     * Returns this union of this Predicate with another.
     *
     * <p>Unlike {@link #or}, the other predicate must be on this column, and
     * the result is a column predicate.
     *
     * @param predicate Another predicate on this column
     * @return Union predicate on this column
     */
    StarColumnPredicate orColumn(StarColumnPredicate predicate);

    /**
     * This method is required because unfortunately some ColumnPredicate
     * objects are created without a column.
     *
     * <p>We call this method to provide a fake column, then call
     * {@link #toSql(mondrian.rolap.sql.SqlQuery, StringBuilder)}.
     *
     * <p>todo: remove this method when
     * {@link mondrian.util.Bug#BugMondrian313Fixed bug MONDRIAN-313} and
     * {@link mondrian.util.Bug#BugMondrian314Fixed bug MONDRIAN-314} are fixed.
     */
    StarColumnPredicate cloneWithColumn(RolapStar.Column column);

    /**
     * Returned by
     * {@link mondrian.rolap.StarColumnPredicate#intersect},
     * describes whether two predicates overlap, and if so, the remaining
     * predicate.
     */
    public static class Overlap {
        public final boolean matched;
        public final StarColumnPredicate remaining;
        public final float selectivity;

        public Overlap(
            boolean matched,
            StarColumnPredicate remaining,
            float selectivity)
        {
            this.matched = matched;
            this.remaining = remaining;
            this.selectivity = selectivity;
        }
    }
}

// End StarColumnPredicate.java
