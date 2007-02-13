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

import java.util.Collection;

/**
 * Refinement of {@link StarPredicate} which constrains precisely one column.
 *
 * <p>The convenience methods
 *
 * @author jhyde
 * @version $Id$
 * @since Jan 15, 2007
 */
public interface StarColumnPredicate extends StarPredicate {
    /**
     * Adds the values in this constraint to a collection.
     */
    public abstract void values(Collection collection);

    /**
     * Returns whether this constraint would return <code>true</code> for a
     * given value.
     */
    public abstract boolean evaluate(Object value);

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
     * @param predicate
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
