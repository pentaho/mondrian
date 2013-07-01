/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.rolap.*;

import java.util.*;

/**
 * A <code>StarPredicate</code> which evaluates to true if its
 * first child evaluates to true and its second child evaluates to false.
 *
 * @author jhyde
 * @since Nov 6, 2006
 */
public class MinusStarPredicate extends AbstractColumnPredicate {
    private final StarColumnPredicate plus;
    private final StarColumnPredicate minus;

    /**
     * Creates a MinusStarPredicate.
     *
     * @param plus Positive predicate
     * @param minus Negative predicate
     */
    public MinusStarPredicate(
        StarColumnPredicate plus,
        StarColumnPredicate minus)
    {
        super(plus.getColumn());
        assert minus != null;
        this.plus = plus;
        this.minus = minus;
    }

    public boolean equals(Object obj) {
        if (obj instanceof MinusStarPredicate) {
            MinusStarPredicate that = (MinusStarPredicate) obj;
            return this.plus.equals(that.plus)
                && this.minus.equals(that.minus);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return plus.hashCode() * 31
            + minus.hashCode();
    }

    public void values(Collection<Object> collection) {
        Set<Object> plusValues = new HashSet<Object>();
        plus.values(plusValues);
        List<Object> minusValues = new ArrayList<Object>();
        minus.values(minusValues);
        plusValues.removeAll(minusValues);
        collection.addAll(plusValues);
    }

    public boolean evaluate(Object value) {
        return plus.evaluate(value)
            && !minus.evaluate(value);
    }

    public void describe(StringBuilder buf) {
        buf.append("(").append(plus).append(" - ").append(minus).append(")");
    }

    public Overlap intersect(StarColumnPredicate predicate) {
        throw new UnsupportedOperationException();
    }

    public boolean mightIntersect(StarPredicate other) {
        // Approximately, this constraint might intersect if it intersects
        // with the 'plus' side. It's possible that the 'minus' side might
        // wipe out all of those intersections, but we don't consider that.
        return plus.mightIntersect(other);
    }

    public StarColumnPredicate minus(StarPredicate predicate) {
        assert predicate != null;
        if (predicate instanceof ValueColumnPredicate) {
            ValueColumnPredicate valuePredicate =
                (ValueColumnPredicate) predicate;
            if (!evaluate(valuePredicate.getValue())) {
                // Case 3: 'minus' is a list, 'constraint' is a value
                // which is not matched by this
                return this;
            }
        }
        if (minus instanceof ListColumnPredicate) {
            ListColumnPredicate minusList = (ListColumnPredicate) minus;
            PredicateColumn column = plus.getColumn();
            if (predicate instanceof ListColumnPredicate) {
                // Case 1: 'minus' and 'constraint' are both lists.
                ListColumnPredicate list =
                    (ListColumnPredicate) predicate;
                List<StarColumnPredicate> unionList =
                    new ArrayList<StarColumnPredicate>();
                unionList.addAll(minusList.getPredicates());
                unionList.addAll(list.getPredicates());
                return new MinusStarPredicate(
                    plus,
                    new ListColumnPredicate(column, unionList));
            }
            if (predicate instanceof ValueColumnPredicate) {
                ValueColumnPredicate valuePredicate =
                    (ValueColumnPredicate) predicate;
                if (!evaluate(valuePredicate.getValue())) {
                    // Case 3: 'minus' is a list, 'constraint' is a value
                    // which is not matched by this
                    return this;
                }
                // Case 2: 'minus' is a list, 'constraint' is a value.
                List<StarColumnPredicate> unionList =
                    new ArrayList<StarColumnPredicate>();
                unionList.addAll(minusList.getPredicates());
                unionList.add(
                    new ValueColumnPredicate(
                        column, valuePredicate.getValue()));
                return new MinusStarPredicate(
                    plus,
                    new ListColumnPredicate(column, unionList));
            }
        }
        return new MinusStarPredicate(
            this,
            (StarColumnPredicate) predicate);
    }

}

// End MinusStarPredicate.java
