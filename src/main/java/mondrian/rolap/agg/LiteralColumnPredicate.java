/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.rolap.RolapSchema;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.StarPredicate;
import mondrian.spi.Dialect;

import java.util.Collection;
import java.util.List;

/**
 * A column predicate that always returns true or false.
 *
 * @author jhyde
 * @since Nov 2, 2006
 */
public class LiteralColumnPredicate extends AbstractColumnPredicate {
    private final boolean value;

    /**
     * Creates a LiteralColumnPredicate.
     *
     * @param column Constrained column
     * @param value Truth value
     */
    public LiteralColumnPredicate(
        PredicateColumn column,
        boolean value)
    {
        super(column);
        this.value = value;
    }

    public int hashCode() {
        return value ? 2 : 1;
    }

    public boolean equals(Object obj) {
        return this == obj
            || (obj instanceof LiteralColumnPredicate)
               && this.constrainedColumn
                  == ((LiteralColumnPredicate) obj).constrainedColumn
               && this.value == ((LiteralColumnPredicate) obj).value;
    }

    public boolean evaluate(List<Object> valueList) {
        return value;
    }

    public boolean equalConstraint(StarPredicate that) {
        return this.equals(that)
               || that == (value
                      ? LiteralStarPredicate.TRUE
                      : LiteralStarPredicate.FALSE);
    }

    public String toString() {
        return Boolean.toString(value);
    }

    public void toSql(Dialect dialect, StringBuilder buf) {
        buf.append(value);
    }

    public void values(Collection<Object> collection) {
        collection.add(value);
    }

    public boolean evaluate(Object value) {
        return this.value;
    }

    public void describe(StringBuilder buf) {
        buf.append("=any");
    }

    public Overlap intersect(
        StarColumnPredicate predicate)
    {
        return new Overlap(value, null, 0f);
    }

    public boolean mightIntersect(StarPredicate other) {
        // FALSE intersects nothing
        // TRUE intersects everything except FALSE
        if (!value) {
            return false;
        } else if (other instanceof LiteralColumnPredicate) {
            return ((LiteralColumnPredicate) other).value;
        } else {
            return true;
        }
    }

    public StarColumnPredicate minus(StarPredicate predicate) {
        assert predicate != null;
        if (value) {
            // We have no 'not' operator, so there's no shorter way to represent
            // "true - constraint".
            return new MinusStarPredicate(
                this, (StarColumnPredicate) predicate);
        } else {
            // "false - constraint" is "false"
            return this;
        }
    }

    public boolean getValue() {
        return value;
    }
}

// End LiteralColumnPredicate.java
