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
import mondrian.spi.Dialect;

import java.util.Collections;
import java.util.List;

/**
 * A constraint which always returns true or false.
 *
 * @see Predicates#wildcard(PredicateColumn, boolean)
 *
 * @author jhyde
 * @since Nov 2, 2006
 */
public class LiteralStarPredicate implements StarPredicate {
    private final boolean value;

    public static final LiteralStarPredicate TRUE =
        new LiteralStarPredicate(true);
    public static final LiteralStarPredicate FALSE =
        new LiteralStarPredicate(false);

    /**
     * Creates a LiteralStarPredicate.
     *
     * @param value Truth value
     */
    public LiteralStarPredicate(boolean value) {
        this.value = value;
    }

    public int hashCode() {
        return value ? 2 : 1;
    }

    public boolean equals(Object obj) {
        return this == obj
            || (obj instanceof LiteralStarPredicate)
               && this.value == ((LiteralStarPredicate) obj).value;
    }

    public List<RolapSchema.PhysRouter> getRouters() {
        return Collections.emptyList();
    }

    public boolean evaluate(List<Object> valueList) {
        assert valueList.isEmpty();
        return value;
    }

    public boolean equalConstraint(StarPredicate that) {
        throw new UnsupportedOperationException();
    }

    public String toString() {
        return Boolean.toString(value);
    }

    public void describe(StringBuilder buf) {
        buf.append("=any");
    }

    public List<PredicateColumn> getColumnList() {
        return Collections.emptyList();
    }

    public BitKey getConstrainedColumnBitKey() {
        return BitKey.EMPTY;
    }

    public StarPredicate or(StarPredicate predicate) {
        return value
            ? this
            : predicate;
    }

    public StarPredicate and(StarPredicate predicate) {
        return value
            ? predicate
            : this;
    }

    public StarPredicate minus(StarPredicate predicate) {
        assert predicate != null;
        if (value) {
            // TODO: implement this. We'd need a variant of MinusStarPredicate
            // that does not require a StarColumnPredicate as argument.
            throw new UnsupportedOperationException();
        } else {
            // "false - constraint" is "false"
            return this;
        }
    }

    public boolean getValue() {
        return value;
    }

    public void toSql(Dialect dialect, StringBuilder buf) {
        // e.g. "true"
        buf.append(value);
    }
}

// End LiteralStarPredicate.java
