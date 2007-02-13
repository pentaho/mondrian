/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.RolapStar;
import mondrian.rolap.StarPredicate;

import java.util.List;
import java.util.Collections;

/**
 * A <code>AbstractColumnPredicate</code> is an abstract implementation for
 * {@link mondrian.rolap.StarColumnPredicate}.
 *
 * @version $Id$
 */
public abstract class AbstractColumnPredicate implements StarColumnPredicate {
    private final RolapStar.Column constrainedColumn;

    /**
     * Creates an AbstractColumnPredicate.
     *
     * @param constrainedColumn Constrained column
     */
    protected AbstractColumnPredicate(RolapStar.Column constrainedColumn) {
        this.constrainedColumn = constrainedColumn;
    }

    public RolapStar.Column getConstrainedColumn() {
        return constrainedColumn;
    }

    public List<RolapStar.Column> getConstrainedColumnList() {
        return Collections.singletonList(constrainedColumn);
    }

    public boolean evaluate(List<Object> valueList) {
        assert valueList.size() == 1;
        return evaluate(valueList.get(0));
    }

    public boolean equalConstraint(StarPredicate that) {
        return false;
    }


    public static class Factory {
        /**
         * Returns a predicate which tests whether the column's
         * value is equal to a given constant.
         *
         * @param column Constrained column
         * @param value Value
         * @return Predicate which tests whether the column's value is equal
         *   to a column constraint's value
         */
        public static StarColumnPredicate equal(
            RolapStar.Column column,
            Object value)
        {
            return new ValueColumnPredicate(column, value);
        }

        /**
         * Returns predicate which is the OR of a list of predicates.
         *
         * @param list List of predicates
         * @return Predicate which is an OR of the list of predicates
         */
        public static StarColumnPredicate or(
            RolapStar.Column column,
            List<StarColumnPredicate> list)
        {
            return new ListColumnPredicate(column, list);
        }

        /**
         * Returns a predicate which always evaluates to TRUE or FALSE.
         * @param b Truth value
         * @return Predicate which always evaluates to truth value
         */
        public static LiteralStarPredicate bool(boolean b) {
            return b ? LiteralStarPredicate.TRUE : LiteralStarPredicate.FALSE;
        }

        /**
         * Returns a predicate which tests whether the column's
         * value is equal to column predicate's value.
         *
         * @param predicate Column predicate
         * @return Predicate which tests whether the column's value is equal
         *   to a column predicate's value
         */
        public static StarColumnPredicate equal(ValueColumnPredicate predicate) {
            return equal(
                predicate.getConstrainedColumn(),
                predicate.getValue());
        }
    }
}

// End AbstractColumnPredicate.java
