/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.rolap.*;
import mondrian.olap.Util;
import mondrian.spi.Dialect;

import java.util.List;
import java.util.Collections;
import java.util.ArrayList;

/**
 * A <code>AbstractColumnPredicate</code> is an abstract implementation for
 * {@link mondrian.rolap.StarColumnPredicate}.
 *
 * @version $Id$
 */
public abstract class AbstractColumnPredicate implements StarColumnPredicate {
    protected final RolapSchema.PhysColumn constrainedColumn;
    private BitKey constrainedColumnBitKey;

    /**
     * Creates an AbstractColumnPredicate.
     *
     * @param constrainedColumn Constrained column
     */
    protected AbstractColumnPredicate(RolapSchema.PhysColumn constrainedColumn)
    {
        this.constrainedColumn = constrainedColumn;
        assert constrainedColumn != null;
    }

    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append(constrainedColumn.toSql());
        describe(buf);
        return buf.toString();
    }

    public RolapSchema.PhysColumn getColumn() {
        return constrainedColumn;
    }

    public List<RolapSchema.PhysColumn> getColumnList() {
        return Collections.singletonList(constrainedColumn);
    }

    public List<RolapStar.Column> getStarColumnList(RolapStar star) {
        return Collections.singletonList(
            star.getColumn(constrainedColumn, true));
    }

    public BitKey getConstrainedColumnBitKey() {
        if (constrainedColumnBitKey == null) {
            constrainedColumnBitKey =
                BitKey.Factory.makeBitKey(
                    constrainedColumn.relation.getSchema().getColumnCount());
            constrainedColumnBitKey.set(constrainedColumn.ordinal());
        }
        return constrainedColumnBitKey;
    }

    public boolean evaluate(List<Object> valueList) {
        assert valueList.size() == 1;
        return evaluate(valueList.get(0));
    }

    public boolean equalConstraint(StarPredicate that) {
        return false;
    }

    public StarPredicate or(StarPredicate predicate) {
        if (predicate instanceof StarColumnPredicate) {
            StarColumnPredicate starColumnPredicate =
                (StarColumnPredicate) predicate;
            if (starColumnPredicate.getColumn()
                == getColumn())
            {
                return orColumn(starColumnPredicate);
            }
        }
        final List<StarPredicate> list = new ArrayList<StarPredicate>(2);
        list.add(this);
        list.add(predicate);
        return new OrPredicate(list);
    }

    public StarColumnPredicate orColumn(StarColumnPredicate predicate) {
        assert predicate.getColumn() == getColumn();
        if (predicate instanceof ListColumnPredicate) {
            ListColumnPredicate that = (ListColumnPredicate) predicate;
            final List<StarColumnPredicate> list =
                new ArrayList<StarColumnPredicate>();
            list.add(this);
            list.addAll(that.getPredicates());
            return new ListColumnPredicate(
                getColumn(),
                list);
        } else {
            final List<StarColumnPredicate> list =
                new ArrayList<StarColumnPredicate>(2);
            list.add(this);
            list.add(predicate);
            return new ListColumnPredicate(
                getColumn(),
                list);
        }
    }

    public StarPredicate and(StarPredicate predicate) {
        final List<StarPredicate> list = new ArrayList<StarPredicate>(2);
        list.add(this);
        list.add(predicate);
        return new AndPredicate(list);
    }

    public void toSql(Dialect dialect, StringBuilder buf) {
        throw Util.needToImplement(this);
    }

    /**
     * Factory for {@link mondrian.rolap.StarPredicate}s and
     * {@link mondrian.rolap.StarColumnPredicate}s.
     */
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
            RolapSchema.PhysColumn column,
            Object value)
        {
            return new ValueColumnPredicate(column, value);
        }

        /**
         * Returns predicate which is the OR of a list of predicates.
         *
         * @param column Column being constrained
         * @param list List of predicates
         * @return Predicate which is an OR of the list of predicates
         */
        public static StarColumnPredicate or(
            RolapSchema.PhysColumn column,
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
        public static StarColumnPredicate equal(
            ValueColumnPredicate predicate)
        {
            return equal(
                predicate.getColumn(),
                predicate.getValue());
        }
    }
}

// End AbstractColumnPredicate.java
