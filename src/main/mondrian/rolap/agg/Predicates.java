/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2013 Pentaho
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.spi.Dialect;

import java.util.Collections;
import java.util.List;

/**
 * Utility methods relating to {@link mondrian.rolap.StarPredicate}
 * and sub-classes.
 *
 * @author jhyde
 * @since May 5, 2011
 */
public abstract class Predicates
{
    public static StarPredicate memberPredicate(
        RolapSchema.PhysRouter router,
        RolapMember member)
    {
        List<RolapSchema.PhysColumn> keyList =
            member.getLevel().getAttribute().getKeyList();
        int size = keyList.size();
        switch (size) {
        case 1:
            return new MemberColumnPredicate(
                new PredicateColumn(
                    router,
                    keyList.get(0)),
                member);
        default:
            RolapSchema.PhysSchema schema = keyList.get(0).relation.getSchema();
            return new MemberTuplePredicate(
                router,
                schema,
                keyList,
                Collections.singletonList(
                    MemberTuplePredicate.createPoint(member)));
        }
    }

    public static RangeColumnPredicate rangePredicate(
        RolapSchema.PhysRouter router,
        boolean lowerInclusive,
        RolapMember lowerBound,
        boolean upperInclusive,
        RolapMember upperBound)
    {
        assert lowerBound == null
            || upperBound == null
            || lowerBound.getLevel() == upperBound.getLevel();
        assert lowerBound != null
            || upperBound != null;
        final RolapCubeLevel level =
            Util.first(lowerBound, upperBound).getLevel();
        if (level.getAttribute().getKeyList().size() == 1) {
            return new RangeColumnPredicate(
                new PredicateColumn(
                    router,
                    level.getAttribute().getKeyList().get(0)),
                lowerInclusive,
                (lowerBound == null
                 ? null
                 : (ValueColumnPredicate) memberPredicate(router, lowerBound)),
                upperInclusive,
                (upperBound == null
                 ? null
                 : (ValueColumnPredicate) memberPredicate(router, upperBound)));
        } else {
            throw new UnsupportedOperationException("TODO:");
        }
    }

    public static BitKey getBitKey(StarPredicate predicate, RolapStar star) {
        Util.deprecated("not used", true);
        BitKey bitKey = BitKey.Factory.makeBitKey(star.getColumnCount());
        for (PredicateColumn predicateColumn : predicate.getColumnList()) {
            final RolapSchema.PhysPath path =
                predicateColumn.router.path(predicateColumn.physColumn);
            final RolapStar.Table table = star.lookupTable(path);
            final RolapStar.Column column =
                table.lookupColumnByExpression(
                    predicateColumn.physColumn, false, null, null);
            bitKey.set(column.getBitPosition());
        }
        return bitKey;
    }

    /**
     * Creates a MemberTuplePredicate that evaluates to true for a given
     * range of members.
     *
     * <p>The range can be open above or below, but at least one bound is
     * required.
     *
     * @param physSchema Measure group
     * @param lower Member which forms the lower bound, or null if range is
     *   open below
     * @param lowerStrict Whether lower bound of range is strict
     * @param upper Member which forms the upper bound, or null if range is
     *   open above
     * @param upperStrict Whether upper bound of range is strict
     */
    public static MemberTuplePredicate range(
        RolapSchema.PhysSchema physSchema,
        RolapSchema.PhysRouter router,
        RolapMember lower,
        boolean lowerStrict,
        RolapMember upper,
        boolean upperStrict)
    {
        RolapMember member = lower != null ? lower : upper;
        return new MemberTuplePredicate(
            router,
            physSchema,
            member.getLevel().getAttribute().getKeyList(),
            Collections.singletonList(
                MemberTuplePredicate.createRange(
                    lower, lowerStrict, upper, upperStrict)));
    }

    /**
     * Creates a MemberTuplePredicate that evaluates to true for a given
     * list of members.
     *
     * @param physSchema Physical schema
     * @param members List of members
     */
    public static MemberTuplePredicate list(
        RolapSchema.PhysSchema physSchema,
        RolapSchema.PhysRouter router,
        RolapLevel level,
        List<RolapMember> members)
    {
        assert members.size() > 0;
        return new MemberTuplePredicate(
            router,
            physSchema,
            level.getAttribute().getKeyList(),
            MemberTuplePredicate.createList(members));
    }

    /**
     * Creates a ColumnPredicate that matches any value of the column.
     *
     * <p>Unlike {@link LiteralStarPredicate}, it references a column and has
     * a route to the fact table.
     *
     * @param column Column
     * @param value Value
     * @return Predicate that always evaluates to given value
     */
    public static StarColumnPredicate wildcard(
        PredicateColumn column,
        boolean value)
    {
        return new LiteralColumnPredicate(column, value);
    }

    /**
     * Generates a predicate that a column matches one of a list of values.
     *
     * <p>
     * Several possible outputs, depending upon whether the there are
     * nulls:<ul>
     *
     * <li>One not-null value: <code>foo.bar = 1</code>
     *
     * <li>All values not null: <code>foo.bar in (1, 2, 3)</code></li>
     *
     * <li>Null and not null values:
     * <code>(foo.bar is null or foo.bar in (1, 2))</code></li>
     *
     * <li>Only null values:
     * <code>foo.bar is null</code></li>
     *
     * <li>String values: <code>foo.bar in ('a', 'b', 'c')</code></li>
     *
     * </ul>
     */
    public static String toSql(
        StarPredicate predicate,
        Dialect dialect)
    {
        StringBuilder buf = new StringBuilder(64);
        predicate.toSql(dialect, buf);
        return buf.toString();
    }

    public static StarPredicate and(List<StarPredicate> predicateList) {
        final int size = predicateList.size();
        switch (size) {
        case 1:
            return predicateList.get(0);
        case 0:
            return null;
        default:
            return new AndPredicate(predicateList);
        }
    }

    public static StarPredicate or(List<StarPredicate> predicateList) {
        final int size = predicateList.size();
        switch (size) {
        case 1:
            return predicateList.get(0);
        case 0:
            return null;
        default:
            return new OrPredicate(predicateList);
        }
    }

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
        PredicateColumn column,
        Comparable value)
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
        PredicateColumn column,
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

// End Predicates.java
