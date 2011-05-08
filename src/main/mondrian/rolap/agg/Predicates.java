/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.rolap.*;
import mondrian.spi.Dialect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility methods relating to {@link mondrian.rolap.StarPredicate}
 * and sub-classes.
 *
 * @author jhyde
 * @version $Id$
 * @since May 5, 2011
 */
public abstract class Predicates
{
    public static StarPredicate memberPredicate(RolapMember member)
    {
        List<RolapSchema.PhysColumn> keyList =
            member.getLevel().getAttribute().keyList;
        int size = keyList.size();
        switch (size) {
        case 1:
            return new MemberColumnPredicate(
                keyList.get(0),
                member);
        default:
            RolapSchema.PhysSchema schema = keyList.get(0).relation.getSchema();
            return new MemberTuplePredicate(
                schema,
                keyList,
                Collections.singletonList(
                    MemberTuplePredicate.createPoint(member)));
        }
    }

    public static RangeColumnPredicate rangePredicate(
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
        final RolapLevel level =
            (lowerBound == null ? upperBound : lowerBound).getLevel();
        if (level.getAttribute().keyList.size() == 1) {
            return new RangeColumnPredicate(
                level.getAttribute().keyList.get(0),
                lowerInclusive,
                (lowerBound == null
                 ? null
                 : (ValueColumnPredicate) memberPredicate(lowerBound)),
                upperInclusive,
                (upperBound == null
                 ? null
                 : (ValueColumnPredicate) memberPredicate(upperBound)));
        } else {
            throw new UnsupportedOperationException("TODO:");
        }
    }

    public static BitKey getBitKey(StarPredicate predicate, RolapStar star) {
        BitKey bitKey = BitKey.Factory.makeBitKey(star.getColumnCount());
        for (RolapStar.Column column : predicate.getStarColumnList(star)) {
            bitKey.set(column.getBitPosition());
        }
        return bitKey;
    }

    public static List<RolapStar.Column> starify(
        RolapStar star,
        List<RolapSchema.PhysColumn> columnList)
    {
        List<RolapStar.Column> list = new ArrayList<RolapStar.Column>();
        for (RolapSchema.PhysColumn column : columnList) {
            list.add(star.getColumn(column, true));
        }
        return list;
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
        RolapMember lower,
        boolean lowerStrict,
        RolapMember upper,
        boolean upperStrict)
    {
        RolapMember member = lower != null ? lower : upper;
        return new MemberTuplePredicate(
            physSchema,
            member.getLevel().getAttribute().keyList,
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
        RolapLevel level,
        List<RolapMember> members)
    {
        return new MemberTuplePredicate(
            physSchema,
            level.getAttribute().keyList,
            MemberTuplePredicate.createList(members));
    }

    /**
     * Creates a ColumnPredicate that matches any value of the column.
     *
     * <p>Unlike {@link LiteralStarPredicate}, it references a column.
     *
     * @param column Column
     * @param value Value
     * @return Predicate that always evaluates to given value
     */
    public static StarColumnPredicate wildcard(
        RolapSchema.PhysColumn column,
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
}

// End Predicates.java
