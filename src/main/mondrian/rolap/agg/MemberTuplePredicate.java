/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.spi.Dialect;

import java.util.*;

/**
 * Predicate which constrains a column to a particular member, or a range
 * above or below a member, or a range between two members.
 *
 * @author jhyde
 * @version $Id$
 */
public class MemberTuplePredicate implements StarPredicate {
    private final List<Interval> intervals;
    private final List<RolapSchema.PhysColumn> columnList;
    private final BitKey columnBitKey;

    /**
     * Creates a MemberTuplePredicate.
     *
     * @param physSchema Physical Schema
     * @param columnList List of constrained columns
     * @param intervals Upper/lower bounds of this predicate
     */
    MemberTuplePredicate(
        RolapSchema.PhysSchema physSchema,
        List<RolapSchema.PhysColumn> columnList,
        List<Interval> intervals)
    {
        this.columnBitKey =
            BitKey.Factory.makeBitKey(physSchema.getColumnCount());
        this.columnList = columnList;
        this.intervals = intervals;
    }

    @Override
    public String toString() {
        return columnList + " " + intervals;
    }

    public int hashCode() {
        int h = columnBitKey.hashCode();
        return Util.hash(h, intervals);
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof MemberTuplePredicate)) {
            return false;
        }
        MemberTuplePredicate that =
            (MemberTuplePredicate) obj;
        return this.columnBitKey.equals(that.columnBitKey)
            && this.columnList.equals(that.columnList)
            && this.intervals.equals(that.intervals);
    }

    public List<RolapStar.Column> getStarColumnList(RolapStar star) {
        return Predicates.starify(star, columnList);
    }

    public List<RolapSchema.PhysColumn> getColumnList() {
        return columnList;
    }

    public BitKey getConstrainedColumnBitKey() {
        return columnBitKey;
    }

    public boolean equalConstraint(StarPredicate that) {
        throw new UnsupportedOperationException();
    }

    public StarPredicate minus(StarPredicate predicate) {
        throw new UnsupportedOperationException();
    }

    public StarPredicate or(StarPredicate predicate) {
        throw new UnsupportedOperationException();
    }

    public StarPredicate and(StarPredicate predicate) {
        throw new UnsupportedOperationException();
    }

    /**
     * Evaluates a constraint against a list of values.
     *
     * @param valueList List of values, one for each constrained column
     * @return Whether constraint holds for given set of values
     */
    public boolean evaluate(List<Object> valueList) {
        for (Interval interval : intervals) {
            if (interval.evaluate(valueList)) {
                return true;
            }
        }
        return false;
    }

    public void describe(StringBuilder buf) {
        int k = 0;
        for (Interval interval : intervals) {
            if (k++ > 0) {
                buf.append(" OR ");
            }
            interval.describe(buf);
        }
    }

    public void toSql(Dialect dialect, StringBuilder buf) {
        for (int i = 0; i < intervals.size(); i++) {
            Interval interval = intervals.get(i);
            if (i > 0) {
                buf.append(" or ");
            }
            interval.toSql(dialect, columnList, buf);
        }
    }

    static Interval createRange(
        RolapMember lower,
        boolean lowerStrict,
        RolapMember upper,
        boolean upperStrict)
    {
        if (lower == null) {
            assert upper != null;
            return new Interval(
                new Bound(
                    upper,
                    upperStrict
                        ? RelOp.LT
                        : RelOp.LE));
        } else if (upper == null) {
            return new Interval(
                new Bound(
                    lower,
                    lowerStrict
                        ? RelOp.GT
                        : RelOp.GE));
        } else {
            return new Interval(
                new Bound(
                    lower,
                    lowerStrict
                        ? RelOp.GT
                        : RelOp.GE),
                new Bound(
                    upper,
                    upperStrict
                        ? RelOp.LT
                        : RelOp.LE));
        }
    }

    static Interval createPoint(RolapMember member) {
        return new Interval(new Bound(member, RelOp.EQ));
    }

    static List<Interval> createList(List<RolapMember> members) {
        List<Interval> list = new ArrayList<Interval>(members.size());
        for (RolapMember member : members) {
            list.add(createPoint(member));
        }
        return list;
    }

    private enum RelOp {
        LT("<"),
        LE("<="),
        GT(">"),
        GE(">="),
        EQ("=");

        private final String op;
        private final List<RelOp> singletonList;

        RelOp(String op) {
            this.op = op;
            this.singletonList = Collections.singletonList(this);
        }

        String getOp() {
            return op;
        }

        /**
         * If this is a strict operator (LT, GT) returns the non-strict
         * equivalent (LE, GE); otherwise returns this operator.
         *
         * @return less strict version of this operator
         */
        public RelOp destrict() {
            switch (this) {
            case GT:
                return RelOp.GE;
            case LT:
                return RelOp.LE;
            default:
                return this;
            }
        }

        /**
         * Returns a list of a given size, with entries {uop, uop, ... , op}.
         * Where 'op' is this operator, and 'uop' is the non-strict form of this
         * operator.
         *
         * @param size Number of entries
         * @return List of operators
         */
        public List<RelOp> list(final int size) {
            if (size == 1) {
                return singletonList;
            }
            switch (this) {
            case EQ:
            case LT:
            case GT:
                return Collections.nCopies(size, this);
            }
            return new AbstractList<RelOp>() {
                public RelOp get(int index) {
                    return index == size - 1 ? RelOp.this : destrict();
                }

                public int size() {
                    return size;
                }
            };
        }
    }

    private static class Bound {
        private final RolapMember member;
        private final List<Object> values;
        private final List<RelOp> relOps;

        Bound(RolapMember member, RelOp relOp) {
            this.member = member;
            this.values = member.getKeyAsList();
            this.relOps = relOp.list(values.size());
        }


        public int hashCode() {
            return Util.hashV(0, member, values, relOps);
        }

        public boolean equals(Object obj) {
            if (obj instanceof Bound) {
                Bound that = (Bound) obj;
                return this.member.equals(that.member)
                    && this.values.equals(that.values)
                    && this.relOps.equals(that.relOps);
            } else {
                return false;
            }
        }

        public void describe(StringBuilder buf) {
            buf.append(relOps.get(relOps.size() - 1).getOp());
            buf.append(' ');
            buf.append(member);
        }

        public void toSql(
            Dialect dialect,
            List<RolapSchema.PhysColumn> columns,
            StringBuilder buf)
        {
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) {
                    buf.append(" and ");
                }
                toSql(
                    dialect, buf, relOps.get(i).getOp(), values.get(i),
                    columns.get(i));
            }
        }

        private void toSql(
            Dialect dialect,
            StringBuilder buf,
            String relOp,
            Object value,
            RolapSchema.PhysColumn column)
        {
            buf.append(column.toSql());
            buf.append(' ');
            buf.append(relOp);
            buf.append(' ');
            dialect.quote(buf, value, column.getDatatype());
        }
    }

    private static class Interval {
        protected final Bound[] bounds;

        Interval(Bound... bounds) {
            this.bounds = bounds;
        }

        boolean evaluate(List<Object> valueList) {
            for (Bound bound : bounds) {
                for (int k = 0; k < bound.values.size(); ++k) {
                    Object value = valueList.get(k);
                    if (value == WILDCARD) {
                        return false;
                    }
                    Object boundValue = bound.values.get(k);
                    RelOp relOp = bound.relOps.get(k);
                    int c = Util.compareKey(value, boundValue);
                    switch (relOp) {
                    case GT:
                        if (c > 0) {
                            break;
                        } else {
                            return false;
                        }
                    case GE:
                        if (c > 0) {
                            return true;
                        } else if (c == 0) {
                            break;
                        } else {
                            return false;
                        }
                    case LT:
                        if (c < 0) {
                            break;
                        } else {
                            return false;
                        }
                    case LE:
                        if (c < 0) {
                            return true;
                        } else if (c == 0) {
                            break;
                        } else {
                            return false;
                        }
                    case EQ:
                        if (c != 0) {
                            return false;
                        } else {
                            break;
                        }
                    }
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bounds);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj
                   || obj instanceof Interval
                      && Arrays.equals(this.bounds, ((Interval) obj).bounds);
        }

        @Override
        public String toString() {
            return Arrays.toString(bounds);
        }

        public void describe(StringBuilder buf) {
            int k = 0;
            for (Bound bound : bounds) {
                if (k++ > 0) {
                    buf.append(" AND ");
                }
                bound.describe(buf);
            }
        }

        public void toSql(
            Dialect dialect,
            List<RolapSchema.PhysColumn> columnList,
            StringBuilder buf)
        {
            for (int i = 0; i < bounds.length; i++) {
                Bound bound = bounds[i];
                if (i > 0) {
                    buf.append(" and ");
                }
                bound.toSql(dialect, columnList, buf);
            }
        }
    }
}

// End MemberTuplePredicate.java
