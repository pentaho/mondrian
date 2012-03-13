/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.rolap.sql.SqlQuery;

import java.util.*;

/**
 * Predicate which constrains a column to a particular member, or a range
 * above or below a member, or a range between two members.
 *
 * @author jhyde
 */
public class MemberTuplePredicate implements StarPredicate {
    private final Bound[] bounds;
    private final List<RolapStar.Column> columnList;
    private BitKey columnBitKey;

    /**
     * Creates a MemberTuplePredicate which evaluates to true for a given
     * range of members.
     *
     * <p>The range can be open above or below, but at least one bound is
     * required.
     *
     * @param baseCube base cube for virtual members
     * @param lower Member which forms the lower bound, or null if range is
     *   open below
     * @param lowerStrict Whether lower bound of range is strict
     * @param upper Member which forms the upper bound, or null if range is
     *   open above
     * @param upperStrict Whether upper bound of range is strict
     */
    public MemberTuplePredicate(
        RolapCube baseCube,
        RolapMember lower,
        boolean lowerStrict,
        RolapMember upper,
        boolean upperStrict)
    {
        columnBitKey = null;
        this.columnList =
            computeColumnList(lower != null ? lower : upper, baseCube);

        if (lower == null) {
            assert upper != null;
            bounds = new Bound[] {
                new Bound(upper, upperStrict ? RelOp.LT : RelOp.LE)
            };
        } else if (upper == null) {
            bounds = new Bound[] {
                new Bound(lower, lowerStrict ? RelOp.GT : RelOp.GE)
            };
        } else {
            bounds = new Bound[] {
                new Bound(lower, lowerStrict ? RelOp.GT : RelOp.GE),
                new Bound(upper, upperStrict ? RelOp.LT : RelOp.LE)
            };
        }
    }

    /**
     * Creates a MemberTuplePredicate which evaluates to true for a given
     * member.
     *
     * @param baseCube base cube for virtual members
     * @param member Member
     */
    public MemberTuplePredicate(RolapCube baseCube, RolapCubeMember member) {
        this.columnList = computeColumnList(member, baseCube);

        this.bounds = new Bound[] {
            new Bound(member, RelOp.EQ)
        };
    }

    public int hashCode() {
        return this.columnList.hashCode() * 31
            + Arrays.hashCode(this.bounds) * 31;
    }

    public boolean equals(Object obj) {
        if (obj instanceof MemberTuplePredicate) {
            MemberTuplePredicate that =
                (MemberTuplePredicate) obj;
            return this.columnList.equals(that.columnList)
                && Arrays.equals(this.bounds, that.bounds);
        } else {
            return false;
        }
    }

    private List<RolapStar.Column> computeColumnList(
        RolapMember member,
        RolapCube baseCube)
    {
        List<RolapStar.Column> columnList = new ArrayList<RolapStar.Column>();
        while (true) {
            RolapLevel level = member.getLevel();
            RolapStar.Column column = null;
            if (level instanceof RolapCubeLevel) {
                column = ((RolapCubeLevel)level)
                                .getBaseStarKeyColumn(baseCube);
            } else {
                (new Exception()).printStackTrace();
            }

            if (columnBitKey == null) {
                columnBitKey =
                    BitKey.Factory.makeBitKey(
                        column.getStar().getColumnCount());
                columnBitKey.clear();
            }
            columnBitKey.set(column.getBitPosition());
            columnList.add(0, column);
            if (level.isUnique()) {
                return columnList;
            }
            member = member.getParentMember();
        }
    }

    /**
     * Returns a list of constrained columns.
     *
     * @return List of constrained columns
     */
    public List<RolapStar.Column> getConstrainedColumnList() {
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
        for (Bound bound : bounds) {
            for (int k = 0; k < bound.values.length; ++k) {
                Object value = valueList.get(k);
                if (value == WILDCARD) {
                    return false;
                }
                Object boundValue = bound.values[k];
                RelOp relOp = bound.relOps[k];
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
                }
            }
        }
        return true;
    }

    public void describe(StringBuilder buf) {
        int k = 0;
        for (Bound bound : bounds) {
            if (k++ > 0) {
                buf.append(" AND ");
            }
            buf.append(bound.relOps[bound.relOps.length - 1].getOp());
            buf.append(' ');
            buf.append(bound.member);
        }
    }

    private enum RelOp {
        LT("<"),
        LE("<="),
        GT(">"),
        GE(">="),
        EQ("=");

        private final String op;

        RelOp(String op) {
            this.op = op;
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
        public RelOp desctrict() {
            switch (this) {
            case GT:
                return RelOp.GE;
            case LT:
                return RelOp.LE;
            default:
                return this;
            }
        }
    }

    private static class Bound {
        private final RolapMember member;
        private final Object[] values;
        private final RelOp[] relOps;

        Bound(RolapMember member, RelOp relOp) {
            this.member = member;
            List<Object> valueList = new ArrayList<Object>();
            List<RelOp> relOpList = new ArrayList<RelOp>();
            while (true) {
                valueList.add(0, member.getKey());
                relOpList.add(0, relOp);
                if (member.getLevel().isUnique()) {
                    break;
                }
                member = member.getParentMember();
                relOp = relOp.desctrict();
            }
            this.values = valueList.toArray(new Object[valueList.size()]);
            this.relOps = relOpList.toArray(new RelOp[relOpList.size()]);
        }


        public int hashCode() {
            int h = member.hashCode();
            h = h * 31 + Arrays.hashCode(values);
            h = h * 31 + Arrays.hashCode(relOps);
            return h;
        }

        public boolean equals(Object obj) {
            if (obj instanceof Bound) {
                Bound that = (Bound) obj;
                return this.member.equals(that.member)
                    && Arrays.equals(this.values, that.values)
                    && Arrays.equals(this.relOps, that.relOps);
            } else {
                return false;
            }
        }
    }

    public void toSql(SqlQuery sqlQuery, StringBuilder buf) {
        throw Util.needToImplement(this);
    }
}

// End MemberTuplePredicate.java
