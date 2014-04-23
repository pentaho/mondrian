/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.olap.fun.*;
import mondrian.spi.Dialect;
import mondrian.spi.Dialect.Datatype;

import java.util.List;

/**
 * Describes an aggregation operator, such as "sum" or "count".
 *
 * @author jhyde
 * @since Jul 9, 2003
 */
public abstract class RolapAggregator
    extends EnumeratedValues.BasicValue
    implements Aggregator
{
    private static int index = 0;

    public static final RolapAggregator Sum =
        new RolapAggregator("sum", index++, false) {
            public Object aggregate(
                Evaluator evaluator, TupleList members, Calc exp)
            {
                return FunUtil.sum(evaluator, members, exp);
            }
            public boolean supportsFastAggregates(Dialect.Datatype dataType) {
                switch (dataType) {
                case Integer:
                case Numeric:
                    return true;
                default:
                    return false;
                }
            };
            public Object aggregate(List<Object> rawData, Datatype datatype) {
                assert rawData.size() > 0;
                switch (datatype) {
                case Integer:
                    int sumInt = Integer.MIN_VALUE;
                    for (Object data : rawData) {
                        if (data != null) {
                            if (sumInt == Integer.MIN_VALUE) {
                                sumInt = 0;
                            }
                            if (data instanceof Double) {
                                data = ((Double) data).intValue();
                            }
                            sumInt += (Integer) data;
                        }
                    }
                    return sumInt == Integer.MIN_VALUE
                        ? null
                        : sumInt;
                case Numeric:
                    double sumDouble = Double.MIN_VALUE;
                    for (Object data : rawData) {
                        if (data != null) {
                            if (sumDouble == Double.MIN_VALUE) {
                                sumDouble = 0;
                            }
                            sumDouble += ((Number) data).doubleValue();
                        }
                    }
                    return sumDouble == Double.MIN_VALUE
                        ? null
                        : sumDouble;
                default:
                    throw new MondrianException(
                        "Aggregator " + this.name
                        + " does not support datatype" + datatype.name());
                }
            }
        };

    public static final RolapAggregator Count =
        new RolapAggregator("count", index++, false) {
            public RolapAggregator getRollup() {
                return Sum;
            }

            public Object aggregate(
                Evaluator evaluator, TupleList members, Calc exp)
            {
                return FunUtil.count(evaluator, members, false);
            }

            public Dialect.Datatype deriveDatatype(
                List<Dialect.Datatype> parameterDatatypes)
            {
                return Dialect.Datatype.Integer;
            }
        };

    public static final RolapAggregator Min =
        new RolapAggregator("min", index++, false) {
            public Object aggregate(
                Evaluator evaluator, TupleList members, Calc exp)
            {
                return FunUtil.min(evaluator, members, exp);
            }

            public boolean supportsFastAggregates(Dialect.Datatype dataType) {
                switch (dataType) {
                case Integer:
                case Numeric:
                    return true;
                default:
                    return false;
                }
            };
            public Object aggregate(List<Object> rawData, Datatype datatype) {
                assert rawData.size() > 0;
                switch (datatype) {
                case Integer:
                    int minInt = Integer.MAX_VALUE;
                    for (Object data : rawData) {
                        if (data != null) {
                            minInt = Math.min(minInt, (Integer)data);
                        }
                    }
                    return minInt == Integer.MAX_VALUE
                        ? null
                        : minInt;
                case Numeric:
                    double minDouble = Double.MAX_VALUE;
                    for (Object data : rawData) {
                        if (data != null) {
                            minDouble =
                                Math.min(
                                    minDouble,
                                    ((Number)data).doubleValue());
                        }
                    }
                    return minDouble == Double.MAX_VALUE
                        ? null
                        : minDouble;
                default:
                    throw new MondrianException(
                        "Aggregator " + this.name
                        + " does not support datatype" + datatype.name());
                }
            }
        };

    public static final RolapAggregator Max =
        new RolapAggregator("max", index++, false) {
            public Object aggregate(
                Evaluator evaluator, TupleList members, Calc exp)
            {
                return FunUtil.max(evaluator, members, exp);
            }

            public boolean supportsFastAggregates(Dialect.Datatype dataType) {
                switch (dataType) {
                case Integer:
                case Numeric:
                    return true;
                default:
                    return false;
                }
            };
            public Object aggregate(List<Object> rawData, Datatype datatype) {
                assert rawData.size() > 0;
                switch (datatype) {
                case Integer:
                    int maxInt = Integer.MIN_VALUE;
                    for (Object data : rawData) {
                        if (data != null) {
                            maxInt = Math.max(maxInt, (Integer)data);
                        }
                    }
                    return maxInt == Integer.MIN_VALUE
                        ? null
                        : maxInt;
                case Numeric:
                    double maxDouble = Double.MIN_VALUE;
                    for (Object data : rawData) {
                        if (data != null) {
                            maxDouble =
                                Math.max(
                                    maxDouble,
                                    ((Number)data).doubleValue());
                        }
                    }

                    return maxDouble == Double.MIN_VALUE
                        ? null
                        : maxDouble;
                default:
                    throw new MondrianException(
                        "Aggregator " + this.name
                        + " does not support datatype" + datatype.name());
                }
            }
        };

    public static final RolapAggregator Avg =
        new RolapAggregator("avg", index++, false) {
            public RolapAggregator getRollup() {
                return new RolapAggregator("avg", index, false) {
                    public Object aggregate(
                        Evaluator evaluator,
                        TupleList members,
                        Calc calc)
                    {
                        return AggregateFunDef.avg(evaluator, members, calc);
                    }
                };
            }
            public Object aggregate(
                Evaluator evaluator, TupleList members, Calc exp)
            {
                return AggregateFunDef.avg(evaluator, members, exp);
            }
        };

    public static final RolapAggregator DistinctCount =
        new RolapAggregator("distinct-count", index++, true) {
            public RolapAggregator getRollup() {
                // Distinct counts cannot always be rolled up, when they can,
                // it's using Sum.
                return Sum;
            }

            public RolapAggregator getNonDistinctAggregator() {
                return Count;
            }

            public Object aggregate(
                Evaluator evaluator, TupleList members, Calc exp)
            {
                throw new UnsupportedOperationException();
            }

            public String getExpression(String operand) {
                return "count(distinct " + operand + ")";
            }

            public boolean supportsFastAggregates(
                mondrian.spi.Dialect.Datatype dataType)
            {
                // We can't rollup using the raw data, because this is
                // a distinct-count operation.
                return false;
            }

            public Dialect.Datatype deriveDatatype(
                List<Dialect.Datatype> parameterDatatypes)
            {
                return Dialect.Datatype.Integer;
            }
        };

    /**
     * List of all valid aggregation operators.
     */
    public static final EnumeratedValues<RolapAggregator> enumeration =
        new EnumeratedValues<RolapAggregator>(
            new RolapAggregator[] {Sum, Count, Min, Max, Avg, DistinctCount});

    /**
     * This is the base class for implementing aggregators over sum and
     * average columns in an aggregate table. These differ from the above
     * aggregators in that these require not oly the operand to create
     * the aggregation String expression, but also, the aggregate table's
     * fact count column expression.
     * These aggregators are NOT singletons like the above aggregators; rather,
     * each is different because of the fact count column expression.
     */
    protected static abstract class BaseAggor extends RolapAggregator {
        protected final String factCountExpr;

        protected BaseAggor(final String name, final String factCountExpr) {
            super(name, index++, false);
            this.factCountExpr = factCountExpr;
        }

        public Object aggregate(
            Evaluator evaluator, TupleList members, Calc exp)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Aggregator used for aggregate tables implementing the
     * average aggregator.
     *
     * <p>It uses the aggregate table fact_count column
     * and a sum measure to create the query used to generate an average:
     * <blockquote>
     * <code>
     *    avg == sum(column_sum) / sum(factcount).
     * </code>
     * </blockquote>
     *
     * <p>If the fact table has both a sum and average over the same column and
     * the aggregate table only has a sum and fact count column, then the
     * average aggregator can be generated using this aggregator.
     */
    public static class AvgFromSum extends BaseAggor {
        public AvgFromSum(String factCountExpr) {
            super("AvgFromSum", factCountExpr);
        }
        public String getExpression(String operand) {
            StringBuilder buf = new StringBuilder(64);
            buf.append("sum(");
            buf.append(operand);
            buf.append(") / sum(");
            buf.append(factCountExpr);
            buf.append(')');
            return buf.toString();
        }
    }

    /**
     * Aggregator used for aggregate tables implementing the
     * average aggregator.
     *
     * <p>It uses the aggregate table fact_count column
     * and an average measure to create the query used to generate an average:
     * <blockquote>
     * <code>
     *    avg == sum(column_sum * factcount) / sum(factcount).
     * </code>
     * </blockquote>
     *
     * <p>If the fact table has both a sum and average over the same column and
     * the aggregate table only has a average and fact count column, then the
     * average aggregator can be generated using this aggregator.
     */
    public static class AvgFromAvg extends BaseAggor {
        public AvgFromAvg(String factCountExpr) {
            super("AvgFromAvg", factCountExpr);
        }

        public String getExpression(String operand) {
            StringBuilder buf = new StringBuilder(64);
            buf.append("sum(");
            buf.append(operand);
            buf.append(" * ");
            buf.append(factCountExpr);
            buf.append(") / sum(");
            buf.append(factCountExpr);
            buf.append(')');
            return buf.toString();
        }
    }
    /**
     * This is an aggregator used for aggregate tables implementing the
     * sum aggregator. It uses the aggregate table fact_count column
     * and an average measure to create the query used to generate a sum:
     * <pre>
     *    sum == sum(column_avg * factcount)
     * </pre>
     * If the fact table has both a sum and average over the same column and
     * the aggregate table only has an average and fact count column, then the
     * sum aggregator can be generated using this aggregator.
     */
    public static class SumFromAvg extends BaseAggor {
        public SumFromAvg(String factCountExpr) {
            super("SumFromAvg", factCountExpr);
        }

        public String getExpression(String operand) {
            StringBuilder buf = new StringBuilder(64);
            buf.append("sum(");
            buf.append(operand);
            buf.append(" * ");
            buf.append(factCountExpr);
            buf.append(')');
            return buf.toString();
        }
    }




    private final boolean distinct;

    public RolapAggregator(String name, int ordinal, boolean distinct) {
        super(name, ordinal, null);
        this.distinct = distinct;
    }

    public boolean isDistinct() {
        return distinct;
    }

    /**
     * Returns the expression to apply this aggregator to an operand.
     * For example, <code>getExpression("emp.sal")</code> returns
     * <code>"sum(emp.sal)"</code>.
     */
    public String getExpression(String operand) {
        StringBuilder buf = new StringBuilder(64);
        buf.append(name);
        buf.append('(');
        if (distinct) {
            buf.append("distinct ");
        }
        buf.append(operand);
        buf.append(')');
        return buf.toString();
    }

    /**
     * If this is a distinct aggregator, returns the corresponding non-distinct
     * aggregator, otherwise throws an error.
     */
    public RolapAggregator getNonDistinctAggregator() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the aggregator used to roll up. By default, aggregators roll up
     * themselves.
     */
    public RolapAggregator getRollup() {
        return this;
    }

    /**
     * By default, fast rollup is not supported for all classes.
     */
    public boolean supportsFastAggregates(Dialect.Datatype dataType) {
        return false;
    }

    public Object aggregate(List<Object> rawData, Dialect.Datatype datatype)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Derives the data type of a call to this aggregator.
     *
     * <p>The default implementation asserts that there is precisely one
     * argument and returns the type of that argument.
     *
     * @param parameterDatatypes Data types of arguments. Each argument's type
     *                          may be null, meaning not known
     *
     * @return Data type of call to this aggregator
     */
    public Dialect.Datatype deriveDatatype(
        List<Dialect.Datatype> parameterDatatypes)
    {
        assert parameterDatatypes.size() == 1;
        return parameterDatatypes.get(0);
    }
}

// End RolapAggregator.java
