/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.Aggregator;
import mondrian.olap.EnumeratedValues;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.fun.FunUtil;

import java.util.List;

/**
 * Describes an aggregation operator, such as "sum" or "count".
 *
 * @author jhyde$
 * @since Jul 9, 2003$
 * @version $Id$
 */
public abstract class RolapAggregator
        extends EnumeratedValues.BasicValue
        implements Aggregator {

    public static final RolapAggregator Sum = new RolapAggregator("sum", 0, false) {
        public Object aggregate(Evaluator evaluator, List members, Exp exp) {
            return FunUtil.sum(evaluator, members, exp);
        }
    };
    public static final RolapAggregator Count = new RolapAggregator("count", 1, false) {
        public Aggregator getRollup() {
            return Sum;
        }
        public Object aggregate(Evaluator evaluator, List members, Exp exp) {
            return FunUtil.count(evaluator, members, false);
        }
    };
    public static final RolapAggregator Min = new RolapAggregator("min", 2, false) {
        public Object aggregate(Evaluator evaluator, List members, Exp exp) {
            return FunUtil.min(evaluator, members, exp);
        }
    };
    public static final RolapAggregator Max = new RolapAggregator("max", 3, false) {
        public Object aggregate(Evaluator evaluator, List members, Exp exp) {
            return FunUtil.max(evaluator, members, exp);
        }
    };
    public static final RolapAggregator Avg = new RolapAggregator("avg", 4, false) {
        public Aggregator getRollup() {
            return null;
        }
        public Object aggregate(Evaluator evaluator, List members, Exp exp) {
            return FunUtil.avg(evaluator, members, exp);
        }
    };
    public static final RolapAggregator DistinctCount = new RolapAggregator("distinct count", 5, true) {
        public RolapAggregator getNonDistinctAggregator() {
            return Count;
        }
        public Object aggregate(Evaluator evaluator, List members, Exp exp) {
            throw new UnsupportedOperationException();
        }

        public String getExpression(String operand) {
            return "count(distinct " + operand + ")";
        }

    };
    /**
     * List of all valid aggregation operators.
     */
    public static final EnumeratedValues enumeration = new EnumeratedValues (
            new RolapAggregator[] {Sum, Count, Min, Max, Avg, DistinctCount}
    );

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
        StringBuffer buf = new StringBuffer(64);
        buf.append(name_);
        buf.append('(');                                                                if (distinct) {
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
    public Aggregator getRollup() {
        return this;
    }
}

// End RolapAggregator.java
