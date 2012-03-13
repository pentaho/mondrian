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

import mondrian.rolap.*;
import mondrian.rolap.sql.SqlQuery;

import java.util.*;

/**
 * Predicate which is the intersection of a list of predicates. It evaluates to
 * true if all of the predicates evaluate to true.
 *
 * @see OrPredicate
 *
 * @author jhyde
 */
public class AndPredicate extends ListPredicate {

    public AndPredicate(List<StarPredicate> predicateList) {
        super(predicateList);
    }

    public boolean evaluate(List<Object> valueList) {
        // NOTE: If we know that every predicate in the list is a
        // ValueColumnPredicate, we could optimize the evaluate method by
        // building a value list at construction time. But it's a tradeoff,
        // considering the extra time and space required.
        for (StarPredicate childPredicate : children) {
            if (childPredicate.evaluate(valueList)) {
                return true;
            }
        }
        return false;
    }

    public StarPredicate and(StarPredicate predicate) {
        if (predicate instanceof AndPredicate) {
            ListPredicate that = (ListPredicate) predicate;
            final List<StarPredicate> list =
                new ArrayList<StarPredicate>(children);
            list.addAll(that.children);
            return new AndPredicate(list);
        } else {
            final List<StarPredicate> list =
                new ArrayList<StarPredicate>(children);
            list.add(predicate);
            return new AndPredicate(list);
        }
    }


    public StarPredicate or(StarPredicate predicate) {
        List<StarPredicate> list = new ArrayList<StarPredicate>();
        list.add(this);
        list.add(predicate);
        return new OrPredicate(list);
    }

    public BitKey checkInList(SqlQuery sqlQuery, BitKey inListLHSBitKey) {
        // AND predicate by itself is not using IN list; when it is
        // one of the children to an OR predicate, then using IN list
        // is helpful. The later is checked by passing in a bitmap that
        // represent the LHS or the IN list, i.e. the columns that are
        // constrained by the OR.

        // If the child predicates contains null values, those predicates cannot
        // be translated as IN list; however, the rest of the child predicates
        // might still be translated to IN.  For example, neither of the two AND
        // conditions below(part of an OR list) can be translated using IN list,
        // covering all the levels
        //
        //  (null, null, San Francisco)
        //  (null, null, New York)
        //
        // However, after extracting the null part, they can be translated to:
        //
        // (country is null AND state is null AND city IN ("San Fancisco", "New
        // York"))
        //
        // which is still more compact than the default AND/OR translation:
        //
        // (country is null AND state is null AND city = "San Francisco") OR
        // (country is null AND state is null AND city = "New York")
        //
        // This method will mark all the columns that can be translated as part
        // of IN list, so that similar predicates can be grouped together to
        // form partial IN list sql. By default, all columns constrained by this
        // predicates can be part of an IN list.
        //
        // This is very similar to the logic in
        // SqlConstraintUtil.generateMultiValueInExpr().  The only difference
        // being that the predicates here are all "flattened" so the hierarchy
        // information is no longer available to guide the grouping of
        // predicates with common parents. So some optimization possible in
        // generateMultiValueInExpr() is not tried here, as they require
        // implementing "longest common prefix" algorithm which is an overkill.
        BitKey inListRHSBitKey = inListLHSBitKey.copy();

        if (!getConstrainedColumnBitKey().equals(inListLHSBitKey)
            || (children.size() > 1
             && !sqlQuery.getDialect().supportsMultiValueInExpr()))
        {
            inListRHSBitKey.clear();
        } else {
            for (StarPredicate predicate : children) {
                // If any predicate requires comparison to null value, cannot
                // use IN list for this predicate.
                if (predicate instanceof ValueColumnPredicate) {
                    ValueColumnPredicate columnPred =
                        ((ValueColumnPredicate) predicate);
                    if (columnPred.getValue() == RolapUtil.sqlNullValue) {
                        // This column predicate cannot be translated to IN
                        inListRHSBitKey.clear(
                            columnPred.getConstrainedColumn().getBitPosition());
                    }
                    // else do nothing because this column predicate can be
                    // translated to IN
                } else {
                    inListRHSBitKey.clear();
                    break;
                }
            }
        }
        return inListRHSBitKey;
    }

    /*
     * Generate value list for this predicate to be used in an IN-list
     * sql predicate.
     *
     * The values in a multi-column IN list predicates are generated in the
     * same order, based on the bit position from the columnBitKey.
     *
     */
    public void toInListSql(
        SqlQuery sqlQuery,
        StringBuilder buf,
        BitKey inListRHSBitKey)
    {
        boolean firstValue = true;
        buf.append("(");
        /*
         * Arranging children according to the bit position. This is required
         * as RHS of IN list needs to list the column values in the same order.
         */
        Set<ValueColumnPredicate> sortedPredicates =
            new TreeSet<ValueColumnPredicate>();

        for (StarPredicate predicate : children) {
            // inListPossible() checks gaurantees that predicate is of type
            // ValueColumnPredicate
            assert predicate instanceof ValueColumnPredicate;
            if (inListRHSBitKey.get(
                    ((ValueColumnPredicate) predicate).getConstrainedColumn()
                        .getBitPosition()))
            {
                sortedPredicates.add((ValueColumnPredicate)predicate);
            }
        }

        for (ValueColumnPredicate predicate : sortedPredicates) {
            if (firstValue) {
                firstValue = false;
            } else {
                buf.append(", ");
            }
            sqlQuery.getDialect().quote(
                buf, predicate.getValue(),
                predicate.getConstrainedColumn().getDatatype());
        }
        buf.append(")");
    }

    protected String getOp() {
        return "and";
    }
}

// End AndPredicate.java
