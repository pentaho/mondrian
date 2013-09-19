/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.rolap.agg;

import mondrian.rolap.*;
import mondrian.rolap.sql.SqlQuery;

import java.util.*;

/**
 * Predicate which is the union of a list of predicates. It evaluates to
 * true if any of the predicates evaluates to true.
 *
 * @see OrPredicate
 *
 * @author jhyde
 */
public class OrPredicate extends ListPredicate {

    public OrPredicate(List<StarPredicate> predicateList) {
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

    public StarPredicate or(StarPredicate predicate) {
        if (predicate instanceof OrPredicate
            && predicate.getConstrainedColumnBitKey().equals(
                getConstrainedColumnBitKey()))
        {
            // Do not collapse OrPredicates with different number of columns.
            // Keeping them separate helps the SQL translation to IN-list.
            ListPredicate that = (ListPredicate) predicate;
            final List<StarPredicate> list =
                new ArrayList<StarPredicate>(children);
            list.addAll(that.children);
            return new OrPredicate(list);
        } else {
            final List<StarPredicate> list =
                new ArrayList<StarPredicate>(children);
            list.add(predicate);
            return new OrPredicate(list);
        }
    }

    public StarPredicate and(StarPredicate predicate) {
        List<StarPredicate> list = new ArrayList<StarPredicate>();
        list.add(this);
        list.add(predicate);
        return new AndPredicate(list);
    }

    /**
     * Checks whether a predicate can be translated using an IN list, and groups
     * predicates based on how many columns can be translated using IN list. If
     * none of the columns can be made part of IN, the entire predicate will be
     * translated using AND/OR. This method identifies all the columns that can
     * be part of IN and and categorizes this predicate based on number of
     * column values to use in the IN list.
     *
     * @param predicate predicate to analyze
     * @param sqlQuery Query
     * @param predicateMap the map containing predicates analyzed so far
     */
    private void checkInListForPredicate(
        StarPredicate predicate,
        SqlQuery sqlQuery,
        Map<BitKey, List<StarPredicate>> predicateMap)
    {
        BitKey inListRhsBitKey;
        BitKey columnBitKey = getConstrainedColumnBitKey();
        if (predicate instanceof ValueColumnPredicate) {
            // OR of column values from the same column
            inListRhsBitKey =
                ((ValueColumnPredicate) predicate).checkInList(columnBitKey);
        } else if (predicate instanceof AndPredicate) {
            // OR of ANDs over a set of values over the same column set
            inListRhsBitKey =
                ((AndPredicate) predicate).checkInList(sqlQuery, columnBitKey);
        } else {
            inListRhsBitKey = columnBitKey.emptyCopy();
        }
        List<StarPredicate> predicateGroup =
            predicateMap.get(inListRhsBitKey);
        if (predicateGroup == null) {
            predicateGroup = new ArrayList<StarPredicate> ();
            predicateMap.put(inListRhsBitKey, predicateGroup);
        }
        predicateGroup.add(predicate);
    }

    private void checkInList(
        SqlQuery sqlQuery,
        Map<BitKey, List<StarPredicate>> predicateMap)
    {
        for (StarPredicate predicate : children) {
            checkInListForPredicate(predicate, sqlQuery, predicateMap);
        }
    }

    /**
     * Translates a list of predicates over the same set of columns into sql
     * using IN list where possible.
     *
     * @param sqlQuery Query
     * @param buf buffer to build sql
     * @param inListRhsBitKey which column positions are included in
     *     the IN predicate; the non included positions corresponde to
     *     columns that are nulls
     * @param predicateList the list of predicates to translate.
     */
    private void toInListSql(
        SqlQuery sqlQuery,
        StringBuilder buf,
        BitKey inListRhsBitKey,
        List<StarPredicate> predicateList)
    {
        // Make a col position to column map to aid search.
        Map<Integer, RolapStar.Column> columnMap =
            new HashMap<Integer, RolapStar.Column>();

        for (RolapStar.Column column : columns) {
            columnMap.put(column.getBitPosition(), column);
        }

        buf.append("(");
        // First generate nulls for the columns which will not be included
        // in the IN list

        boolean firstNullColumnPredicate = true;
        for (Integer colPos
            : getConstrainedColumnBitKey().andNot(inListRhsBitKey))
        {
            if (firstNullColumnPredicate) {
                firstNullColumnPredicate = false;
            } else {
                buf.append(" and ");
            }
            String expr = columnMap.get(colPos).generateExprString(sqlQuery);
            buf.append(expr);
            buf.append(" is null");
        }

        // Now the IN list part
        if (inListRhsBitKey.isEmpty()) {
            return;
        }

        if (firstNullColumnPredicate) {
            firstNullColumnPredicate = false;
        } else {
            buf.append(" and ");
        }

        // First add the column names;
        boolean multiInList = inListRhsBitKey.toBitSet().cardinality() > 1;
        if (multiInList) {
            // Multi-IN list
            buf.append("(");
        }

        boolean firstColumn = true;
        for (Integer colPos : inListRhsBitKey) {
            if (firstColumn) {
                firstColumn = false;
            } else {
                buf.append(", ");
            }
            String expr = columnMap.get(colPos).generateExprString(sqlQuery);
            buf.append(expr);
        }
        if (multiInList) {
            // Multi-IN list
            buf.append(")");
        }
        buf.append(" in (");

        boolean firstPredicate = true;
        for (StarPredicate predicate : predicateList) {
            if (firstPredicate) {
                firstPredicate = false;
            } else {
                buf.append(", ");
            }

            if (predicate instanceof AndPredicate) {
                ((AndPredicate) predicate).toInListSql(
                    sqlQuery, buf, inListRhsBitKey);
            } else {
                assert predicate instanceof ValueColumnPredicate;
                ((ValueColumnPredicate) predicate).toInListSql(sqlQuery, buf);
            }
        }
        buf.append(")");
        buf.append(")");
    }

    public void toSql(SqlQuery sqlQuery, StringBuilder buf) {
        //
        // If possible, translate the predicate using IN lists.
        //
        // Two possibilities:
        // (1) top-level can be directly tranlated to IN-list
        //  examples:
        //   (country IN (USA, Canada))
        //
        //   ((country, satte) in ((USA, CA), (USA, OR)))
        //
        // (2) child level can be translated to IN list: this allows IN list
        // predicates generated such as:
        //   (country, state) IN ((USA, CA), (USA, OR))
        //   OR
        //   (country, state, city) IN ((USA, CA, SF), (USA, OR, Portland))
        //
        // The second case is handled by calling toSql on the children in
        // super.toSql().
        //
        final Map<BitKey, List<StarPredicate>> predicateMap =
            new LinkedHashMap<BitKey, List<StarPredicate>> ();

        boolean first = true;
        checkInList(sqlQuery, predicateMap);
        buf.append("(");

        for (BitKey columnKey : predicateMap.keySet()) {
            List<StarPredicate> predList = predicateMap.get(columnKey);
            if (columnKey.isEmpty() || predList.size() <= 1) {
                // Not possible to have IN list, or only one predicate
                // in the group.
                for (StarPredicate pred : predList) {
                    if (first) {
                        first = false;
                    } else {
                        buf.append(" or ");
                    }
                    pred.toSql(sqlQuery, buf);
                }
            } else {
                // Translate the rest
                if (first) {
                    first = false;
                } else {
                    buf.append(" or ");
                }
                toInListSql(sqlQuery, buf, columnKey, predList);
            }
        }

        buf.append(")");
    }

    protected String getOp() {
        return "or";
    }
}

// End OrPredicate.java
