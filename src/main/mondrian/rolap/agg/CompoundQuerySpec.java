/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap.agg;

import mondrian.rolap.RolapStar;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.StarPredicate;
import mondrian.rolap.sql.SqlQuery;

import java.util.List;

/**
 * Definition of a query which retrieves a single cell with a complex
 * combination of predicates.
 *
 * <p>It is typically used to retrieve the value of a cell whose measure is
 * distinct-count (or another aggregate which cannot be rolled up)
 * and whose context contains one or more compound measures.
 *
 * <p>For example, given an eval context with a compound member:
 *
 * <blockquote><pre>
 * [Measures].[Customer Count]
 * [Time].[1997].[Q1]
 * [Store Type].[All Store Types]
 * Aggregate({[Store].[USA].[CA], [Store].[USA].[OR].[Portland]})
 *  ... everything else 'all'
 * </pre></blockquote>
 *
 * we want to generate the SQL
 *
 * <blockquote><pre>
 * SELECT COUNT(DISTINCT cust_id)
 * FROM sales_fact_1997 AS sales, store, time
 * WHERE store.store_id = sales.store_id
 * AND time.time_id = sales.time_id
 * AND time.year = 1997
 * AND time.quarter = 'Q1'
 * AND (store.store_state ='USA'
 *     AND (    store.store_state = 'CA'
 *          OR (    store.store_state = 'OR'
 *              AND store.store_City = 'Portland')))
 * </pre></blockquote>
 *
 * <p>Note that because the members in the Store hierarchy were at
 * different levels, the WHERE clause contains an AND of an OR of an AND.
 *
 * @author jhyde
 * @version $Id$
 */
public class CompoundQuerySpec extends AbstractQuerySpec {
    private final RolapStar.Measure measure;
    private final RolapStar.Column[] columns;
    private final List<StarColumnPredicate> columnPredicateList;
    private final List<StarPredicate> predicateList;

    CompoundQuerySpec(
        RolapStar.Measure measure,
        RolapStar.Column[] columns,
        List<StarColumnPredicate> columnPredicateList,
        List<StarPredicate> predicateList)
    {
        // It's a bit of a stretch to extend SegmentArrayQuerySpec. We inherit
        // a few of the methods, but we don't use the data members.
        super(measure.getStar(), true);
        this.measure = measure;
        this.columns = columns;
        this.columnPredicateList = columnPredicateList;
        this.predicateList = predicateList;
    }

    public int getMeasureCount() {
        return 1;
    }

    protected void addMeasure(int i, SqlQuery sqlQuery) {
        throw new UnsupportedOperationException();
    }

    protected boolean isAggregate() {
        return true;
    }

    public RolapStar.Measure getMeasure(final int i) {
        assert i == 0;
        return measure;
    }

    public String getMeasureAlias(final int i) {
        return "c";
    }

    public String getColumnAlias(int i) {
        throw new UnsupportedOperationException();
    }

    public RolapStar.Column[] getColumns() {
        return columns;
    }

    public StarColumnPredicate getColumnPredicate(final int i) {
        return columnPredicateList.get(i);
    }

    protected List<StarPredicate> getPredicateList() {
        return predicateList;
    }

    protected void nonDistinctGenerateSql(SqlQuery sqlQuery)
    {
        // add constraining dimensions
        int arity = columns.length;

        addMeasure(0, sqlQuery);

        for (int i = 0; i < arity; i++) {
            RolapStar.Column column = columns[i];
            RolapStar.Table table = column.getTable();
            if (table.isFunky()) {
                // this is a funky dimension -- ignore for now
                continue;
            }
            table.addToFrom(sqlQuery, false, true);

            String expr = column.generateExprString(sqlQuery);

            final StarColumnPredicate predicate = columnPredicateList.get(i);
            final String where = RolapStar.Column.createInExpr(
                expr,
                predicate,
                column.getDatatype(),
                sqlQuery);
            if (!where.equals("true")) {
                sqlQuery.addWhere(where);
            }
        }

        extraPredicates(sqlQuery);
    }

    public static String compoundGenerateSql(
        RolapStar.Measure measure,
        RolapStar.Column[] columns,
        List<StarColumnPredicate> columnPredicateList,
        List<StarPredicate> predicateList)
    {
        final CompoundQuerySpec querySpec =
            new CompoundQuerySpec(
                measure, columns, columnPredicateList, predicateList);
        return querySpec.generateSqlQuery();
    }
}

// End CompoundQuerySpec.java
