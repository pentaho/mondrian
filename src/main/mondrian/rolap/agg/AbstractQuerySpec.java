/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap.agg;

import mondrian.rolap.RolapStar;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.StarPredicate;
import mondrian.rolap.sql.SqlQuery;
import mondrian.olap.Util;
import mondrian.spi.Dialect;

import java.util.*;

/**
 * Base class for {@link QuerySpec} implementations.
 *
 * @author jhyde
 * @author Richard M. Emberson
 * @version $Id$
 */
public abstract class AbstractQuerySpec implements QuerySpec {
    private final RolapStar star;
    protected final boolean countOnly;

    /**
     * Creates an AbstractQuerySpec.
     *
     * @param star Star which defines columns of interest and their
     * relationships
     *
     * @param countOnly If true, generate no GROUP BY clause, so the query
     * returns a single row containing a grand total
     */
    protected AbstractQuerySpec(final RolapStar star, boolean countOnly) {
        this.star = star;
        this.countOnly = countOnly;
    }

    /**
     * Creates a query object.
     *
     * @return a new query object
     */
    protected SqlQuery newSqlQuery() {
        return getStar().getSqlQuery();
    }

    public RolapStar getStar() {
        return star;
    }

    /**
     * Adds a measure to a query.
     *
     * @param i Ordinal of measure
     * @param sqlQuery Query object
     */
    protected void addMeasure(final int i, final SqlQuery sqlQuery) {
        RolapStar.Measure measure = getMeasure(i);
        Util.assertTrue(measure.getTable() == getStar().getFactTable());
        measure.getTable().addToFrom(sqlQuery, false, true);

        String exprInner = measure.generateExprString(sqlQuery);
        String exprOuter = measure.getAggregator().getExpression(exprInner);
        sqlQuery.addSelect(exprOuter, getMeasureAlias(i));
    }

    protected abstract boolean isAggregate();

    protected void nonDistinctGenerateSql(SqlQuery sqlQuery)
    {
        // add constraining dimensions
        RolapStar.Column[] columns = getColumns();
        int arity = columns.length;
        if (countOnly) {
            sqlQuery.addSelect("count(*)");
        }
        for (int i = 0; i < arity; i++) {
            RolapStar.Column column = columns[i];
            RolapStar.Table table = column.getTable();
            if (table.isFunky()) {
                // this is a funky dimension -- ignore for now
                continue;
            }
            table.addToFrom(sqlQuery, false, true);

            String expr = column.generateExprString(sqlQuery);

            StarColumnPredicate predicate = getColumnPredicate(i);
            final String where = RolapStar.Column.createInExpr(
                expr,
                predicate,
                column.getDatatype(),
                sqlQuery);
            if (!where.equals("true")) {
                sqlQuery.addWhere(where);
            }

            if (countOnly) {
                continue;
            }

            // some DB2 (AS400) versions throw an error, if a column alias is
            // there and *not* used in a subsequent order by/group by
            final Dialect dialect = sqlQuery.getDialect();
            final String c;
            final Dialect.DatabaseProduct databaseProduct =
                dialect.getDatabaseProduct();
            if (databaseProduct == Dialect.DatabaseProduct.DB2_AS400) {
                c = sqlQuery.addSelect(expr, null);
            } else {
                c = sqlQuery.addSelect(expr, getColumnAlias(i));
            }

            if (isAggregate()) {
                if (dialect.requiresGroupByAlias()) {
                    sqlQuery.addGroupBy(c);
                } else {
                    sqlQuery.addGroupBy(expr);
                }
            }

            // Add ORDER BY clause to make the results deterministic.
            // Derby has a bug with ORDER BY, so ignore it.
            if (isOrdered()) {
                sqlQuery.addOrderBy(expr, true, false, false);
            }
        }

        // Add compound member predicates
        extraPredicates(sqlQuery);

        // add measures
        if (!countOnly) {
            for (int i = 0, count = getMeasureCount(); i < count; i++) {
                addMeasure(i, sqlQuery);
            }
        }
    }

    /**
     * Whether to add an ORDER BY clause to make results deterministic.
     * Necessary if query returns more than one row and results are for
     * human consumption.
     *
     * @return whether to sort query
     */
    protected boolean isOrdered() {
        return false;
    }

    public String generateSqlQuery() {
        SqlQuery sqlQuery = newSqlQuery();

        int k = getDistinctMeasureCount();
        final Dialect dialect = sqlQuery.getDialect();
        if (!dialect.allowsCountDistinct() && k > 0 ||
            !dialect.allowsMultipleCountDistinct() && k > 1) {
            distinctGenerateSql(sqlQuery, countOnly);
        } else {
            nonDistinctGenerateSql(sqlQuery);
        }
        if (!countOnly) {
            addGroupingFunction(sqlQuery);
            addGroupingSets(sqlQuery);
        }
        return sqlQuery.toString();
    }

    protected void addGroupingFunction(SqlQuery sqlQuery) {
        throw new UnsupportedOperationException();
    }

    protected void addGroupingSets(SqlQuery sqlQuery) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the number of measures whose aggregation function is
     * distinct-count.
     *
     * @return Number of distinct-count measures
     */
    protected int getDistinctMeasureCount() {
        int k = 0;
        for (int i = 0, count = getMeasureCount(); i < count; i++) {
            RolapStar.Measure measure = getMeasure(i);
            if (measure.getAggregator().isDistinct()) {
                ++k;
            }
        }
        return k;
    }

    /**
     * Generates a SQL query to retrieve the values in this segment using
     * an algorithm which converts distinct-aggregates to non-distinct
     * aggregates over subqueries.
     *
     * @param outerSqlQuery Query to modify
     * @param countOnly If true, only generate a single row: no need to
     *   generate a GROUP BY clause or put any constraining columns in the
     *   SELECT clause
     */
    protected void distinctGenerateSql(
        final SqlQuery outerSqlQuery,
        boolean countOnly)
    {
        final Dialect dialect = outerSqlQuery.getDialect();
        // Generate something like
        //
        //  select d0, d1, count(m0)
        //  from (
        //    select distinct dim1.x as d0, dim2.y as d1, f.z as m0
        //    from f, dim1, dim2
        //    where dim1.k = f.k1
        //    and dim2.k = f.k2) as dummyname
        //  group by d0, d1
        //
        // or, if countOnly=true
        //
        //  select count(m0)
        //  from (
        //    select distinct f.z as m0
        //    from f, dim1, dim2
        //    where dim1.k = f.k1
        //    and dim2.k = f.k2) as dummyname

        final SqlQuery innerSqlQuery = newSqlQuery();
        innerSqlQuery.setDistinct(true);

        // add constraining dimensions
        RolapStar.Column[] columns = getColumns();
        int arity = columns.length;
        for (int i = 0; i < arity; i++) {
            RolapStar.Column column = columns[i];
            RolapStar.Table table = column.getTable();
            if (table.isFunky()) {
                // this is a funky dimension -- ignore for now
                continue;
            }
            table.addToFrom(innerSqlQuery, false, true);
            String expr = column.generateExprString(innerSqlQuery);
            StarColumnPredicate predicate = getColumnPredicate(i);
            final String where = RolapStar.Column.createInExpr(
                expr,
                predicate,
                column.getDatatype(),
                innerSqlQuery);
            if (!where.equals("true")) {
                innerSqlQuery.addWhere(where);
            }
            if (countOnly) {
                continue;
            }
            final String alias = "d" + i;
            innerSqlQuery.addSelect(expr, alias);
            final String quotedAlias = dialect.quoteIdentifier(alias);
            outerSqlQuery.addSelectGroupBy(quotedAlias);
        }

        // add predicates not associated with columns
        extraPredicates(innerSqlQuery);

        // add measures
        for (int i = 0, count = getMeasureCount(); i < count; i++) {
            RolapStar.Measure measure = getMeasure(i);

            Util.assertTrue(measure.getTable() == getStar().getFactTable());
            measure.getTable().addToFrom(innerSqlQuery, false, true);

            String alias = getMeasureAlias(i);
            String expr = measure.generateExprString(outerSqlQuery);
            innerSqlQuery.addSelect(expr, alias);

            outerSqlQuery.addSelect(
                measure.getAggregator().getNonDistinctAggregator().getExpression(
                    dialect.quoteIdentifier(alias)));
        }
        outerSqlQuery.addFrom(innerSqlQuery, "dummyname", true);
    }

    /**
     * Adds predicates not associated with columns.
     *
     * @param sqlQuery Query
     */
    protected void extraPredicates(SqlQuery sqlQuery) {
        List<StarPredicate> predicateList = getPredicateList();
        for (StarPredicate predicate : predicateList) {
            for (RolapStar.Column column :
                predicate.getConstrainedColumnList()) {
                final RolapStar.Table table = column.getTable();
                table.addToFrom(sqlQuery, false, true);
            }
            StringBuilder buf = new StringBuilder();
            predicate.toSql(sqlQuery, buf);
            final String where = buf.toString();
            if (!where.equals("true")) {
                sqlQuery.addWhere(where);
            }
        }
    }

    /**
     * Returns a list of predicates not associated with a particular column.
     *
     * @return list of non-column predicates
     */
    protected List<StarPredicate> getPredicateList() {
        return Collections.emptyList();
    }
}


// End AbstractQuerySpec.java
