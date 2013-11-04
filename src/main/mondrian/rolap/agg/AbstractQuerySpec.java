/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.rolap.*;
import mondrian.rolap.sql.*;
import mondrian.spi.Dialect;
import mondrian.util.*;

import java.util.*;

/**
 * Base class for {@link QuerySpec} implementations.
 *
 * @author jhyde
 * @author Richard M. Emberson
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
    protected SqlQueryBuilder createQueryBuilder(String desc) {
        return new SqlQueryBuilder(
            getStar().getSqlQueryDialect(),
            desc,
            new SqlTupleReader.ColumnLayoutBuilder());
    }

    public RolapStar getStar() {
        return star;
    }

    /**
     * Adds a measure to a query.
     *
     * @param measure Measure
     * @param alias Measure alias
     * @param queryBuilder Query builder
     */
    protected void addMeasure(
        RolapStar.Measure measure,
        String alias,
        final SqlQueryBuilder queryBuilder)
    {
        if (!isPartOfSelect(measure)) {
            return;
        }
        assert measure.getTable() == getStar().getFactTable();

        String exprInner;
        if (measure.getExpression() == null) {
            exprInner = "*";
        } else {
            exprInner = measure.getExpression().toSql();
            queryBuilder.addColumn(
                queryBuilder.column(
                    measure.getExpression(), measure.getTable()),
                Clause.FROM);
        }
        String exprOuter = measure.getAggregator().getExpression(exprInner);
        queryBuilder.sqlQuery.addSelect(
            exprOuter,
            measure.getInternalType(),
            alias);
    }

    protected abstract boolean isAggregate();

    protected Map<String, String> nonDistinctGenerateSql(
        SqlQueryBuilder queryBuilder)
    {
        // add constraining dimensions
        if (countOnly) {
            queryBuilder.sqlQuery.addSelect("count(*)", SqlStatement.Type.INT);
            queryBuilder.addRelation(
                queryBuilder.table(
                    star.getFactTable().getPath()),
                SqlQueryBuilder.NullJoiner.INSTANCE);
        }
        for (Ord<Pair<RolapStar.Column, String>> c : Ord.zip(getColumns())) {
            final RolapStar.Column column = c.e.left;
            final String alias = c.e.right;
            StarColumnPredicate predicate = getColumnPredicate(c.i);
            final Dialect dialect = queryBuilder.getDialect();
            final String where = Predicates.toSql(predicate, dialect);
            if (!where.equals("true")) {
                queryBuilder.sqlQuery.addWhere(where);
            }

            final SqlQueryBuilder.Column queryColumn =
                queryBuilder.column(
                    column.getExpression(), column.getTable());

            if (countOnly || !isPartOfSelect(column)) {
                queryBuilder.addColumn(queryColumn, Clause.FROM);
            } else {
                queryBuilder.addColumn(
                    queryColumn,
                    Clause.SELECT
                        .maybeGroup(isAggregate())
                        .maybeOrder(isOrdered()),
                    SqlQueryBuilder.NullJoiner.INSTANCE,
                    alias);
            }
        }

        // Add compound member predicates
        extraPredicates(queryBuilder);

        // add measures
        for (Pair<RolapStar.Measure, String> measureAlias : getMeasures()) {
            addMeasure(measureAlias.left, measureAlias.right, queryBuilder);
        }

        return Collections.emptyMap();
    }

    /**
     * Allows subclasses to specify if a given column must
     * be returned as part of the result set, in the select clause.
     */
    protected boolean isPartOfSelect(RolapStar.Column col) {
        return true;
    }

    /**
     * Allows subclasses to specify if a given column must
     * be returned as part of the result set, in the select clause.
     */
    protected boolean isPartOfSelect(RolapStar.Measure measure) {
        return true;
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

    public Pair<String, List<SqlStatement.Type>> generateSqlQuery(String desc) {
        SqlQueryBuilder queryBuilder = createQueryBuilder(desc);

        int k = getDistinctMeasureCount();
        final Dialect dialect = queryBuilder.getDialect();
        final Map<String, String> groupingSetsAliases;
        if (!dialect.allowsCountDistinct() && k > 0
            || !dialect.allowsMultipleCountDistinct() && k > 1)
        {
            groupingSetsAliases =
                distinctGenerateSql(queryBuilder, countOnly);
        } else {
            groupingSetsAliases =
                nonDistinctGenerateSql(queryBuilder);
        }
        if (!countOnly) {
            addGroupingFunction(queryBuilder);
            addGroupingSets(queryBuilder, groupingSetsAliases);
        }
        return queryBuilder.toSqlAndTypes();
    }

    protected void addGroupingFunction(SqlQueryBuilder queryBuilder) {
        throw new UnsupportedOperationException();
    }

    protected void addGroupingSets(
        SqlQueryBuilder queryBuilder,
        Map<String, String> groupingSetsAliases)
    {
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
        for (Pair<RolapStar.Measure, String> pair : getMeasures()) {
            if (pair.left.getAggregator().isDistinct()) {
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
     * @param outerQueryBuilder Query to modify
     * @param countOnly If true, only generate a single row: no need to
     *   generate a GROUP BY clause or put any constraining columns in the
     *   SELECT clause
     * @return A map of aliases used in the inner query if grouping sets
     * were enabled.
     */
    protected Map<String, String> distinctGenerateSql(
        final SqlQueryBuilder outerQueryBuilder,
        boolean countOnly)
    {
        final Dialect dialect = outerQueryBuilder.getDialect();
        final Dialect.DatabaseProduct databaseProduct =
            dialect.getDatabaseProduct();
        final Map<String, String> groupingSetsAliases =
            new HashMap<String, String>();
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

        final SqlQueryBuilder innerQueryBuilder =
            createQueryBuilder("distinct segment");
        final boolean distinct =
            databaseProduct != Dialect.DatabaseProduct.GREENPLUM;
        innerQueryBuilder.sqlQuery.setDistinct(distinct);

        // add constraining dimensions
        for (Ord<Pair<RolapStar.Column, String>> c : Ord.zip(getColumns())) {
            final RolapStar.Column column = c.e.left;
            String expr = column.getExpression().toSql();
            final SqlQueryBuilder.Column queryBuilderColumn =
                innerQueryBuilder.column(
                    column.getExpression(), column.getTable());

            StarColumnPredicate predicate = getColumnPredicate(c.i);
            final String where = Predicates.toSql(predicate, dialect);
            if (!where.equals("true")) {
                innerQueryBuilder.sqlQuery.addWhere(where);
            }
            if (countOnly) {
                innerQueryBuilder.addColumn(queryBuilderColumn, Clause.FROM);
                continue;
            }
            int x = innerQueryBuilder.addColumn(
                queryBuilderColumn,
                Clause.SELECT.maybeGroup(!distinct),
                null,
                "d" + c.i);
            final String alias = innerQueryBuilder.sqlQuery.getAlias(x);
            final String quotedAlias = dialect.quoteIdentifier(alias);
            outerQueryBuilder.sqlQuery.addSelectGroupBy(quotedAlias, null);
            // Add this alias to the map of grouping sets aliases
            groupingSetsAliases.put(
                expr,
                dialect.quoteIdentifier(
                    "dummyname." + alias));
        }

        // add predicates not associated with columns
        extraPredicates(innerQueryBuilder);

        // add measures
        for (Pair<RolapStar.Measure, String> pair : getMeasures()) {
            RolapStar.Measure measure = pair.left;

            assert measure.getTable() == getStar().getFactTable();
            final int x =
                innerQueryBuilder.addColumn(
                    innerQueryBuilder.column(
                        measure.getExpression(), measure.getTable()),
                    Clause.SELECT.maybeGroup(!distinct),
                    null,
                    pair.right);

            String alias = innerQueryBuilder.sqlQuery.getAlias(x);
            final String quotedAlias = dialect.quoteIdentifier(alias);
            outerQueryBuilder.sqlQuery.addSelect(
                measure.getAggregator().getNonDistinctAggregator()
                    .getExpression(quotedAlias),
                null);
        }
        outerQueryBuilder.sqlQuery.addFrom(
            innerQueryBuilder.sqlQuery, "dummyname", true);
        return groupingSetsAliases;
    }

    /**
     * Adds predicates not associated with columns.
     *
     * @param queryBuilder Query
     */
    protected void extraPredicates(SqlQueryBuilder queryBuilder) {
        for (StarPredicate predicate : getPredicateList()) {
            for (PredicateColumn pair : predicate.getColumnList()) {
                queryBuilder.addColumn(
                    queryBuilder.column(pair.physColumn, pair.router),
                    Clause.FROM);
            }
            StringBuilder buf = new StringBuilder();
            predicate.toSql(queryBuilder.getDialect(), buf);
            final String where = buf.toString();
            if (!where.equals("true")) {
                queryBuilder.sqlQuery.addWhere(where);
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
