/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap.agg;

import mondrian.rolap.RolapStar;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.sql.SqlQuery;

/**
 * Base class for {@link QuerySpec} implementations.
 *
 * @author jhyde
 * @author Richard M. Emberson
 * @version $Id$
 */
public abstract class AbstractQuerySpec implements QuerySpec {
    private final RolapStar star;

    protected AbstractQuerySpec(final RolapStar star) {
        this.star = star;
    }

    protected SqlQuery newSqlQuery() {
        return getStar().getSqlQuery();
    }

    public RolapStar getStar() {
        return star;
    }

    protected abstract void addMeasure(final int i, final SqlQuery sqlQuery);
    protected abstract boolean isAggregate();

    protected void nonDistinctGenerateSql(
        final SqlQuery sqlQuery, boolean ordered, boolean countOnly)
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
                sqlQuery.getDialect());
            if (!where.equals("true")) {
                sqlQuery.addWhere(where);
            }

            if (countOnly) {
                continue;
            }

            // some DB2 (AS400) versions throw an error, if a column alias is
            // there and *not* used in a subsequent order by/group by
            final SqlQuery.Dialect dialect = sqlQuery.getDialect();
            if (dialect.isAS400()) {
                sqlQuery.addSelect(expr, null);
            } else {
                sqlQuery.addSelect(expr, getColumnAlias(i));
            }

            if (isAggregate()) {
                sqlQuery.addGroupBy(expr);
            }

            // Add ORDER BY clause to make the results deterministic.
            // Derby has a bug with ORDER BY, so ignore it.
            if (ordered) {
                sqlQuery.addOrderBy(expr, true, false, false);
            }
        }

        // add measures
        if (!countOnly) {
            for (int i = 0, count = getMeasureCount(); i < count; i++) {
                addMeasure(i, sqlQuery);
            }
        }
    }
}


// End AbstractQuerySpec.java
