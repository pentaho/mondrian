/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap.agg;

import mondrian.rolap.RolapStar;
import mondrian.rolap.sql.SqlQuery;
import mondrian.util.Bug;

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

    public abstract int getMeasureCount();
    public abstract RolapStar.Measure getMeasure(final int i);
    public abstract String getMeasureAlias(final int i);
    public abstract RolapStar.Column[] getColumns();
    public abstract String getColumnAlias(final int i);
    public abstract ColumnConstraint[] getConstraints(final int i);
    public abstract String generateSqlQuery();

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

            ColumnConstraint[] constraints = getConstraints(i);
            if (constraints != null) {
                sqlQuery.addWhere(RolapStar.Column.createInExpr(expr,
                                               constraints,
                                               column.isNumeric()));
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
