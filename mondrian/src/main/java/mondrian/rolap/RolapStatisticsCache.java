/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.rolap;

import mondrian.olap.MondrianDef;
import mondrian.rolap.sql.SqlQuery;
import mondrian.server.Execution;
import mondrian.spi.Dialect;
import mondrian.spi.StatisticsProvider;

import java.util.*;
import javax.sql.DataSource;

/**
 * Provides and caches statistics.
 *
 * <p>Wrapper around a chain of {@link mondrian.spi.StatisticsProvider}s,
 * followed by a cache to store the results.</p>
 */
public class RolapStatisticsCache {
    private final RolapStar star;
    private final Map<List, Integer> columnMap = new HashMap<List, Integer>();
    private final Map<List, Integer> tableMap = new HashMap<List, Integer>();
    private final Map<String, Integer> queryMap =
        new HashMap<String, Integer>();

    public RolapStatisticsCache(RolapStar star) {
        this.star = star;
    }

    public int getRelationCardinality(
        MondrianDef.Relation relation,
        String alias,
        int approxRowCount)
    {
        if (approxRowCount >= 0) {
            return approxRowCount;
        }
        if (relation instanceof MondrianDef.Table) {
            final MondrianDef.Table table = (MondrianDef.Table) relation;
            return getTableCardinality(
                null, table.schema, table.name);
        } else {
            final SqlQuery sqlQuery = star.getSqlQuery();
            sqlQuery.addSelect("*", null);
            sqlQuery.addFrom(relation, null, true);
            return getQueryCardinality(sqlQuery.toString());
        }
    }

    private int getTableCardinality(
        String catalog,
        String schema,
        String table)
    {
        final List<String> key = Arrays.asList(catalog, schema, table);
        int rowCount = -1;
        if (tableMap.containsKey(key)) {
            rowCount = tableMap.get(key);
        } else {
            final Dialect dialect = star.getSqlQueryDialect();
            final List<StatisticsProvider> statisticsProviders =
                dialect.getStatisticsProviders();
            final Execution execution =
                new Execution(
                    star.getSchema().getInternalConnection()
                        .getInternalStatement(),
                    0);
            for (StatisticsProvider statisticsProvider : statisticsProviders) {
                rowCount = statisticsProvider.getTableCardinality(
                    dialect,
                    star.getDataSource(),
                    catalog,
                    schema,
                    table,
                    execution);
                if (rowCount >= 0) {
                    break;
                }
            }

            // Note: If all providers fail, we put -1 into the cache, to ensure
            // that we won't try again.
            tableMap.put(key, rowCount);
        }
        return rowCount;
    }

    private int getQueryCardinality(String sql) {
        int rowCount = -1;
        if (queryMap.containsKey(sql)) {
            rowCount = queryMap.get(sql);
        } else {
            final Dialect dialect = star.getSqlQueryDialect();
            final List<StatisticsProvider> statisticsProviders =
                dialect.getStatisticsProviders();
            final Execution execution =
                new Execution(
                    star.getSchema().getInternalConnection()
                        .getInternalStatement(),
                    0);
            for (StatisticsProvider statisticsProvider : statisticsProviders) {
                rowCount = statisticsProvider.getQueryCardinality(
                    dialect, star.getDataSource(), sql, execution);
                if (rowCount >= 0) {
                    break;
                }
            }

            // Note: If all providers fail, we put -1 into the cache, to ensure
            // that we won't try again.
            queryMap.put(sql, rowCount);
        }
        return rowCount;
    }

    public int getColumnCardinality(
        MondrianDef.Relation relation,
        MondrianDef.Expression expression,
        int approxCardinality)
    {
        if (approxCardinality >= 0) {
            return approxCardinality;
        }
        if (relation instanceof MondrianDef.Table
            && expression instanceof MondrianDef.Column)
        {
            final MondrianDef.Table table = (MondrianDef.Table) relation;
            final MondrianDef.Column column = (MondrianDef.Column) expression;
            return getColumnCardinality(
                null,
                table.schema,
                table.name,
                column.name);
        } else {
            final SqlQuery sqlQuery = star.getSqlQuery();
            sqlQuery.setDistinct(true);
            sqlQuery.addSelect(expression.getExpression(sqlQuery), null);
            sqlQuery.addFrom(relation, null, true);
            return getQueryCardinality(sqlQuery.toString());
        }
    }

    private int getColumnCardinality(
        String catalog,
        String schema,
        String table,
        String column)
    {
        final List<String> key = Arrays.asList(catalog, schema, table, column);
        int rowCount = -1;
        if (columnMap.containsKey(key)) {
            rowCount = columnMap.get(key);
        } else {
            final Dialect dialect = star.getSqlQueryDialect();
            final List<StatisticsProvider> statisticsProviders =
                dialect.getStatisticsProviders();
            final Execution execution =
                new Execution(
                    star.getSchema().getInternalConnection()
                        .getInternalStatement(),
                    0);
            for (StatisticsProvider statisticsProvider : statisticsProviders) {
                rowCount = statisticsProvider.getColumnCardinality(
                    dialect,
                    star.getDataSource(),
                    catalog,
                    schema,
                    table,
                    column,
                    execution);
                if (rowCount >= 0) {
                    break;
                }
            }

            // Note: If all providers fail, we put -1 into the cache, to ensure
            // that we won't try again.
            columnMap.put(key, rowCount);
        }
        return rowCount;
    }

    public int getColumnCardinality2(
        DataSource dataSource,
        Dialect dialect,
        String catalog,
        String schema,
        String table,
        String column)
    {
        return -1;
    }
}

// End RolapStatisticsCache.java
