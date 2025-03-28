/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.spi.impl;

import mondrian.olap.Util;
import mondrian.server.Execution;
import mondrian.spi.Dialect;
import mondrian.spi.StatisticsProvider;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.sql.*;
import javax.sql.DataSource;

/**
 * Implementation of {@link mondrian.spi.StatisticsProvider} that uses JDBC
 * metadata calls to count rows and distinct values.
 */
public class JdbcStatisticsProvider implements StatisticsProvider {
    private static final Logger LOG =
        LogManager.getLogger(JdbcStatisticsProvider.class);
    public long getTableCardinality(
        Dialect dialect,
        DataSource dataSource,
        String catalog,
        String schema,
        String table,
        Execution execution)
    {
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            resultSet =
                connection
                    .getMetaData()
                    .getIndexInfo(catalog, schema, table, false, true);
            int maxNonUnique = -1;
            while (resultSet.next()) {
                final int type = resultSet.getInt(7); // "TYPE" column
                final int cardinality = resultSet.getInt(11);
                final boolean unique =
                    !resultSet.getBoolean(4); // "NON_UNIQUE" column
                switch (type) {
                case DatabaseMetaData.tableIndexStatistic:
                    return cardinality; // "CARDINALITY" column
                }
                if (!unique) {
                    maxNonUnique = Math.max(maxNonUnique, cardinality);
                }
            }
            // The cardinality of each non-unique index will be the number of
            // non-NULL values in that index. Unless we're unlucky, one of those
            // columns will cover most of the table.
            return maxNonUnique;
        } catch (SQLException e) {
            // We will have to try a count() operation or some other
            // statistics provider in the chain.
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "JdbcStatisticsProvider failed to get the cardinality of the table "
                        + table,
                    e);
            }
            return -1;
        } finally {
            Util.close(resultSet, null, connection);
        }
    }

    public long getQueryCardinality(
        Dialect dialect,
        DataSource dataSource,
        String sql,
        Execution execution)
    {
        // JDBC cannot help with this. Defer to another statistics provider.
        return -1;
    }

    public long getColumnCardinality(
        Dialect dialect,
        DataSource dataSource,
        String catalog,
        String schema,
        String table,
        String column,
        Execution execution)
    {
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            resultSet =
                connection
                    .getMetaData()
                    .getIndexInfo(catalog, schema, table, false, true);
            while (resultSet.next()) {
                int type = resultSet.getInt(7); // "TYPE" column
                switch (type) {
                case DatabaseMetaData.tableIndexStatistic:
                    continue;
                default:
                    String columnName = resultSet.getString(9); //COLUMN_NAME
                    if (columnName != null && columnName.equals(column)) {
                        return resultSet.getInt(11); // "CARDINALITY" column
                    }
                }
            }
            return -1; // information not available, apparently
        } catch (SQLException e) {
            // We will have to try a count() operation or some other
            // statistics provider in the chain.
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "JdbcStatisticsProvider failed to get the cardinality of the table "
                        + table,
                    e);
            }
            return -1;
        } finally {
            Util.close(resultSet, null, connection);
        }
    }
}

// End JdbcStatisticsProvider.java
