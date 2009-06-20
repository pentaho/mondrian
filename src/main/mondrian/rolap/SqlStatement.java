/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.Util;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * SqlStatement contains a SQL statement and associated resources throughout
 * its lifetime.
 *
 * <p>The goal of SqlStatement is to make tracing, error-handling and
 * resource-management easier. None of the methods throws a SQLException;
 * if an error occurs in one of the methods, the method wraps the exception
 * in a {@link RuntimeException} describing the high-level operation, logs
 * that the operation failed, and throws that RuntimeException.
 *
 * <p>If methods succeed, the method generates lifecycle logging such as
 * the elapsed time and number of rows fetched.
 *
 * <p>There are a few obligations on the caller. The caller must:<ul>
 * <li>call the {@link #handle(Exception)} method if one of the contained
 *     objects (say the {@link java.sql.ResultSet}) gives an error;
 * <li>call the {@link #close()} method if all operations complete
 *     successfully.
 * <li>increment the {@link #rowCount} field each time a row is fetched.
 * </ul>
 *
 * <p>The {@link #close()} method is idempotent. You are welcome to call it
 * more than once.
 *
 * <p>SqlStatement is not thread-safe.
 *
 * @version $Id$
 * @author jhyde
 * @since 2.3
 */
public class SqlStatement {

    private final DataSource dataSource;
    private Connection jdbcConnection;
    private ResultSet resultSet;
    private final String sql;
    private final int maxRows;
    private final String component;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private final RolapUtil.Semaphore querySemaphore = RolapUtil
        .getQuerySemaphore();
    private final String message;
    private boolean haveSemaphore;
    public int rowCount;
    private long startTime;

    // used for SQL logging, allows for a SQL Statement UID
    private static long executeCount = -1;

    SqlStatement(
        DataSource dataSource,
        String sql,
        int maxRows,
        String component,
        String message,
        int resultSetType,
        int resultSetConcurrency)
    {
        this.dataSource = dataSource;
        this.sql = sql;
        this.maxRows = maxRows;
        this.component = component;
        this.message = message;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
    }

    public void execute() throws SQLException {
        this.jdbcConnection = dataSource.getConnection();
        querySemaphore.enter();
        haveSemaphore = true;
        Statement statement = null;
        String status = "failed";
        long currId = 0;
        // Trace start of execution.
        if (RolapUtil.SQL_LOGGER.isDebugEnabled()) {
            currId = ++executeCount;
            StringBuffer sqllog = new StringBuffer();
            sqllog.append(currId + ": " + component + ": executing sql [");
            if (sql.indexOf('\n') >= 0) {
                // SQL appears to be formatted as multiple lines. Make it
                // start on its own line.
                sqllog.append("\n");
            }
            sqllog.append(sql);
            sqllog.append(']');
            RolapUtil.SQL_LOGGER.debug(sqllog.toString());
        }

        // Execute hook.
        RolapUtil.ExecuteQueryHook hook = RolapUtil.threadHooks.get();
        if (hook != null) {
            hook.onExecuteQuery(sql);
        }
        try {
            startTime = System.currentTimeMillis();
            if (resultSetType < 0 || resultSetConcurrency < 0) {
                statement = jdbcConnection.createStatement();
            } else {
                statement = jdbcConnection.createStatement(
                    resultSetType,
                    resultSetConcurrency);
            }
            if (maxRows > 0) {
                statement.setMaxRows(maxRows);
            }
            this.resultSet = statement.executeQuery(sql);
            long time = System.currentTimeMillis();
            final long execMs = time - startTime;
            Util.addDatabaseTime(execMs);
            status = ", exec " + execMs + " ms";
        } catch (SQLException e) {
            status = ", failed (" + e + ")";
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e2) {
                // ignore
            }
            throw handle(e);
        } finally {
            RolapUtil.SQL_LOGGER.debug(currId + ": " + status);

            if (RolapUtil.LOGGER.isDebugEnabled()) {
                RolapUtil.LOGGER.debug(
                    component + ": executing sql [" + sql + "]" + status);
            }
        }
    }

    /**
     * Closes all resources (statement, result set) held by this
     * SqlStatement.
     *
     * <p>If any of them fails, wraps them in a
     * {@link RuntimeException} describing the high-level operation which
     * this statement was performing. No further error-handling is required
     * to produce a descriptive stack trace, unless you want to absorb the
     * error.
     */
    public void close() {
        if (haveSemaphore) {
            haveSemaphore = false;
            querySemaphore.leave();
        }
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                throw Util.newError(message + "; sql=[" + sql + "]");
            } finally {
                resultSet = null;
            }
        }
        if (jdbcConnection != null) {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                throw Util.newError(message + "; sql=[" + sql + "]");
            } finally {
                jdbcConnection = null;
            }
        }
        long time = System.currentTimeMillis();
        long totalMs = time - startTime;
        String status = ", exec+fetch " + totalMs + " ms, " + rowCount + " rows";

        RolapUtil.SQL_LOGGER.debug(executeCount + ": " + status);

        if (RolapUtil.LOGGER.isDebugEnabled()) {
            RolapUtil.LOGGER.debug(
                component + ": done executing sql [" + sql + "]" + status);
        }
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    /**
     * Handles an exception thrown from the ResultSet, implicitly calls
     * {@link #close}, and returns an exception which includes the full
     * stack, including a description of the high-level operation.
     *
     * @param e Exception
     * @return Runtime exception
     */
    public RuntimeException handle(Exception e) {
        RuntimeException runtimeException =
            Util.newError(e, message + "; sql=[" + sql + "]");
        try {
            close();
        } catch (RuntimeException re) {
            // ignore
        }
        return runtimeException;
    }
}

// End SqlStatement.java
