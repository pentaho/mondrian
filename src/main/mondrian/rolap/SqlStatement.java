/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.Util;

import mondrian.spi.Dialect;
import mondrian.util.DelegatingInvocationHandler;

import javax.sql.DataSource;

import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

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
    private final List<Type> types;
    private final int maxRows;
    private final int firstRowOrdinal;
    private final String component;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private final RolapUtil.Semaphore querySemaphore =
        RolapUtil.getQuerySemaphore();
    private final String message;
    private boolean haveSemaphore;
    public int rowCount;
    private long startTime;
    private final List<Accessor> accessors = new ArrayList<Accessor>();

    // used for SQL logging, allows for a SQL Statement UID
    private static long executeCount = -1;
    private boolean done;

    private static final Logger LOG = Logger.getLogger(SqlStatement.class);
    private final Dialect dialect;

    /**
     * Creates a SqlStatement.
     *
     * @param dataSource Data source
     * @param sql SQL
     * @param types Suggested types of columns, or null;
     *     if present, must have one element for each SQL column;
     *     each not-null entry overrides deduced JDBC type of the column
     * @param maxRows Maximum rows; <= 0 means no maximum
     * @param firstRowOrdinal Ordinal of first row to skip to; <= 0 do not skip
     * @param component Description of component/purpose of this statement
     * @param message Error message
     * @param resultSetType Result set type
     * @param resultSetConcurrency Result set concurrency
     */
    SqlStatement(
        DataSource dataSource,
        String sql,
        List<Type> types,
        int maxRows,
        int firstRowOrdinal,
        String component,
        String message,
        int resultSetType,
        int resultSetConcurrency,
        Dialect dialect)
    {
        this.dataSource = dataSource;
        this.sql = sql;
        this.types = types;
        this.maxRows = maxRows;
        this.firstRowOrdinal = firstRowOrdinal;
        this.component = component;
        this.message = message;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.dialect = dialect;
    }

    /**
     * Executes the current statement, and handles any SQLException.
     */
    public void execute() {
        long currId = 0;
        String status = "failed";
        Statement statement = null;
        try {
            this.jdbcConnection = dataSource.getConnection();
            querySemaphore.enter();
            haveSemaphore = true;
            // Trace start of execution.
            if (RolapUtil.SQL_LOGGER.isDebugEnabled()) {
                currId = ++executeCount;
                StringBuilder sqllog = new StringBuilder();
                sqllog.append(currId)
                    .append(": ")
                    .append(component)
                    .append(": executing sql [");
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

            // skip to first row specified in request
            this.done = false;
            if (firstRowOrdinal > 0) {
                if (resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
                    for (int i = 0; i < firstRowOrdinal; ++i) {
                        if (!this.resultSet.next()) {
                            this.done = true;
                            break;
                        }
                    }
                } else {
                    this.done = !this.resultSet.absolute(firstRowOrdinal);
                }
            }

            long time = System.currentTimeMillis();
            final long execMs = time - startTime;
            Util.addDatabaseTime(execMs);
            status = ", exec " + execMs + " ms";

            // Compute accessors. They ensure that we use the most efficient
            // method (e.g. getInt, getDouble, getObject) for the type of the
            // column. Even if you are going to box the result into an object,
            // it is better to use getInt than getObject; the latter might
            // return something daft like a BigDecimal (does, on the Oracle JDBC
            // driver).
            accessors.clear();
            for (Type type : guessTypes()) {
                accessors.add(createAccessor(accessors.size(), type));
            }
        } catch (Exception e) {
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

        // According to the JDBC spec, closing a statement automatically closes
        // its result sets, and closing a connection automatically closes its
        // statements. But let's be conservative and close everything
        // explicitly.
        Statement statement = null;
        if (resultSet != null) {
            try {
                statement = resultSet.getStatement();
                resultSet.close();
            } catch (SQLException e) {
                throw Util.newError(message + "; sql=[" + sql + "]");
            } finally {
                resultSet = null;
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                throw Util.newError(message + "; sql=[" + sql + "]");
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
        String status =
            ", exec+fetch " + totalMs + " ms, " + rowCount + " rows";

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

    private static Type getDecimalType(
        int precision,
        int scale,
        Dialect dialect)
    {
        // Dialect might be null. This can happen when Mondrian issues a first
        // query and tries to figure out what dialect to use. Watch out
        // for NPEs.
        if (dialect != null
            && dialect.getDatabaseProduct() == Dialect.DatabaseProduct.NETEZZA
            && scale == 0
            && precision == 38)
        {
            // Neteeza marks longs as scale 0 and precision 38.
            // An int would overflow.
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Using type LONG for Neteeza scale 0 and precision 38.");
            }
            return Type.LONG;
        } else if ((scale == 0 || scale == -127)
            && (precision <= 9 || precision == 38))
        {
            // An int (up to 2^31 = 2.1B) can hold any NUMBER(10, 0) value
            // (up to 10^9 = 1B). NUMBER(38, 0) is conventionally used in
            // Oracle for integers of unspecified precision, so let's be
            // bold and assume that they can fit into an int.
            //
            // Oracle also seems to sometimes represent integers as
            // (type=NUMERIC, precision=0, scale=-127) for reasons unknown.
            return Type.INT;
        } else {
            return Type.DOUBLE;
        }
    }

    /**
     * Chooses the most appropriate type for accessing the values of a
     * column in a result set.
     *
     * <p>NOTE: It is possible that this method is driver-dependent. If this is
     * the case, move it to {@link mondrian.spi.Dialect}.
     *
     * @param suggestedType Type suggested by Level.internalType attribute
     * @param metaData Result set metadata
     * @param i Column ordinal (0-based)
     * @return Best client type
     * @throws SQLException on error
     */
    public static Type guessType(
        Type suggestedType,
        ResultSetMetaData metaData,
        int i,
        Dialect dialect)
        throws SQLException
    {
        final String columnName = metaData.getColumnName(i + 1);
        final String typeName = metaData.getColumnTypeName(i + 1);
        if (suggestedType != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "SqlStatement.guessType - Column "
                    + columnName
                    + " is of explicit type "
                    + suggestedType.name());
            }
            return suggestedType;
        }
        final int columnType = metaData.getColumnType(i + 1);
        final int precision = metaData.getPrecision(i + 1);
        final int scale = metaData.getScale(i + 1);
        final String columnLabel = metaData.getColumnLabel(i + 1);
        switch (columnType) {
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BOOLEAN:
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "SqlStatement.guessType - Column "
                    + columnName
                    + " is of internal type INT. JDBC type was "
                    + columnType);
            }
            return Type.INT;
        case Types.NUMERIC:
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "SqlStatement.guessType - Column "
                    + columnName
                    + " has precision "
                    + precision
                    + " and scale "
                    + scale
                    + " for JDBC type "
                    + typeName);
            }
            if (precision == 0
                && (scale == 0 || scale == -127)
                && (typeName.equalsIgnoreCase("NUMBER")
                    || (typeName.equalsIgnoreCase("NUMERIC"))))
            {
                // An int (up to 2^31 = 2.1B) can hold any NUMBER(10, 0) value
                // (up to 10^9 = 1B). NUMBER(38, 0) is conventionally used in
                // Oracle for integers of unspecified precision, so let's be
                // bold and assume that they can fit into an int.
                //
                // There is a further problem. In GROUPING SETS queries, Oracle
                // loosens the type of columns compared to mere GROUP BY
                // queries. We need integer GROUP BY columns to remain integers,
                // otherwise the segments won't be found; but if we convert
                // measure (whose column names are like "m0", "m1") to integers,
                // data loss will occur.
                if (columnName.startsWith("m")) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                            "SqlStatement.guessType - Column "
                            + columnName
                            + " is of internal type OBJECT. JDBC type was "
                            + columnType);
                    }
                    return Type.OBJECT;
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                            "SqlStatement.guessType - Column "
                            + columnName
                            + " is of internal type INT. JDBC type was "
                            + columnType);
                    }
                    return Type.INT;
                }
            }
            final Type decimalType = getDecimalType(precision, scale, dialect);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "SqlStatement.guessType - Column "
                    + columnName
                    + " is of internal type "
                    + decimalType.name()
                    + ". JDBC type was "
                    + columnType);
            }
            return decimalType;
        case Types.DECIMAL:
            final Type dt = getDecimalType(precision, scale, dialect);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "SqlStatement.guessType - Column "
                    + columnName
                    + " is of internal type "
                    + dt.name()
                    + ". JDBC type was "
                    + columnType);
            }
            return dt;

        case Types.DOUBLE:
        case Types.FLOAT:
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "SqlStatement.guessType - Column "
                    + columnName
                    + " is of internal type DOUBLE. JDBC type was "
                    + columnType);
            }
            return Type.DOUBLE;
        case Types.BIGINT:
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "SqlStatement.guessType - Column "
                    + columnName
                    + " is of internal type LONG. JDBC type was "
                    + columnType);
            }
            return Type.LONG;
        default:
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "SqlStatement.guessType - Column "
                    + columnName
                    + " is of internal type OBJECT. JDBC type was "
                    + columnType);
            }
            return Type.OBJECT;
        }
    }

    private Accessor createAccessor(int column, Type type) {
        final int columnPlusOne = column + 1;
        switch (type) {
        case OBJECT:
            return new Accessor() {
                public Object get() throws SQLException {
                    return resultSet.getObject(columnPlusOne);
                }
            };
        case STRING:
            return new Accessor() {
                public Object get() throws SQLException {
                    return resultSet.getString(columnPlusOne);
                }
            };
        case INT:
            return new Accessor() {
                public Object get() throws SQLException {
                    final int val = resultSet.getInt(columnPlusOne);
                    if (val == 0 && resultSet.wasNull()) {
                        return null;
                    }
                    return val;
                }
            };
        case LONG:
            return new Accessor() {
                public Object get() throws SQLException {
                    final long val = resultSet.getLong(columnPlusOne);
                    if (val == 0 && resultSet.wasNull()) {
                        return null;
                    }
                    return val;
                }
            };
        case DOUBLE:
            return new Accessor() {
                public Object get() throws SQLException {
                    final double val = resultSet.getDouble(columnPlusOne);
                    if (val == 0 && resultSet.wasNull()) {
                        return null;
                    }
                    return val;
                }
            };
        default:
            throw Util.unexpected(type);
        }
    }

    public List<Type> guessTypes() throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        assert this.types == null || this.types.size() == columnCount;
        List<Type> types = new ArrayList<Type>();
        for (int i = 0; i < columnCount; i++) {
            final Type suggestedType =
                this.types == null ? null : this.types.get(i);
            types.add(
                guessType(
                    suggestedType, metaData, i,
                     dialect));
        }
        return types;
    }

    public List<Accessor> getAccessors() throws SQLException {
        return accessors;
    }

    public boolean isDone() {
        return done;
    }

    /**
     * Returns the result set in a proxy which automatically closes this
     * SqlStatement (and hence also the statement and result set) when the
     * result set is closed.
     *
     * <p>This helps to prevent connection leaks. The caller still has to
     * remember to call ResultSet.close(), of course.
     *
     * @return Wrapped result set
     */
    public ResultSet getWrappedResultSet() {
        return (ResultSet) Proxy.newProxyInstance(
            null,
            new Class<?>[] {ResultSet.class},
            new MyDelegatingInvocationHandler(this));
    }

    /**
     * The approximate JDBC type of a column.
     *
     * <p>This type affects which {@link ResultSet} method we use to get values
     * of this column: the default is {@link java.sql.ResultSet#getObject(int)},
     * but we'd prefer to use native values {@code getInt} and {@code getDouble}
     * if possible.
     */
    public enum Type {
        OBJECT,
        DOUBLE,
        INT,
        LONG,
        STRING;

        public Object get(ResultSet resultSet, int column) throws SQLException {
            switch (this) {
            case OBJECT:
                return resultSet.getObject(column + 1);
            case STRING:
                return resultSet.getString(column + 1);
            case INT:
                return resultSet.getInt(column + 1);
            case LONG:
                return resultSet.getLong(column + 1);
            case DOUBLE:
                return resultSet.getDouble(column + 1);
            default:
                throw Util.unexpected(this);
            }
        }
    }

    public interface Accessor {
        Object get() throws SQLException;
    }

    /**
     * Reflectively implements the {@link ResultSet} interface by routing method
     * calls to the result set inside a {@link mondrian.rolap.SqlStatement}.
     * When the result set is closed, so is the SqlStatement, and hence the
     * JDBC connection and statement also.
     */
    // must be public for reflection to work
    public static class MyDelegatingInvocationHandler
        extends DelegatingInvocationHandler
    {
        private final SqlStatement sqlStatement;

        /**
         * Creates a MyDelegatingInvocationHandler.
         *
         * @param sqlStatement SQL statement
         */
        MyDelegatingInvocationHandler(SqlStatement sqlStatement) {
            this.sqlStatement = sqlStatement;
        }

        protected Object getTarget() {
            return sqlStatement.getResultSet();
        }

        /**
         * Helper method to implement {@link java.sql.ResultSet#close()}.
         *
         * @throws SQLException on error
         */
        public void close() throws SQLException {
            sqlStatement.close();
        }
    }
}

// End SqlStatement.java