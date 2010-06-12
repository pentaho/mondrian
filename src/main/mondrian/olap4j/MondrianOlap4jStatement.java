/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import mondrian.olap.*;
import org.olap4j.*;
import org.olap4j.Cell;
import org.olap4j.mdx.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link org.olap4j.OlapStatement}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @version $Id$
 * @since May 24, 2007
 */
class MondrianOlap4jStatement implements OlapStatement {
    final MondrianOlap4jConnection olap4jConnection;
    private boolean closed;

    /**
     * Current cell set, or null if the statement is not executing anything.
     * Any method which modifies this member must synchronize
     * on the MondrianOlap4jStatement.
     */
    MondrianOlap4jCellSet openCellSet;
    int timeoutSeconds;

    MondrianOlap4jStatement(
        MondrianOlap4jConnection olap4jConnection)
    {
        assert olap4jConnection != null;
        this.olap4jConnection = olap4jConnection;
        this.closed = false;
    }

    // implement Statement

    public ResultSet executeQuery(String mdx) throws SQLException {
        QueryPart parseTree;
        try {
            parseTree = olap4jConnection.connection.parseStatement(mdx);
        } catch (MondrianException e) {
            throw olap4jConnection.helper.createException(
                "mondrian gave exception while parsing query", e);
        }
        if (!(parseTree instanceof DrillThrough)) {
            throw olap4jConnection.helper.createException(
                "Query does not have relational result. Use a DRILLTHROUGH "
                + "query, or execute using the executeOlapQuery method.");
        }
        DrillThrough drillThrough = (DrillThrough) parseTree;
        CellSet cellSet = executeOlapQueryInternal(drillThrough.getQuery());
        final List<Integer> coords = Collections.nCopies(
            cellSet.getAxes().size(), 0);
        final MondrianOlap4jCell cell =
            (MondrianOlap4jCell) cellSet.getCell(coords);
        return cell.drillThroughInternal(
            drillThrough.getMaxRowCount(),
            drillThrough.getFirstRowOrdinal());
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw olap4jConnection.helper.createException("closed");
        }
    }

    public int executeUpdate(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public synchronized void close() throws SQLException {
        if (!closed) {
            closed = true;
            if (openCellSet != null) {
                CellSet c = openCellSet;
                openCellSet = null;
                c.close();
            }
        }
    }

    public int getMaxFieldSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setMaxFieldSize(int max) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxRows() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setMaxRows(int max) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getQueryTimeout() throws SQLException {
        return timeoutSeconds;
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw olap4jConnection.helper.createException(
                "illegal timeout value " + seconds);
        }
        this.timeoutSeconds = seconds;
    }

    public synchronized void cancel() throws SQLException {
        if (openCellSet != null) {
            openCellSet.query.cancel();
        }
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setCursorName(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean execute(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getResultSet() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getUpdateCount() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean getMoreResults() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getResultSetConcurrency() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getResultSetType() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void addBatch(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void clearBatch() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int[] executeBatch() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Connection getConnection() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int executeUpdate(
        String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int executeUpdate(
        String sql, int columnIndexes[]) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int executeUpdate(
        String sql, String columnNames[]) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public boolean execute(
        String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public boolean execute(
        String sql, int columnIndexes[]) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public boolean execute(
        String sql, String columnNames[]) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isClosed() throws SQLException {
        return closed;
    }

    public void setPoolable(boolean poolable) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    // implement Wrapper

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw olap4jConnection.helper.createException(
            "does not implement '" + iface + "'");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    // implement OlapStatement

    public CellSet executeOlapQuery(String mdx) throws OlapException {
        Query query;
        try {
            query = olap4jConnection.connection.parseQuery(mdx);
        } catch (MondrianException e) {
            throw olap4jConnection.helper.createException(
                "mondrian gave exception while parsing query", e);
        }
        return executeOlapQueryInternal(query);
    }

    /**
     * Executes a parsed query, closing any previously open cellset.
     *
     * @param query Parsed query
     * @return Cell set
     * @throws OlapException if a database error occurs
     */
    protected CellSet executeOlapQueryInternal(
        Query query) throws OlapException
    {
        // Close the previous open CellSet, if there is one.
        synchronized (this) {
            if (openCellSet != null) {
                final MondrianOlap4jCellSet cs = openCellSet;
                openCellSet = null;
                try {
                    cs.close();
                } catch (SQLException e) {
                    throw olap4jConnection.helper.createException(
                        null, "Error while closing previous CellSet", e);
                }
            }

            openCellSet = olap4jConnection.factory.newCellSet(this, query);
        }
        // Release the monitor before executing, to give another thread the
        // opportunity to call cancel.
        try {
            openCellSet.execute();
        } catch (QueryCanceledException e) {
            throw olap4jConnection.helper.createException("Query canceled");
        } catch (QueryTimeoutException e) {
            throw olap4jConnection.helper.createException(e.getMessage());
        }
        return openCellSet;
    }

    public CellSet executeOlapQuery(SelectNode selectNode)
        throws OlapException
    {
        final String mdx = toString(selectNode);
        return executeOlapQuery(mdx);
    }

    /**
     * Converts a {@link org.olap4j.mdx.ParseTreeNode} to MDX string.
     *
     * @param node Parse tree node
     * @return MDX text
     */
    private static String toString(ParseTreeNode node) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ParseTreeWriter parseTreeWriter = new ParseTreeWriter(pw);
        node.unparse(parseTreeWriter);
        pw.flush();
        return sw.toString();
    }
}

// End MondrianOlap4jStatement.java
