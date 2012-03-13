/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.olap4j;

import mondrian.rolap.RolapConnection;

import org.olap4j.OlapException;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

/**
 * Implementation of {@link mondrian.olap4j.Factory} for JDBC 4.1.
 *
 * @author jhyde
 */
class FactoryJdbc41Impl implements Factory {
    public Connection newConnection(
        MondrianOlap4jDriver driver,
        String url,
        Properties info)
        throws SQLException
    {
        return new MondrianOlap4jConnectionJdbc41(this, driver, url, info);
    }

    public EmptyResultSet newEmptyResultSet(
        MondrianOlap4jConnection olap4jConnection)
    {
        List<String> headerList = Collections.emptyList();
        List<List<Object>> rowList = Collections.emptyList();
        return new EmptyResultSetJdbc41(
            olap4jConnection, headerList, rowList);
    }

    public ResultSet newFixedResultSet(
        MondrianOlap4jConnection olap4jConnection,
        List<String> headerList,
        List<List<Object>> rowList)
    {
        return new EmptyResultSetJdbc41(
            olap4jConnection, headerList, rowList);
    }

    public MondrianOlap4jCellSet newCellSet(
        MondrianOlap4jStatement olap4jStatement)
    {
        return new MondrianOlap4jCellSetJdbc41(olap4jStatement);
    }

    public MondrianOlap4jStatement newStatement(
        MondrianOlap4jConnection olap4jConnection)
    {
        return new MondrianOlap4jStatementJdbc41(olap4jConnection);
    }

    public MondrianOlap4jPreparedStatement newPreparedStatement(
        String mdx,
        MondrianOlap4jConnection olap4jConnection)
        throws OlapException
    {
        return new MondrianOlap4jPreparedStatementJdbc41(olap4jConnection, mdx);
    }

    public MondrianOlap4jDatabaseMetaData newDatabaseMetaData(
        MondrianOlap4jConnection olap4jConnection,
        RolapConnection mondrianConnection)
    {
        return new MondrianOlap4jDatabaseMetaDataJdbc41(
            olap4jConnection, mondrianConnection);
    }

    // Inner classes

    private static class EmptyResultSetJdbc41
        extends FactoryJdbc4Plus.AbstractEmptyResultSet
    {
        EmptyResultSetJdbc41(
            MondrianOlap4jConnection olap4jConnection,
            List<String> headerList,
            List<List<Object>> rowList)
        {
            super(olap4jConnection, headerList, rowList);
        }

        public <T> T getObject(
            int columnIndex,
            Class<T> type) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public <T> T getObject(
            String columnLabel,
            Class<T> type) throws SQLException
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class MondrianOlap4jConnectionJdbc41
        extends FactoryJdbc4Plus.AbstractConnection
    {
        MondrianOlap4jConnectionJdbc41(
            Factory factory,
            MondrianOlap4jDriver driver,
            String url,
            Properties info) throws SQLException
        {
            super(factory, driver, url, info);
        }

        public void abort(Executor executor) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public void setNetworkTimeout(
            Executor executor,
            int milliseconds) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public int getNetworkTimeout() throws SQLException
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class MondrianOlap4jCellSetJdbc41
        extends FactoryJdbc4Plus.AbstractCellSet
    {
        public MondrianOlap4jCellSetJdbc41(
            MondrianOlap4jStatement olap4jStatement)
        {
            super(olap4jStatement);
        }

        public <T> T getObject(
            int columnIndex,
            Class<T> type) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public <T> T getObject(
            String columnLabel,
            Class<T> type) throws SQLException
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class MondrianOlap4jStatementJdbc41
        extends MondrianOlap4jStatement
    {
        public MondrianOlap4jStatementJdbc41(
            MondrianOlap4jConnection olap4jConnection)
        {
            super(olap4jConnection);
        }

        public void closeOnCompletion() throws SQLException {
            closeOnCompletion = true;
        }

        public boolean isCloseOnCompletion() throws SQLException {
            return closeOnCompletion;
        }
    }

    private static class MondrianOlap4jPreparedStatementJdbc41
        extends FactoryJdbc4Plus.AbstractPreparedStatement
    {
        public MondrianOlap4jPreparedStatementJdbc41(
            MondrianOlap4jConnection olap4jConnection,
            String mdx)
            throws OlapException
        {
            super(olap4jConnection, mdx);
        }

        public void closeOnCompletion() throws SQLException {
            closeOnCompletion = true;
        }

        public boolean isCloseOnCompletion() throws SQLException {
            return closeOnCompletion;
        }
    }

    private static class MondrianOlap4jDatabaseMetaDataJdbc41
        extends FactoryJdbc4Plus.AbstractDatabaseMetaData
    {
        public MondrianOlap4jDatabaseMetaDataJdbc41(
            MondrianOlap4jConnection olap4jConnection,
            RolapConnection mondrianConnection)
        {
            super(olap4jConnection, mondrianConnection);
        }

        public ResultSet getPseudoColumns(
            String catalog,
            String schemaPattern,
            String tableNamePattern,
            String columnNamePattern) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public boolean generatedKeyAlwaysReturned() throws SQLException {
            throw new UnsupportedOperationException();
        }
    }
}

// End FactoryJdbc41Impl.java
