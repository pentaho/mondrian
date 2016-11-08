/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.spi.impl;

import mondrian.olap.Util;
import mondrian.spi.Dialect;
import mondrian.spi.DialectFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import javax.sql.DataSource;

/**
 * Implementation of {@link mondrian.spi.DialectFactory} for subclasses
 * of {@link mondrian.spi.impl.JdbcDialectImpl}.
 *
 * <p>Assumes that the dialect has a public constructor that takes a
 * {@link java.sql.Connection} as a parameter.
 */
public class JdbcDialectFactory implements DialectFactory {
    private final Dialect.DatabaseProduct databaseProduct;
    private final Constructor<? extends JdbcDialectImpl> constructor;

    /**
     * Creates a JdbcDialectFactory.
     *
     * @param dialectClass Dialect class
     * @param databaseProduct Database type (e.g. Oracle) if this is a
     * common RDBMS, or null if it is an uncommon one
     */
    public JdbcDialectFactory(
        Class<? extends JdbcDialectImpl> dialectClass,
        Dialect.DatabaseProduct databaseProduct)
    {
        this.databaseProduct = databaseProduct;
        try {
            constructor = dialectClass.getConstructor(Connection.class);
        } catch (NoSuchMethodException e) {
            throw Util.newError(
                e,
                "Class does not contain constructor "
                + "'public <init>(Connection connection)' required "
                + "for subclasses of JdbcDialectImpl");
        }
    }

    /**
     * Creates a temporary connection and calls
     * {@link mondrian.spi.DialectFactory#createDialect(javax.sql.DataSource, java.sql.Connection)}.
     *
     * <p>Helper method, called when {@code createDialect} is called without a
     * {@link java.sql.Connection} and the dialect factory
     * cannot create a dialect with {@link javax.sql.DataSource} alone.
     *
     * <p>It is a user error if {@code dataSource} is null (since this implies
     * that {@code createDialect} was called with {@code dataSource} and
     * {@code connection} both null.</p>
     *
     * @param factory Dialect factory
     * @param dataSource Data source, must not be null
     * @return Dialect, or null if factory cannot create suitable dialect
     */
    public static Dialect createDialectHelper(
        DialectFactory factory,
        DataSource dataSource)
    {
        if (dataSource == null) {
            throw new IllegalArgumentException(
                "Must specify either dataSource or connection");
        }
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            if (connection == null) {
                // DataSource.getConnection does not return null. But
                // a null value here would cause infinite recursion, so
                // let's be cautious.
                throw new IllegalArgumentException();
            }
            final Dialect dialect =
                factory.createDialect(dataSource, connection);

            // Close the connection in such a way that if there is a
            // SQLException,
            // (a) we propagate the exception,
            // (b) we don't try to close the connection again.
            Connection connection2 = connection;
            connection = null;
            connection2.close();
            return dialect;
        } catch (SQLException e) {
            throw Util.newError(
                e,
                "Error while creating dialect");
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    public Dialect createDialect(DataSource dataSource, Connection connection) {
        // If connection is null, create a temporary connection and
        // recursively call this method.
        if (connection == null) {
            return createDialectHelper(this, dataSource);
        }

        assert connection != null;
        if (acceptsConnection(connection)) {
            try {
                return constructor.newInstance(connection);
            } catch (InstantiationException e) {
                throw Util.newError(
                    e, "Error while instantiating dialect");
            } catch (IllegalAccessException e) {
                throw Util.newError(
                    e, "Error while instantiating dialect");
            } catch (InvocationTargetException e) {
                throw Util.newError(
                    e, "Error while instantiating dialect");
            }
        }
        return null;
    }

    /**
     * Returns whether this dialect is suitable for the given connection.
     *
     * @param connection Connection
     * @return Whether suitable
     */
    protected boolean acceptsConnection(Connection connection) {
        try {
            final DatabaseMetaData metaData = connection.getMetaData();
            final String productName = metaData.getDatabaseProductName();
            final String productVersion = metaData.getDatabaseProductVersion();
            final Dialect.DatabaseProduct product =
                JdbcDialectImpl.getProduct(productName, productVersion);
            return product == this.databaseProduct;
        } catch (SQLException e) {
            throw Util.newError(
                e, "Error while instantiating dialect");
        }
    }
}

// End JdbcDialectFactory.java
