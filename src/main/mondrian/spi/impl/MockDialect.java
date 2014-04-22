/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.spi.*;
import mondrian.util.DelegatingInvocationHandler;

import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Creates a mock {@link Dialect} for purposes such as testing.
 */
public class MockDialect {
    /**
     * Creates a dialect without using a connection.
     *
     * @param product Database product
     * @return dialect of an required persuasion
     */
    public static Dialect of(Dialect.DatabaseProduct product) {
        final DatabaseMetaData metaData =
            (DatabaseMetaData) Proxy.newProxyInstance(
                null,
                new Class<?>[]{DatabaseMetaData.class},
                new DatabaseMetaDataInvocationHandler(product));
        final java.sql.Connection connection =
            (java.sql.Connection) Proxy.newProxyInstance(
                null,
                new Class<?>[] {java.sql.Connection.class},
                new ConnectionInvocationHandler(metaData));
        final Dialect dialect = DialectManager.createDialect(null, connection);
        assert dialect.getDatabaseProduct() == product;
        return dialect;
    }

    // Public only because required for reflection to work.
    @SuppressWarnings("UnusedDeclaration")
    public static class ConnectionInvocationHandler
        extends DelegatingInvocationHandler
    {
        private final DatabaseMetaData metaData;

        ConnectionInvocationHandler(DatabaseMetaData metaData) {
            this.metaData = metaData;
        }

        /** Proxy for {@link java.sql.Connection#getMetaData()}. */
        public DatabaseMetaData getMetaData() {
            return metaData;
        }

        public Statement createStatement() throws SQLException {
            throw new SQLException();
        }
    }

    // Public only because required for reflection to work.
    @SuppressWarnings("UnusedDeclaration")
    public static class DatabaseMetaDataInvocationHandler
        extends DelegatingInvocationHandler
    {
        private final Dialect.DatabaseProduct product;

        DatabaseMetaDataInvocationHandler(
            Dialect.DatabaseProduct product)
        {
            this.product = product;
        }

        /** Proxy for
         * {@link DatabaseMetaData#supportsResultSetConcurrency(int, int)}. */
        public boolean supportsResultSetConcurrency(int type, int concurrency) {
            return false;
        }

        /** Proxy for {@link DatabaseMetaData#getDatabaseProductName()}. */
        public String getDatabaseProductName() {
            switch (product) {
            case GREENPLUM:
                return "postgres greenplum";
            default:
                return product.name();
            }
        }

        /** Proxy for {@link DatabaseMetaData#getIdentifierQuoteString()}. */
        public String getIdentifierQuoteString() {
            return "\"";
        }

        /** Proxy for {@link DatabaseMetaData#getDatabaseProductVersion()}. */
        public String getDatabaseProductVersion() {
            return "1.0";
        }

        /** Proxy for {@link DatabaseMetaData#isReadOnly()}. */
        public boolean isReadOnly() {
            return true;
        }

        /** Proxy for {@link DatabaseMetaData#getMaxColumnNameLength()}. */
        public int getMaxColumnNameLength() {
            return 30;
        }

        /** Proxy for {@link DatabaseMetaData#getDriverName()}. */
        public String getDriverName() {
            switch (product) {
            case GREENPLUM:
                return "Mondrian fake dialect for Greenplum";
            default:
                return "Mondrian fake dialect";
            }
        }

        /** Proxy for {@link DatabaseMetaData#getExtraNameCharacters()}. */
        public String getExtraNameCharacters() {
            return "";
        }

        /** Proxy for
         * {@link DatabaseMetaData#supportsMixedCaseQuotedIdentifiers()}. */
        public boolean supportsMixedCaseQuotedIdentifiers() {
            return true;
        }

        /** Proxy for
         * {@link DatabaseMetaData#supportsMixedCaseIdentifiers()}. */
        public boolean supportsMixedCaseIdentifiers() {
            return true;
        }

        /** Proxy for
         * {@link DatabaseMetaData#storesUpperCaseQuotedIdentifiers()}. */
        public boolean storesUpperCaseQuotedIdentifiers() {
            return false;
        }

        /** Proxy for
         * {@link DatabaseMetaData#storesLowerCaseQuotedIdentifiers()}. */
        public boolean storesLowerCaseQuotedIdentifiers() {
            return false;
        }

        /** Proxy for
         * {@link DatabaseMetaData#storesMixedCaseQuotedIdentifiers()}. */
        public boolean storesMixedCaseQuotedIdentifiers() {
            return false;
        }

        /** Proxy for {@link DatabaseMetaData#storesUpperCaseIdentifiers()}. */
        public boolean storesUpperCaseIdentifiers() {
            return false;
        }

        /** Proxy for {@link DatabaseMetaData#storesLowerCaseIdentifiers()}. */
        public boolean storesLowerCaseIdentifiers() {
            return false;
        }

        /** Proxy for {@link DatabaseMetaData#storesMixedCaseIdentifiers()}. */
        public boolean storesMixedCaseIdentifiers() {
            return false;
        }
    }
}

// End MockDialect.java
