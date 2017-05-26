/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2017 Pentaho Corporation..  All rights reserved.
*/
package mondrian.spi.impl;

import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Vertica database.
 *
 * @author LBoudreau
 * @since Sept 11, 2009
 */
public class VectorwiseDialect extends IngresDialect {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            VectorwiseDialect.class,
            DatabaseProduct.VECTORWISE);

    /**
     * Creates a VectorwiseDialect.
     *
     * @param connection Connection
     */
    public VectorwiseDialect(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    public DatabaseProduct getDatabaseProduct() {
        return DatabaseProduct.VECTORWISE;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return false;
    }

    @Override
    public boolean requiresHavingAlias() {
        return true;
    }

    @Override
    public boolean requiresAliasForFromQuery() {
        return true;
    }

    @Override
    protected String generateOrderByNulls(
        String expr,
        boolean ascending,
        boolean collateNullsLast)
    {
        // expr may be a column number
        if (StringUtils.isNumeric(expr)) {
            expr = new Integer(expr).toString();
        }
        return super.generateOrderByNulls(
            expr,
            ascending,
            collateNullsLast);
    }
}

// End VectorwiseDialect.java
