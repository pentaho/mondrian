/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2010 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the LucidDB database.
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class MonetDbDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            MonetDbDialect.class,
            DatabaseProduct.MONETDB);

    /**
     * Creates a LucidDbDialect.
     *
     * @param connection Connection
     *
     * @throws java.sql.SQLException on error
     */
    public MonetDbDialect(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    public boolean allowsMultipleDistinctSqlMeasures() {
        return false;
    }
    
    @Override
    public boolean allowsCountDistinct() {
    	return false;
    }
    
    @Override
    public boolean requiresAliasForFromQuery() {
    	return true;
    }
    
    @Override
    public boolean allowsCompoundCountDistinct() {
    	return false;
    }
    
    @Override
    public boolean supportsGroupByExpressions() {
    	return false;
    }
}

// End MonetDbDialect.java
