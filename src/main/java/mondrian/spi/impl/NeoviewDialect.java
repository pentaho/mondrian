/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2009-2009 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Neoview database.
 *
 * @author jhyde
 * @since Dec 4, 2009
 */
public class NeoviewDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            NeoviewDialect.class,
            DatabaseProduct.NEOVIEW);

    /**
     * Creates a NeoviewDialect.
     *
     * @param connection Connection
     */
    public NeoviewDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean _supportsOrderByNullsLast() {
        return true;
    }

    public boolean requiresOrderByAlias() {
        return true;
    }

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    public boolean allowsDdl() {
        // We get the following error in the test environment. It might be a bit
        // pessimistic to say DDL is never allowed.
        //
        // ERROR[1116] The current partitioning scheme requires a user-specified
        // clustering key on object NEO.PENTAHO."foo"
        return false;
    }

    public boolean supportsGroupByExpressions() {
        return false;
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineForAnsi(
            "t", columnNames, columnTypes, valueList, true);
    }
}

// End NeoviewDialect.java
