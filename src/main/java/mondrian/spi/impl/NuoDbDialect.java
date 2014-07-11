/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.olap.Util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the NuoDB database.
 *
 * <p>In order to use NuoDB with Pentaho Mondrian users can only use NuoDB
 * version 2.0.4 or newer.
 *
 * @author rbuck
 * @since Mar 20, 2014
 */
public class NuoDbDialect extends JdbcDialectImpl {
    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            NuoDbDialect.class,
            DatabaseProduct.NUODB);

    /**
     * Creates a NuoDbDialect.
     *
     * @param connection Connection
     */
    public NuoDbDialect(Connection connection) throws SQLException {
        super(connection);
    }

    /**
     * {@inheritDoc}
     *
     * <p>In order to generate a SQL statement to represent an inline dataset,
     * NuoDB requires that you use FROM DUAL.
     *
     * @param columnNames the list of column names
     * @param columnTypes the list of column types
     * @param valueList   the value list
     * @return the generated SQL statement for an inline dataset
     */
    @Override
    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineGeneric(
            columnNames, columnTypes, valueList,
            " FROM DUAL", false);
    }

    /**
     * {@inheritDoc}
     *
     * <p>NuoDB does not yet support ANSI SQL:2003 for DATE literals so we have
     * to cast dates using a function.
     *
     * @param buf   Buffer to append to
     * @param value Value as string
     * @param date  Value as date
     */
    @Override
    protected void quoteDateLiteral(
        StringBuilder buf,
        String value,
        Date date)
    {
        buf.append("DATE(");
        Util.singleQuoteString(value, buf);
        buf.append(")");
    }

    /**
     * {@inheritDoc}
     *
     * <p>The NuoDB JDBC driver lists " " as the string to use for quoting,
     * but we
     * know better. Ideally the quotation character ought to have been "`" but
     * if that is used and a generated query uses non quoted object names, not-
     * found exceptions will occur for the object. So we here fall back to using
     * the double quote character. We ought to investigate why back-tick won't
     * work. But for now this makes all the tests work with Nuo (besides the
     * tweaks above).
     *
     * @param databaseMetaData the database metadata from the connection
     * @return the quotation character
     */
    @Override
    protected String deduceIdentifierQuoteString(
        DatabaseMetaData databaseMetaData)
    {
        String identifierQuoteString =
            super.deduceIdentifierQuoteString(databaseMetaData);
        if (" ".equals(identifierQuoteString)) {
            identifierQuoteString = "\"";
        }
        return identifierQuoteString;
    }
}

// End NuoDbDialect.java
