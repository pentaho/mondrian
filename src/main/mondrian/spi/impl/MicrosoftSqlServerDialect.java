/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Microsoft SQL Server
 * database.
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class MicrosoftSqlServerDialect extends JdbcDialectImpl {

    private final DateFormat df =
        new SimpleDateFormat("yyyyMMdd");

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            MicrosoftSqlServerDialect.class,
            DatabaseProduct.MSSQL);

    /**
     * Creates a MicrosoftSqlServerDialect.
     *
     * @param connection Connection
     */
    public MicrosoftSqlServerDialect(Connection connection) throws SQLException
    {
        super(connection);
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineGeneric(
            columnNames, columnTypes, valueList, null, false);
    }

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    public boolean requiresUnionOrderByOrdinal() {
        return false;
    }

    protected void quoteDateLiteral(StringBuilder buf, String value, Date date)
    {
        buf.append("CONVERT(DATE, '");
        buf.append(df.format(date));
        // Format 112 is equivalent to "yyyyMMdd" in Java.
        // See http://msdn.microsoft.com/en-us/library/ms187928.aspx
        buf.append("', 112)");
    }
}

// End MicrosoftSqlServerDialect.java
