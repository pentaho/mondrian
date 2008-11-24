/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2008-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import java.util.List;
import java.util.Set;
import java.util.Calendar;
import java.sql.Date;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Microsoft Access
 * database (also called the JET Engine).
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2008
 */
public class AccessDialect extends JdbcDialectImpl {

    /**
     * Creates an AccessDialect.
     *
     * @param quoteIdentifierString String used to quote identifiers
     * @param productName Product name per JDBC driver
     * @param productVersion Product version per JDBC driver
     * @param supportedResultSetTypes Supported result set types
     * @param readOnly Whether database is read-only
     * @param maxColumnNameLength Maximum column name length
     */
    AccessDialect(
        String quoteIdentifierString,
        String productName,
        String productVersion,
        Set<List<Integer>> supportedResultSetTypes,
        boolean readOnly,
        int maxColumnNameLength)
    {
        super(
            quoteIdentifierString,
            productName,
            productVersion,
            supportedResultSetTypes,
            readOnly,
            maxColumnNameLength);
    }

    public String toUpper(String expr) {
        return "UCASE(" + expr + ")";
    }

    public String caseWhenElse(String cond, String thenExpr, String elseExpr) {
        return "IIF(" + cond + "," + thenExpr + "," + elseExpr + ")";
    }

    protected void quoteDateLiteral(
        StringBuilder buf,
        String value,
        Date date)
    {
        // Access accepts #01/23/2008# but not SQL:2003 format.
        buf.append("#");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        buf.append(calendar.get(Calendar.MONTH) + 1);
        buf.append("/");
        buf.append(calendar.get(Calendar.DAY_OF_MONTH));
        buf.append("/");
        buf.append(calendar.get(Calendar.YEAR));
        buf.append("#");
    }

    public boolean allowsCountDistinct() {
        return false;
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        // Fall back to using the FoodMart 'days' table, because
        // Access SQL has no way to generate values not from a table.
        return generateInlineGeneric(
            columnNames, columnTypes, valueList,
            " from `days` where `day` = 1", false);
    }
}

// End AccessDialect.java
