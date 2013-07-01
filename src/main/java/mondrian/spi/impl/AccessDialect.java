/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import java.sql.*;
import java.util.Calendar;
import java.util.List;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Microsoft Access
 * database (also called the JET Engine).
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class AccessDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            AccessDialect.class,
            DatabaseProduct.ACCESS);

    /**
     * Creates an AccessDialect.
     *
     * @param connection Connection
     */
    public AccessDialect(Connection connection) throws SQLException {
        super(connection);
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

    @Override
    protected String generateOrderByNulls(
        String expr,
        boolean ascending,
        boolean collateNullsLast)
    {
        if (collateNullsLast) {
            if (ascending) {
                return "Iif(" + expr + " IS NULL, 1, 0), " + expr + " ASC";
            } else {
                return "Iif(" + expr + " IS NULL, 1, 0), " + expr + " DESC";
            }
        } else {
            if (ascending) {
                return "Iif(" + expr + " IS NULL, 0, 1), " + expr + " ASC";
            } else {
                return "Iif(" + expr + " IS NULL, 0, 1), " + expr + " DESC";
            }
        }
    }

    public boolean requiresUnionOrderByExprToBeInSelectClause() {
        return true;
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
