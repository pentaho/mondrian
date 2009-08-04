/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import mondrian.spi.Dialect;

import java.util.List;
import java.util.Calendar;
import java.sql.*;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Microsoft Access
 * database (also called the JET Engine).
 *
 * @author jhyde
 * @version $Id$
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

    public NullCollation getNullCollation() {
        return NullCollation.NEGINF;
    }

    public String generateOrderItem(
        String expr, boolean nullable, boolean ascending)
    {
        if (ascending && nullable) {
            return "Iif(" + expr + " IS NULL, 1, 0), " + expr + " ASC";
        } else {
            return super.generateOrderItem(expr, nullable, ascending);
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
