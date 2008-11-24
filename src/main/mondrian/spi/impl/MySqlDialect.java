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

/**
 * Implementation of {@link mondrian.spi.Dialect} for the MySQL database.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2008
 */
public class MySqlDialect extends JdbcDialectImpl {

    /**
     * Creates a MySqlDialect.
     *
     * @param quoteIdentifierString String used to quote identifiers
     * @param productName Product name per JDBC driver
     * @param productVersion Product version per JDBC driver
     * @param supportedResultSetTypes Supported result set types
     * @param readOnly Whether database is read-only
     * @param maxColumnNameLength Maximum column name length
     */
    MySqlDialect(
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

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    public boolean allowsFromQuery() {
        // MySQL before 4.0 does not allow FROM
        // subqueries in the FROM clause.
        return productVersion.compareTo("4.") >= 0;
    }

    public boolean allowsCompoundCountDistinct() {
        return true;
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineGeneric(
            columnNames, columnTypes, valueList, null, false);
    }

    public boolean isNullsCollateLast() {
        return false;
    }

    public String forceNullsCollateLast(String expr) {
        String addIsNull = "ISNULL(" + expr + "), ";
        expr = addIsNull + expr;
        return expr;
    }

    public boolean requiresOrderByAlias() {
        return true;
    }

    public boolean supportsMultiValueInExpr() {
        return true;
    }
}

// End MySqlDialect.java
