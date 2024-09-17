/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Teradata database.
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class TeradataDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            TeradataDialect.class,
            DatabaseProduct.TERADATA);

    /**
     * Creates a TeradataDialect.
     *
     * @param connection Connection
     */
    public TeradataDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        String fromClause = null;
        if (valueList.size() > 1) {
            // In Teradata, "SELECT 1,2" is valid but "SELECT 1,2 UNION
            // SELECT 3,4" gives "3888: SELECT for a UNION,INTERSECT or
            // MINUS must reference a table."
            fromClause = " FROM (SELECT 1 a) z ";
        }
        return generateInlineGeneric(
            columnNames, columnTypes, valueList, fromClause, true);
    }

    public boolean supportsGroupingSets() {
        return true;
    }

    public boolean requiresUnionOrderByOrdinal() {
        return true;
    }
}

// End TeradataDialect.java
