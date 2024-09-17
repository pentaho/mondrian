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
 * Implementation of {@link mondrian.spi.Dialect} for the Ingres database.
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class IngresDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            IngresDialect.class,
            DatabaseProduct.INGRES);

    /**
     * Creates an IngresDialect.
     *
     * @param connection Connection
     */
    public IngresDialect(Connection connection) throws SQLException {
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

    public boolean requiresOrderByAlias() {
        return true;
    }
}

// End IngresDialect.java
