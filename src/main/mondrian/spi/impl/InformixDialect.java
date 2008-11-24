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
 * Implementation of {@link mondrian.spi.Dialect} for the Informix database.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2008
 */
public class InformixDialect extends JdbcDialectImpl {

    /**
     * Creates an InformixDialect.
     *
     * @param quoteIdentifierString String used to quote identifiers
     * @param productName Product name per JDBC driver
     * @param productVersion Product version per JDBC driver
     * @param supportedResultSetTypes Supported result set types
     * @param readOnly Whether database is read-only
     * @param maxColumnNameLength Maximum column name length
     */
    InformixDialect(
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

    public boolean allowsFromQuery() {
        return false;
    }
}

// End InformixDialect.java
