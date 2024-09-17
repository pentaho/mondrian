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

import mondrian.spi.DataSourceResolver;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 * Implementation of {@link mondrian.spi.DataSourceResolver} that looks up
 * a data source using JNDI.
 *
 * @author jhyde
 */
public class JndiDataSourceResolver implements DataSourceResolver {
    /**
     * Public constructor, required for plugin instantiation.
     */
    public JndiDataSourceResolver() {
    }

    public DataSource lookup(String dataSourceName) throws NamingException {
        return (DataSource) new InitialContext().lookup(dataSourceName);
    }
}

// End JndiDataSourceResolver.java
