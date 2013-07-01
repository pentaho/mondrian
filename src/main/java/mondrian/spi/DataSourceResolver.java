/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2009-2009 Pentaho
// All Rights Reserved.
*/
package mondrian.spi;

import javax.sql.DataSource;

/**
 * Plugin class that resolves data source name to {@link javax.sql.DataSource}
 * object.
 *
 * <p>The property
 * {@link mondrian.olap.MondrianProperties#DataSourceResolverClass} determines
 * which class to use. The default implementation is
 * {@link mondrian.spi.impl.JndiDataSourceResolver}.
 *
 * @author jhyde
 */
public interface DataSourceResolver {

    /**
     * Converts a data source name to a JDBC data source object.
     *
     * @param dataSourceName Data source name
     * @return JDBC data source, or null if not found
     * @throws Exception on error
     */
    DataSource lookup(String dataSourceName) throws Exception;

}

// End DataSourceResolver.java
