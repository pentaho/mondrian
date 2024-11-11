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
