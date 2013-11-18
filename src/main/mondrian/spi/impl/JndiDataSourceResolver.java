/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2009-2009 Pentaho
// All Rights Reserved.
*/
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
        // FIXME MONGO This needs to be designed with MONDRIAN-1773 in mind.
        // The DS infos have to be stored somewhere outside of the schema.
//        +      // TODO: Extend this API
//        +        if (dataSourceName.equals("mongodb")) {
//        +          return new MongoDBDataSource();
//        +        } else {
//        +          return (DataSource) new InitialContext().lookup(dataSourceName);
//        +//          return new RolapConnection.DelegatingDataSource((DataSource)new InitialContext().lookup(dataSourceName));
//        +        }
        return (DataSource) new InitialContext().lookup(dataSourceName);
    }
}

// End JndiDataSourceResolver.java
