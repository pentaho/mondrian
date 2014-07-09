/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.server;

import mondrian.olap.MondrianServer;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapSchema;

import org.olap4j.OlapConnection;
import org.olap4j.impl.Olap4jUtil;

import java.util.*;

/**
 * Implementation of {@link Repository} for
 * a server that doesn't have a repository: each connection in the server
 * has its own catalog (specified in the connect string) and therefore the
 * catalog and schema metadata will be whatever pertains to that connection.
 * (That's why the methods have a connection parameter.)
 *
 * @author Julian Hyde
 */
public class ImplicitRepository implements Repository {
    public ImplicitRepository() {
        super();
    }

    public List<String> getCatalogNames(
        RolapConnection connection,
        String databaseName)
    {
        // In an implicit repository, we assume that there is a single
        // database, a single catalog and a single schema.
        return
            Collections.singletonList(
                connection.getSchema().getName());
    }

    public List<String> getDatabaseNames(RolapConnection connection)
    {
        // In an implicit repository, we assume that there is a single
        // database, a single catalog and a single schema.
        return
            Collections.singletonList(
                connection.getSchema().getName());
    }

    public Map<String, RolapSchema> getRolapSchemas(
        RolapConnection connection,
        String databaseName,
        String catalogName)
    {
        final RolapSchema schema = connection.getSchema();
        assert schema.getName().equals(catalogName);
        return Collections.singletonMap(schema.getName(), schema);
    }

    public OlapConnection getConnection(
        MondrianServer server,
        String databaseName,
        String catalogName,
        String roleName,
        Properties props)
    {
        // This method does not make sense in an ImplicitRepository. The
        // catalog and schema are gleaned from the connection, not vice
        // versa.
        throw new UnsupportedOperationException();
    }

    public List<Map<String, Object>> getDatabases(
        RolapConnection connection)
    {
        return Collections.singletonList(
            Olap4jUtil.<String, Object>mapOf(
                "DataSourceName", connection.getSchema().getName(),
                "DataSourceDescription", null,
                "URL", null,
                "DataSourceInfo", connection.getSchema().getName(),
                "ProviderName", "Mondrian",
                "ProviderType", "MDP",
                "AuthenticationMode", "Unauthenticated"));
    }

    public void shutdown() {
        // ignore.
    }
}

// End ImplicitRepository.java
