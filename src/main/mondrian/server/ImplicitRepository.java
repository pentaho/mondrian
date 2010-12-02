/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
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
 * @version $Id$
 * @author Julian Hyde
 */
public class ImplicitRepository implements Repository {
    public ImplicitRepository() {
        super();
    }

    public List<String> getCatalogNames(RolapConnection connection)
    {
        // A mondrian instance contains one schema, so implicitly it
        // contains one catalog
        return Collections.singletonList(
            connection.getSchema().getName());
    }

    public Map<String, RolapSchema> getCatalogSchemas(
        RolapConnection connection, String catalogName)
    {
        final RolapSchema schema = (RolapSchema) connection.getSchema();
        assert schema.getName().equals(catalogName);
        return Collections.singletonMap(schema.getName(), schema);
    }

    public OlapConnection getConnection(
        MondrianServer server,
        String catalogName,
        String schemaName,
        String roleName)
    {
        // This method does not make sense in an ImplicitRepository. The
        // catalog and schema are gleaned from the connection, not vice
        // versa.
        throw new UnsupportedOperationException();
    }

    public List<Map<String, Object>> getDataSources(
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
}

// End ImplicitRepository.java
