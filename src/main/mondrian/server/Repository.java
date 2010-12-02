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

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
* Callback by which a {@link mondrian.olap.MondrianServer} finds its catalogs
 * and schemas.
 *
 * <p>An important implementation is {@link ImplicitRepository}. This
 * encapsulates the behavior of embedded mondrian: there is no repository,
 * and each connection specifies schema and catalog on the connect string.
 * This is the reason that several methods contain a
 * {@link mondrian.rolap.RolapConnection connection} parameter. Other
 * implementations of this interface will probably ignore the connection.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public interface Repository {
    List<String> getCatalogNames(
        RolapConnection connection);

    Map<String, RolapSchema> getCatalogSchemas(
        RolapConnection connection,
        String catalogName);

    List<Map<String, Object>> getDataSources(
        RolapConnection connection);

    OlapConnection getConnection(
        MondrianServer server,
        String catalogName,
        String schemaName,
        String roleName)
        throws SQLException;
}

// End Repository.java
