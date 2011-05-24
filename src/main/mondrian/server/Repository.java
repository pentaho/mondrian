/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2011 Julian Hyde
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
import java.util.Properties;

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
    /**
     * Returns a list of database names found in this repository.
     * @param connection A connection object from which to obtain
     * the metadata. May be null or the Repository implementation
     * itself might ignore it.
     * @return A list of database names found in this repository.
     */
    List<String> getDatabaseNames(
        RolapConnection connection);

    /**
     * Returns a list of catalog names found in the repository.
     * @param connection A connection object from which to obtain
     * the metadata. May be null or the Repository implementation
     * itself might ignore it.
     * @param databaseName The parent database name of which we
     * want to list the catalogs.
     * @return A list of catalog names found in this repository.
     */
    List<String> getCatalogNames(
        RolapConnection connection,
        String databaseName);

    /**
     * Must return a map of schema names and schema objects
     * who are children of the specified datasource and catalog.
     * @param connection The connection from which to obtain
     * the metadata. May be null or the Repository implementation
     * itself might ignore it.
     * @param databaseName The database name predicate for the list
     * of returned schemas.
     * @param catalogName The catalog name predicate for the list
     * of returned schemas.
     * @return A map of schema names associated to schema objects,
     * as found in the repository..
     */
    Map<String, RolapSchema> getRolapSchemas(
        RolapConnection connection,
        String databaseName,
        String catalogName);

    /**
     * Returns a list of databases properties collections,
     * one per database configured on this server.
     * @param connection The connection from which to obtain
     * the metadata. May be null or the Repository implementation
     * itself might ignore it.
     * @return A list of databases properties collections
     */
    List<Map<String, Object>> getDatabases(
        RolapConnection connection);

    OlapConnection getConnection(
        MondrianServer server,
        String catalogName,
        String schemaName,
        String roleName,
        Properties props)
        throws SQLException;

    /**
     * Shuts down and terminates a repository.
     */
    void shutdown();
}

// End Repository.java
