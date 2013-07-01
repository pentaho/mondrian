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

import java.sql.SQLException;
import java.util.*;

 /**
 * Callback by which a {@link mondrian.olap.MondrianServer} finds its
 * databases, catalogs and schemas.
 *
 * <p>An important implementation is {@link ImplicitRepository}. This
 * encapsulates the behavior of embedded mondrian: there is no repository,
 * and each connection specifies schema and catalog on the connect string.
 * This is the reason that several methods contain a
 * {@link mondrian.rolap.RolapConnection connection} parameter. Other
 * implementations of this interface will probably ignore the connection.
 *
 * @author Julian Hyde
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

    /**
     * Returns an OlapConnection object.
     * @param server The MondrianServer to use.
     * @param databaseName The database name. Can be null.
     * @param catalogName The catalog name. Can be null.
     * @param roleName The role name. Can be null.
     * @param props Additional connection properties.
     * @return An opened olap connection.
     * @throws SQLException If an error is encountered while
     * creating the connection.
     */
    OlapConnection getConnection(
        MondrianServer server,
        String databaseName,
        String catalogName,
        String roleName,
        Properties props)
        throws SQLException;

    /**
     * Shuts down and terminates a repository.
     */
    void shutdown();
}

// End Repository.java
