/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap;

import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapResultShepherd;
import mondrian.rolap.agg.AggregationManager;
import mondrian.server.*;
import mondrian.server.monitor.Monitor;
import mondrian.spi.CatalogLocator;
import mondrian.util.LockBox;

import org.olap4j.OlapConnection;

import java.sql.SQLException;
import java.util.*;

/**
 * Interface by which to control an instance of Mondrian.
 *
 * <p>Typically, there is only one instance of Mondrian per JVM. However, you
 * access a MondrianServer via the {@link #forConnection} method for future
 * expansion.
 *
 * @author jhyde
 * @since Jun 25, 2006
 */
public abstract class MondrianServer {
    /**
     * Returns the MondrianServer that hosts a given connection.
     *
     * @param connection Connection (not null)
     * @return server this connection belongs to (not null)
     */
    public static MondrianServer forConnection(Connection connection) {
        return ((RolapConnection) connection).getServer();
    }

    /**
     * Creates a server.
     *
     * <p>When creating a server, the calling code must call the
     * {@link MondrianServer#shutdown()} method to dispose of it.
     *
     * @param contentFinder Repository content finder
     * @param catalogLocator Catalog locator
     * @return Server that reads from the given repository
     */
    public static MondrianServer createWithRepository(
        RepositoryContentFinder contentFinder,
        CatalogLocator catalogLocator)
    {
        return MondrianServerRegistry.INSTANCE.createWithRepository(
            contentFinder,
            catalogLocator);
    }

    /**
     * Returns the server with the given id.
     *
     * <p>If id is null, returns the catalog-less server. (The catalog-less
     * server can also be acquired using its id.)</p>
     *
     * <p>If server is not found, returns null.</p>
     *
     * @param instanceId Server instance id
     * @return Server, or null if no server with this id
     */
    public static MondrianServer forId(String instanceId) {
        return MondrianServerRegistry.INSTANCE.serverForId(instanceId);
    }

    /**
     * Disposes of a server and cleans up everything.
     *
     * @param instanceId The instance ID of the server
     * to shutdown gracefully.
     */
    public static void dispose(String instanceId) {
        final MondrianServer server =
            forId(instanceId);
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Returns an integer uniquely identifying this server within its JVM.
     *
     * @return Server's unique identifier
     */
    public abstract int getId();

    /**
     * Returns the version of this MondrianServer.
     *
     * @return Server's version
     */
    public MondrianVersion getVersion() {
        return MondrianServerRegistry.INSTANCE.getVersion();
    }

    /**
     * Returns a list of MDX keywords.
     * @return list of MDX keywords
     */
    public abstract List<String> getKeywords();

    public abstract RolapResultShepherd getResultShepherd();

    /**
     * Returns the lock box that can be used to pass objects via their string
     * key.
     *
     * @return Lock box for this server
     */
    public abstract LockBox getLockBox();

    /**
     * Gets a Connection given a catalog (and implicitly the catalog's data
     * source) and the name of a user role.
     *
     * <p>If you want to pass in a role object, and you are making the call
     * within the same JVM (i.e. not RPC), register the role using
     * {@link MondrianServer#getLockBox()} and pass in the moniker
     * for the generated lock box entry. The server will retrieve the role from
     * the moniker.
     *
     * @param catalogName Catalog name
     * @param schemaName Schema name
     * @param roleName User role name
     * @return Connection
     * @throws SQLException If error occurs
     * @throws SecurityException If security error occurs
     */
    public abstract OlapConnection getConnection(
        String catalogName,
        String schemaName,
        String roleName)
        throws SQLException, SecurityException;

    /**
     * Extended version of
     * {@link MondrianServer#getConnection(String, String, String)}
     * taking a list of properties to pass down to the native connection.
     *
     * <p>Gets a Connection given a catalog (and implicitly the catalog's data
     * source) and the name of a user role.
     *
     * <p>If you want to pass in a role object, and you are making the call
     * within the same JVM (i.e. not RPC), register the role using
     * {@link MondrianServer#getLockBox()} and pass in the moniker
     * for the generated lock box entry. The server will retrieve the role from
     * the moniker.
     *
     * @param catalogName Catalog name
     * @param schemaName Schema name
     * @param roleName User role name
     * @param props Properties to pass down to the native driver.
     * @return Connection
     * @throws SQLException If error occurs
     * @throws SecurityException If security error occurs
     */
    public abstract OlapConnection getConnection(
        String catalogName,
        String schemaName,
        String roleName,
        Properties props)
        throws SQLException, SecurityException;

    /**
     * Returns a list of the databases in this server. One element
     * per database, each element a map whose keys are the XMLA fields
     * describing a data source: "DataSourceName", "DataSourceDescription",
     * "URL", etc. Unrecognized fields are ignored.
     *
     * @return List of data source definitions
     * @param connection Connection
     */
    public abstract List<Map<String, Object>> getDatabases(
        RolapConnection connection);

    public abstract CatalogLocator getCatalogLocator();

    /**
     * Called when the server must terminate all background tasks
     * and cleanup all potential memory leaks.
     */
    public abstract void shutdown();

    /**
     * Called just after a connection has been created.
     *
     * @param connection Connection
     */
    public abstract void addConnection(RolapConnection connection);

    /**
     * Called when a connection is closed.
     *
     * @param connection Connection
     */
    public abstract void removeConnection(RolapConnection connection);

    /**
     * Retrieves a connection.
     *
     * @param connectionId Connection id, per
     *   {@link mondrian.rolap.RolapConnection#getId()}
     *
     * @return Connection, or null if connection is not registered
     */
    public abstract RolapConnection getConnection(int connectionId);

    /**
     * Called just after a statement has been created.
     *
     * @param statement Statement
     */
    public abstract void addStatement(Statement statement);

    /**
     * Called when a statement is closed.
     *
     * @param statement Statement
     */
    public abstract void removeStatement(Statement statement);

    public abstract Monitor getMonitor();

    public abstract AggregationManager getAggregationManager();

    /**
     * Description of the version of the server.
     */
    public interface MondrianVersion {
        /**
         * Returns the version string, for example "2.3.0".
         *
         * @see java.sql.DatabaseMetaData#getDatabaseProductVersion()
         * @return Version of this server
         */
        String getVersionString();

        /**
         * Returns the major part of the version number.
         *
         * <p>For example, if the full version string is "2.3.0", the major
         * version is 2.
         *
         * @return major part of the version number
         * @see java.sql.DatabaseMetaData#getDatabaseMajorVersion()
         */
        int getMajorVersion();

        /**
         * Returns the minor part of the version number.
         *
         * <p>For example, if the full version string is "2.3.0", the minor
         * version is 3.
         *
         * @return minor part of the version number
         *
         * @see java.sql.DatabaseMetaData#getDatabaseProductVersion()
         */
        int getMinorVersion();

        /**
         * Retrieves the name of this database product.
         *
         * @return database product name
         * @see java.sql.DatabaseMetaData#getDatabaseProductName()
         */
        String getProductName();
    }

}

// End MondrianServer.java
