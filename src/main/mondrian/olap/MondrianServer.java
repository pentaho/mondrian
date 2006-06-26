/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

/**
 * Interface by which to control an instance of Mondrian.
 *
 * <p>Typically, there is only one instance of Mondrian per JVM. However, you
 * access a MondrianServer via the {@link #forConnection} method for future
 * expansion.
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 25, 2006
 */
public abstract class MondrianServer {
    private static MondrianServer instance = new MondrianServerImpl();

    /**
     * Returns the MondrianServer which hosts a given connection.
     * @param connection Connection
     */
    public static MondrianServer forConnection(Connection connection) {
        // Mondrian server is currently a singleton, so the connection is
        // irrelevant.
        Util.discard(connection);
        return instance;
    }

    /**
     * Flushes the cache which maps schema URLs to metadata.
     *
     * <p>This cache is referenced only when creating a new connection, so
     * existing connections will continue to use the same schema definition.
     */
    public abstract void flushSchemaCache();

    /**
     * Flushes the cache which contains cached aggregate values.
     *
     * <p>Typically, you would do this when records in the fact table have been
     * modified.
     *
     * <p>Note that flushing the data cache may have serious performance
     * implications for all connections to this Mondrian server. Aggregate data
     * for all cubes in all schemas will be flushed.
     */
    public abstract void flushDataCache();
}

// End MondrianServer.java
