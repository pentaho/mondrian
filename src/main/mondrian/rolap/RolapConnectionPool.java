/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2003 Robin Bagot, Julian Hyde and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.Util;
import org.apache.commons.dbcp.*;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.util.HashMap;

/**
 * Singleton class that holds a connection pool.
 * Call RolapConnectionPool.instance().getPoolingDataSource(connectionFactory)
 * to get a DataSource in return that is a pooled data source.
 */
class RolapConnectionPool {

    private static final RolapConnectionPool instance = new RolapConnectionPool();
    private final HashMap mapConnectKeyToPool = new HashMap();

    private RolapConnectionPool() {
    }

    public static RolapConnectionPool instance() {
        return instance;
    }

    /**
     * Sets up a pooling data source for connection pooling.
     * This can be used if the application server does not have a pooling
     * dataSource already configured.
     * This takes a normal jdbc connection string, and requires a jdbc
     * driver to be loaded, and then uses a
     * {@link DriverManagerConnectionFactory} to create connections to the
     * database.
     * An alternative method of configuring a pooling driver is to use an external
     * configuration file. See the the Apache jakarta-commons commons-pool
     * documentation.
     *
     * @param key Identifies which connection factory to use. A typical key is
     *   a JDBC connect string, since each JDBC connect string requires a
     *   different connection factory.
     * @param connectionFactory Creates connections from an underlying
     *   JDBC connect string or DataSource
     * @return a pooling DataSource object
     */
    public synchronized DataSource getPoolingDataSource(
            Object key, ConnectionFactory connectionFactory) {
        ObjectPool connectionPool = getPool(key, connectionFactory);
        // create pooling datasource
        return new PoolingDataSource(connectionPool);
    }

    /**
     * Clears the connection pool for testing purposes
     */
    void clearPool() {
        mapConnectKeyToPool.clear();
    }

    /**
     * Gets or creates a connection pool for a particular connect
     * specification.
     */
    private synchronized ObjectPool getPool(
            Object key, ConnectionFactory connectionFactory) {
        ObjectPool connectionPool = (ObjectPool) mapConnectKeyToPool.get(key);
        if (connectionPool == null) {
            // use GenericObjectPool, which provides for resource limits
            connectionPool = new GenericObjectPool(
                null, // PoolableObjectFactory, can be null
                50, // max active
                GenericObjectPool.WHEN_EXHAUSTED_FAIL, // action when exhausted
                3000, // max wait (milli seconds)
                10, // max idle
                false, // test on borrow
                false, // test on return
                60000, // time between eviction runs (millis)
                5, // number to test on eviction run
                30000, // min evictable idle time (millis)
                true // test while idle
                );

            // create a PoolableConnectionFactory
            AbandonedConfig abandonedConfig = new AbandonedConfig();
            abandonedConfig.setRemoveAbandoned(true); // flag to remove abandoned connections from pool
            abandonedConfig.setRemoveAbandonedTimeout(300); // timeout (seconds) before removing abandoned connections
            abandonedConfig.setLogAbandoned(true); // Flag to log stack traces for application code which abandoned a Statement or Connection
            PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(
                    connectionFactory, // the connection factory
                    connectionPool, // the object pool
                    null, // statement pool factory for pooling prepared statements, or null for no pooling
                    null, // validation query (must return at least 1 row e.g. Oracle: select count(*) from dual) to test connection, can be null
                    false, // default "read only" setting for borrowed connections
                    true, // default "auto commit" setting for returned connections
                    abandonedConfig // AbandonedConfig object configures how to handle abandoned connections
            );
            // "poolableConnectionFactory" has registered itself with
            // "connectionPool", somehow, so we don't need the value any more.
            Util.discard(poolableConnectionFactory);
            mapConnectKeyToPool.put(key, connectionPool);
        }
        return connectionPool;
    }

}

// End RolapConnectionPool.java
