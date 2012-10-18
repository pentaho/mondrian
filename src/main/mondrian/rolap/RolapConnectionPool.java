/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2006 Robin Bagot and others
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Util;

import org.apache.commons.dbcp.*;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.util.*;
import javax.sql.DataSource;

/**
 * Singleton class that holds a connection pool.
 * Call RolapConnectionPool.instance().getPoolingDataSource(connectionFactory)
 * to get a DataSource in return that is a pooled data source.
 *
 * @author jhyde
 * @author Robin Bagot
 * @since 7 July, 2003
 */
class RolapConnectionPool {

    public static RolapConnectionPool instance() {
        return instance;
    }

    private static final RolapConnectionPool instance =
        new RolapConnectionPool();

    private final Map<Object, ObjectPool> mapConnectKeyToPool =
        new HashMap<Object, ObjectPool>();

    private final Map<DataSourceKey, DataSource> dataSourceMap =
        new WeakHashMap<DataSourceKey, DataSource>();

    private RolapConnectionPool() {
    }


    /**
     * Sets up a pooling data source for connection pooling.
     * This can be used if the application server does not have a pooling
     * dataSource already configured.
     *
     * <p>This takes a normal jdbc connection string, and requires a jdbc
     * driver to be loaded, and then uses a
     * {@link DriverManagerConnectionFactory} to create connections to the
     * database.
     *
     * <p>An alternative method of configuring a pooling driver is to use an
     * external configuration file. See the the Apache jakarta-commons
     * commons-pool documentation.
     *
     * @param key Identifies which connection factory to use. A typical key is
     *   a JDBC connect string, since each JDBC connect string requires a
     *   different connection factory.
     * @param connectionFactory Creates connections from an underlying
     *   JDBC connect string or DataSource
     * @return a pooling DataSource object
     */
    synchronized DataSource getPoolingDataSource(
        PoolKey key,
        ConnectionFactory connectionFactory,
        boolean mysql)
    {
        ObjectPool connectionPool = getPool(key, connectionFactory, mysql);
        // create pooling datasource
        return new PoolingDataSource(connectionPool);
    }

    /**
     * Clears the connection pool for testing purposes
     */
    void clearPool() {
        mapConnectKeyToPool.clear();
    }

    public synchronized DataSource getDriverManagerPoolingDataSource(
        String jdbcConnectString,
        Properties jdbcProperties,
        boolean mysql)
    {
        // First look for a data source with identical specification. This in
        // turn helps us to use the cache of Dialect objects.

        // Need to include user name to define the pool key as some DBMSs
        // like Oracle don't include schemas in the JDBC URL - instead the
        // user drives the schema. This makes JDBC pools act like JNDI pools,
        // with, in effect, a pool per DB user.

        final DataSourceKey key =
            DataSourceKey.of(
                jdbcConnectString,
                jdbcProperties);
        DataSource dataSource = dataSourceMap.get(key);
        if (dataSource != null) {
            return dataSource;
        }

        // use the DriverManagerConnectionFactory to create connections
        ConnectionFactory connectionFactory =
            new DriverManagerConnectionFactory(
                jdbcConnectString,
                jdbcProperties);

        try {
            dataSource = getPoolingDataSource(
                PoolKey.of(jdbcConnectString, jdbcProperties),
                connectionFactory,
                mysql);
        } catch (Throwable e) {
            throw Util.newInternal(
                e,
                "Error while creating connection pool (with URI "
                + jdbcConnectString + ")");
        }
        dataSourceMap.put(key, dataSource);
        return dataSource;
    }

    public synchronized DataSource getDataSourcePoolingDataSource(
        DataSource dataSource,
        String dataSourceName,
        String jdbcUser,
        String jdbcPassword)
    {
        // First look for a data source with identical specification. This in
        // turn helps us to use the cache of Dialect objects.
        DataSourceKey key =
            DataSourceKey.of(
                dataSource,
                jdbcUser,
                jdbcPassword);
        DataSource pooledDataSource = dataSourceMap.get(key);
        if (pooledDataSource != null) {
            return pooledDataSource;
        }

        ConnectionFactory connectionFactory;
        if (jdbcUser != null || jdbcPassword != null) {
            connectionFactory =
                new DataSourceConnectionFactory(
                    dataSource, jdbcUser, jdbcPassword);
        } else {
            connectionFactory =
                new DataSourceConnectionFactory(dataSource);
        }
        try {
            pooledDataSource =
                getPoolingDataSource(
                    PoolKey.of(dataSourceName),
                    connectionFactory,
                    false); // REVIEW: we don't know whether it's MySQL
        } catch (Exception e) {
            throw Util.newInternal(
                e,
                "Error while creating connection pool (with URI "
                + dataSourceName + ")");
        }
        dataSourceMap.put(key, pooledDataSource);
        return dataSource;
    }

    /**
     * Gets or creates a connection pool for a particular connect
     * specification.
     */
    private synchronized ObjectPool getPool(
        PoolKey key,
        ConnectionFactory connectionFactory,
        boolean mysql)
    {
        ObjectPool connectionPool = mapConnectKeyToPool.get(key);
        if (connectionPool == null) {
            // use GenericObjectPool, which provides for resource limits
            connectionPool = new GenericObjectPool(
                null, // PoolableObjectFactory, can be null
                5000, // max active
                GenericObjectPool.WHEN_EXHAUSTED_GROW, // action when exhausted
                3000, // max wait (milli seconds)
                10, // max idle
                mysql, // test on borrow
                false, // test on return
                60000, // time between eviction runs (millis)
                5, // number to test on eviction run
                30000, // min evictable idle time (millis)
                true); // test while idle

            // create a PoolableConnectionFactory
            AbandonedConfig abandonedConfig = new AbandonedConfig();
            // flag to remove abandoned connections from pool
            abandonedConfig.setRemoveAbandoned(true);
            // timeout (seconds) before removing abandoned connections
            abandonedConfig.setRemoveAbandonedTimeout(300);
            // Flag to log stack traces for application code which abandoned a
            // Statement or Connection
            abandonedConfig.setLogAbandoned(true);
            PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(
                    // the connection factory
                    connectionFactory,
                    // the object pool
                    connectionPool,
                    // statement pool factory for pooling prepared statements,
                    // or null for no pooling
                    null,
                    // validation query (must return at least 1 row e.g. Oracle:
                    // select count(*) from dual) to test connection, can be
                    // null
                    mysql ? "SELECT 1" : null,
                    // default "read only" setting for borrowed connections
                    false,
                    // default "auto commit" setting for returned connections
                    true,
                    // AbandonedConfig object configures how to handle abandoned
                    // connections
                    abandonedConfig);

            // "poolableConnectionFactory" has registered itself with
            // "connectionPool", somehow, so we don't need the value any more.
            Util.discard(poolableConnectionFactory);
            mapConnectKeyToPool.put(key, connectionPool);
        }
        return connectionPool;
    }

    /** Abstract base class for keys based upon a list and cached hash code. */
    private static abstract class ListKey {
        private final int hashCode;
        private final Object[] values;

        // Must be protected. Factory method in derived class must ensure values
        // is a private copy.
        protected ListKey(Object[] values) {
            this.values = values;
            this.hashCode = Arrays.hashCode(values);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj
                || obj instanceof ListKey
                && this.hashCode == ((ListKey) obj).hashCode
                && getClass() == obj.getClass()
                && Arrays.equals(this.values, ((ListKey) obj).values);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public String toString() {
            return Arrays.toString(values) + "#" + hashCode;
        }
    }

    private static class DataSourceKey extends ListKey {
        private DataSourceKey(Object[] values) {
            super(values);
        }

        @Override
        public String toString() {
            return "DataSourceKey" + super.toString();
        }

        /** Creates a key from a connect string and properties. */
        public static DataSourceKey of(
            String jdbcConnectString,
            Properties jdbcProperties)
        {
            @SuppressWarnings("unchecked")
            final Map<String, String> map = (Map) jdbcProperties;
            String[] values = new String[jdbcProperties.size() * 2 + 1];
            int i = 0;
            values[i++] = jdbcConnectString;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                values[i++] = entry.getKey();
                values[i++] = entry.getValue();
            }
            return new DataSourceKey(values);
        }

        /** Creates a key from a data source and user/password. */
        public static DataSourceKey of(
            DataSource dataSource,
            String jdbcUser,
            String jdbcPassword)
        {
            Object[] values1 = {dataSource, jdbcUser, jdbcPassword};
            return new DataSourceKey(values1);
        }
    }

    private static class PoolKey extends ListKey {
        private PoolKey(Object[] values) {
            super(values);
        }

        @Override
        public String toString() {
            return "PoolKey" + super.toString();
        }

        /** Creates a key based upon a data source name. */
        public static PoolKey of(String dataSourceName) {
            return new PoolKey(new String[] {dataSourceName});
        }

        /** Creates a key based upon a connect string and property set. */
        public static PoolKey of(
            String jdbcConnectString,
            Properties properties)
        {
            // Flatten properties into an array. Ensures immutability.
            @SuppressWarnings("unchecked")
            final Map<String, String> map = (Map) properties;
            String[] values = new String[properties.size() * 2 + 1];
            int i = 0;
            values[i++] = jdbcConnectString;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                values[i++] = entry.getKey();
                values[i++] = entry.getValue();
            }
            return new PoolKey(values);
        }
    }
}

// End RolapConnectionPool.java
