/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.xmla.impl;

import mondrian.olap.Util;
import mondrian.xmla.XmlaHandler;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.DelegatingConnection;
import org.apache.log4j.Logger;

import org.olap4j.OlapConnection;
import org.olap4j.OlapWrapper;

import java.lang.reflect.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

/**
 * XMLA servlet that gets its connections from an olap4j data source.
 *
 * @author Julian Hyde
 * @author Michele Rossi
 */
public class Olap4jXmlaServlet extends DefaultXmlaServlet {
    private static final Logger LOGGER =
        Logger.getLogger(Olap4jXmlaServlet.class);

    private static final String OLAP_DRIVER_CLASS_NAME_PARAM =
        "OlapDriverClassName";

    private static final String OLAP_DRIVER_CONNECTION_STRING_PARAM =
        "OlapDriverConnectionString";

    private static final String OLAP_DRIVER_CONNECTION_PROPERTIES_PREFIX =
        "OlapDriverConnectionProperty.";

    private static final String
        OLAP_DRIVER_PRECONFIGURED_DISCOVER_DATASOURCES_RESPONSE =
        "OlapDriverUsePreConfiguredDiscoverDatasourcesResponse";

    private static final String OLAP_DRIVER_IDLE_CONNECTIONS_TIMEOUT_MINUTES =
        "OlapDriverIdleConnectionsTimeoutMinutes";

    private static final String
        OLAP_DRIVER_PRECONFIGURED_DISCOVER_DATASOURCES_PREFIX =
        "OlapDriverDiscoverDatasources.";

    /**
     * Name of property used by JDBC to hold user name.
     */
    private static final String JDBC_USER = "user";

    /**
     * Name of property used by JDBC to hold password.
     */
    private static final String JDBC_PASSWORD = "password";

    /** idle connections are cleaned out after 5 minutes by default */
    private static final int DEFAULT_IDLE_CONNECTIONS_TIMEOUT_MS =
        5 * 60 * 1000;

    private static final String OLAP_DRIVER_MAX_NUM_CONNECTIONS_PER_USER =
        "OlapDriverMaxNumConnectionsPerUser";

    /**
     * Unwraps a given interface from a given connection.
     *
     * @param connection Connection object
     * @param clazz Interface to unwrap
     * @param <T> Type of interface
     * @return Unwrapped object; never null
     * @throws java.sql.SQLException if cannot convert
     */
    private static <T> T unwrap(Connection connection, Class<T> clazz)
        throws SQLException
    {
        // Invoke Wrapper.unwrap(). Works for JDK 1.6 and later, but we use
        // reflection so that it compiles on JDK 1.5.
        try {
            final Class<?> wrapperClass = Class.forName("java.sql.Wrapper");
            if (wrapperClass.isInstance(connection)) {
                Method unwrapMethod = wrapperClass.getMethod("unwrap");
                return clazz.cast(unwrapMethod.invoke(connection, clazz));
            }
        } catch (ClassNotFoundException e) {
            // ignore
        } catch (NoSuchMethodException e) {
            // ignore
        } catch (InvocationTargetException e) {
            // ignore
        } catch (IllegalAccessException e) {
            // ignore
        }
        if (connection instanceof OlapWrapper) {
            OlapWrapper olapWrapper = (OlapWrapper) connection;
            return olapWrapper.unwrap(clazz);
        }
        throw new SQLException("not an instance");
    }

    @Override
    protected XmlaHandler.ConnectionFactory createConnectionFactory(
        ServletConfig servletConfig)
        throws ServletException
    {
        final String olap4jDriverClassName =
            servletConfig.getInitParameter(OLAP_DRIVER_CLASS_NAME_PARAM);
        final String olap4jDriverConnectionString =
            servletConfig.getInitParameter(OLAP_DRIVER_CONNECTION_STRING_PARAM);
        final String olap4jUsePreConfiguredDiscoverDatasourcesRes =
            servletConfig.getInitParameter(
                OLAP_DRIVER_PRECONFIGURED_DISCOVER_DATASOURCES_RESPONSE);
        boolean hardcodedDiscoverDatasources =
            olap4jUsePreConfiguredDiscoverDatasourcesRes != null
            && Boolean.parseBoolean(
                olap4jUsePreConfiguredDiscoverDatasourcesRes);

        final String idleConnTimeoutStr =
            servletConfig.getInitParameter(
                OLAP_DRIVER_IDLE_CONNECTIONS_TIMEOUT_MINUTES);
        final int idleConnectionsCleanupTimeoutMs =
            idleConnTimeoutStr != null
            ? Integer.parseInt(idleConnTimeoutStr) * 60 * 1000
            : DEFAULT_IDLE_CONNECTIONS_TIMEOUT_MS;

        final String maxNumConnPerUserStr =
            servletConfig.getInitParameter(
                OLAP_DRIVER_MAX_NUM_CONNECTIONS_PER_USER);
        int maxNumConnectionsPerUser =
            maxNumConnPerUserStr != null
            ? Integer.parseInt(maxNumConnPerUserStr)
            : 1;
        try {
            Map<String, String> connectionProperties =
                getOlap4jConnectionProperties(
                    servletConfig,
                    OLAP_DRIVER_CONNECTION_PROPERTIES_PREFIX);
            final Map<String, Object> ddhcRes;
            if (hardcodedDiscoverDatasources) {
                ddhcRes =
                    getDiscoverDatasourcesPreConfiguredResponse(servletConfig);
            } else {
                ddhcRes = null;
            }

            return new Olap4jPoolingConnectionFactory(
                olap4jDriverClassName,
                olap4jDriverConnectionString,
                connectionProperties,
                idleConnectionsCleanupTimeoutMs,
                maxNumConnectionsPerUser,
                ddhcRes);
        } catch (Exception ex) {
            String msg =
                "Exception [" + ex + "] while trying to create "
                + "olap4j connection to ["
                + olap4jDriverConnectionString + "] using driver "
                + "[" + olap4jDriverClassName + "]";
            LOGGER.error(msg, ex);
            throw new ServletException(msg, ex);
        }
    }

    private static Map<String, Object>
    getDiscoverDatasourcesPreConfiguredResponse(
        ServletConfig servletConfig)
    {
        final Map<String, Object> map = new LinkedHashMap<String, Object>();
        foo(map, "DataSourceName", servletConfig, "dataSourceName");
        foo(
            map, "DataSourceDescription",
            servletConfig, "dataSourceDescription");
        foo(map, "URL", servletConfig, "url");
        foo(map, "DataSourceInfo", servletConfig, "dataSourceInfo");
        foo(map, "ProviderName", servletConfig, "providerName");
        foo(map, "ProviderType", servletConfig, "providerType");
        foo(map, "AuthenticationMode", servletConfig, "authenticationMode");
        return map;
    }

    private static void foo(
        Map<String, Object> map,
        String targetProp,
        ServletConfig servletConfig,
        String sourceProp)
    {
        final String value =
            servletConfig.getInitParameter(
                OLAP_DRIVER_PRECONFIGURED_DISCOVER_DATASOURCES_PREFIX
                + sourceProp);
        map.put(targetProp, value);
    }

    private static class Olap4jPoolingConnectionFactory
        implements XmlaHandler.ConnectionFactory
    {
        private final String olap4jDriverConnectionString;
        private final Properties connProperties;
        private final Map<String, Object> discoverDatasourcesResponse;
        private final String olap4jDriverClassName;
        private final Map<String, BasicDataSource> datasourcesPool =
            new HashMap<String, BasicDataSource>();
        private final int idleConnectionsCleanupTimeoutMs;
        private final int maxPerUserConnectionCount;

        /**
         * Creates an Olap4jPoolingConnectionFactory.
         *
         * @param olap4jDriverClassName Driver class name
         * @param olap4jDriverConnectionString Connect string
         * @param connectionProperties Connection properties
         * @param maxPerUserConnectionCount max number of connections to create
         *     for every different username
         * @param idleConnectionsCleanupTimeoutMs pooled connections inactive
         *     for longer than this period of time can be cleaned up
         * @param discoverDatasourcesResponse Pre-configured response to
         *     DISCOVER_DATASOURCES request, or null
         * @throws ClassNotFoundException if driver class is not found
         */
        public Olap4jPoolingConnectionFactory(
            final String olap4jDriverClassName,
            final String olap4jDriverConnectionString,
            final Map<String, String> connectionProperties,
            final int idleConnectionsCleanupTimeoutMs,
            final int maxPerUserConnectionCount,
            final Map<String, Object> discoverDatasourcesResponse)
            throws ClassNotFoundException
        {
            Class.forName(olap4jDriverClassName);
            this.maxPerUserConnectionCount = maxPerUserConnectionCount;
            this.idleConnectionsCleanupTimeoutMs =
                idleConnectionsCleanupTimeoutMs;
            this.olap4jDriverClassName = olap4jDriverClassName;
            this.olap4jDriverConnectionString = olap4jDriverConnectionString;
            this.connProperties = new Properties();
            this.connProperties.putAll(connectionProperties);
            this.discoverDatasourcesResponse = discoverDatasourcesResponse;
        }

        public OlapConnection getConnection(
            String catalog,
            String schema,
            String roleName,
            Properties props)
            throws SQLException
        {
            final String user = props.getProperty(JDBC_USER);
            final String pwd = props.getProperty(JDBC_PASSWORD);

            // note: this works also for un-authenticated connections; they will
            // simply all be created by the same BasicDataSource object
            final String dataSourceKey = user + "_" + pwd;

            BasicDataSource bds;
            synchronized (datasourcesPool) {
                bds = datasourcesPool.get(dataSourceKey);
                if (bds == null) {
                    bds = new BasicDataSource() {
                        {
                            connectionProperties.putAll(connProperties);
                        }
                    };
                    bds.setDefaultReadOnly(true);
                    bds.setDriverClassName(olap4jDriverClassName);
                    bds.setPassword(pwd);
                    bds.setUsername(user);
                    bds.setUrl(olap4jDriverConnectionString);
                    bds.setPoolPreparedStatements(false);
                    bds.setMaxIdle(maxPerUserConnectionCount);
                    bds.setMaxActive(maxPerUserConnectionCount);
                    bds.setMinEvictableIdleTimeMillis(
                        idleConnectionsCleanupTimeoutMs);
                    bds.setAccessToUnderlyingConnectionAllowed(true);
                    bds.setInitialSize(1);
                    bds.setTimeBetweenEvictionRunsMillis(60000);
                    if (catalog != null) {
                        bds.setDefaultCatalog(catalog);
                    }
                    datasourcesPool.put(dataSourceKey, bds);
                }
            }

            Connection connection = bds.getConnection();
            DelegatingConnection dc = (DelegatingConnection) connection;
            Connection underlyingOlapConnection = dc.getInnermostDelegate();
            OlapConnection olapConnection =
                unwrap(underlyingOlapConnection, OlapConnection.class);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "Obtained connection object [" + olapConnection
                    + "] (ext pool wrapper " + connection + ") for key "
                    + dataSourceKey);
            }
            if (catalog != null) {
                olapConnection.setCatalog(catalog);
            }
            if (schema != null) {
                olapConnection.setSchema(schema);
            }
            if (roleName != null) {
                olapConnection.setRoleName(roleName);
            }

            return createDelegatingOlapConnection(connection, olapConnection);
        }

        public Map<String, Object> getPreConfiguredDiscoverDatasourcesResponse()
        {
            return discoverDatasourcesResponse;
        }
    }

    /**
     * Obtains connection properties from the
     * ServletConfig init parameters and from System properties.
     *
     * <p>The properties found in the System properties override the ones in
     * the ServletConfig.
     *
     * <p>copies the values of init parameters / properties which
     * start with the given prefix to a target Map object stripping out the
     * configured prefix from the property name.
     *
     * <p>The following example uses prefix "olapConn.":
     *
     * <code><pre>
     *  &lt;init-param&gt;
     *      &lt;param-name&gt;olapConn.User&lt;/param-name&gt;
     *      &lt;param-value&gt;mrossi&lt;/param-value&gt;
     *  &lt;/init-param&gt;
     *  &lt;init-param&gt;
     *      &lt;param-name&gt;olapConn.Password&lt;/param-name&gt;
     *      &lt;param-value&gt;manhattan&lt;/param-value&gt;
     *  &lt;/init-param&gt;
     *
     * </pre></code>
     *
     * <p>This will result in a connection properties object with entries
     * <code>{("User", "mrossi"), ("Password", "manhattan")}</code>.
     *
     * @param prefix Prefix to property name
     * @param servletConfig Servlet config
     * @return Map containing property names and values
     */
    private static Map<String, String> getOlap4jConnectionProperties(
        final ServletConfig servletConfig,
        final String prefix)
    {
        Map<String, String> options = new LinkedHashMap<String, String>();

        // Get properties from servlet config.
        @SuppressWarnings({"unchecked"})
        java.util.Enumeration<String> en =
            servletConfig.getInitParameterNames();
        while (en.hasMoreElements()) {
            String paramName = en.nextElement();
            if (paramName.startsWith(prefix)) {
                String paramValue = servletConfig.getInitParameter(paramName);
                String prefixRemovedParamName =
                    paramName.substring(prefix.length());
                options.put(prefixRemovedParamName, paramValue);
            }
        }

        // Get system properties.
        final Map<String, String> systemProps =
            Util.toMap(System.getProperties());
        for (Map.Entry<String, String> entry : systemProps.entrySet()) {
            String sk = entry.getKey();
            if (sk.startsWith(prefix)) {
                String value = entry.getValue();
                String prefixRemovedKey = sk.substring(prefix.length());
                options.put(prefixRemovedKey, value);
            }
        }

        return options;
    }

    /**
     * Returns something that implements {@link OlapConnection} but still
     * behaves as the wrapper returned by the connection pool.
     *
     * <p>In other words we want the "close" method to play nice and do all the
     * pooling actions while we want all the olap methods to execute directly on
     * the un-wrapped OlapConnection object.
     */
    private static OlapConnection createDelegatingOlapConnection(
        final Connection connection,
        final OlapConnection olapConnection)
    {
        return (OlapConnection) Proxy.newProxyInstance(
            olapConnection.getClass().getClassLoader(),
            new Class[] {OlapConnection.class},
            new InvocationHandler() {
                public Object invoke(
                    Object proxy,
                    Method method,
                    Object[] args)
                    throws Throwable
                {
                    if ("unwrap".equals(method.getName())
                        || OlapConnection.class
                        .isAssignableFrom(method.getDeclaringClass()))
                    {
                        return method.invoke(olapConnection, args);
                    } else {
                        return method.invoke(connection, args);
                    }
                }
            }
        );
    }
}

// End Olap4jXmlaServlet.java
