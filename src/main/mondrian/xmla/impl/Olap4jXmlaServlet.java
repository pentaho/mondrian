/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla.impl;

import mondrian.olap.Util;
import mondrian.util.Bug;
import mondrian.xmla.XmlaHandler;

import org.olap4j.OlapConnection;
import org.olap4j.OlapWrapper;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * XMLA servlet that gets its connections from an olap4j data source.
 *
 * @version $Id$
 * @author Julian Hyde
 * @author Michele Rossi
 */
public class Olap4jXmlaServlet extends DefaultXmlaServlet {
    private static final String OLAP_DRIVER_CLASS_NAME_PARAM =
        "OlapDriverClassName";

    private static final String OLAP_DRIVER_CONNECTION_STRING_PARAM =
        "OlapDriverConnectionString";

    private static final String OLAP_DRIVER_CONNECTION_PROPERTIES_PREFIX =
        "OlapDriverConnectionProperty.";

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
        try {
            Map<String, String> connectionProperties =
                getOlap4jConnectionProperties(
                    servletConfig,
                    OLAP_DRIVER_CONNECTION_PROPERTIES_PREFIX);
            return new Olap4jConnectionFactory(
                olap4jDriverClassName,
                olap4jDriverConnectionString,
                connectionProperties);
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

    /**
     * A {@link mondrian.xmla.XmlaHandler.ConnectionFactory} implementation that
     * creates a {@link org.olap4j.OlapConnection} from the specified olap4j
     * driver.
     */
    private static class Olap4jConnectionFactory
        implements XmlaHandler.ConnectionFactory
    {
        private final String olap4jDriverConnectionString;
        private final Map<String, String> connectionProperties;

        /**
         * Creates an Olap4jConnectionFactory.
         *
         * @param olap4jDriverClassName Driver class name
         * @param olap4jDriverConnectionString Connect string
         * @param connectionProperties Connection properties
         * @throws ClassNotFoundException if driver class is not found
         */
        public Olap4jConnectionFactory(
            final String olap4jDriverClassName,
            final String olap4jDriverConnectionString,
            final Map<String, String> connectionProperties)
            throws ClassNotFoundException
        {
            Class.forName(olap4jDriverClassName);
            this.olap4jDriverConnectionString = olap4jDriverConnectionString;
            this.connectionProperties = connectionProperties;
        }

        public OlapConnection getConnection(
            final String catalog,
            final String schema,
            final String roleName,
            final Properties props)
            throws SQLException
        {
            final Properties properties = new Properties();
            properties.putAll(connectionProperties);
            Connection connection =
                DriverManager.getConnection(
                    olap4jDriverConnectionString,
                    properties);
            OlapConnection olapConnection =
                unwrap(connection, OlapConnection.class);

            if (catalog != null) {
                olapConnection.setCatalog(catalog);
            }
            if (schema != null) {
                olapConnection.setSchema(schema);
            }
            if (roleName != null) {
                olapConnection.setRoleName(roleName);
            }
            return olapConnection;
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

        Bug.olap4jUpgrade(
            "move Util.toMap to Olap4jUtil; this file should no longer "
            + "depend on mondrian.util");

        // Get system properties.
        Map<String, String> systemProps = Util.toMap(System.getProperties());
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
}

// End Olap4jXmlaServlet.java
