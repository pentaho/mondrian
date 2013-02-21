/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.olap4j;

import mondrian.olap.*;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.spi.CatalogLocator;

import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.logging.Logger;

/**
 * Base class for an Olap4j driver.
 *
 * <p>A concrete derived class must have a {@code static} block that calls
 * {@link DriverManager#registerDriver(java.sql.Driver)} with an instance of
 * this driver, per the JDBC specification.</p>
 */
public class MondrianBaseOlap4jDriver implements Driver {
    protected final Factory factory;
    final Plugin plugin;

    /**
     * Creates a MondrianBaseOlap4jDriver.
     */
    protected MondrianBaseOlap4jDriver(
        Factory factory,
        Plugin plugin)
    {
        this.factory = factory;
        this.plugin = plugin;
    }

    /** Creates an instance of the default factory, appropriate for the current
     * JDK. Derived classes may use this factory or may roll their own. */
    protected static Factory createFactory() {
        final String factoryClassName = getFactoryClassName();
        try {
            // Cannot use ClassResolver here, because factory's constructor has
            // package access.
            final Class<?> clazz = Class.forName(factoryClassName);
            return (Factory) clazz.newInstance();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    protected static String getFactoryClassName() {
        try {
            // If java.sql.PseudoColumnUsage is present, we are running JDBC 4.1
            // or later.
            Class.forName("java.sql.PseudoColumnUsage");
            return "mondrian.olap4j.FactoryJdbc41Impl";
        } catch (ClassNotFoundException e) {
            // java.sql.PseudoColumnUsage is not present. This means we are
            // running JDBC 4.0 or earlier.
            try {
                Class.forName("java.sql.Wrapper");
                return "mondrian.olap4j.FactoryJdbc4Impl";
            } catch (ClassNotFoundException e2) {
                // java.sql.Wrapper is not present. This means we are running
                // JDBC 3.0 or earlier (probably JDK 1.5). Load the JDBC 3.0
                // factory.
                return "mondrian.olap4j.FactoryJdbc3Impl";
            }
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        final String urlPrefix = plugin.getUrlPrefix();
        if (!url.startsWith(urlPrefix)) {
            // This is not a URL we can handle.
            // DriverManager should not have invoked us.
            throw new AssertionError(
                "does not start with '" + urlPrefix + "'");
        }
        final String x = url.substring(urlPrefix.length());
        Util.PropertyList propertyList = Util.parseConnectString(x);
        final Map<String, String> map = Util.toMap(info);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            propertyList.put(entry.getKey(), entry.getValue());
        }

        // Which server instance do we wish to connect to?
        final String instance =
            propertyList.get(RolapConnectionProperties.Instance.name());
        MondrianServer server = MondrianServer.forId(instance);
        if (server == null) {
            throw Util.newError("Unknown server instance: " + instance);
        }

        // Catalog locator converts catalog names appropriately for the
        // container. (It is not often used.)
        CatalogLocator locator = server.getCatalogLocator();
        if (locator != null) {
            String catalog = propertyList.get(
                RolapConnectionProperties.Catalog.name());
            propertyList.put(
                RolapConnectionProperties.Catalog.name(),
                locator.locate(catalog));
        }

        MondrianServer.User user = server.authenticate(propertyList);
        if (user == null) {
            throw new SQLException("authentication failed");
        }
        return factory.newConnection(this, server, propertyList, user);
    }

    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(plugin.getUrlPrefix());
    }

    public DriverPropertyInfo[] getPropertyInfo(
        String url, Properties info) throws SQLException
    {
        List<DriverPropertyInfo> list = new ArrayList<DriverPropertyInfo>();

        // First, add the contents of info
        for (Map.Entry<Object, Object> entry : info.entrySet()) {
            list.add(
                new DriverPropertyInfo(
                    (String) entry.getKey(),
                    (String) entry.getValue()));
        }
        // Next, add property defns not mentioned in info
        plugin.collectPropertyInfo(url, info, list);
        return list.toArray(new DriverPropertyInfo[list.size()]);
    }

    // JDBC 4.1 support (JDK 1.7 and higher)
    public Logger getParentLogger() {
        return plugin.getParentLogger();
    }

    /**
     * Returns the driver name. Not in the JDBC API.
     * @return Driver name
     */
    String getName() {
        return plugin.getDriverName();
    }

    /**
     * Returns the driver version. Not in the JDBC API.
     * @return Driver version
     */
    String getVersion() {
        return plugin.getDriverVersion();
    }

    public int getMajorVersion() {
        return plugin.getMajorVersion();
    }

    public int getMinorVersion() {
        return plugin.getMinorVersion();
    }

    public boolean jdbcCompliant() {
        return false;
    }

    interface Plugin {
        int getMajorVersion();

        int getMinorVersion();

        String getUrlPrefix();

        String getDriverName();

        String getDriverVersion();

        Logger getParentLogger();

        void collectPropertyInfo(
            String url, Properties info, List<DriverPropertyInfo> list);
    }

    /** Implementation of {@link Plugin} for a driver that talks to Mondrian
     * without authentication. */
    static class PluginImpl implements Plugin {
        private static final String CONNECT_STRING_PREFIX = "jdbc:mondrian:";

        public int getMajorVersion() {
            return MondrianOlap4jDriverVersion.MAJOR_VERSION;
        }

        public int getMinorVersion() {
            return MondrianOlap4jDriverVersion.MINOR_VERSION;
        }

        public String getUrlPrefix() {
            return CONNECT_STRING_PREFIX;
        }

        public String getDriverName() {
            return MondrianOlap4jDriverVersion.NAME;
        }

        public String getDriverVersion() {
            return MondrianOlap4jDriverVersion.VERSION;
        }

        public Logger getParentLogger() {
            return Logger.getLogger("");
        }

        public void collectPropertyInfo(
            String url,
            Properties info,
            List<DriverPropertyInfo> list)
        {
            for (RolapConnectionProperties p
                : RolapConnectionProperties.values())
            {
                if (info.containsKey(p.name())) {
                    continue;
                }
                list.add(
                    new DriverPropertyInfo(
                        p.name(),
                        null));
            }
        }
    }
}

// End MondrianBaseOlap4jDriver.java
