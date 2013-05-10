/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.olap4j;

import mondrian.rolap.RolapConnectionProperties;
import mondrian.xmla.XmlaHandler;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Olap4j driver for Mondrian.
 *
 * <p>Since olap4j is a superset of JDBC, you register this driver as you would
 * any JDBC driver:
 *
 * <blockquote>
 * <code>Class.forName("mondrian.olap4j.MondrianOlap4jDriver");</code>
 * </blockquote>
 *
 * <p>Then create a connection using a URL with the prefix "jdbc:mondrian:".
 * For example,
 *
 * <blockquote>
 * <code>import java.sql.Connection;<br/>
 * import java.sql.DriverManager;<br/>
 * import org.olap4j.OlapConnection;<br/>
 * <br/>
 * Connection connection =<br/>
 * &nbsp;&nbsp;DriverManager.getConnection(<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;"jdbc:mondrian:Jdbc=jdbc:mysql://localhost/foodmart;
 * JdbcUser=foodmart; JdbcPassword=foodmart;
 * Catalog=file:/mondrian/demo/FoodMart.mondrian.xml;
 * JdbcDrivers=com.mysql.jdbc.Driver");<br/>
 * OlapConnection olapConnection =<br/>
 * &nbsp;&nbsp;connection.unwrap(OlapConnection.class);</code>
 * </blockquote>
 *
 * <p>Note how we use the {@link Connection#unwrap(Class)} method to down-cast
 * the JDBC connection object to the extension {@link org.olap4j.OlapConnection}
 * object. This method is only available in the JDBC 4.0 (JDK 1.6 onwards).
 *
 * <h3>Connection properties</h3>
 *
 * <p>The driver supports the same set of properties as a traditional mondrian
 * connection. See {@link mondrian.rolap.RolapConnectionProperties}.
 *
 * <h3>Catalogs and schemas</h3>
 *
 * <p>Mondrian has a sole catalog, called "LOCALDB". You will get an error
 * if you attempt to use {@link java.sql.Connection#setCatalog(String)} to set
 * it to anything else.
 *
 * @author jhyde
 * @since May 22, 2007
 */
public class MondrianOlap4jDriver implements Driver {
    public static final XmlaHandler.XmlaExtra EXTRA =
        MondrianOlap4jExtra.INSTANCE;

    protected final Factory factory;

    static {
        try {
            register();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates a MondrianOlap4jDriver.
     */
    public MondrianOlap4jDriver() {
        this.factory = createFactory();
    }

    private static Factory createFactory() {
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

    private static String getFactoryClassName() {
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

    /**
     * Registers an instance of MondrianOlap4jDriver.
     *
     * <p>Called implicitly on class load, and implements the traditional
     * 'Class.forName' way of registering JDBC drivers.
     *
     * @throws SQLException on error
     */
    private static void register() throws SQLException {
        DriverManager.registerDriver(new MondrianOlap4jDriver());
    }

    public Connection connect(String url, Properties info) throws SQLException {
        if (!MondrianOlap4jConnection.acceptsURL(url)) {
            return null;
        }
        return factory.newConnection(this, url, info);
    }

    public boolean acceptsURL(String url) throws SQLException {
        return MondrianOlap4jConnection.acceptsURL(url);
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
        for (RolapConnectionProperties p : RolapConnectionProperties.values()) {
            if (info.containsKey(p.name())) {
                continue;
            }
            list.add(
                new DriverPropertyInfo(
                    p.name(),
                    null));
        }
        return list.toArray(new DriverPropertyInfo[list.size()]);
    }

    // JDBC 4.1 support (JDK 1.7 and higher)
    public Logger getParentLogger() {
        return Logger.getLogger("");
    }

    /**
     * Returns the driver name. Not in the JDBC API.
     * @return Driver name
     */
    String getName() {
        return MondrianOlap4jDriverVersion.NAME;
    }

    /**
     * Returns the driver version. Not in the JDBC API.
     * @return Driver version
     */
    String getVersion() {
        return MondrianOlap4jDriverVersion.VERSION;
    }

    public int getMajorVersion() {
        return MondrianOlap4jDriverVersion.MAJOR_VERSION;
    }

    public int getMinorVersion() {
        return MondrianOlap4jDriverVersion.MINOR_VERSION;
    }

    public boolean jdbcCompliant() {
        return false;
    }
}

// End MondrianOlap4jDriver.java
