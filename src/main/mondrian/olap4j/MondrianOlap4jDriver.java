/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import mondrian.rolap.RolapConnectionProperties;

import java.sql.*;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

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
 * Then create a connection using a URL with the prefix "jdbc:mondrian:".
 * For example,
 *
 * <blockquote>
 * <code>import java.sql.Connection;<br/>
 * import java.sql.DriverManager;<br/>
 * import org.olap4j.OlapConnection;<br/>
 * <br/>
 * Connection connection =<br/>
 * &nbsp;&nbsp;&nbsp;DriverManager.getConnection(<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"jdbc:mondrian:Jdbc=jdbc:odbc:MondrianFoodMart; Catalog=file:/mondrian/demo/FoodMart.xml; JdbcDrivers=sun.jdbc.odbc.JdbcOdbcDriver");<br/>
 * OlapConnection olapConnection =<br/>
 * &nbsp;&nbsp;&nbsp;connection.unwrap(OlapConnection.class);</code>
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
 * @version $Id$
 * @since May 22, 2007
 */
public class MondrianOlap4jDriver implements Driver {
    public static final String NAME = "Mondrian olap4j driver";
    public static final String VERSION = "2.4";
    public static final int MAJOR_VERSION = 2;
    public static final int MINOR_VERSION = 4;
    private final Factory factory;

    static {
        try {
            register();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    MondrianOlap4jDriver() {
        String factoryClassName;
        try {
            Class.forName("java.sql.Wrapper");
            factoryClassName = "mondrian.olap4j.FactoryJdbc4Impl";
        } catch (ClassNotFoundException e) {
            // java.sql.Wrapper is not present. This means we are running JDBC
            // 3.0 or earlier (probably JDK 1.5). Load the JDBC 3.0 factory
            factoryClassName = "mondrian.olap4j.FactoryJdbc3Impl";
        }
        try {
            final Class clazz = Class.forName(factoryClassName);
            factory = (Factory) clazz.newInstance();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void register() throws SQLException {
        DriverManager.registerDriver(new MondrianOlap4jDriver());
    }

    public Connection connect(String url, Properties info) throws SQLException {
        if (!MondrianOlap4jConnection.acceptsURL(url)) {
            return null;
        }
        return factory.newConnection(url, info);
    }

    public boolean acceptsURL(String url) throws SQLException {
        return MondrianOlap4jConnection.acceptsURL(url);
    }

    public DriverPropertyInfo[] getPropertyInfo(
        String url, Properties info) throws SQLException
    {
        List<DriverPropertyInfo> list = new ArrayList<DriverPropertyInfo>();

        // First, add the contents of info
        for (Map.Entry<Object,Object> entry : info.entrySet()) {
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

    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    public boolean jdbcCompliant() {
        return false;
    }
}

// End MondrianOlap4jDriver.java
