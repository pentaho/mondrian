/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import java.sql.*;

/**
 * Olap4j driver for Mondrian that behaves as a server.
 *
 * <p>Unlike the client driver {@link mondrian.olap4j.MondrianOlap4jDriver},
 * this driver knows the list of schemas and catalogs, by parsing
 * datasources.xml.
 *
 * <p>Since olap4j is a superset of JDBC, you register this driver as you would
 * any JDBC driver:
 *
 * <blockquote>
 * <code>Class.forName("mondrian.olap4j.MondrianOlap4jEngineDriver");</code>
 * </blockquote>
 *
 * <p>Then create a connection using a URL with the prefix
 * "jdbc:mondrian:engine:".
 * For example,
 *
 * <blockquote>
 * <code>import java.sql.Connection;<br/>
 * import java.sql.DriverManager;<br/>
 * import org.olap4j.OlapConnection;<br/>
 * <br/>
 * Connection connection =<br/>
 * &nbsp;&nbsp;DriverManager.getConnection(<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;"jdbc:mondrian:engine:Schema=FoodMart");<br/>
 * OlapConnection olapConnection =<br/>
 * &nbsp;&nbsp;connection.unwrap(OlapConnection.class);</code>
 * </blockquote>
 *
 * <p>Note how we use the {@link java.sql.Connection#unwrap(Class)} method to
 * down-cast
 * the JDBC connection object to the extension {@link org.olap4j.OlapConnection}
 * object. This method is only available in the JDBC 4.0 (JDK 1.6 onwards).
 *
 * <h3>Connection properties</h3>
 *
 * <p>The driver supports the same set of properties as a traditional mondrian
 * connection. See {@link mondrian.rolap.RolapConnectionProperties}. Properties
 * that allow the client to make connections to arbitrary databases ("Jdbc",
 * "JdbcUser", "JdbcPassword", "Catalog") may be disabled.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 12, 2010
 */
public class MondrianOlap4jEngineDriver extends MondrianOlap4jDriver {
    static {
        try {
            register();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates a MondrianOlap4jEngineDriver.
     */
    MondrianOlap4jEngineDriver() {
        super();
        factory.setCatalogFinder(new EngineCatalogFinder());
    }

    /**
     * Registers an instance of MondrianOlap4jEngineDriver.
     *
     * <p>Called implicitly on class load, and implements the traditional
     * 'Class.forName' way of registering JDBC drivers.
     *
     * @throws java.sql.SQLException on error
     */
    private static void register() throws SQLException {
        DriverManager.registerDriver(new MondrianOlap4jEngineDriver());
    }

    String getName() {
        // TODO: engine driver should have different name. Version can be
        //   the same as the client driver.
        return MondrianOlap4jDriverVersion.NAME;
    }

    /**
     * Implementation of {@link mondrian.olap4j.CatalogFinder} that reads
     * from a {@code datasources.xml} file.
     */
    private static class EngineCatalogFinder implements CatalogFinder {
        EngineCatalogFinder() {
        }
    }
}

// End MondrianOlap4jEngineDriver.java
