/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jng, 16 April, 2004
*/
package mondrian.rolap;

import junit.framework.TestCase;
import mondrian.olap.Util;
import mondrian.test.TestContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Unit test for {@link RolapConnection}.
 *
 * @author jng
 * @since 16 April, 2004
 * @version $Id$
 */
public class RolapConnectionTest extends TestCase {
    public RolapConnectionTest(String name) {
        super(name);
    }

    public void testPooledConnectionWithProperties() throws SQLException {
        final String connectString = TestContext.getConnectString();
        Util.PropertyList properties = Util.parseConnectString(connectString);

        // Only the JDBC-ODBC bridge gives the error necessary for this
        // test to succeed. So trivially succeed for all other JDBC
        // drivers.
        final String jdbc = properties.get("Jdbc");
        if (jdbc != null &&
                !jdbc.startsWith("jdbc:odbc:")) {
            return;
        }

        // JDBC-ODBC driver does not support UTF-16, so this test succeeds
        // because creating the connection from the DataSource will fail.
        properties.put("jdbc.charSet", "UTF-16");
        DataSource dataSource = RolapConnection.createDataSource(properties);
        Connection connection;
        try {
            connection = dataSource.getConnection();
            connection.close();
            fail("Expected exception");
        } catch (SQLException e) {
            if (e.getClass().getName().equals(
                "org.apache.commons.dbcp.DbcpException")) {
                // This is expected. (We use string-comparison so that the
                // compiler doesn't warn about using a deprecated class.)
            } else if (e.getClass() == SQLException.class &&
                e.getCause() == null &&
                e.getMessage() != null &&
                e.getMessage().equals("")) {
                // This is expected, from a later version of Dbcp.
            } else {
                fail("Expected exception, but got a different one: " + e);
            }
        } catch (IllegalArgumentException e) {
            // Workaround Java bug, logged on 2006/12/09 with synopsis
            // "DriverManager.getConnection throws IllegalArgumentException".
            if (!System.getProperties().getProperty("java.version").startsWith("1.6.")) {
                fail("Expect IllegalArgumentException only in JDK 1.6");
            }
        } finally {
            RolapConnectionPool.instance().clearPool();
        }
    }

    public void testNonPooledConnectionWithProperties() {
        final String connectString = TestContext.getConnectString();
        Util.PropertyList properties = Util.parseConnectString(connectString);

        // Only the JDBC-ODBC bridge gives the error necessary for this
        // test to succeed. So trivially succeed for all other JDBC
        // drivers.
        final String jdbc = properties.get("Jdbc");
        if (jdbc != null &&
                !jdbc.startsWith("jdbc:odbc:")) {
            return;
        }

        // This test is just like the test testPooledConnectionWithProperties
        // except with non-pooled connections.
        properties.put("jdbc.charSet", "UTF-16");
        properties.put(RolapConnectionProperties.PoolNeeded, "false");
        DataSource dataSource = RolapConnection.createDataSource(properties);
        Connection connection;
        try {
            connection = dataSource.getConnection();
            connection.close();
            fail("Expected exception");
        } catch (SQLException se) {
            // this is expected
        } catch (IllegalArgumentException e) {
            // Workaround Java bug, logged on 2006/12/09 with synopsis
            // "DriverManager.getConnection throws IllegalArgumentException".
            if (!System.getProperties().getProperty("java.version").startsWith("1.6.")) {
                fail("Expect IllegalArgumentException only in JDK 1.6");
            }
        }
    }
}

// End RolapConnectionTest.java
