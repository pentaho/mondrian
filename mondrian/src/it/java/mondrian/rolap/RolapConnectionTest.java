/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2019 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.DriverManager;
import mondrian.olap.MondrianException;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.spi.Dialect;
import mondrian.test.TestContext;
import mondrian.util.Pair;

import junit.framework.TestCase;

import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import javax.naming.spi.InitialContextFactoryBuilder;
import javax.naming.spi.NamingManager;
import javax.sql.DataSource;

import static org.mockito.Mockito.*;

/**
 * Unit test for {@link RolapConnection}.
 *
 * @author jng
 * @since 16 April, 2004
 */
public class RolapConnectionTest extends TestCase {
    private static final ThreadLocal<InitialContext> THREAD_INITIAL_CONTEXT =
        new ThreadLocal<InitialContext>();

    public RolapConnectionTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();

        if (!NamingManager.hasInitialContextFactoryBuilder()) {
            NamingManager.setInitialContextFactoryBuilder(
                new InitialContextFactoryBuilder() {
                    public InitialContextFactory createInitialContextFactory(
                        Hashtable<?, ?> environment)
                        throws NamingException
                    {
                        return new InitialContextFactory() {
                            public Context getInitialContext(
                                Hashtable<?, ?> environment)
                                throws NamingException
                            {
                                return THREAD_INITIAL_CONTEXT.get();
                            }
                        };
                    }
                }
           );
        }
    }

    public void testPooledConnectionWithProperties() throws SQLException {
        Util.PropertyList properties =
            TestContext.instance().getConnectionProperties().clone();

        // Only the JDBC-ODBC bridge gives the error necessary for this
        // test to succeed. So trivially succeed for all other JDBC
        // drivers.
        final String jdbc = properties.get("Jdbc");
        if (jdbc != null
            && !jdbc.startsWith("jdbc:odbc:"))
        {
            return;
        }

        // JDBC-ODBC driver does not support UTF-16, so this test succeeds
        // because creating the connection from the DataSource will fail.
        properties.put("jdbc.charSet", "UTF-16");

        final StringBuilder buf = new StringBuilder();
        DataSource dataSource =
            RolapConnection.createDataSource(null, properties, buf);
        final String desc = buf.toString();
        assertTrue(desc.startsWith("Jdbc="));

        Connection connection;
        try {
            connection = dataSource.getConnection();
            connection.close();
            fail("Expected exception");
        } catch (SQLException e) {
            if (e.getClass().getName().equals(
                    "org.apache.commons.dbcp.DbcpException"))
            {
                // This is expected. (We use string-comparison so that the
                // compiler doesn't warn about using a deprecated class.)
            } else if (e.getClass() == SQLException.class
                && e.getCause() == null
                && e.getMessage() != null
                && e.getMessage().equals(""))
            {
                // This is expected, from a later version of Dbcp.
            } else {
                fail("Expected exception, but got a different one: " + e);
            }
        } catch (IllegalArgumentException e) {
            handleIllegalArgumentException(properties, e);
        } finally {
            RolapConnectionPool.instance().clearPool();
        }
    }

    public void testNonPooledConnectionWithProperties() {
        Util.PropertyList properties =
            TestContext.instance().getConnectionProperties().clone();

        // Only the JDBC-ODBC bridge gives the error necessary for this
        // test to succeed. So trivially succeed for all other JDBC
        // drivers.
        final String jdbc = properties.get("Jdbc");
        if (jdbc != null
            && !jdbc.startsWith("jdbc:odbc:"))
            {
            return;
        }

        // This test is just like the test testPooledConnectionWithProperties
        // except with non-pooled connections.
        properties.put("jdbc.charSet", "UTF-16");
        properties.put(RolapConnectionProperties.PoolNeeded.name(), "false");

        final StringBuilder buf = new StringBuilder();
        DataSource dataSource =
            RolapConnection.createDataSource(null, properties, buf);
        final String desc = buf.toString();
        assertTrue(desc.startsWith("Jdbc="));

        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            fail("Expected exception");
        } catch (SQLException se) {
            // this is expected
        } catch (IllegalArgumentException e) {
            handleIllegalArgumentException(properties, e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Handle an {@link IllegalArgumentException} which occurs when have
     * tried to create a connection with an illegal charset.
     */
    private void handleIllegalArgumentException(
        Util.PropertyList properties,
        IllegalArgumentException e)
    {
        // Workaround Java bug #6504538 (see http://bugs.sun.com) with synopsis
        // "DriverManager.getConnection throws IllegalArgumentException".
        if (System.getProperties().getProperty("java.version")
            .startsWith("1.6."))
        {
            properties.remove("jdbc.charSet");

            final StringBuilder buf = new StringBuilder();
            DataSource dataSource =
                RolapConnection.createDataSource(null, properties, buf);
            final String desc = buf.toString();
            assertTrue(desc.startsWith("Jdbc="));

            try {
                Connection connection1 = dataSource.getConnection();
                connection1.close();
            } catch (SQLException e1) {
                // ignore
            }
        } else {
            fail("Expect IllegalArgumentException only in JDK 1.6, got " + e);
        }
    }

    /**
     * Tests that the FORMAT function uses the connection's locale.
     */
    public void testFormatLocale() {
        String expr = "FORMAT(1234.56, \"#,##.#\")";
        checkLocale("es_ES", expr, "1.234,6", false);
        checkLocale("es_MX", expr, "1,234.6", false);
        checkLocale("en_US", expr, "1,234.6", false);
    }

    /**
     * Tests that measures are formatted using the connection's locale.
     */
    public void testFormatStringLocale() {
        checkLocale("es_ES", "1234.56", "1.234,6", true);
        checkLocale("es_MX", "1234.56", "1,234.6", true);
        checkLocale("en_US", "1234.56", "1,234.6", true);
    }

    private static void checkLocale(
        final String localeName, String expr, String expected, boolean isQuery)
    {
        TestContext testContextSpain = new TestContext() {
            public mondrian.olap.Connection getConnection() {
                Util.PropertyList properties =
                    Util.parseConnectString(getConnectString());
                properties.put(
                    RolapConnectionProperties.Locale.name(),
                    localeName);
                return DriverManager.getConnection(properties, null);
            }
        };
        if (isQuery) {
            String query = "WITH MEMBER [Measures].[Foo] AS '" + expr + "',\n"
                + " FORMAT_STRING = '#,##.#' \n"
                + "SELECT {[MEasures].[Foo]} ON COLUMNS FROM [Sales]";
            String expected2 =
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[Foo]}\n"
                + "Row #0: " + expected + "\n";
            testContextSpain.assertQueryReturns(query, expected2);
        } else {
            testContextSpain.assertExprReturns(expr, expected);
        }
    }

    public void testConnectSansCatalogFails() {
        Util.PropertyList properties =
            TestContext.instance().getConnectionProperties().clone();
        properties.remove(RolapConnectionProperties.Catalog.name());
        properties.remove(RolapConnectionProperties.CatalogContent.name());

        if (RolapUtil.SQL_LOGGER.isDebugEnabled()) {
            RolapUtil.SQL_LOGGER.debug(
                this.getName() + "\n  [Connection Properties | " + properties
                + "]\n");
        } else {
            System.out.println(properties);
        }

        try {
            DriverManager.getConnection(
                properties,
                null);
            fail("expected exception");
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().indexOf(
                    "Connect string must contain property 'Catalog' or "
                    + "property 'CatalogContent'")
                >= 0);
        }
    }

    public void testJndiConnection() throws NamingException {
        // Cannot guarantee that this test will work if they have chosen to
        // resolve data sources other than by JNDI.
        if (MondrianProperties.instance().DataSourceResolverClass.isSet()) {
            return;
        }
        // get a regular connection
        Util.PropertyList properties =
            TestContext.instance().getConnectionProperties().clone();
        final StringBuilder buf = new StringBuilder();
        final DataSource dataSource =
            RolapConnection.createDataSource(null, properties, buf);
        // Don't know what the connect string is - it differs with database
        // and with the user's set up - but we know that it contains a JDBC
        // connect string. Best we can do is check that createDataSource is
        // setting it to something.
        final String desc = buf.toString();
        assertTrue(desc, desc.startsWith("Jdbc="));

        final List<String> lookupCalls = new ArrayList<String>();
        // mock the JNDI naming manager to provide that datasource
        THREAD_INITIAL_CONTEXT.set(
            // Use lazy initialization. Otherwise during initialization of this
            // initial context JNDI tries to create a default initial context
            // and bumps into itself coming the other way.
            new InitialContext(true) {
                public Object lookup(String str) {
                    lookupCalls.add("Called");
                    return dataSource;
                }
            }
       );

        // Use the datasource property to connect to the database.
        // Remove user and password, because some data sources (those using
        // pools) don't allow you to override user.
        Util.PropertyList properties2 =
            TestContext.instance().getConnectionProperties().clone();
        properties2.remove(RolapConnectionProperties.Jdbc.name());
        properties2.remove(RolapConnectionProperties.JdbcUser.name());
        properties2.remove(RolapConnectionProperties.JdbcPassword.name());
        properties2.put(
            RolapConnectionProperties.DataSource.name(), "jnditest");
        DriverManager.getConnection(properties2, null);

        // if we've made it here with lookupCalls,
        // we've successfully used JNDI
        assertTrue(lookupCalls.size() > 0);
    }

    public void testDataSourceOverrideUserPass()
        throws SQLException, NamingException
    {
        // use the datasource property to connect to the database
        Util.PropertyList properties =
            spy(TestContext.instance().getConnectionProperties().clone());
        final Dialect dialect = TestContext.instance().getDialect();
        if (dialect.getDatabaseProduct() == Dialect.DatabaseProduct.ACCESS) {
            // Access doesn't accept user/password, so this test is pointless.
            return;
        }

        final String jdbcUser =
            properties.get(RolapConnectionProperties.JdbcUser.name());
        final String jdbcPassword =
            properties.get(RolapConnectionProperties.JdbcPassword.name());
        if (jdbcUser == null || jdbcPassword == null) {
            // Can only run this test if username and password are explicit.
            return;
        }

        // Define a data source with bogus user and password.
        properties.put(
            RolapConnectionProperties.JdbcUser.name(),
            "bogususer");
        properties.put(
            RolapConnectionProperties.JdbcPassword.name(),
            "boguspassword");
        properties.put(
            RolapConnectionProperties.PoolNeeded.name(),
            "false");
        final StringBuilder buf = new StringBuilder();
        final DataSource dataSource =
            RolapConnection.createDataSource(null, properties, buf);
        final String desc = buf.toString();
        assertTrue(desc, desc.startsWith("Jdbc="));
        assertTrue(desc, desc.indexOf("JdbcUser=bogususer") >= 0);
        verify(
            properties,
            atLeastOnce()).get(RolapConnectionProperties.JdbcPassword.name());
        final String jndiName = "jndiDataSource";
        THREAD_INITIAL_CONTEXT.set(
            new InitialContext() {
                public Object lookup(String str) {
                    return str.equals(jndiName)
                        ? dataSource
                        : null;
                }
            }
       );

        // Create a property list that we will use for the actual mondrian
        // connection. Replace the original JDBC info with the data source we
        // just created.
        final Util.PropertyList properties2 = new Util.PropertyList();
        for (Pair<String, String> entry : properties) {
            properties2.put(entry.getKey(), entry.getValue());
        }
        properties2.remove(RolapConnectionProperties.Jdbc.name());
        properties2.put(
            RolapConnectionProperties.DataSource.name(),
            jndiName);

        // With JdbcUser and JdbcPassword credentials in the mondrian connect
        // string, the data source's "user" and "password" properties are
        // overridden and the connection succeeds.
        properties2.put(
            RolapConnectionProperties.JdbcUser.name(),
            jdbcUser);
        properties2.put(
            RolapConnectionProperties.JdbcPassword.name(),
            jdbcPassword);
        mondrian.olap.Connection connection = null;
        try {
            connection =
                DriverManager.getConnection(properties2, null);
            Query query = connection.parseQuery("select from [Sales]");
            final Result result = connection.execute(query);
            assertNotNull(result);
        } finally {
            if (connection != null) {
                connection.close();
                connection = null;
            }
        }

        // If we don't specify JdbcUser and JdbcPassword in the mondrian
        // connection properties, mondrian uses the data source's
        // bogus credentials, and the connection fails.
        properties2.remove(RolapConnectionProperties.JdbcUser.name());
        properties2.remove(RolapConnectionProperties.JdbcPassword.name());
        for (String poolNeeded : Arrays.asList("false", "true")) {
            // Important to test with & without pooling. Connection pools
            // typically do not let you change user, so it's important that
            // mondrian handles these right.
            properties2.put(
                RolapConnectionProperties.PoolNeeded.name(), poolNeeded);
            try {
                connection = DriverManager.getConnection(properties2, null);
                fail("Expected exception");
            } catch (MondrianException e) {
                final String s = TestContext.getStackTrace(e);
                assertTrue(
                    s,
                    s.indexOf(
                        "Error while creating SQL connection: "
                        + "DataSource=jndiDataSource") >= 0);
                switch (dialect.getDatabaseProduct()) {
                case DERBY:
                    assertTrue(
                        s,
                        s.indexOf(
                            "Caused by: java.sql.SQLException: "
                            + "Schema 'BOGUSUSER' does not exist") >= 0);
                    break;
                case ORACLE:
                    assertTrue(
                        s,
                        s.indexOf(
                            "Caused by: java.sql.SQLException: ORA-01017: "
                            + "invalid username/password; logon denied") >= 0);
                    break;
                case MYSQL:
                case MARIADB:
                    assertTrue(
                        s,
                        s.indexOf(
                            "Caused by: java.sql.SQLException: Access denied "
                            + "for user 'bogususer'") >= 0);
                    break;
                case POSTGRESQL:
                    assertTrue(
                        s,
                        s.indexOf(
                            "Caused by: org.postgresql.util.PSQLException: "
                            + "FATAL: password authentication failed for "
                            + "user \"bogususer\"") >= 0);
                    break;
                }
            } finally {
                if (connection != null) {
                    connection.close();
                    connection = null;
                }
            }
        }
    }

    public void testGetJdbcConnectionWhenJdbcIsNull() {
        final StringBuilder connectInfo = new StringBuilder();
        Util.PropertyList properties =
          TestContext.instance().getConnectionProperties().clone();
        properties.remove(RolapConnectionProperties.Jdbc.name());
        try {
            DataSource dataSource =
              RolapConnection.createDataSource(null, properties, connectInfo);
        } catch (RuntimeException ex) {
            assertTrue(
              connectInfo.toString(),
              StringUtils.isBlank(connectInfo.toString()));
        }
    }

    public void testJdbcConnectionString() {
        final StringBuilder connectInfo = new StringBuilder();
        Map<String, String> connectInfoMap = new HashMap<>();

        Util.PropertyList properties =
          TestContext.instance().getConnectionProperties().clone();
        properties.put(
          RolapConnectionProperties.JdbcUser.name(), "sqlserver://localhost");
        properties.put("databaseName", "databaseTest");
        properties.put("integratedSecurity", "true");

        DataSource dataSource =
          RolapConnection.createDataSource(null, properties, connectInfo);

        assertFalse(StringUtils.isBlank(connectInfo.toString()));

        String[] parseconnectInfo = connectInfo.toString().split(";");
        for (String parseconnectInfoVals : parseconnectInfo) {
            String[] parseconnectInfoVal = parseconnectInfoVals.split("=");
            connectInfoMap.put(parseconnectInfoVal[0], parseconnectInfoVal[1]);
        }

        assertTrue(connectInfoMap.get("databaseName"), true);
        assertEquals(connectInfoMap.get("databaseName"), "databaseTest");
        assertTrue(connectInfoMap.get("integratedSecurity"), true);
        assertEquals(connectInfoMap.get("integratedSecurity"), "true");
    }

    public void testJdbcConnectionStringWithoutDatabase() {
        final StringBuilder connectInfo = new StringBuilder();
        Util.PropertyList properties =
          TestContext.instance().getConnectionProperties().clone();
        properties.put(
          RolapConnectionProperties.JdbcUser.name(), "sqlserver://localhost");

        DataSource dataSource =
          RolapConnection.createDataSource(null, properties, connectInfo);

        assertFalse(StringUtils.isBlank(connectInfo.toString()));
        assertFalse(connectInfo.toString().contains("databaseName"));
    }

    public void testJdbcConnectionStringWithoutIntegratedSecurity() {
        final StringBuilder connectInfo = new StringBuilder();
        Util.PropertyList properties =
          TestContext.instance().getConnectionProperties().clone();
        properties.put(
          RolapConnectionProperties.JdbcUser.name(), "sqlserver://localhost");

        DataSource dataSource =
          RolapConnection.createDataSource(null, properties, connectInfo);

        assertFalse(StringUtils.isBlank(connectInfo.toString()));
        assertFalse(connectInfo.toString().contains("integratedSecurity"));
    }

}

// End RolapConnectionTest.java
