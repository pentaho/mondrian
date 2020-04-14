/*
 // This software is subject to the terms of the Eclipse Public License v1.0
 // Agreement, available at the following URL:
 // http://www.eclipse.org/legal/epl-v10.html.
 // You must accept the terms of that agreement to use this software.
 //
 // Copyright (C) 2018 - 2018 Hitachi Vantara
 // All Rights Reserved.
 */
package mondrian.spi.impl;

import junit.framework.TestCase;
import mondrian.spi.Dialect;
import mondrian.spi.DialectManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Petr Procházka (petrprochy)
 * @since 2018/12/14
 */
public class MariaDBDialectTest extends TestCase {
    private final String version = "10.2.17-MariaDB-log";

    private Connection connection = mock(Connection.class);
    private Statement statement = mock(Statement.class);
    private MariaDBDialect dialect;

    @Override
    public void setUp() throws Exception {
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getDatabaseProductName()).thenReturn("MySQL");
        when(metaData.getDatabaseProductVersion()).thenReturn(version);
        when(connection.createStatement()).thenReturn(statement);
        dialect = new MariaDBDialect(connection);
    }

    @SuppressWarnings("SqlNoDataSourceInspection")
    public void testCreateDialect() throws Exception {
        final DataSource dataSource = mock(DataSource.class);
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString(1)).thenReturn(version);
        when(statement.executeQuery("select version()")).thenReturn(resultSet);

        final Dialect dialect = DialectManager.createDialect(dataSource, connection);
        assertEquals("Implementation class", dialect.getClass(), MariaDBDialect.class);
    }

    public void testAllowFromQuery() {
        assertTrue("Allow from query", dialect.allowsFromQuery());
    }
}
