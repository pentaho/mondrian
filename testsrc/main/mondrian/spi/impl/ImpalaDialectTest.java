/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2015 Pentaho Corporation.
// All rights reserved.
 */
package mondrian.spi.impl;

import mondrian.spi.Dialect;

import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Khayrutdinov
 */
public class ImpalaDialectTest extends TestCase {

    public void testGenerateRegularExpression_InvalidRegex()
        throws Exception
    {
        assertNull(
            "Invalid regex should be ignored",
            callGenerateRegularExpression("table.column", "(a"));
    }

    public void testGenerateRegularExpression_CaseInsensitive()
        throws Exception
    {
        String sql = callGenerateRegularExpression("table.column", "(?i).*1.*");
        assertSqlWithRegex(false, sql, "'.*1.*'");
    }

    public void testGenerateRegularExpression_CaseSensitive()
        throws Exception
    {
        String sql = callGenerateRegularExpression("table.column", ".*1.*");
        assertSqlWithRegex(true, sql, "'.*1.*'");
    }

    private String callGenerateRegularExpression(String source, String regex)
        throws Exception
    {
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(metaData.getDatabaseProductName())
            .thenReturn(Dialect.DatabaseProduct.IMPALA.name());

        Connection connection = mock(Connection.class);
        when(connection.getMetaData()).thenReturn(metaData);

        return new ImpalaDialect(connection)
            .generateRegularExpression(source, regex);
    }

    private void assertSqlWithRegex(
        boolean isCaseSensitive,
        String sql,
        String quotedRegex) throws Exception
    {
        assertNotNull("Sql should be generated", sql);
        assertEquals(sql, isCaseSensitive, !sql.contains("UPPER"));
        assertTrue(sql, sql.contains("cast(table.column as string)"));
        assertTrue(sql, sql.contains("REGEXP"));
        assertTrue(sql, sql.contains(quotedRegex));
    }
}
// End ImpalaDialectTest.java
