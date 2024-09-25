/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2024 Hitachi Vantara.
// All rights reserved.
 */
package mondrian.spi.impl;

import junit.framework.TestCase;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Date;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;

/**
 * Tests for SybaseDialect
 *
 * @author Yury Bakhmutski
 */
public class SybaseDialectTest extends TestCase {
    @Mock
    SybaseDialect sybaseDialectMock;

    @Override
    protected void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test for MONDRIAN-2259 issue.
     * Is assumed SybaseDialect methods are called.
     */
    public void testQuoteDateLiteral() {
        String input = "1997-01-03 00:00:00.0";

        doCallRealMethod().when(sybaseDialectMock).quoteDateLiteral(
            any(StringBuilder.class), anyString(), any(Date.class));

        doCallRealMethod().when(sybaseDialectMock).quoteDateLiteral(
            any(StringBuilder.class), anyString());

        StringBuilder buffer = new StringBuilder();
        sybaseDialectMock.quoteDateLiteral(buffer, input);
        String actual = buffer.toString();
        String expected = "'1997-01-03'";
        assertEquals(expected, actual);
    }

}

// End SybaseDialectTest.java
