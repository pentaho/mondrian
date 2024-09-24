/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
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
