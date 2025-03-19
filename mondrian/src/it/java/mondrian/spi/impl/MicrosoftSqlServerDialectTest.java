/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package mondrian.spi.impl;

import java.sql.Statement;
import junit.framework.TestCase;
import mondrian.olap.Util;
import mondrian.spi.Dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MicrosoftSqlServerDialectTest extends TestCase {

  private static final String ILLEGAL_BOOLEAN_LITERAL =
      "illegal for this dialect boolean literal";
  private static final String ILLEGAL_BOOLEAN_LITERAL_MESSAGE =
      "Illegal BOOLEAN literal:  ";
  private static final String BOOLEAN_LITERAL_TRUE = "True";
  private static final String BOOLEAN_LITERAL_FALSE = "False";
  private static final String BOOLEAN_LITERAL_ONE = "1";
  private static final String BOOLEAN_LITERAL_ZERO = "0";
  private Connection connection = mock(Connection.class);
  private DatabaseMetaData metaData = mock(DatabaseMetaData.class);
  Statement statmentMock = mock(Statement.class);
  private MicrosoftSqlServerDialect dialect;
  private StringBuilder buf;

  @Override
  protected void setUp() throws Exception {
    when(metaData.getDatabaseProductName()).thenReturn(
        Dialect.DatabaseProduct.MSSQL.name());
    when(statmentMock.execute(any())).thenReturn(false);
    when(connection.getMetaData()).thenReturn(metaData);
    when(connection.createStatement()).thenReturn(statmentMock);
    dialect = new MicrosoftSqlServerDialect(connection);
    buf = new StringBuilder();
  }

  public void testQuoteBooleanLiteral_True() throws Exception {
    assertEquals(0, buf.length());
    dialect.quoteBooleanLiteral(buf, BOOLEAN_LITERAL_TRUE);
    assertEquals(Util.singleQuoteString(BOOLEAN_LITERAL_TRUE), buf.toString());
  }

  public void testQuoteBooleanLiteral_False() throws Exception {
    assertEquals(0, buf.length());
    dialect.quoteBooleanLiteral(buf, BOOLEAN_LITERAL_FALSE);
    assertEquals(Util.singleQuoteString(
        BOOLEAN_LITERAL_FALSE), buf.toString());
  }

  public void testQuoteBooleanLiteral_One() throws Exception {
    assertEquals(0, buf.length());
    dialect.quoteBooleanLiteral(buf, BOOLEAN_LITERAL_ONE);
    assertEquals(Util.singleQuoteString(BOOLEAN_LITERAL_ONE), buf.toString());
  }

  public void testQuoteBooleanLiteral_Zero() throws Exception {
    assertEquals(0, buf.length());
    dialect.quoteBooleanLiteral(buf, BOOLEAN_LITERAL_ZERO);
    assertEquals(Util.singleQuoteString(BOOLEAN_LITERAL_ZERO), buf.toString());
  }

  public void testQuoteBooleanLiteral_TrowsException() throws Exception {
    assertEquals(0, buf.length());
    try {
    dialect.quoteBooleanLiteral(buf, ILLEGAL_BOOLEAN_LITERAL);
    fail(
        "The illegal boolean literal exception should appear BUT it was not.");
    } catch (NumberFormatException e) {
      assertEquals(
          ILLEGAL_BOOLEAN_LITERAL_MESSAGE
          + ILLEGAL_BOOLEAN_LITERAL,
          e.getMessage());
    }
  }

  public void testQuoteStringLiteral() throws Exception {
    StringBuilder buf = new StringBuilder();
    String stringToQuote = "test";
    dialect.quoteStringLiteral(buf, stringToQuote);
    assertEquals("N'test'", buf.toString());
  }
}
// End MicrosoftSqlServerDialectTest.java
