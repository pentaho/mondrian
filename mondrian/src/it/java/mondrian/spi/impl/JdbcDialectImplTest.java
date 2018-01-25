/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2018 Pentaho Corporation.
// All rights reserved.
 */
package mondrian.spi.impl;

import junit.framework.TestCase;

public class JdbcDialectImplTest extends TestCase {
  private static final String ILLEGAL_BOOLEAN_LITERAL =
      "illegal for base dialect implemetation boolean literal";
  private static final String ILLEGAL_BOOLEAN_LITERAL_MESSAGE =
      "Illegal BOOLEAN literal:  ";
  private static final String BOOLEAN_LITERAL_TRUE = "True";
  private static final String BOOLEAN_LITERAL_FALSE = "False";
  private static final String BOOLEAN_LITERAL_ONE = "1";
  private static final String BOOLEAN_LITERAL_ZERO = "0";

  private JdbcDialectImpl jdbcDialect = new JdbcDialectImpl();
  private StringBuilder buf;

  @Override
  protected void setUp() throws Exception {
    buf = new StringBuilder();
  }

  public void testAllowsRegularExpressionInWhereClause() {
    assertFalse(jdbcDialect.allowsRegularExpressionInWhereClause());
  }

  public void testGenerateRegularExpression() {
    assertNull(jdbcDialect.generateRegularExpression(null, null));
  }

  public void testQuoteBooleanLiteral_True() throws Exception {
    assertEquals(0, buf.length());
    jdbcDialect.quoteBooleanLiteral(buf, BOOLEAN_LITERAL_TRUE);
    assertEquals(BOOLEAN_LITERAL_TRUE, buf.toString());
  }

  public void testQuoteBooleanLiteral_False() throws Exception {
    assertEquals(0, buf.length());
    jdbcDialect.quoteBooleanLiteral(buf, BOOLEAN_LITERAL_FALSE);
    assertEquals(BOOLEAN_LITERAL_FALSE, buf.toString());
  }

  public void testQuoteBooleanLiteral_OneIllegaLiteral() throws Exception {
    assertEquals(0, buf.length());
    try {
      jdbcDialect.quoteBooleanLiteral(buf, BOOLEAN_LITERAL_ONE);
    fail(
        "The illegal boolean literal exception should appear BUT it was not.");
    } catch (NumberFormatException e) {
      assertEquals(
          ILLEGAL_BOOLEAN_LITERAL_MESSAGE
          + BOOLEAN_LITERAL_ONE,
          e.getMessage());
    }
  }

  public void testQuoteBooleanLiteral_ZeroIllegaLiteral() throws Exception {
    assertEquals(0, buf.length());
    try {
      jdbcDialect.quoteBooleanLiteral(buf, BOOLEAN_LITERAL_ZERO);
    fail(
        "The illegal boolean literal exception should appear BUT it was not.");
    } catch (NumberFormatException e) {
      assertEquals(
          ILLEGAL_BOOLEAN_LITERAL_MESSAGE
          + BOOLEAN_LITERAL_ZERO,
          e.getMessage());
    }
  }

  public void testQuoteBooleanLiteral_TrowsExceptionOnIllegaLiteral()
      throws Exception {
    assertEquals(0, buf.length());
    try {
      jdbcDialect.quoteBooleanLiteral(buf, ILLEGAL_BOOLEAN_LITERAL);
    fail(
        "The illegal boolean literal exception should appear BUT it was not.");
    } catch (NumberFormatException e) {
      assertEquals(
          ILLEGAL_BOOLEAN_LITERAL_MESSAGE
          + ILLEGAL_BOOLEAN_LITERAL,
          e.getMessage());
    }
  }

}
// End JdbcDialectImplTest.java
