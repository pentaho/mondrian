/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2017 Pentaho Corporation.
// All rights reserved.
 */
package mondrian.spi.impl;

import junit.framework.TestCase;

public class JdbcDialectImplTest extends TestCase {
  private JdbcDialectImpl jdbcDialect = new JdbcDialectImpl();

  public void testAllowsRegularExpressionInWhereClause() {
    assertFalse( jdbcDialect.allowsRegularExpressionInWhereClause() );
  }

  public void testGenerateRegularExpression() {
    assertNull( jdbcDialect.generateRegularExpression( null, null ) );
  }

}
//End JdbcDialectImplTest.java
