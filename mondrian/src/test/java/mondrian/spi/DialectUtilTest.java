/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2017 Hitachi Vantara.
// All rights reserved.
 */
package mondrian.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class DialectUtilTest{

  @Test
  public void testCleanUnicodeAwareCaseFlag_InputNull() {
    String inputExpression = null;
    String cleaned = DialectUtil.cleanUnicodeAwareCaseFlag( inputExpression );
    assertNull( cleaned );
  }

  @Test
  public void testCleanUnicodeAwareCaseFlag_InputContainsFlag() {
    String inputExpression = "(?i)|(?u).*ａ.*";
    String expectedExpression = "(?i).*ａ.*";
    String cleaned = DialectUtil.cleanUnicodeAwareCaseFlag( inputExpression );
    assertEquals( expectedExpression, cleaned );
  }

  @Test
  public void testCleanUnicodeAwareCaseFlag_InputNotContainsFlag() {
    String inputExpression = "(?i).*ａ.*";
    String expectedExpression = "(?i).*ａ.*";
    String cleaned = DialectUtil.cleanUnicodeAwareCaseFlag( inputExpression );
    assertEquals( expectedExpression, cleaned );
  }

}
//End DialectUtilTest.java
