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
package mondrian.spi;

import junit.framework.TestCase;

public class DialectUtilTest extends TestCase {

  public void testCleanUnicodeAwareCaseFlag_InputNull() {
    String inputExpression = null;
    String cleaned = DialectUtil.cleanUnicodeAwareCaseFlag( inputExpression );
    assertNull( cleaned );
  }

  public void testCleanUnicodeAwareCaseFlag_InputContainsFlag() {
    String inputExpression = "(?i)|(?u).*ａ.*";
    String expectedExpression = "(?i).*ａ.*";
    String cleaned = DialectUtil.cleanUnicodeAwareCaseFlag( inputExpression );
    assertEquals( expectedExpression, cleaned );
  }

  public void testCleanUnicodeAwareCaseFlag_InputNotContainsFlag() {
    String inputExpression = "(?i).*ａ.*";
    String expectedExpression = "(?i).*ａ.*";
    String cleaned = DialectUtil.cleanUnicodeAwareCaseFlag( inputExpression );
    assertEquals( expectedExpression, cleaned );
  }

}
//End DialectUtilTest.java
