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

import java.util.regex.Pattern;

public class DialectUtil {

  private static final Pattern UNICODE_CASE_FLAG_IN_JAVA_REG_EXP_PATTERN = Pattern.compile( "\\|\\(\\?u\\)" );
  private static final String EMPTY = "";

  /**
   * Cleans up the reqular expression from the unicode-aware case folding embedded flag expression (?u)
   *
   * @param javaRegExp
   *          the regular expression to clean up
   * @return the cleaned regular expression
   */
  public static String cleanUnicodeAwareCaseFlag( String javaRegExp ) {
    String cleaned = javaRegExp;
    if ( cleaned != null && isUnicodeCaseFlagInRegExp( cleaned ) ) {
      cleaned = UNICODE_CASE_FLAG_IN_JAVA_REG_EXP_PATTERN.matcher( cleaned ).replaceAll( EMPTY );
    }
    return cleaned;
  }

  private static boolean isUnicodeCaseFlagInRegExp( String javaRegExp ) {
    return UNICODE_CASE_FLAG_IN_JAVA_REG_EXP_PATTERN.matcher( javaRegExp ).find();
  }

}

//End DialectUtil.java
