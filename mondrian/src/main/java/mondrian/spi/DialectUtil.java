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
