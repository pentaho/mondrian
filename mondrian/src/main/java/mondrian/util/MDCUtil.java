/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2020 Hitachi Vantara..  All rights reserved.
*/

package mondrian.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.ThreadContext;

/**
 * MdcUtil is a small helper class for copying log4j MDC context between threads. The main use case is for maintaining
 * session, IP and user logging information down to the SQL and MDX level to track sensitive data access.
 *
 * @author benny
 */
public class MDCUtil {
  private final Map<String, String> mdc = new HashMap<>();

  /**
   * Constructor is called on parent thread so a snapshot of the MDC context is saved here.
   * 
   */
  public MDCUtil() {
    if ( ThreadContext.getContext() != null ) {
      this.mdc.putAll( ThreadContext.getContext() );
    }
  }

  /**
   * This is called on the child thread so the saved MDC context is copied into the child thread.
   * 
   */
  public void setContextMap() {
    final Map<String, String> old = ThreadContext.getContext();
    if ( old != null ) {
      old.clear();
    }
    for ( Entry<String, String> entry : mdc.entrySet() ) {
      ThreadContext.put( entry.getKey(), entry.getValue() );
    }
  }

}

// End MdcUtil.java
