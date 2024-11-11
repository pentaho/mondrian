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
