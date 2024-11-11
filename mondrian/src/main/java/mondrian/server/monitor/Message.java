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


package mondrian.server.monitor;

/**
 * Message sent to a {@link Monitor} indicating an event of interest or a command to execute.
 */
public interface Message {
  /**
   * Dispatches a call to the appropriate {@code visit} method on {@link mondrian.server.monitor.Visitor}.
   *
   * @param visitor
   *          Visitor
   * @param <T>
   *          Return type
   * @return Value returned by the {@code visit} method
   */
  <T> T accept( Visitor<T> visitor );

  /**
   * Sets the MDC context into the current thread
   */
  void setContextMap();
}

// End Message.java
