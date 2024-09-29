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


package mondrian.server.monitor;

import mondrian.rolap.RolapUtil;
import mondrian.util.MDCUtil;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Base class for an event of interest.
 *
 * <p>
 * This class, and subclasses, is an immutable but serializable.
 * </p>
 */
public abstract class Event implements Message {
  /**
   * When the event occurred. Milliseconds since the epoch UTC, just like {@link System#currentTimeMillis()}.
   */
  public final long timestamp;

  /**
   * When {@link RolapUtil#MONITOR_LOGGER} is set to TRACE, this field will contain the stack of the code which created
   * this event.
   */
  public final String stack;

  private final MDCUtil mdc = new MDCUtil();

  /**
   * Creates an Event.
   *
   * @param timestamp
   *          Timestamp
   *
   */
  public Event( long timestamp ) {
    this.timestamp = timestamp;
    if ( RolapUtil.MONITOR_LOGGER.isTraceEnabled() ) {
      try {
        throw new Exception();
      } catch ( Exception e ) {
        StringWriter sw = new StringWriter();
        e.printStackTrace( new PrintWriter( sw, true ) );
        this.stack = sw.toString();
      }
    } else {
      this.stack = null;
    }
  }

  @Override
  public void setContextMap() {
    mdc.setContextMap();
  }
}

// End Event.java
