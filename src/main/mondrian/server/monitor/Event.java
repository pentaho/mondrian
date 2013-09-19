/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.server.monitor;

import mondrian.rolap.RolapUtil;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Base class for an event of interest.
 *
 * <p>This class, and subclasses, is an immutable but serializable.</p>
 */
public abstract class Event implements Message {
    /**
     * When the event occurred. Milliseconds since the epoch UTC, just like
     * {@link System#currentTimeMillis()}.
     */
    public final long timestamp;

    /**
     * When {@link RolapUtil#MONITOR_LOGGER} is set to TRACE,
     * this field will contain the stack of the code which
     * created this event.
     */
    public final String stack;

    /**
     * Creates an Event.
     *
     * @param timestamp Timestamp
     *
     */
    public Event(
        long timestamp)
    {
        this.timestamp = timestamp;
        if (RolapUtil.MONITOR_LOGGER.isTraceEnabled()) {
            try {
                throw new Exception();
            } catch (Exception e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw, true));
                this.stack = sw.toString();
            }
        } else {
            this.stack = null;
        }
    }
}

// End Event.java
