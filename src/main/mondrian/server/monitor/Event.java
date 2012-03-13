/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.server.monitor;

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
     * Creates an Event.
     *
     * @param timestamp Timestamp
     *
     */
    public Event(
        long timestamp)
    {
        this.timestamp = timestamp;
    }
}

// End Event.java
