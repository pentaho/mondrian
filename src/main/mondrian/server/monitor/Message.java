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
 * Message sent to a {@link Monitor} indicating an event of interest or a
 * command to execute.
 */
public interface Message {
    /**
     * Dispatches a call to the appropriate {@code visit} method on
     * {@link mondrian.server.monitor.Visitor}.
     *
     * @param visitor Visitor
     * @param <T> Return type
     * @return Value returned by the {@code visit} method
     */
    <T> T accept(Visitor<T> visitor);
}

// End Message.java
