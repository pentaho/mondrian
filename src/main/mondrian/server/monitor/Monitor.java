/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.server.monitor;

import java.util.List;

/**
 * Information about a Mondrian server.
 *
 * <p>Also, the {@link #sendEvent(Event)} allows a Mondrian subsystem to
 * notify the monitor of some event. The event is handled asynchronously.
 * We strongly recommend that the event's fields simple, final values; one
 * would not want the contents to have changed when the event is processed, or
 * for the event to prevent a resource from being garbage-collected.</p>
 *
 * <p>All methods are thread-safe.</p>
 */
public interface Monitor {
    ServerInfo getServer();

    List<ConnectionInfo> getConnections();

    List<StatementInfo> getStatements();

    List<SqlStatementInfo> getSqlStatements();

    /**
     * Sends an event to the monitor.
     *
     * @param event Event
     */
    void sendEvent(Event event);
}

// End Monitor.java
