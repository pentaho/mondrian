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
