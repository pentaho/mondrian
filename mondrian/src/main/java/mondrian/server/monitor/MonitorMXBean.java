/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.server.monitor;

import java.util.List;

/**
 * Defines the MXBean interface required to register
 * MonitorImpl with a JMX agent.  This simply lists
 * the attributes we want exposed to a JMX client.
 */
public interface MonitorMXBean {

    ServerInfo getServer();

    List<ConnectionInfo> getConnections();

    List<StatementInfo> getStatements();

    List<SqlStatementInfo> getSqlStatements();
}

// End MonitorMXBean.java