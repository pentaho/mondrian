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