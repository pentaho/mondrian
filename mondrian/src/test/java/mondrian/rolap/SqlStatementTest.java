/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2015-2015 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.MondrianServer;
import mondrian.olap.QueryCanceledException;
import mondrian.resource.MondrianResource;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.server.StatementImpl;
import mondrian.server.monitor.Monitor;

import junit.framework.TestCase;

import static org.mockito.Mockito.*;

/**
 * @author Andrey Khayrutdinov
 */
public class SqlStatementTest extends TestCase {

  public void testPrintingNilDurationIfCancelledBeforeStart() throws Exception {
    Monitor monitor = mock(Monitor.class);

    MondrianServer srv = mock(MondrianServer.class);
    when(srv.getMonitor()).thenReturn(monitor);

    RolapConnection rolapConnection = mock(RolapConnection.class);
    when(rolapConnection.getServer()).thenReturn(srv);

    StatementImpl statMock = mock(StatementImpl.class);
    when(statMock.getMondrianConnection()).thenReturn(rolapConnection);

    Execution execution = new Execution(statMock, 0);
    execution = spy(execution);
    doThrow(MondrianResource.instance().QueryCanceled.ex())
      .when(execution).checkCancelOrTimeout();

    Locus locus = new Locus(execution, "component", "message");
    SqlStatement statement =
      new SqlStatement(null, "sql", null, 0, 0, locus, 0, 0, null);
    statement = spy(statement);

    try {
      statement.execute();
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if (!(cause instanceof QueryCanceledException)) {
        String message = "Expected QueryCanceledException but caught "
          + ((cause == null) ? null : cause.getClass().getSimpleName());
        fail(message);
      }
    }

    verify(statement).formatTimingStatus(eq(0L), anyInt());
  }
}
// End SqlStatementTest.java