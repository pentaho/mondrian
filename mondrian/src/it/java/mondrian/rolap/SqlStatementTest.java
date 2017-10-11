/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2015-2017 Hitachi Vantara and others
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
import mondrian.spi.Dialect;

import static org.mockito.Mockito.*;

/**
 * @author Andrey Khayrutdinov
 */
public class SqlStatementTest extends TestCase {

  private Monitor monitor;
  private MondrianServer srv;
  private RolapConnection rolapConnection;
  private StatementImpl statMock;
  private Execution execution;
  private Locus locus;
  private SqlStatement statement;

  public void setUp() {
    monitor = mock(Monitor.class);

    srv = mock(MondrianServer.class);
    when(srv.getMonitor()).thenReturn(monitor);

    rolapConnection = mock(RolapConnection.class);
    when(rolapConnection.getServer()).thenReturn(srv);

    statMock = mock(StatementImpl.class);
    when(statMock.getMondrianConnection()).thenReturn(rolapConnection);

    execution = new Execution(statMock, 0);
    execution = spy(execution);
    doThrow(MondrianResource.instance().QueryCanceled.ex())
            .when(execution).checkCancelOrTimeout();

    locus = new Locus(execution, "component", "message");

    statement = new SqlStatement(null, "sql", null, 0, 0, locus, 0, 0, null);
    statement = spy(statement);
  }

  public void testPrintingNilDurationIfCancelledBeforeStart() throws Exception {
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

  public void testGetDialectSchemaAndConnectionNull() {
    try {
      this.statement.getDialect(null);
      fail("Should throw exception");
    } catch (Exception e) {
      verify(statement).createDialect();
    }
  }

  public void testGetDialectDialectNull() {
    RolapSchema schema = mock(RolapSchema.class);
    when(schema.getDialect()).thenReturn(null);
    try {
      statement.getDialect(schema);
      fail("Should throw exception");
    } catch (Exception e) {
      verify(statement).createDialect();
    }
  }

  public void testGetDialect() {
    RolapSchema schema = mock(RolapSchema.class);
    Dialect dialect = mock(Dialect.class);
    when(schema.getDialect()).thenReturn(dialect);
    Dialect dialectReturn = statement.getDialect(schema);
    assertNotNull(dialectReturn);
    assertEquals(dialect, dialectReturn);
  }

  public void testCreateDialect() {
    statement = mock(SqlStatement.class);
    Dialect dialect = mock(Dialect.class);
    when(statement.getDialect(any())).thenCallRealMethod();
    when(statement.createDialect()).thenReturn(dialect);
    Dialect dialectReturn = statement.getDialect(null);
    assertNotNull(dialectReturn);
    assertEquals(dialect, dialectReturn);
  }
}
// End SqlStatementTest.java
