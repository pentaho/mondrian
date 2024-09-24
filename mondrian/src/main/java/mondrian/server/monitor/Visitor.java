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

/**
 * Visitor for events.
 */
public interface Visitor<T> {
    T visit(ConnectionStartEvent event);
    T visit(ConnectionEndEvent event);
    T visit(StatementStartEvent event);
    T visit(StatementEndEvent event);
    T visit(ExecutionStartEvent event);
    T visit(ExecutionPhaseEvent event);
    T visit(ExecutionEndEvent event);
    T visit(SqlStatementStartEvent event);
    T visit(SqlStatementExecuteEvent event);
    T visit(SqlStatementEndEvent event);
    T visit(CellCacheSegmentCreateEvent event);
    T visit(CellCacheSegmentDeleteEvent event);
}

// End Visitor.java
