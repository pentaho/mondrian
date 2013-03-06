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
 * Information about a statement executed by Mondrian.
 */
public class StatementInfo extends Info {
    public final long statementId;
    public final int executeStartCount;
    public final int executeEndCount;
    public final int phaseCount;
    public final long cellCacheRequestCount;
    public final long cellCacheHitCount;
    public final long cellCacheMissCount;
    public final long cellCachePendingCount;
    public final int sqlStatementStartCount;
    public final int sqlStatementExecuteCount;
    public final int sqlStatementEndCount;
    public final long sqlStatementRowFetchCount;
    public final long sqlStatementExecuteNanos;
    public final int cellRequestCount;

    public StatementInfo(
        String stack,
        long statementId,
        int executeStartCount,
        int executeEndCount,
        int phaseCount,
        long cellCacheRequestCount,
        long cellCacheHitCount,
        long cellCacheMissCount,
        long cellCachePendingCount,
        int sqlStatementStartCount,
        int sqlStatementExecuteCount,
        int sqlStatementEndCount,
        long sqlStatementRowFetchCount,
        long sqlStatementExecuteNanos,
        int cellRequestCount)
    {
        super(stack);
        this.statementId = statementId;
        this.cellCacheRequestCount = cellCacheRequestCount;
        this.phaseCount = phaseCount;
        this.cellCacheHitCount = cellCacheHitCount;
        this.cellCacheMissCount = cellCacheMissCount;
        this.cellCachePendingCount = cellCachePendingCount;
        this.executeStartCount = executeStartCount;
        this.executeEndCount = executeEndCount;
        this.sqlStatementStartCount = sqlStatementStartCount;
        this.sqlStatementExecuteCount = sqlStatementExecuteCount;
        this.sqlStatementEndCount = sqlStatementEndCount;
        this.sqlStatementRowFetchCount = sqlStatementRowFetchCount;
        this.sqlStatementExecuteNanos = sqlStatementExecuteNanos;
        this.cellRequestCount = cellRequestCount;
    }

    /**
     * @return Whether the statement is currently executing.
     */
    public boolean executing() {
        return executeStartCount > executeEndCount;
    }
}

// End StatementInfo.java
