/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.server.monitor;

/**
 * Information about the execution of an MDX statement.
 */
public class ExecutionInfo extends Info {
    public final long executionId;
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

    public ExecutionInfo(
        String stack,
        long executionId,
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
        this.executionId = executionId;
        this.phaseCount = phaseCount;
        this.cellCacheRequestCount = cellCacheRequestCount;
        this.cellCacheHitCount = cellCacheHitCount;
        this.cellCacheMissCount = cellCacheMissCount;
        this.cellCachePendingCount = cellCachePendingCount;
        this.sqlStatementStartCount = sqlStatementStartCount;
        this.sqlStatementExecuteCount = sqlStatementExecuteCount;
        this.sqlStatementEndCount = sqlStatementEndCount;
        this.sqlStatementRowFetchCount = sqlStatementRowFetchCount;
        this.sqlStatementExecuteNanos = sqlStatementExecuteNanos;
        this.cellRequestCount = cellRequestCount;
        assert cellCacheRequestCount
               == cellCacheHitCount
                  + cellCacheMissCount
                  + cellCachePendingCount;
    }
}

// End ExecutionInfo.java
