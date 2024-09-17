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
    public final int expCacheHitCount;
    public final int expCacheMissCount;

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
        int cellRequestCount,
        int expCacheHitCount,
        int expCacheMissCount)
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
        this.expCacheHitCount = expCacheHitCount;
        this.expCacheMissCount = expCacheMissCount;
        assert cellCacheRequestCount
               == cellCacheHitCount
                  + cellCacheMissCount
                  + cellCachePendingCount;
    }
}

// End ExecutionInfo.java
