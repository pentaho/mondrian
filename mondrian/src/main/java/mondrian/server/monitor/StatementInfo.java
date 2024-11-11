/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

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
    public boolean getExecuting() {
        return executeStartCount > executeEndCount;
    }

    public long getStatementId() {
        return statementId;
    }

    public int getExecuteStartCount() {
        return executeStartCount;
    }

    public int getExecuteEndCount() {
        return executeEndCount;
    }

    public int getPhaseCount() {
        return phaseCount;
    }

    public long getCellCacheRequestCount() {
        return cellCacheRequestCount;
    }

    public long getCellCacheHitCount() {
        return cellCacheHitCount;
    }

    public long getCellCacheMissCount() {
        return cellCacheMissCount;
    }

    public long getCellCachePendingCount() {
        return cellCachePendingCount;
    }

    public int getSqlStatementStartCount() {
        return sqlStatementStartCount;
    }

    public int getSqlStatementExecuteCount() {
        return sqlStatementExecuteCount;
    }

    public int getSqlStatementEndCount() {
        return sqlStatementEndCount;
    }

    public long getSqlStatementRowFetchCount() {
        return sqlStatementRowFetchCount;
    }

    public long getSqlStatementExecuteNanos() {
        return sqlStatementExecuteNanos;
    }

    public int getCellRequestCount() {
        return cellRequestCount;
    }
}

// End StatementInfo.java
