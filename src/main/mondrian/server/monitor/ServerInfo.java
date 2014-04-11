/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.server.monitor;

/**
 * Information about a Mondrian server.
 */
public class ServerInfo extends Info {
    public final int connectionStartCount;
    public final int connectionEndCount;
    public final int statementStartCount;
    public final int statementEndCount;
    public final int sqlStatementStartCount;
    public final int sqlStatementExecuteCount;
    public final int sqlStatementEndCount;

    /**
     * Cumulative number of rows fetched from SQL statements.
     */
    public final long sqlStatementRowFetchCount;

    /**
     * Cumulative time spent executing SQL statements.
     */
    public final long sqlStatementExecuteNanos;

    /**
     * Total, over all SQL statements that are fetching cells into cache, of
     * the number of requested cells that will be satisfied by those SQL
     * statements. Note that a given SQL statement may round out the predicates
     * and bring back a few more cells than it was asked for.
     */
    public final int sqlStatementCellRequestCount;
    public final int cellCacheRequestCount;
    public final int cellCacheHitCount;
    public final int cellCacheMissCount;
    public final int cellCachePendingCount;
    public final int executeStartCount;
    public final int executeEndCount;
    public final long jvmHeapBytesUsed;
    public final long jvmHeapBytesCommitted;
    public final long jvmHeapBytesMax;

    /**
     * The number of segments currently in cache.
     */
    public final int segmentCount;

    /**
     * The number of segments that have been created since the server started.
     * (Should equal the sum {@link #segmentCreateViaExternalCount}
     * + {@link #segmentCreateViaRollupCount}
     * + {@link #segmentCreateViaSqlCount}.)
     */
    public final int segmentCreateCount;

    /**
     * The number of segments that have been created via external since the
     * server started.
     */
    public final int segmentCreateViaExternalCount;

    /**
     * The number of segments that have been deleted via external since the
     * server started.
     */
    public final int segmentDeleteViaExternalCount;

    /**
     * The number of segments that have been created via rollup since the server
     * started.
     */
    public final int segmentCreateViaRollupCount;

    /**
     * The number of segments that have been created via SQL since the server
     * started.
     */
    public final int segmentCreateViaSqlCount;

    /**
     * The number of cells currently in cache.
     */
    public final int cellCount;

    /**
     * The sum of the dimensionality of every cells currently in cache.
     *
     * <p>For example, each cell in the segment (State={CA, TX} * Year={2011})
     * has two coordinates.</p>
     *
     * <p>From this, we can compute the average dimensionality of segments
     * in cache, weighted by the number of cells. It gives an idea of the
     * memory overhead for segment axes.</p>
     */
    public final int cellCoordinateCount;

    public ServerInfo(
        String stack,
        int connectionStartCount,
        int connectionEndCount,
        int statementStartCount,
        int statementEndCount,
        int sqlStatementStartCount,
        int sqlStatementExecuteCount,
        int sqlStatementEndCount,
        long sqlStatementRowFetchCount,
        long sqlStatementExecuteNanos,
        int sqlStatementCellRequestCount,
        int cellCacheHitCount,
        int cellCacheRequestCount,
        int cellCacheMissCount,
        int cellCachePendingCount,
        int executeStartCount,
        int executeEndCount,
        long jvmHeapBytesUsed,
        long jvmHeapBytesCommitted,
        long jvmHeapBytesMax,
        int segmentCount,
        int segmentCreateCount,
        int segmentCreateViaExternalCount,
        int segmentDeleteViaExternalCount,
        int segmentCreateViaRollupCount,
        int segmentCreateViaSqlCount,
        int cellCount,
        int cellCoordinateCount)
    {
        super(stack);
        this.connectionStartCount = connectionStartCount;
        this.connectionEndCount = connectionEndCount;
        this.statementStartCount = statementStartCount;
        this.statementEndCount = statementEndCount;
        this.sqlStatementStartCount = sqlStatementStartCount;
        this.sqlStatementExecuteCount = sqlStatementExecuteCount;
        this.sqlStatementEndCount = sqlStatementEndCount;
        this.sqlStatementRowFetchCount = sqlStatementRowFetchCount;
        this.sqlStatementExecuteNanos = sqlStatementExecuteNanos;
        this.sqlStatementCellRequestCount = sqlStatementCellRequestCount;
        this.cellCacheRequestCount = cellCacheRequestCount;
        this.cellCacheHitCount = cellCacheHitCount;
        this.cellCacheMissCount = cellCacheMissCount;
        this.cellCachePendingCount = cellCachePendingCount;
        this.executeStartCount = executeStartCount;
        this.executeEndCount = executeEndCount;
        this.jvmHeapBytesUsed = jvmHeapBytesUsed;
        this.jvmHeapBytesCommitted = jvmHeapBytesCommitted;
        this.jvmHeapBytesMax = jvmHeapBytesMax;
        this.segmentCount = segmentCount;
        this.segmentCreateCount = segmentCreateCount;
        this.segmentCreateViaExternalCount = segmentCreateViaExternalCount;
        this.segmentDeleteViaExternalCount = segmentDeleteViaExternalCount;
        this.segmentCreateViaRollupCount = segmentCreateViaRollupCount;
        this.segmentCreateViaSqlCount = segmentCreateViaSqlCount;
        this.cellCount = cellCount;
        this.cellCoordinateCount = cellCoordinateCount;
    }

    public int getCellCacheMissCount() {
        return cellCacheRequestCount - cellCacheHitCount;
    }

    /**
     * @return number of SQL statements currently executing
     */
    public int getSqlStatementCurrentlyOpenCount() {
        return sqlStatementStartCount - sqlStatementEndCount;
    }

    /**
     * @return number of statements currently executing
     */
    public int getStatementCurrentlyExecutingCount() {
        return executeStartCount - executeEndCount;
    }

    /**
     * @return number of statements currently open
     */
    public int getStatementCurrentlyOpenCount() {
        return statementStartCount - statementEndCount;
    }

    /**
     */
    public int getConnectionCurrentlyOpenCount() {
        return connectionStartCount - connectionEndCount;
    }

    public int getConnectionStartCount() {
        return connectionStartCount;
    }

    public int getConnectionEndCount() {
        return connectionEndCount;
    }

    public int getStatementStartCount() {
        return statementStartCount;
    }

    public int getStatementEndCount() {
        return statementEndCount;
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

    public int getSqlStatementCellRequestCount() {
        return sqlStatementCellRequestCount;
    }

    public int getCellCacheRequestCount() {
        return cellCacheRequestCount;
    }

    public int getCellCacheHitCount() {
        return cellCacheHitCount;
    }

    public int getCellCachePendingCount() {
        return cellCachePendingCount;
    }

    public int getExecuteStartCount() {
        return executeStartCount;
    }

    public int getExecuteEndCount() {
        return executeEndCount;
    }

    public long getJvmHeapBytesUsed() {
        return jvmHeapBytesUsed;
    }

    public long getJvmHeapBytesCommitted() {
        return jvmHeapBytesCommitted;
    }

    public long getJvmHeapBytesMax() {
        return jvmHeapBytesMax;
    }

    public int getSegmentCount() {
        return segmentCount;
    }

    public int getSegmentCreateCount() {
        return segmentCreateCount;
    }

    public int getSegmentCreateViaExternalCount() {
        return segmentCreateViaExternalCount;
    }

    public int getSegmentDeleteViaExternalCount() {
        return segmentDeleteViaExternalCount;
    }

    public int getSegmentCreateViaRollupCount() {
        return segmentCreateViaRollupCount;
    }

    public int getSegmentCreateViaSqlCount() {
        return segmentCreateViaSqlCount;
    }

    public int getCellCount() {
        return cellCount;
    }

    public int getCellCoordinateCount() {
        return cellCoordinateCount;
    }
}

// End ServerInfo.java
