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
 * Information about a Mondrian connection.
 */
public class ConnectionInfo extends Info {
    public final int cellCacheHitCount;
    public final int cellCacheRequestCount;
    public final int cellCacheMissCount;
    public final int cellCachePendingCount;
    public final int statementStartCount;
    public final int statementEndCount;
    public final int executeStartCount;
    public final int executeEndCount;

    public ConnectionInfo(
        String stack,
        int cellCacheHitCount,
        int cellCacheRequestCount,
        int cellCacheMissCount,
        int cellCachePendingCount,
        int statementStartCount,
        int statementEndCount,
        int executeStartCount,
        int executeEndCount)
    {
        super(stack);
        this.cellCacheHitCount = cellCacheHitCount;
        this.cellCacheRequestCount = cellCacheRequestCount;
        this.cellCacheMissCount = cellCacheMissCount;
        this.cellCachePendingCount = cellCachePendingCount;
        this.statementStartCount = statementStartCount;
        this.statementEndCount = statementEndCount;
        this.executeStartCount = executeStartCount;
        this.executeEndCount = executeEndCount;
    }

    public int getStatementStartCount() {
        return statementStartCount;
    }

    public int getCellCacheHitCount() {
        return cellCacheHitCount;
    }

    public int getCellCacheRequestCount() {
        return cellCacheRequestCount;
    }

    public int getCellCacheMissCount() {
        return cellCacheMissCount;
    }

    public int getCellCachePendingCount() {
        return cellCachePendingCount;
    }

    public int getStatementEndCount() {
        return statementEndCount;
    }

    public int getExecuteStartCount() {
        return executeStartCount;
    }

    public int getExecuteEndCount() {
        return executeEndCount;
    }
}

// End ConnectionInfo.java
