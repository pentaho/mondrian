/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
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
}

// End ConnectionInfo.java
