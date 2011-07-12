/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.server;

import mondrian.olap.MemoryLimitExceededException;
import mondrian.olap.QueryTiming;
import mondrian.resource.MondrianResource;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Execution context.
 *
 * <p>Loosely corresponds to a CellSet. A given statement may be executed
 * several times over its lifetime, but at most one execution can be going
 * on at a time.</p>
 *
 * @version $Id$
 * @author jhyde
 */
public class Execution {
    /**
     * Used for MDX logging, allows for a MDX Statement UID.
     */
    private static AtomicLong SEQ = new AtomicLong();

    final StatementImpl statement;

    /**
     * Whether cancel has been requested.
     */
    private boolean canceled;

    /**
     * If not <code>null</code>, this query was notified that it
     * might cause an OutOfMemoryError.
     */
    private String outOfMemoryMsg;
    private long startTimeMillis;
    private long timeoutTimeMillis;
    private long timeoutIntervalMillis;
    private boolean executing;
    private final QueryTiming queryTiming = new QueryTiming();

    /**
     * Execution id, global within this JVM instance.
     */
    private final long id;

    public static final Execution NONE = new Execution(null, 0);

    public Execution(Statement statement, long timeoutIntervalMillis) {
        this.id = SEQ.getAndIncrement();
        this.statement = (StatementImpl) statement;
        this.timeoutIntervalMillis = timeoutIntervalMillis;
    }

    public void start() {
        this.startTimeMillis = System.currentTimeMillis();
        this.timeoutTimeMillis =
            timeoutIntervalMillis > 0
                ? this.startTimeMillis + timeoutIntervalMillis
                : 0L;
        this.executing = true;
        this.queryTiming.init(true);
    }

    public void cancel() {
        canceled = true;
    }

    public final void setOutOfMemory(String msg) {
        this.outOfMemoryMsg = msg;
    }

    public void checkCancelOrTimeout() {
        if (canceled) {
            throw MondrianResource.instance().QueryCanceled.ex();
        }
        if (timeoutTimeMillis > 0) {
            long currTime = System.currentTimeMillis();
            if (currTime > timeoutTimeMillis) {
                throw MondrianResource.instance().QueryTimeout.ex(
                    timeoutIntervalMillis / 1000);
            }
        }
        if (outOfMemoryMsg != null) {
            throw new MemoryLimitExceededException(outOfMemoryMsg);
        }
    }

    /**
     * Called when query execution has completed.  Once query execution has
     * ended, it is not possible to cancel or timeout the query until it
     * starts executing again.
     */
    void end() {
        executing = false;
        queryTiming.done();
    }

    public final long getStartTime() {
        return startTimeMillis;
    }

    public final Statement getMondrianStatement() {
        return statement;
    }

    public final QueryTiming getQueryTiming() {
        return queryTiming;
    }

    public final long getId() {
        return id;
    }

    public final long getElapsedMillis() {
        return System.currentTimeMillis() - startTimeMillis;
    }
}

// End Execution.java
