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

import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapConnection;
import mondrian.server.monitor.*;

import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
    private final static Logger LOGGER = Logger.getLogger(Execution.class);
    /**
     * Used for MDX logging, allows for a MDX Statement UID.
     */
    private static AtomicLong SEQ = new AtomicLong();

    final StatementImpl statement;

    /**
     * Holds a collection of the SqlStatements which were used by this
     * execution instance.
     */
    private final Map<Locus, java.sql.Statement> statements =
        new HashMap<Locus, java.sql.Statement>();

    private State state = State.FRESH;

    /**
     * If not <code>null</code>, this query was notified that it
     * might cause an OutOfMemoryError.
     */
    private String outOfMemoryMsg;
    private long startTimeMillis;
    private long timeoutTimeMillis;
    private long timeoutIntervalMillis;
    private final QueryTiming queryTiming = new QueryTiming();
    private int phase;
    private int cellCacheHitCount;
    private int cellCacheMissCount;
    private int cellCachePendingCount;

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
        this.state = State.RUNNING;
        this.queryTiming.init(true);

        final RolapConnection connection =
            statement.getMondrianConnection();
        final MondrianServer server = connection.getServer();
        server.getMonitor().sendEvent(
            new ExecutionStartEvent(
                startTimeMillis,
                server.getId(),
                connection.getId(),
                statement.getId(),
                id,
                getMdx()));
    }

    protected String getMdx() {
        return null;
    }

    public void tracePhase(
        int hitCount,
        int missCount,
        int pendingCount)
    {
        final RolapConnection connection = statement.getMondrianConnection();
        final MondrianServer server = connection.getServer();
        final int hitCountInc = hitCount - this.cellCacheHitCount;
        final int missCountInc = missCount - this.cellCacheMissCount;
        final int pendingCountInc = pendingCount - this.cellCachePendingCount;
        server.getMonitor().sendEvent(
            new ExecutionPhaseEvent(
                System.currentTimeMillis(),
                server.getId(),
                connection.getId(),
                statement.getId(),
                id,
                phase,
                hitCountInc,
                missCountInc,
                pendingCountInc));
        ++phase;
        this.cellCacheHitCount = hitCount;
        this.cellCacheMissCount = missCount;
        this.cellCachePendingCount = pendingCount;
    }

    public void cancel() {
        this.state = State.CANCEL_REQUESTED;
    }

    public final void setOutOfMemory(String msg) {
        assert msg != null;
        this.outOfMemoryMsg = msg;
        this.state = State.ERROR;
    }

    public void checkCancelOrTimeout() {
        switch (this.state) {
        case CANCEL_REQUESTED:
        case CANCELED:
            cleanStatements();
            throw MondrianResource.instance().QueryCanceled.ex();
        case RUNNING:
            if (timeoutTimeMillis > 0) {
                long currTime = System.currentTimeMillis();
                if (currTime > timeoutTimeMillis) {
                    this.state = State.TIMEOUT;
                    cleanStatements();
                    throw MondrianResource.instance().QueryTimeout.ex(
                        timeoutIntervalMillis / 1000);
                }
            }
            break;
        case ERROR:
            cleanStatements();
            throw new MemoryLimitExceededException(outOfMemoryMsg);
        }
    }

    /**
     * Returns whether this execution is currently in a 'timeout'
     * state and will throw an exception as soon as the next check
     * is performed using {@link Execution#checkCancelOrTimeout()}.
     * @return True or false, depending on the timeout state.
     */
    public boolean isCancelOrTimeout() {
        if (state == State.CANCEL_REQUESTED
            || (state == State.RUNNING
                && timeoutTimeMillis > 0
                && System.currentTimeMillis() > timeoutTimeMillis)
            || outOfMemoryMsg != null)
        {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Called when the execution needs to clean all of its resources
     * for whatever reasons, typically when an exception has occurred
     * or the execution has ended. Any currently running SQL statements
     * will be canceled.
     *
     * @param state New state
     */
    public void cleanStatements() {
        final RolapConnection connection =
            statement.getMondrianConnection();
        final MondrianServer server = connection.getServer();
        server.getMonitor().sendEvent(
            new ExecutionEndEvent(
                startTimeMillis,
                server.getId(),
                connection.getId(),
                statement.getId(),
                id,
                phase,
                state,
                cellCacheHitCount,
                cellCacheMissCount,
                cellCachePendingCount));

        for (Entry<Locus, java.sql.Statement> entry : statements.entrySet()) {
            final Locus locus = entry.getKey();
            final java.sql.Statement stmt = entry.getValue();
            try {
                stmt.cancel();
                stmt.close();
            } catch (SQLException e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        MondrianResource.instance()
                        .ExecutionStatementCleanupException.ex(locus.message),
                        e);
                }
            }
        }
    }

    /**
     * Called when query execution has completed.  Once query execution has
     * ended, it is not possible to cancel or timeout the query until it
     * starts executing again.
     */
    public void end() {
        queryTiming.done();
        this.state = State.DONE;
        cleanStatements();
    }

    public final long getStartTime() {
        return startTimeMillis;
    }

    public final mondrian.server.Statement getMondrianStatement() {
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

    /**
     * This method is typically called by SqlStatement at construction time.
     * It ties all Statement objects to a particular Execution instance
     * so that we can audit, monitor and gracefully cancel an execution.
     * @param statement The statement used by this execution.
     */
    public void registerStatement(Locus locus, java.sql.Statement statement) {
        this.statements.put(locus, statement);
    }

    public enum State {
        FRESH,
        RUNNING,
        ERROR,
        CANCEL_REQUESTED,
        CANCELED,
        TIMEOUT,
        DONE,
    }
}

// End Execution.java
