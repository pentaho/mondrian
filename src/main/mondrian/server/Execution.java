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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.MDC;

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
    /*
     * Used for MDX logging, allows for a MDX Statement UID.
     */
    private static AtomicLong SEQ = new AtomicLong();

    final StatementImpl statement;

    /*
     * Holds a collection of the SqlStatements which were used by this
     * execution instance. All operations on the map must be synchronized
     * on it.
     */
    private final Map<Locus, java.sql.Statement> statements =
        new HashMap<Locus, java.sql.Statement>();

    private State state = State.FRESH;

    /*
     * State of the SQL statements.
     */
    private SqlState sqlState = SqlState.CLEAN;

    /*
     * Lock monitor for SQL statements. All operations on the
     * {@link Execution#sqlState} and {@link Execution#statements}
     * need to be synchrinized on this.
     */
    private final Object sqlStateLock = new Object();

    /*
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

    private Map<String, Object> mdc = null;

    public Execution(Statement statement, long timeoutIntervalMillis) {
        this.id = SEQ.getAndIncrement();
        this.statement = (StatementImpl) statement;
        this.timeoutIntervalMillis = timeoutIntervalMillis;
    }

    /**
     * Copy the current MDC so it can be used later
     */
    public void copyMDC() {
        this.mdc = new HashMap<String, Object>();
        if(MDC.getContext() == null) {
            return;
        } else {
            this.mdc.putAll(MDC.getContext());
        }
    }

    /**
     * Set the copied mdc into the current MDC. This should be called
     * any time there will be logging in a thread handled by the
     * RolapResultShepherd where original MDC needs to be retrieved
     */
    public void setContextMap() {
        if(mdc == null) {
            return;
        }
        Map<String, Object> old = MDC.getContext();
        old.clear();
        old.putAll(mdc);
    }

    /**
     * Marks the start of an Execution instance. It is called by
     * {@link Statement#start(Execution)} automatically. Users don't
     * need to call this method.
     */
    public void start() {
        assert this.state == State.FRESH;
        this.startTimeMillis = System.currentTimeMillis();
        this.timeoutTimeMillis =
            timeoutIntervalMillis > 0
                ? this.startTimeMillis + timeoutIntervalMillis
                : 0L;
        this.state = State.RUNNING;
        this.queryTiming.init(true);
        fireExecutionStartEvent();
    }

    private String getMdx() {
        final Query query =
            statement.query;
        return query != null
            ? Util.unparse(query)
            : null;
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

    /**
     * Cancels the execution instance. Cleanup of the
     * resources used by this execution instance will be performed
     * in the background later on.
     */
    public void cancel() {
        this.state = State.CANCELED;
        fireExecutionEndEvent();
    }

    /**
     * This method will change the state of this execution to
     * {@link State#ERROR} and will set the message to display.
     * Cleanup of the resources used by this execution instance
     * will be performed in the background later on.
     * @param msg The message to display to the user, describing
     * the problem encountered with the memory space.
     */
    public final void setOutOfMemory(String msg) {
        assert msg != null;
        this.outOfMemoryMsg = msg;
        this.state = State.ERROR;
    }

    /**
     * Checks the state of this Execution and throws an exception
     * if something is wrong. This method should be called by the
     * user thread.
     * <p>It won't throw anything if the query has successfully completed.
     * @throws MondrianException The exception encountered.
     */
    public void checkCancelOrTimeout() throws MondrianException {
        switch (this.state) {
        case CANCELED:
            fireExecutionEndEvent();
            throw MondrianResource.instance().QueryCanceled.ex();
        case RUNNING:
            if (timeoutTimeMillis > 0) {
                long currTime = System.currentTimeMillis();
                if (currTime > timeoutTimeMillis) {
                    this.state = State.TIMEOUT;
                    fireExecutionEndEvent();
                    throw MondrianResource.instance().QueryTimeout.ex(
                        timeoutIntervalMillis / 1000);
                }
            }
            break;
        case ERROR:
            fireExecutionEndEvent();
            throw new MemoryLimitExceededException(outOfMemoryMsg);
        }
    }

    /**
     * Returns whether this execution is currently in a failed
     * state and will throw an exception as soon as the next check
     * is performed using {@link Execution#checkCancelOrTimeout()}.
     * @return True or false, depending on the timeout state.
     */
    public boolean isCancelOrTimeout() {
        if (state == State.CANCELED
            || state == State.ERROR
            || (state == State.RUNNING
                && timeoutTimeMillis > 0
                && System.currentTimeMillis() > timeoutTimeMillis))
        {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Called by the RolapResultShepherd when the execution needs to clean all
     * of its resources for whatever reasons, typically when an exception
     * has occurred or the execution has ended. Any currently running SQL
     * statements will be canceled. It should only be called if
     * {@link Execution#isCancelOrTimeout()} returns true.
     * <p>This method doesn't need to be called by a user. It will be called
     * internally by Mondrian when the system is ready to clean the remaining
     * resources.
     * <p>To check if this execution is failed, use
     * {@link Execution#isCancelOrTimeout()} instead.
     */
    public void cancelSqlStatements() {
        synchronized (sqlStateLock) {
            if (sqlState == SqlState.CLEAN) {
                return;
            }
            sqlState = SqlState.CLEAN;

            for (Entry<Locus, java.sql.Statement> entry
                : statements.entrySet())
            {
                Util.cancelAndCloseStatement(entry.getValue());
            }
            statements.clear();
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
        statements.clear();
        fireExecutionEndEvent();
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
        synchronized (sqlStateLock) {
            if (state == State.FRESH) {
                start();
            }
            if (state == State.RUNNING) {
                this.sqlState = SqlState.DIRTY;
                this.statements.put(locus, statement);
            }
        }
    }

    private void fireExecutionEndEvent() {
        final RolapConnection connection =
            statement.getMondrianConnection();
        final MondrianServer server = connection.getServer();
        server.getMonitor().sendEvent(
            new ExecutionEndEvent(
                this.startTimeMillis,
                server.getId(),
                connection.getId(),
                this.statement.getId(),
                this.id,
                this.phase,
                this.state,
                this.cellCacheHitCount,
                this.cellCacheMissCount,
                this.cellCachePendingCount));
    }

    private void fireExecutionStartEvent() {
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

    /**
     * Enumeration of the states of an Execution instance.
     */
    public enum State {
        /**
         * Identifies the state in which an execution is before it has
         * started resolving the query. This doesn't mean that there are
         * no current SQL statements already beeing executed.
         */
        FRESH,
        RUNNING,
        ERROR,
        CANCELED,
        TIMEOUT,
        DONE,
    }

    /**
     * Enumeration of the possible states of the SQL statements
     * associated to an Execution instance.
     */
    private enum SqlState {
        /**
         * Indicates that there might be SQL statements being
         * run right now.
         */
        DIRTY,
        /**
         * Indicates that all SQL statements have been canceled.
         */
        CLEAN
    }
}

// End Execution.java
