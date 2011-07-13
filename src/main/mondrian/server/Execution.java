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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import java.sql.SQLException;

import org.apache.log4j.Logger;

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
     * Whether cancel has been requested.
     */
    private boolean canceled;

    /**
     * Holds a collection of the SqlStatements which were used by this
     * execution instance.
     */
    private final Map<Locus, java.sql.Statement> statements =
        new HashMap<Locus, java.sql.Statement>();

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
        this.canceled = true;
    }

    public final void setOutOfMemory(String msg) {
        this.outOfMemoryMsg = msg;
    }

    public void checkCancelOrTimeout() {
        if (canceled) {
            cleanStatements();
            throw MondrianResource.instance().QueryCanceled.ex();
        }
        if (timeoutTimeMillis > 0) {
            long currTime = System.currentTimeMillis();
            if (currTime > timeoutTimeMillis) {
                cleanStatements();
                throw
                    MondrianResource.instance().QueryTimeout.ex(
                        timeoutIntervalMillis / 1000);
            }
        }
        if (outOfMemoryMsg != null) {
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
        if (canceled
            || (timeoutTimeMillis > 0
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
     */
    public void cleanStatements() {
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
    void end() {
        executing = false;
        queryTiming.done();
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
}

// End Execution.java
