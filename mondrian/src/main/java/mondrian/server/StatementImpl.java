/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.server;

import mondrian.olap.*;
import mondrian.rolap.RolapSchema;
import mondrian.spi.ProfileHandler;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of {@link Statement}.
 *
 * <p>Not part of Mondrian's public API. This class may change without
 * notice.</p>
 *
 * @author jhyde
 */
public abstract class StatementImpl implements Statement {
    private static AtomicLong SEQ = new AtomicLong();

    /**
     * Writer to which to send profiling information, or null if profiling is
     * disabled.
     */
    private ProfileHandler profileHandler;

    protected Query query;

    /**
     * Query timeout, in milliseconds
     */
    protected long queryTimeout =
        MondrianProperties.instance().QueryTimeout.get() * 1000;

    /**
     * The current execution context, or null if query is not executing.
     */
    private Execution execution;

    /**
     * Whether {@link #cancel()} was called before the statement was started.
     * When the statement is started, it will immediately be marked canceled.
     */
    private boolean cancelBeforeStart;

    private final long id;

    /**
     * Creates a StatementImpl.
     */
    public StatementImpl() {
        this.id = SEQ.getAndIncrement();
    }

    public synchronized void start(Execution execution) {
        if (this.execution != null) {
            throw new AssertionError();
        }
        if (execution.statement != this) {
            throw new AssertionError();
        }
        this.execution = execution;
        execution.start();
        if (cancelBeforeStart) {
            execution.cancel();
            cancelBeforeStart = false;
        }
    }

    public synchronized void cancel() throws SQLException {
        if (execution == null) {
            // There is no current execution. Flag that we need to cancel as
            // soon as we start execution.
            cancelBeforeStart = true;
        } else {
            execution.cancel();
        }
    }

    public synchronized void end(Execution execution) {
        if (execution == null
            || execution != this.execution)
        {
            throw new IllegalArgumentException(
                execution + " != " + this.execution);
        }
        this.execution = null;
        execution.end();
    }

    public void enableProfiling(ProfileHandler profileHandler) {
        this.profileHandler = profileHandler;
    }

    public ProfileHandler getProfileHandler() {
        return profileHandler;
    }

    public void setQueryTimeoutMillis(long timeoutMillis) {
        this.queryTimeout = timeoutMillis;
    }

    public long getQueryTimeoutMillis() {
        return queryTimeout;
    }

    public void checkCancelOrTimeout() {
        final Execution execution0 = execution;
        if (execution0 == null) {
            return;
        }
        execution0.checkCancelOrTimeout();
    }

    public SchemaReader getSchemaReader() {
        return getMondrianConnection().getSchemaReader().withLocus();
    }

    public RolapSchema getSchema() {
        return getMondrianConnection().getSchema();
    }

    public Object getProperty(String name) {
        return getMondrianConnection().getProperty(name);
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public Execution getCurrentExecution() {
        return execution;
    }

    public long getId() {
        return id;
    }
}

// End StatementImpl.java
