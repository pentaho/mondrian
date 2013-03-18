/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.server;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.RolapUtil;
import mondrian.server.monitor.*;
import mondrian.util.Pair;

import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Process that reads from the monitor stream and updates counters.
 *
 * <p>Internally, uses a dedicated thread to process events. Events received
 * from log4j are placed on a queue. This "Active object" or "Actor" pattern
 * means that the data structures that hold counters do not need to be locked.
 *
 * <p>Command requests are treated like events. They place their result on
 * a result queue.</p>
 *
 * <p>A {@link Visitor visitor} quickly dispatches events and commands
 * to the appropriate handler method.</p>
 *
 * <p>The monitored objects form a hierarchy. For each object type, there is
 * a mutable workspace (whose members are private and non-final) that is
 * converted into a monitor object (whose members are public and final) when
 * its {@code fix()} method is called:</p>
 *
 * <ul>
 *     <li>{@link MutableServerInfo} &rarr; {@link ServerInfo}</li>
 *     <ul>
 *         <li>{@link MutableConnectionInfo} &rarr; {@link ConnectionInfo}</li>
 *         <ul>
 *             <li>{@link MutableStatementInfo}
 *                 &rarr; {@link StatementInfo}</li>
 *             <ul>
 *                 <li>{@link MutableExecutionInfo}
 *                     &rarr; {@link ExecutionInfo}</li>
 *                 <ul>
 *                     <li>{@link MutableSqlStatementInfo}
 *                         &rarr; {@link SqlStatementInfo}</li>
 *                 </ul>
 *             </ul>
 *         </ul>
 *     </ul>
 * </ul>
 */
class MonitorImpl
    implements Monitor
{
    private static final Logger LOGGER = Logger.getLogger(MonitorImpl.class);
    private final Handler handler = new Handler();

    protected static final Util.MemoryInfo MEMORY_INFO = Util.getMemoryInfo();

    private static final Actor ACTOR = new Actor();

    static {
        // Create and start thread for actor.
        //
        // Actor is shared between all servers. This reduces concurrency, but
        // not a concern because monitoring events are not very numerous.
        // We tried creating one actor (and therefore thread) per server, but
        // some applications (and in particular some tests) create lots of
        // servers.
        //
        // The actor is shut down with the JVM.
        final Thread thread = new Thread(ACTOR, "Mondrian Monitor");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Creates a Monitor.
     */
    public MonitorImpl() {
    }

    // Commands

    public void shutdown() {
        // Nothing to do. Cannot shut down actor, because it shared between
        // all servers.
    }

    public void sendEvent(Event event) {
        // The implementation does not need to take any locks.
        try {
            if (Thread.interrupted()) {
                // Interrupt should not happen. Mondrian uses cancel without
                // setting interrupt. But if interrupts are happening, it's
                // best to know now, rather than failing next time we make a
                // blocking system call.
                throw new AssertionError();
            }
            ACTOR.eventQueue.put(Pair.<Handler, Message>of(handler, event));
        } catch (InterruptedException e) {
            throw Util.newError(e, "Exception while sending event " + event);
        }
    }

    public ServerInfo getServer() {
        return (ServerInfo) execute(new ServerCommand());
    }

    public List<ConnectionInfo> getConnections() {
        //noinspection unchecked
        return (List<ConnectionInfo>) execute(new ConnectionsCommand());
    }

    public List<StatementInfo> getStatements() {
        //noinspection unchecked
        return (List<StatementInfo>) execute(new StatementsCommand());
    }

    public List<SqlStatementInfo> getSqlStatements() {
        //noinspection unchecked
        return (List<SqlStatementInfo>) execute(new SqlStatementsCommand());
    }

    private Object execute(Command command) {
        return ACTOR.execute(handler, command);
    }

    // Command and response classes

    /**
     * A kind of message that produces a response. The response may be null,
     * but even so, it will be stored and the caller must collect it.
     */
    static abstract class Command implements Message {
    }

    static class StatementsCommand extends Command {
        public <T> T accept(Visitor<T> visitor) {
            return ((CommandVisitor<T>) visitor).visit(this);
        }
    }

    static class SqlStatementsCommand extends Command {
        public <T> T accept(Visitor<T> visitor) {
            return ((CommandVisitor<T>) visitor).visit(this);
        }
    }

    static class ConnectionsCommand extends Command {
        public <T> T accept(Visitor<T> visitor) {
            return ((CommandVisitor<T>) visitor).visit(this);
        }
    }

    static class ServerCommand extends Command {
        public <T> T accept(Visitor<T> visitor) {
            return ((CommandVisitor<T>) visitor).visit(this);
        }
    }

    static class ShutdownCommand extends Command {
        public <T> T accept(Visitor<T> visitor) {
            return ((CommandVisitor<T>) visitor).visit(this);
        }
    }

    /**
     * Extension to {@link Visitor} to allow commands as well as events.
     *
     * @param <T> Return type
     */
    static interface CommandVisitor<T> extends Visitor<T> {
        T visit(ConnectionsCommand connectionsCommand);
        T visit(ServerCommand serverCommand);
        T visit(SqlStatementsCommand command);
        T visit(StatementsCommand command);
        T visit(ShutdownCommand command);
    }

    /**
     * Workspace to collect statistics about the execution of a Mondrian server.
     */
    private static class MutableServerInfo {
        private final MutableSqlStatementInfo aggSql =
            new MutableSqlStatementInfo(null, -1, null, null);
        private final MutableExecutionInfo aggExec =
            new MutableExecutionInfo(null, -1, null);
        private final MutableStatementInfo aggStmt =
            new MutableStatementInfo(null, -1, null);
        private final MutableConnectionInfo aggConn =
            new MutableConnectionInfo(null);
        private final String stack;

        public MutableServerInfo(String stack) {
            this.stack = stack;
        }

        public ServerInfo fix() {
            Util.MemoryInfo.Usage memoryUsage = MEMORY_INFO.get();
            return new ServerInfo(
                stack,
                aggConn.startCount,
                aggConn.endCount,
                aggStmt.startCount,
                aggStmt.endCount,
                aggSql.startCount,
                aggSql.executeCount,
                aggSql.endCount,
                aggSql.rowFetchCount,
                aggSql.executeNanos,
                aggSql.cellRequestCount,
                aggExec.cellCacheHitCount,
                aggExec.cellCacheRequestCount,
                aggExec.cellCacheMissCount,
                aggExec.cellCachePendingCount,
                aggExec.startCount,
                aggExec.endCount,
                memoryUsage.getUsed(),
                memoryUsage.getCommitted(),
                memoryUsage.getMax(),
                (aggExec.cellCacheSegmentCreateCount
                 - aggExec.cellCacheSegmentDeleteCount),
                aggExec.cellCacheSegmentCreateCount,
                aggExec.cellCacheSegmentCreateViaExternalCount,
                aggExec.cellCacheSegmentDeleteViaExternalCount,
                aggExec.cellCacheSegmentCreateViaRollupCount,
                aggExec.cellCacheSegmentCreateViaSqlCount,
                aggExec.cellCacheSegmentCellCount,
                aggExec.cellCacheSegmentCoordinateSum);
        }
    }

    /**
     * Workspace to collect statistics about the execution of a Mondrian MDX
     * statement. Parent context is the server.
     */
    private static class MutableConnectionInfo {
        private final MutableExecutionInfo aggExec =
            new MutableExecutionInfo(null, -1, null);
        private final MutableStatementInfo aggStmt =
            new MutableStatementInfo(null, -1, null);
        private int startCount;
        private int endCount;
        private final String stack;

        public MutableConnectionInfo(String stack) {
            this.stack = stack;
        }

        public ConnectionInfo fix() {
            return new ConnectionInfo(
                stack,
                aggExec.cellCacheHitCount,
                aggExec.cellCacheRequestCount,
                aggExec.cellCacheMissCount,
                aggExec.cellCachePendingCount,
                aggStmt.startCount,
                aggStmt.endCount,
                aggExec.startCount,
                aggExec.endCount);
        }
    }

    /**
     * Workspace to collect statistics about the execution of a Mondrian MDX
     * statement. Parent context is the connection.
     */
    private static class MutableStatementInfo {
        private final MutableConnectionInfo conn;
        private final long statementId;
        private final MutableExecutionInfo aggExec =
            new MutableExecutionInfo(null, -1, null);
        private final MutableSqlStatementInfo aggSql =
            new MutableSqlStatementInfo(null, -1, null, null);
        private int startCount;
        private int endCount;
        private final String stack;

        public MutableStatementInfo(
            MutableConnectionInfo conn,
            long statementId,
            String stack)
        {
            this.statementId = statementId;
            this.conn = conn;
            this.stack = stack;
        }

        public StatementInfo fix() {
            return new StatementInfo(
                stack,
                statementId,
                aggExec.startCount,
                aggExec.endCount,
                aggExec.phaseCount,
                aggExec.cellCacheRequestCount,
                aggExec.cellCacheHitCount,
                aggExec.cellCacheMissCount,
                aggExec.cellCachePendingCount,
                aggSql.startCount,
                aggSql.executeCount,
                aggSql.endCount,
                aggSql.rowFetchCount,
                aggSql.executeNanos,
                aggSql.cellRequestCount);
        }
    }

    /**
     * <p>Workspace to collect statistics about the execution of a Mondrian MDX
     * statement. A statement execution occurs within the context of a
     * statement.</p>
     *
     * <p>Most statements are executed only once. It is possible
     * (if you use the {@link org.olap4j.PreparedOlapStatement} API for
     * instance) to execute a statement more than once. There can be at most
     * one execution at a time for a given statement. Thus a statement's
     * executeStartCount and executeEndCount should never differ by more than
     * 1.</p>
     */
    private static class MutableExecutionInfo {
        private final MutableStatementInfo stmt;
        private final long executionId;
        private final MutableSqlStatementInfo aggSql =
            new MutableSqlStatementInfo(null, -1, null, null);
        private int startCount;
        private int phaseCount;
        private int endCount;
        private int cellCacheRequestCount;
        private int cellCacheHitCount;
        private int cellCacheMissCount;
        private int cellCachePendingCount;
        private int cellCacheHitCountDelta;
        private int cellCacheMissCountDelta;
        private int cellCachePendingCountDelta;
        private int cellCacheSegmentCreateCount;
        private int cellCacheSegmentCreateViaRollupCount;
        private int cellCacheSegmentCreateViaSqlCount;
        private int cellCacheSegmentCreateViaExternalCount;
        private int cellCacheSegmentDeleteViaExternalCount;
        private int cellCacheSegmentDeleteCount;
        private int cellCacheSegmentCoordinateSum;
        private int cellCacheSegmentCellCount;
        private final String stack;

        public MutableExecutionInfo(
            MutableStatementInfo stmt,
            long executionId,
            String stack)
        {
            this.stmt = stmt;
            this.executionId = executionId;
            this.stack = stack;
        }

        public ExecutionInfo fix() {
            return new ExecutionInfo(
                stack,
                executionId,
                phaseCount,
                cellCacheRequestCount,
                cellCacheHitCount,
                cellCacheMissCount,
                cellCachePendingCount,
                aggSql.startCount,
                aggSql.executeCount,
                aggSql.endCount,
                aggSql.rowFetchCount,
                aggSql.executeNanos,
                aggSql.cellRequestCount);
        }
    }

    /**
     * Workspace to collect statistics about the execution of a SQL statement.
     * A SQL statement execution occurs within the context of a Mondrian MDX
     * statement.
     */
    private static class MutableSqlStatementInfo {
        private final MutableStatementInfo stmt; // parent context
        private final long sqlStatementId;
        private int startCount;
        private int executeCount;
        private int endCount;
        private int cellRequestCount;
        private long executeNanos;
        private long rowFetchCount;
        private final String stack;
        private final String sql;

        public MutableSqlStatementInfo(
            MutableStatementInfo stmt,
            long sqlStatementId,
            String sql,
            String stack)
        {
            this.sqlStatementId = sqlStatementId;
            this.stmt = stmt;
            this.sql = sql;
            this.stack = stack;
        }

        public SqlStatementInfo fix() {
            return new SqlStatementInfo(
                stack,
                sqlStatementId,
                sql);
        }
    }

    private static class Handler implements CommandVisitor<Object> {

        private final MutableServerInfo server =
            new MutableServerInfo(null);

        private final Map<Integer, MutableConnectionInfo> connectionMap =
            new LinkedHashMap<Integer, MutableConnectionInfo>(
                MondrianProperties.instance().ExecutionHistorySize.get(),
                0.8f,
                false)
            {
                private final int maxSize =
                    MondrianProperties.instance().ExecutionHistorySize.get();
                private static final long serialVersionUID = 1L;
                protected boolean removeEldestEntry(
                    Map.Entry<Integer, MutableConnectionInfo> e)
                {
                    if (size() > maxSize) {
                        if (RolapUtil.MONITOR_LOGGER.isTraceEnabled()) {
                            RolapUtil.MONITOR_LOGGER.trace(
                                "ConnectionInfo("
                                + e.getKey()
                                + ") evicted. Stack is:"
                                + Util.nl
                                + e.getValue().stack);
                        }
                        return true;
                    }
                    return false;
                }
            };

        private final Map<Long, MutableSqlStatementInfo> sqlStatementMap =
            new LinkedHashMap<Long, MutableSqlStatementInfo>(
                MondrianProperties.instance().ExecutionHistorySize.get(),
                0.8f,
                false)
            {
                private final int maxSize =
                    MondrianProperties.instance().ExecutionHistorySize.get();
                private static final long serialVersionUID = 1L;
                protected boolean removeEldestEntry(
                    Map.Entry<Long, MutableSqlStatementInfo> e)
                {
                    if (size() > maxSize) {
                        if (RolapUtil.MONITOR_LOGGER.isTraceEnabled()) {
                            RolapUtil.MONITOR_LOGGER.trace(
                                "StatementInfo("
                                + e.getKey()
                                + ") evicted. Stack is:"
                                + Util.nl
                                + e.getValue().stack);
                        }
                        return true;
                    }
                    return false;
                }
            };

        private final Map<Long, MutableStatementInfo> statementMap =
            new LinkedHashMap<Long, MutableStatementInfo>(
                MondrianProperties.instance().ExecutionHistorySize.get(),
                0.8f,
                false)
            {
                private final int maxSize =
                    MondrianProperties.instance().ExecutionHistorySize.get();
                private static final long serialVersionUID = 1L;
                protected boolean removeEldestEntry(
                    Map.Entry<Long, MutableStatementInfo> e)
                {
                    if (size() > maxSize) {
                        if (RolapUtil.MONITOR_LOGGER.isTraceEnabled()) {
                            RolapUtil.MONITOR_LOGGER.trace(
                                "StatementInfo("
                                + e.getKey()
                                + ") evicted. Stack is:"
                                + Util.nl
                                + e.getValue().stack);
                        }
                        return true;
                    }
                    return false;
                }
            };

        private final Map<Long, MutableExecutionInfo> executionMap =
            new LinkedHashMap<Long, MutableExecutionInfo>(
                MondrianProperties.instance().ExecutionHistorySize.get(),
                0.8f,
                false)
            {
                private final int maxSize =
                    MondrianProperties.instance().ExecutionHistorySize.get();
                private static final long serialVersionUID = 1L;
                protected boolean removeEldestEntry(
                    Map.Entry<Long, MutableExecutionInfo> e)
                {
                    if (size() > maxSize) {
                        if (RolapUtil.MONITOR_LOGGER.isTraceEnabled()) {
                            RolapUtil.MONITOR_LOGGER.trace(
                                "ExecutionInfo("
                                + e.getKey()
                                + ") evicted. Stack is:"
                                + Util.nl
                                + e.getValue().stack);
                        }
                        return true;
                    }
                    return false;
                }
        };

        /**
         * Holds info for executions that have ended. Cell cache events may
         * arrive late, and this map lets them get into the system.
         */
        private final Map<Long, MutableExecutionInfo> retiredExecutionMap =
            new LinkedHashMap<Long, MutableExecutionInfo>(
                MondrianProperties.instance().ExecutionHistorySize.get(),
                0.8f,
                false)
            {
                private final int maxSize =
                    MondrianProperties.instance().ExecutionHistorySize.get();
                private static final long serialVersionUID = 1L;
                protected boolean removeEldestEntry(
                    Map.Entry<Long, MutableExecutionInfo> e)
                {
                    if (size() > maxSize) {
                        if (RolapUtil.MONITOR_LOGGER.isTraceEnabled()) {
                            RolapUtil.MONITOR_LOGGER.trace(
                                "Retired ExecutionInfo("
                                + e.getKey()
                                + ") evicted. Stack is:"
                                + Util.nl
                                + e.getValue().stack);
                        }
                        return true;
                    }
                    return false;
                }
        };

        /**
         * Method for debugging that does nothing, but is a place to put a break
         * point to find out places where an event or its parent should be
         * registered but is not.
         *
         * @param event Event
         * @return Always null
         */
        private Object missing(Event event) {
            return null;
        }

        public Object visit(ConnectionStartEvent event) {
            final MutableConnectionInfo conn =
                new MutableConnectionInfo(event.stack);
            connectionMap.put(event.connectionId, conn);
            foo(conn, event);
            foo(server.aggConn, event);
            if (RolapUtil.MONITOR_LOGGER.isTraceEnabled()) {
                RolapUtil.MONITOR_LOGGER.trace(
                    "Connection("
                    + event.connectionId
                    + ") created. stack is:"
                    + Util.nl
                    + event.stack);
            }
            return null;
        }

        private void foo(
            MutableConnectionInfo conn,
            ConnectionStartEvent event)
        {
            ++conn.startCount;
        }

        public Object visit(ConnectionEndEvent event) {
            final MutableConnectionInfo conn =
                connectionMap.remove(event.connectionId);
            if (conn == null) {
                return missing(event);
            }
            foo(conn, event);
            foo(server.aggConn, event);

            // Since the connection info will no longer be in the table,
            // broadcast the final info to anyone who is interested.
            RolapUtil.MONITOR_LOGGER.debug(conn.fix());
            return null;
        }

        private void foo(
            MutableConnectionInfo conn,
            ConnectionEndEvent event)
        {
            ++conn.endCount;
        }

        public Object visit(StatementStartEvent event) {
            final MutableConnectionInfo conn =
                connectionMap.get(event.connectionId);
            if (conn == null) {
                return missing(event);
            }
            final MutableStatementInfo stmt =
                new MutableStatementInfo(
                    conn, event.statementId, event.stack);
            statementMap.put(event.statementId, stmt);
            foo(stmt, event);
            foo(conn.aggStmt, event);
            foo(server.aggStmt, event);
            if (RolapUtil.MONITOR_LOGGER.isTraceEnabled()) {
                RolapUtil.MONITOR_LOGGER.trace(
                    "Statement("
                    + event.statementId
                    + ") created. stack is:"
                    + Util.nl
                    + event.stack);
            }
            return null;
        }

        private void foo(
            MutableStatementInfo stmt,
            StatementStartEvent event)
        {
            ++stmt.startCount;
        }

        public Object visit(StatementEndEvent event) {
            final MutableStatementInfo stmt =
                statementMap.remove(event.statementId);
            if (stmt == null) {
                return missing(event);
            }
            foo(stmt, event);
            foo(stmt.conn.aggStmt, event);
            foo(server.aggStmt, event);

            // Since the statement info will no longer be in the table,
            // broadcast the final info to anyone who is interested.
            RolapUtil.MONITOR_LOGGER.debug(stmt.fix());
            return null;
        }

        private void foo(
            MutableStatementInfo stmt,
            StatementEndEvent event)
        {
            ++stmt.endCount;
        }

        public Object visit(ExecutionStartEvent event) {
            MutableStatementInfo stmt =
                statementMap.get(event.statementId);
            if (stmt == null) {
                return missing(event);
            }
            final MutableExecutionInfo exec =
                new MutableExecutionInfo(
                    stmt, event.executionId, event.stack);
            executionMap.put(event.executionId, exec);

            foo(exec, event);
            foo(stmt.aggExec, event);
            foo(stmt.conn.aggExec, event);
            foo(server.aggExec, event);
            if (RolapUtil.MONITOR_LOGGER.isTraceEnabled()) {
                RolapUtil.MONITOR_LOGGER.trace(
                    "Execution("
                    + event.executionId
                    + ") created. stack is:"
                    + Util.nl
                    + event.stack);
            }
            return null;
        }

        private void foo(
            MutableExecutionInfo exec,
            ExecutionStartEvent event)
        {
            ++exec.startCount;
        }

        public Object visit(ExecutionPhaseEvent event) {
            final MutableExecutionInfo exec =
                executionMap.get(event.executionId);
            if (exec == null) {
                return missing(event);
            }
            executionMap.put(event.executionId, exec);

            foo(exec, event);
            foo(exec.stmt.aggExec, event);
            foo(exec.stmt.conn.aggExec, event);
            foo(server.aggExec, event);
            return null;
        }

        private void foo(
            MutableExecutionInfo exec,
            ExecutionPhaseEvent event)
        {
            ++exec.phaseCount;
            exec.cellCacheHitCountDelta = event.hitCount;
            exec.cellCacheMissCountDelta = event.missCount;
            exec.cellCachePendingCountDelta = event.pendingCount;
        }

        public Object visit(ExecutionEndEvent event) {
            final MutableExecutionInfo exec =
                executionMap.remove(event.executionId);
            if (exec == null) {
                return missing(event);
            }
            retiredExecutionMap.put(exec.executionId, exec);
            foo(exec, event);
            foo(exec.stmt.aggExec, event);
            foo(exec.stmt.conn.aggExec, event);
            foo(server.aggExec, event);

            // Since the execution info will no longer be in the table,
            // broadcast the final info to anyone who is interested.
            RolapUtil.MONITOR_LOGGER.debug(exec.fix());
            return null;
        }

        private void foo(
            MutableExecutionInfo exec,
            ExecutionEndEvent event)
        {
            // NOTE: 'exec.phaseCount += event.phaseCount' would be wrong,
            // because we have already incremented each time we got an
            // ExecutionPhaseEvent. For a similar reason, we do not update
            // exec.cellCacheHitCount etc. each phase.

            ++exec.endCount;
            ++exec.phaseCount;
            exec.cellCacheHitCount += event.cellCacheHitCount;
            exec.cellCacheMissCount += event.cellCacheMissCount;
            exec.cellCachePendingCount += event.cellCachePendingCount;
            exec.cellCacheRequestCount +=
                (event.cellCacheHitCount
                + event.cellCacheMissCount
                + event.cellCachePendingCount);
            exec.cellCacheHitCountDelta = 0;
            exec.cellCacheMissCountDelta = 0;
            exec.cellCachePendingCountDelta = 0;
        }

        public Object visit(CellCacheSegmentCreateEvent event) {
            MutableExecutionInfo exec =
                executionMap.get(event.executionId);
            if (exec == null) {
                // Cache events can sometimes arrive after the execution has
                // ended. So, look into the retired map.
                exec = retiredExecutionMap.get(event.executionId);
                if (exec == null) {
                    return missing(event);
                }
            }

            foo(exec, event);
            foo(exec.stmt.aggExec, event);
            foo(exec.stmt.conn.aggExec, event);
            foo(server.aggExec, event);
            return null;
        }

        private void foo(
            MutableExecutionInfo exec,
            CellCacheSegmentCreateEvent event)
        {
            ++exec.cellCacheSegmentCreateCount;
            exec.cellCacheSegmentCoordinateSum += event.coordinateCount;
            exec.cellCacheSegmentCellCount += event.actualCellCount;
            switch (event.source) {
            case ROLLUP:
                ++exec.cellCacheSegmentCreateViaRollupCount;
                break;
            case EXTERNAL:
                ++exec.cellCacheSegmentCreateViaExternalCount;
                break;
            case SQL:
                ++exec.cellCacheSegmentCreateViaSqlCount;
                break;
            default:
                throw Util.unexpected(event.source);
            }
        }

        public Object visit(CellCacheSegmentDeleteEvent event) {
            final MutableExecutionInfo exec =
                executionMap.get(event.executionId);
            if (exec == null) {
                return missing(event);
            }

            foo(exec, event);
            foo(exec.stmt.aggExec, event);
            foo(exec.stmt.conn.aggExec, event);
            foo(server.aggExec, event);
            return null;
        }

        private void foo(
            MutableExecutionInfo exec,
            CellCacheSegmentDeleteEvent event)
        {
            ++exec.cellCacheSegmentDeleteCount;
            exec.cellCacheSegmentCoordinateSum -= event.coordinateCount;
            switch (event.source) {
            case EXTERNAL:
                ++exec.cellCacheSegmentDeleteViaExternalCount;
                break;
            }
        }

        public Object visit(SqlStatementStartEvent event) {
            final MutableStatementInfo stmt =
                statementMap.get(
                    event.getStatementId());
            if (stmt == null) {
                return missing(event);
            }
            final MutableSqlStatementInfo sql =
                new MutableSqlStatementInfo(
                    stmt,
                    event.sqlStatementId,
                    event.sql,
                    event.stack);
            sqlStatementMap.put(event.sqlStatementId, sql);
            foo(sql, event);
            foo(sql.stmt.aggSql, event);
            foo(server.aggSql, event);
            if (RolapUtil.MONITOR_LOGGER.isTraceEnabled()) {
                RolapUtil.MONITOR_LOGGER.trace(
                    "SqlStatement("
                    + event.sqlStatementId
                    + ") created. stack is:"
                    + Util.nl
                    + event.stack);
            }
            return null;
        }

        private void foo(
            MutableSqlStatementInfo sql,
            SqlStatementStartEvent event)
        {
            ++sql.startCount;
            sql.cellRequestCount += event.cellRequestCount;
        }

        public Object visit(SqlStatementExecuteEvent event) {
            final MutableSqlStatementInfo sql =
                sqlStatementMap.get(event.sqlStatementId);
            if (sql == null) {
                return missing(event);
            }
            foo(sql, event);
            foo(sql.stmt.aggSql, event);
            foo(server.aggSql, event);
            return null;
        }

        private void foo(
            MutableSqlStatementInfo sql,
            SqlStatementExecuteEvent event)
        {
            ++sql.executeCount;
            sql.executeNanos += event.executeNanos;
        }

        public Object visit(SqlStatementEndEvent event) {
            final MutableSqlStatementInfo sql =
                sqlStatementMap.remove(event.sqlStatementId);
            if (sql == null) {
                return missing(event);
            }
            foo(sql, event);
            foo(sql.stmt.aggSql, event);
            foo(server.aggSql, event);

            // Since the SQL statement info will no longer be in the table,
            // broadcast the final info to anyone who is interested.
            RolapUtil.MONITOR_LOGGER.debug(sql.fix());
            return null;
        }

        private void foo(
            MutableSqlStatementInfo sql,
            SqlStatementEndEvent event)
        {
            ++sql.endCount;
            sql.rowFetchCount += event.rowFetchCount;
        }

        public Object visit(ConnectionsCommand connectionsCommand) {
            List<ConnectionInfo> list =
                new ArrayList<ConnectionInfo>();
            for (MutableConnectionInfo info : connectionMap.values()) {
                list.add(info.fix());
            }
            return list;
        }

        public Object visit(ServerCommand serverCommand) {
            return server.fix();
        }

        public Object visit(SqlStatementsCommand command) {
            List<SqlStatementInfo> list =
                new ArrayList<SqlStatementInfo>();
            for (MutableSqlStatementInfo info : sqlStatementMap.values()) {
                list.add(info.fix());
            }
            return list;
        }

        public Object visit(StatementsCommand command) {
            List<StatementInfo> list =
                new ArrayList<StatementInfo>();
            for (MutableStatementInfo info : statementMap.values()) {
                list.add(info.fix());
            }
            return list;
        }

        public Object visit(ShutdownCommand command) {
            return "Shutdown succeeded";
        }
    }

    /**
     * Point for various clients in a request-response pattern to receive the
     * response to their requests.
     *
     * <p>The key type should test for object identity using the == operator,
     * like {@link WeakHashMap}. This allows responses to be automatically
     * removed if the request (key) is garbage collected.</p>
     *
     * <p><b>Thread safety</b>. {@link #queue} is a thread-safe data structure;
     * a thread can safely call {@link #put} while another thread calls
     * {@link #take}. The {@link #taken} map is not thread safe, so you must
     * lock the ResponseQueue before reading or writing it.</p>
     *
     * <p>If requests are processed out of order, this queue is not ideal: until
     * request #1 has received its response, requests #2, #3 etc. will not
     * receive their response. However, this is not a problem for the monitor,
     * which uses an actor model, processing requests in strict order.</p>
     *
     * @param <K> request (key) type
     * @param <V> response (value) type
     */
    private static class ResponseQueue<K, V> {
        private final BlockingQueue<Pair<K, V>> queue;

        /**
         * Entries that have been removed from the queue. If the request
         * is garbage-collected, the map entry is removed.
         */
        private final Map<K, V> taken =
            new WeakHashMap<K, V>();

        /**
         * Creates a ResponseQueue with given capacity.
         *
         * @param capacity Capacity
         */
        public ResponseQueue(int capacity) {
            queue = new ArrayBlockingQueue<Pair<K, V>>(capacity);
        }

        /**
         * Places a (request, response) pair onto the queue.
         *
         * @param k Request
         * @param v Response
         * @throws InterruptedException if interrupted while waiting
         */
        public void put(K k, V v) throws InterruptedException {
            queue.put(Pair.of(k, v));
        }

        /**
         * Retrieves the response from the queue matching the given key,
         * blocking until it is received.
         *
         * @param k Response
         * @return Response
         * @throws InterruptedException if interrupted while waiting
         */
        public synchronized V take(K k) throws InterruptedException {
            final V v = taken.remove(k);
            if (v != null) {
                return v;
            }
            // Take the laundry out of the machine. If it's ours, leave with it.
            // If it's someone else's, fold it neatly and put it on the pile.
            for (;;) {
                final Pair<K, V> pair = queue.take();
                if (pair.left.equals(k)) {
                    return pair.right;
                } else {
                    taken.put(pair.left, pair.right);
                }
            }
        }
    }

    private static class Actor implements Runnable {
        private boolean running = true;

        private final BlockingQueue<Pair<Handler, Message>> eventQueue =
            new ArrayBlockingQueue<Pair<Handler, Message>>(1000);

        private final ResponseQueue<Command, Object> responseQueue =
            new ResponseQueue<Command, Object>(1000);

        public void run() {
            try {
                for (;;) {
                    try {
                        final Pair<Handler, Message> entry = eventQueue.take();
                        final Handler handler = entry.left;
                        final Message message = entry.right;
                        final Object result = message.accept(handler);
                        if (message instanceof Command) {
                            responseQueue.put((Command) message, result);
                        } else {
                            // Broadcast the event to anyone who is interested.
                            RolapUtil.MONITOR_LOGGER.debug(message);
                        }
                        if (message instanceof ShutdownCommand) {
                            LOGGER.debug(
                                "ShutdownCommand received. Monitor thread is shutting down.");
                            return;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOGGER.warn(
                            "Monitor thread interrupted.",
                            e);
                        return;
                    } catch (Throwable t) {
                        LOGGER.error(
                            "Runtime error on the monitor thread.",
                            t);
                    }
                }
            } finally {
                running = false;
            }
        }

        public void shutdown() {
            // No point sending a command if (for some reason) there's no thread
            // listening to the command queue.
            if (running) {
                execute(null, new ShutdownCommand());
            }
        }

        Object execute(Handler handler, Command command) {
            try {
                eventQueue.put(Pair.<Handler, Message>of(handler, command));
            } catch (InterruptedException e) {
                throw Util.newError(e, "Interrupted while sending " + command);
            }
            try {
                return responseQueue.take(command);
            } catch (InterruptedException e) {
                throw Util.newError(e, "Interrupted while awaiting " + command);
            }
        }
    }
}

// End MonitorImpl.java
