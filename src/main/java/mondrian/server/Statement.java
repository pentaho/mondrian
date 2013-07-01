/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.server;

import mondrian.olap.Query;
import mondrian.olap.SchemaReader;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapSchema;
import mondrian.spi.ProfileHandler;

import java.sql.SQLException;

/**
 * Internal context corresponding to a statement.
 *
 * <p>This interface is typically implemented by a MondrianOlap4jStatement,
 * but not necessarily: statements may be created internally, not via olap4j.
 *
 * <p>Not part of Mondrian's public API. This class may change without
 * notice.</p>
 *
 * @author jhyde
 */
public interface Statement {
    /**
     * Closes this statement.
     */
    void close();

    /**
     * Returns this statement's schema reader.
     *
     * @return Schema reader, not null
     */
    SchemaReader getSchemaReader();

    /**
     * Returns this statement's schema.
     *
     * @return Schema, not null
     */
    RolapSchema getSchema();

    /**
     * Returns this statement's connection.
     *
     * @return connection
     */
    RolapConnection getMondrianConnection();

    Object getProperty(String name);

    Query getQuery();

    void setQuery(Query query);

    /**
     * Enables profiling.
     *
     * <p>Profiling information will be sent to the given writer when
     * {@link mondrian.olap.Result#close()} is called.
     *
     * <p>If <tt>profileHandler</tt> is null, disables profiling.
     *
     * @param profileHandler Writer to which to send profiling information
     */
    void enableProfiling(ProfileHandler profileHandler);

    ProfileHandler getProfileHandler();

    /**
     * Sets the timeout of this statement, in milliseconds.
     *
     * <p>Zero means no timeout.
     *
     * <p>Contrast with JDBC's {@link java.sql.Statement#setQueryTimeout(int)}
     * method, which uses an {@code int} value and a granularity of seconds.
     *
     * @param timeoutMillis Timeout in milliseconds
     */
    void setQueryTimeoutMillis(long timeoutMillis);

    /**
     * Returns the query timeout of this statement, in milliseconds.
     *
     * <p>Zero means no timeout.</p>
     *
     * <p>Contrast with JDBC's {@link java.sql.Statement#getQueryTimeout()}
     * method, which uses an {@code int} value and a granularity of seconds.
     *
     * @return Timeout in milliseconds
     */
    long getQueryTimeoutMillis();

    /**
     * Checks if either a cancel request has been issued on the query or
     * the locus time has exceeded the timeout value (if one has been
     * set).  Exceptions are raised if either of these two conditions are
     * met.  This method should be called periodically during query locus
     * to ensure timely detection of these events, particularly before/after
     * any potentially long running operations.
     *
     * @deprecated This method will be removed in mondrian-4.0; use
     *   {@link mondrian.server.Execution#checkCancelOrTimeout()}
     */
    void checkCancelOrTimeout();

    /**
     * Issues a cancel request on this statement.
     *
     * <p>Once the thread running the statement detects the cancel request,
     * locus will throw an exception. See
     * <code>BasicQueryTest.testCancel</code> for an example of usage of this
     * method.</p>
     *
     * @throws java.sql.SQLException on error
     */
    void cancel() throws SQLException;

    /**
     * Returns locus context if currently executing, null otherwise.
     *
     * @return Execution context
     */
    Execution getCurrentExecution();

    /**
     * Ends the current locus.
     *
     * @param execution Execution; must match the locus that was started
     *
     * @throws IllegalArgumentException if not started,
     *     or if locus does not match
     */
    void end(Execution execution);

    /**
     * Starts an locus.
     *
     * @param execution Execution context
     */
    void start(Execution execution);

    /**
     * Returns the ID of this statement, unique within the JVM.
     *
     * @return Unique ID of this statement
     */
    long getId();
}

// End Statement.java
