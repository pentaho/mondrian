/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2000-2002 Kana Software, Inc.
// Copyright (C) 2001-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 29 February, 2000
*/

package mondrian.olap;

import javax.sql.DataSource;
import java.util.Locale;
import java.io.PrintWriter;

/**
 * Connection to a multi-dimensional database.
 *
 * @see DriverManager
 *
 * @version $Id$
 * @author jhyde
 */
public interface Connection {

    /**
     * Get the Connect String associated with this Connection.
     *
     * @return the Connect String (never null).
     */
    String getConnectString();

    /**
     * Get the name of the Catalog associated with this Connection.
     *
     * @return the Catalog name (never null).
     */
    String getCatalogName();

    /**
     * Get the Schema associated with this Connection.
     *
     * @return the Schema (never null).
     */
    Schema getSchema();

    /**
     * Closes this <code>Connection</code>. You may not use this
     * <code>Connection</code> after closing it.
     */
    void close();

    /**
     * Executes a query.
     *
     * @throws RuntimeException if another thread calls {@link Query#cancel()}.
     */
    Result execute(Query query);

    /**
     * Returns the locale this connection belongs to.  Determines, for example,
     * the currency string used in formatting cell values.
     *
     * @see mondrian.util.Format
     */
    Locale getLocale();

    /**
     * Parses an expresion.
     */
    Exp parseExpression(String s);

    /**
     * Parses a query.
     */
    Query parseQuery(String s);

    /**
     * Sets the privileges for the this connection.
     *
     * @pre role != null
     * @pre role.isMutable()
     */
    void setRole(Role role);

    /**
     * Returns the access-control profile for this connection.
     * @post role != null
     * @post role.isMutable()
     */
    Role getRole();

    /**
     * Returns a schema reader with access control appropriate to the current
     * role.
     */
    SchemaReader getSchemaReader();

    /**
     * Returns the value of a connection property.
     *
     * @param name Name of property, for example "JdbcUser".
     * @return Value of property, or null if property is not defined.
     */
    Object getProperty(String name);

    /**
     * Returns an object with which to explicitly control the contents of the
     * cache.
     *
     * @param pw Writer to which to write logging information; may be null
     */
    CacheControl getCacheControl(PrintWriter pw);

    /**
     * Returns the data source this connection uses to create connections
     * to the underlying JDBC database.
     *
     * @return Data source
     */
    DataSource getDataSource();

    /**
     * Creates a Scenario.
     *
     * <p>It does not become the active scenario for the current connection.
     * To do this, call {@link #setScenario(Scenario)}.
     *
     * @see #setScenario
     *
     * @return a new Scenario
     */
    Scenario createScenario();

    /**
     * Sets the active Scenario of this connection.
     *
     * <p>After setting a scenario, the client may call
     * {@link Cell#setValue} to change the value of cells returned
     * from queries. The value of those cells is changed. This operation is
     * referred to as 'writeback', and is used to perform 'what if' analysis,
     * such as budgeting. See {@link mondrian.olap.Scenario} for more details.
     *
     * <p>If {@code scenario} is null, the connection will have no active
     * scenario, and writeback is not allowed.
     *
     * <p>Scenarios are created using {@link #createScenario()}.
     *
     * @param scenario Scenario
     */
    void setScenario(Scenario scenario);

    /**
     * Returns this connection's active Scenario, or null if there is no
     * active Scenario.
     *
     * @return Active scenario, or null
     */
    Scenario getScenario();
}

// End Connection.java
