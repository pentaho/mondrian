/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2000-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import java.io.PrintWriter;
import java.util.Locale;
import javax.sql.DataSource;

/**
 * Connection to a multi-dimensional database.
 *
 * @see DriverManager
 *
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
     * @throws RuntimeException if another thread cancels the query's statement.
     *
     * @deprecated This method is deprecated and will be removed in
     * mondrian-4.0. It operates by internally creating a statement. Better
     * to use olap4j and explicitly create a statement.
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
     * Parses a statement.
     *
     * @param mdx MDX string
     * @return A {@link Query} if it is a SELECT statement, a
     *   {@link DrillThrough} if it is a DRILLTHROUGH statement
     */
    QueryPart parseStatement(String mdx);

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
}

// End Connection.java
