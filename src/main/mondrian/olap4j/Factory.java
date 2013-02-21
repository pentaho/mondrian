/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.olap4j;

import mondrian.olap.*;
import mondrian.rolap.RolapConnection;

import org.olap4j.OlapException;

import java.sql.*;
import java.sql.Connection;
import java.util.*;

/**
 * Instantiates classes to implement the olap4j API against the
 * Mondrian OLAP engine.
 *
 * <p>There are implementations for JDBC 3.0 (which occurs in JDK 1.5)
 * and JDBC 4.0 (which occurs in JDK 1.6).
 *
 * @author jhyde
 * @since Jun 14, 2007
 */
interface Factory {
    /**
     * Creates a connection.
     *
     * @param driver Driver
     * @param server Server
     * @param propertyList Properties defining the connection
     * @param user Authenticated user
     * @return Connection
     * @throws SQLException on error
     */
    Connection newConnection(
        MondrianBaseOlap4jDriver driver,
        MondrianServer server,
        Util.PropertyList propertyList,
        MondrianServer.User user) throws SQLException;

    /**
     * Creates an empty result set.
     *
     * @param olap4jConnection Connection
     * @return Result set
     */
    EmptyResultSet newEmptyResultSet(
        MondrianOlap4jConnection olap4jConnection);

    /**
     * Creates a result set with a fixed set of rows.
     *
     * @param olap4jConnection Connection
     * @param headerList Column headers
     * @param rowList Row values
     * @return Result set
     */
    ResultSet newFixedResultSet(
        MondrianOlap4jConnection olap4jConnection,
        List<String> headerList,
        List<List<Object>> rowList);

    /**
     * Creates a cell set.
     *
     *
     * @param olap4jStatement Statement
     * @return Cell set
     */
    MondrianOlap4jCellSet newCellSet(
        MondrianOlap4jStatement olap4jStatement);

    /**
     * Creates a statement.
     *
     * @param olap4jConnection Connection
     * @return Statement
     */
    MondrianOlap4jStatement newStatement(
        MondrianOlap4jConnection olap4jConnection);

    /**
     * Creates a prepared statement.
     *
     * @param mdx MDX query text
     * @param olap4jConnection Connection
     * @return Prepared statement
     * @throws org.olap4j.OlapException on database error
     */
    MondrianOlap4jPreparedStatement newPreparedStatement(
        String mdx,
        MondrianOlap4jConnection olap4jConnection)
        throws OlapException;

    /**
     * Creates a metadata object.
     *
     * @param olap4jConnection Connection
     * @param mondrianConnection Mondrian connection
     * @return Metadata object
     */
    MondrianOlap4jDatabaseMetaData newDatabaseMetaData(
        MondrianOlap4jConnection olap4jConnection,
        RolapConnection mondrianConnection);
}

// End Factory.java
