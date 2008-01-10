/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import mondrian.olap.Query;

import java.util.Properties;
import java.util.List;
import java.sql.*;

/**
 * Instantiates classes to implement the olap4j API against the
 * Mondrian OLAP engine.
 *
 * <p>There are implementations for JDBC 3.0 (which occurs in JDK 1.5)
 * and JDBC 4.0 (which occurs in JDK 1.6).
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 14, 2007
 */
interface Factory {
    Connection newConnection(String url, Properties info) throws SQLException;

    EmptyResultSet newEmptyResultSet(
        MondrianOlap4jConnection olap4jConnection);

    ResultSet newFixedResultSet(
        MondrianOlap4jConnection olap4jConnection,
        List<String> headerList,
        List<List<Object>> rowList);

    MondrianOlap4jCellSet newCellSet(
        MondrianOlap4jStatement olap4jStatement,
        Query query);

    MondrianOlap4jPreparedStatement newPreparedStatement(
        String mdx, MondrianOlap4jConnection olap4jConnection);

    MondrianOlap4jDatabaseMetaData newDatabaseMetaData(
        MondrianOlap4jConnection olap4jConnection);
}

// End Factory.java
