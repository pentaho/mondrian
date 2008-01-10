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

import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.OlapException;
import org.olap4j.metadata.Property;

import javax.sql.DataSource;
import java.util.List;
import java.util.ArrayList;
import java.sql.*;
import java.lang.reflect.Proxy;

import mondrian.util.DelegatingInvocationHandler;

/**
 * Implementation of {@link Cell}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @version $Id$
 * @since May 24, 2007
 */
class MondrianOlap4jCell implements Cell {
    private final int[] coordinates;
    private final MondrianOlap4jCellSet olap4jCellSet;
    private final mondrian.olap.Cell cell;

    MondrianOlap4jCell(
        int[] coordinates,
        MondrianOlap4jCellSet olap4jCellSet,
        mondrian.olap.Cell cell)
    {
        assert coordinates != null;
        assert olap4jCellSet != null;
        assert cell != null;
        this.coordinates = coordinates;
        this.olap4jCellSet = olap4jCellSet;
        this.cell = cell;
    }

    public CellSet getCellSet() {
        return olap4jCellSet;
    }

    public int getOrdinal() {
        return (Integer) cell.getPropertyValue(
            mondrian.olap.Property.CELL_ORDINAL.name);
    }

    public List<Integer> getCoordinateList() {
        ArrayList<Integer> list = new ArrayList<Integer>(coordinates.length);
        for (int coordinate : coordinates) {
            list.add(coordinate);
        }
        return list;
    }

    public Object getPropertyValue(Property property) {
        // We assume that mondrian properties have the same name as olap4j
        // properties.
        return cell.getPropertyValue(property.getName());
    }

    public boolean isEmpty() {
        // FIXME
        return cell.isNull();
    }

    public boolean isError() {
        return cell.isError();
    }

    public boolean isNull() {
        return cell.isNull();
    }

    public double getDoubleValue() throws OlapException {
        Object o = cell.getValue();
        if (o instanceof Number) {
            Number number = (Number) o;
            return number.doubleValue();
        }
        throw olap4jCellSet.olap4jStatement.olap4jConnection.helper
            .createException(this, "not a number");
    }

    public String getErrorText() {
        Object o = cell.getValue();
        if (o instanceof Throwable) {
            return ((Throwable) o).getMessage();
        } else {
            return null;
        }
    }

    public Object getValue() {
        return cell.getValue();
    }

    public String getFormattedValue() {
        return cell.getFormattedValue();
    }

    public ResultSet drillThrough() throws OlapException {
        if (!cell.canDrillThrough()) {
            return null;
        }
        final String sql = cell.getDrillThroughSQL(false);
        final MondrianOlap4jConnection olap4jConnection =
            this.olap4jCellSet.olap4jStatement.olap4jConnection;
        final DataSource dataSource =
            olap4jConnection.connection.getDataSource();
        try {
            final Connection connection = dataSource.getConnection();
            final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);

            // To prevent a connection leak, wrap the result set in a proxy
            // which automatically closes the connection (and hence also the
            // statement and result set) when the result set is closed.
            // The caller still has to remember to call ResultSet.close(), of
            // course.
            return (ResultSet) Proxy.newProxyInstance(
                null,
                new Class<?>[] {ResultSet.class},
                new MyDelegatingInvocationHandler(resultSet));
        } catch (SQLException e) {
            throw olap4jConnection.helper.toOlapException(e);
        }
    }

    // must be public for reflection to work
    public static class MyDelegatingInvocationHandler
        extends DelegatingInvocationHandler
    {
        private final ResultSet resultSet;

        MyDelegatingInvocationHandler(ResultSet resultSet) {
            this.resultSet = resultSet;
        }

        protected Object getTarget() {
            return resultSet;
        }

        // implement ResultSet.close()
        public void close() throws SQLException {
            resultSet.getStatement().getConnection().close();
        }
    }
}

// End MondrianOlap4jCell.java
