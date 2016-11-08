/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap4j;

import mondrian.olap.*;

import org.olap4j.CellSetAxisMetaData;
import org.olap4j.CellSetMetaData;
import org.olap4j.impl.ArrayNamedListImpl;
import org.olap4j.metadata.*;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Property;

import java.sql.SQLException;

/**
 * Implementation of {@link org.olap4j.CellSetMetaData}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @since Jun 13, 2007
 */
class MondrianOlap4jCellSetMetaData implements CellSetMetaData {
    final MondrianOlap4jStatement olap4jStatement;
    final Query query;
    private final NamedList<CellSetAxisMetaData> axesMetaData =
        new ArrayNamedListImpl<CellSetAxisMetaData>() {
            public String getName(Object axisMetaData) {
                return ((CellSetAxisMetaData)axisMetaData)
                    .getAxisOrdinal().name();
            }
        };
    private final MondrianOlap4jCellSetAxisMetaData filterAxisMetaData;

    MondrianOlap4jCellSetMetaData(
        MondrianOlap4jStatement olap4jStatement,
        Query query)
    {
        this.olap4jStatement = olap4jStatement;
        this.query = query;

        for (final QueryAxis queryAxis : query.getAxes()) {
            axesMetaData.add(
                new MondrianOlap4jCellSetAxisMetaData(
                    this, queryAxis));
        }
        filterAxisMetaData =
            new MondrianOlap4jCellSetAxisMetaData(
                this, query.getSlicerAxis());
    }

    // implement CellSetMetaData

    public NamedList<Property> getCellProperties() {
        final ArrayNamedListImpl<Property> list =
            new ArrayNamedListImpl<Property>() {
                public String getName(Object property) {
                    return ((Property)property).getName();
                }
            };
        for (Property property : Property.StandardCellProperty.values()) {
            if (query.hasCellProperty(property.getName())) {
                list.add(property);
            }
        }
        for (Property property
            : MondrianOlap4jProperty.CELL_EXTENSIONS.values())
        {
            if (query.hasCellProperty(property.getName())) {
                list.add(property);
            }
        }
        return list;
    }

    public Cube getCube() {
        return olap4jStatement.olap4jConnection.toOlap4j(query.getCube());
    }

    public NamedList<CellSetAxisMetaData> getAxesMetaData() {
        return axesMetaData;
    }

    public CellSetAxisMetaData getFilterAxisMetaData() {
        return filterAxisMetaData;
    }

    // implement ResultSetMetaData

    public int getColumnCount() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isSearchable(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isCurrency(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int isNullable(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isSigned(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getColumnLabel(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getColumnName(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getSchemaName(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getPrecision(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getScale(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getTableName(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getCatalogName(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getColumnType(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getColumnTypeName(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isReadOnly(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWritable(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getColumnClassName(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    // implement Wrapper

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw this.olap4jStatement.olap4jConnection.helper.createException(
            "does not implement '" + iface + "'");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

}

// End MondrianOlap4jCellSetMetaData.java
