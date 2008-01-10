/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import org.olap4j.CellSetAxisMetaData;
import org.olap4j.Axis;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Property;
import mondrian.olap.*;
import mondrian.olap.type.*;

import java.util.List;
import java.util.ArrayList;

/**
 * Implementation of {@link org.olap4j.CellSetMetaData}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @version $Id$
* @since Nov 17, 2007
*/
class MondrianOlap4jCellSetAxisMetaData implements CellSetAxisMetaData {
    private final QueryAxis queryAxis;
    private final MondrianOlap4jConnection olap4jConnection;
    private final List<Property> propertyList = new ArrayList<Property>();

    MondrianOlap4jCellSetAxisMetaData(
        MondrianOlap4jConnection olap4jConnection,
        QueryAxis queryAxis)
    {
        if (queryAxis == null) {
            queryAxis = new QueryAxis(
                false, null, AxisOrdinal.SLICER,
                QueryAxis.SubtotalVisibility.Undefined);
        }
        this.queryAxis = queryAxis;
        this.olap4jConnection = olap4jConnection;

        // populate property list
        for (Id id : queryAxis.getDimensionProperties()) {
            propertyList.add(
                Property.StandardMemberProperty.valueOf(
                    id.toStringArray()[0]));
        }
    }

    public Axis getAxisOrdinal() {
        switch (queryAxis.getAxisOrdinal()) {
        case SLICER:
            return Axis.FILTER;
        default:
            return Axis.valueOf(queryAxis.getAxisOrdinal().name());
        }
    }

    public List<Hierarchy> getHierarchies() {
        final Type type;
        switch (queryAxis.getAxisOrdinal()) {
        case SLICER:
            type = queryAxis.getSet().getType();
            break;
        default:
            final SetType setType =
                (SetType) queryAxis.getSet().getType();
            type = setType.getElementType();
        }
        List<Hierarchy> hierarchyList =
            new ArrayList<Hierarchy>();
        if (type instanceof TupleType) {
            final TupleType tupleType = (TupleType) type;
            for (Type elementType : tupleType.elementTypes) {
                hierarchyList.add(
                    olap4jConnection.toOlap4j(
                        elementType.getHierarchy()));
            }
        } else {
            hierarchyList.add(
                olap4jConnection.toOlap4j(type.getHierarchy()));
        }
        return hierarchyList;
    }

    public List<Property> getProperties() {
        return propertyList;
    }
}

// End MondrianOlap4jCellSetAxisMetaData.java
