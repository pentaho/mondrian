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
import org.olap4j.metadata.Dimension;
import mondrian.olap.*;
import mondrian.olap.type.*;

import java.util.*;

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
    private final MondrianOlap4jCellSetMetaData cellSetMetaData;
    private final List<Property> propertyList = new ArrayList<Property>();

    MondrianOlap4jCellSetAxisMetaData(
        MondrianOlap4jCellSetMetaData cellSetMetaData,
        QueryAxis queryAxis)
    {
        if (queryAxis == null) {
            queryAxis = new QueryAxis(
                false, null, AxisOrdinal.SLICER,
                QueryAxis.SubtotalVisibility.Undefined);
        }
        this.queryAxis = queryAxis;
        this.cellSetMetaData = cellSetMetaData;

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
        List<Hierarchy> hierarchyList =
            new ArrayList<Hierarchy>();
        final Type type;
        final Exp exp = queryAxis.getSet();
        switch (queryAxis.getAxisOrdinal()) {
        case SLICER:
            type =
                exp == null
                    ? null
                    : exp.getType();
            break;
        default:
            final SetType setType =
                (SetType) exp.getType();
            type = setType.getElementType();
        }
        final MondrianOlap4jConnection olap4jConnection =
            cellSetMetaData.olap4jStatement.olap4jConnection;
        if (type == null) {
            // nothing; will deal with slicer later
        } else if (type instanceof TupleType) {
            final TupleType tupleType = (TupleType) type;
            for (Type elementType : tupleType.elementTypes) {
                hierarchyList.add(
                    olap4jConnection.toOlap4j(
                        elementType.getHierarchy()));
            }
        } else {
            hierarchyList.add(
                olap4jConnection.toOlap4j(
                    type.getHierarchy()));
        }

        // Slicer contains all dimensions not mentioned on other axes. So, if
        // this is the slicer, now add to the list the default hierarchy of
        // each dimension not already in the slicer or in another axes.
        switch (queryAxis.getAxisOrdinal()) {
        case SLICER:
            Set<Dimension> dimensionSet = new HashSet<Dimension>();
            for (Hierarchy hierarchy : hierarchyList) {
                dimensionSet.add(hierarchy.getDimension());
            }
            for (CellSetAxisMetaData cellSetAxisMetaData
                : cellSetMetaData.getAxesMetaData())
            {
                for (Hierarchy hierarchy
                    : cellSetAxisMetaData.getHierarchies())
                {
                    dimensionSet.add(hierarchy.getDimension());
                }
            }
            for (Dimension dimension
                : cellSetMetaData.getCube().getDimensions()) {
                if (dimensionSet.add(dimension)) {
                    hierarchyList.add(dimension.getDefaultHierarchy());
                }
            }
        }
        return hierarchyList;
    }

    public List<Property> getProperties() {
        return propertyList;
    }
}

// End MondrianOlap4jCellSetAxisMetaData.java
