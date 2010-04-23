/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
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

    /**
     * Creates a MondrianOlap4jCellSetAxisMetaData.
     *
     * @param cellSetMetaData Cell set axis metadata
     * @param queryAxis Query axis
     */
    MondrianOlap4jCellSetAxisMetaData(
        MondrianOlap4jCellSetMetaData cellSetMetaData,
        QueryAxis queryAxis)
    {
        if (queryAxis == null) {
            queryAxis = new QueryAxis(
                false, null, AxisOrdinal.StandardAxisOrdinal.SLICER,
                QueryAxis.SubtotalVisibility.Undefined);
        }
        this.queryAxis = queryAxis;
        this.cellSetMetaData = cellSetMetaData;

        // populate property list
        for (Id id : queryAxis.getDimensionProperties()) {
            final String[] names = id.toStringArray();
            Property property = null;
            if (names.length == 1) {
                property =
                    Util.lookup(
                        Property.StandardMemberProperty.class, names[0]);
            }
            if (property == null) {
                property =
                    (Property)
                    Util.lookup(
                        cellSetMetaData.query,
                        id.getSegments(),
                        true);
            }
            propertyList.add(property);
        }
    }

    public Axis getAxisOrdinal() {
        return Axis.Factory.forOrdinal(
            queryAxis.getAxisOrdinal().logicalOrdinal());
    }

    public List<Hierarchy> getHierarchies() {
        return getHierarchiesNonFilter();
    }

    /**
     * Returns the hierarchies on a non-filter axis.
     *
     * @return List of hierarchies, never null
     */
    private List<Hierarchy> getHierarchiesNonFilter() {
        final Exp exp = queryAxis.getSet();
        if (exp == null) {
            return Collections.emptyList();
        }
        Type type = exp.getType();
        if (type instanceof SetType) {
            type = ((SetType) type).getElementType();
        }
        final MondrianOlap4jConnection olap4jConnection =
            cellSetMetaData.olap4jStatement.olap4jConnection;
        if (type instanceof TupleType) {
            final TupleType tupleType = (TupleType) type;
            List<Hierarchy> hierarchyList =
                new ArrayList<Hierarchy>();
            for (Type elementType : tupleType.elementTypes) {
                hierarchyList.add(
                    olap4jConnection.toOlap4j(
                        elementType.getHierarchy()));
            }
            return hierarchyList;
        } else {
            return Collections.singletonList(
                (Hierarchy) olap4jConnection.toOlap4j(
                    type.getHierarchy()));
        }
    }

    public List<Property> getProperties() {
        return propertyList;
    }
}

// End MondrianOlap4jCellSetAxisMetaData.java
