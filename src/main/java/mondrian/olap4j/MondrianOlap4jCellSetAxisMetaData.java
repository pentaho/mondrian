/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.olap4j;

import mondrian.mdx.LevelExpr;
import mondrian.mdx.UnresolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.TypeUtil;

import org.olap4j.Axis;
import org.olap4j.CellSetAxisMetaData;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Property;

import java.util.*;

/**
 * Implementation of {@link org.olap4j.CellSetMetaData}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
  @since Nov 17, 2007
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
            Property olap4jProperty = null;
            if (names.length == 1) {
                olap4jProperty =
                    Util.lookup(
                        Property.StandardMemberProperty.class, names[0]);
                if (olap4jProperty == null) {
                    olap4jProperty =
                        MondrianOlap4jProperty.MEMBER_EXTENSIONS.get(names[0]);
                }
            }
            if (olap4jProperty == null) {
                final UnresolvedFunCall call =
                    (UnresolvedFunCall)
                        Util.lookup(
                            cellSetMetaData.query, id.getSegments(), true);
                olap4jProperty =
                    new MondrianOlap4jProperty(
                        Util.lookupProperty(
                            ((LevelExpr) call.getArg(0)).getLevel(),
                            call.getFunName()));
            }
            propertyList.add(olap4jProperty);
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
        List<Hierarchy> hierarchyList = new ArrayList<Hierarchy>();
        for (mondrian.olap.Hierarchy hierarchy
            : TypeUtil.getHierarchies(exp.getType()))
        {
            hierarchyList.add(
                cellSetMetaData.olap4jStatement.olap4jConnection.toOlap4j(
                    hierarchy));
        }
        return hierarchyList;
    }

    public List<Property> getProperties() {
        return propertyList;
    }
}

// End MondrianOlap4jCellSetAxisMetaData.java
