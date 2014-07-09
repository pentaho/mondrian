/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;

import org.olap4j.metadata.*;
import org.olap4j.metadata.Dimension;

/**
 * RolapCubeDimension wraps a RolapDimension for a specific Cube.
 *
 * @author Will Gorman, 19 October 2007
 */
public class RolapCubeDimension extends RolapDimension {

    final RolapCube cube;

    final RolapDimension rolapDimension;
    final int cubeOrdinal;

    /**
     * Creates a RolapCubeDimension.
     *
     * @param cube Cube
     * @param rolapDimension Dimension wrapped by this dimension
     * @param name Name of dimension
     * @param cubeOrdinal Ordinal of dimension within cube
     * @param larder Larder
     */
    public RolapCubeDimension(
        RolapCube cube,
        RolapDimension rolapDimension,
        String name,
        int cubeOrdinal,
        Larder larder)
    {
        super(
            cube.getSchema(),
            name,
            rolapDimension.isVisible(),
            rolapDimension.getDimensionType(),
            rolapDimension.hanger,
            larder);
        this.rolapDimension = rolapDimension;
        this.cubeOrdinal = cubeOrdinal;
        this.cube = cube;
        this.keyAttribute = rolapDimension.keyAttribute;
    }

    public RolapCube getCube() {
        return cube;
    }

    public RolapSchema getSchema() {
        return rolapDimension.getSchema();
    }

    @Override
    public RolapCubeHierarchy[] getHierarchies() {
        //noinspection SuspiciousToArrayCall
        return hierarchyList.toArray(
            new RolapCubeHierarchy[hierarchyList.size()]);
    }

    @Override
    public NamedList<? extends RolapCubeHierarchy> getHierarchyList() {
        //noinspection unchecked
        return (NamedList) hierarchyList;
    }

    // this method should eventually replace the call below
    public int getOrdinal() {
        return cubeOrdinal;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RolapCubeDimension)) {
            return false;
        }

        RolapCubeDimension that = (RolapCubeDimension)o;
        if (!cube.equals(that.cube)) {
            return false;
        }
        return getUniqueName().equals(that.getUniqueName());
    }

    public Dimension.Type getDimensionType() {
        return rolapDimension.getDimensionType();
    }

    @Override
    public RolapSchema.PhysPath getKeyPath(RolapSchema.PhysColumn column) {
        return rolapDimension.getKeyPath(column);
    }

    @Override
    public RolapSchema.PhysRelation getKeyTable() {
        return rolapDimension.getKeyTable();
    }

    public RolapAttribute getKeyAttribute() {
        return keyAttribute;
    }
}

// End RolapCubeDimension.java
