/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2010 Pentaho and others
// All Rights Reserved.
*/

package mondrian.rolap;

import mondrian.olap.*;

import java.util.List;

/**
 * RolapCubeDimension wraps a RolapDimension for a specific Cube.
 *
 * @author Will Gorman, 19 October 2007
 */
public class RolapCubeDimension extends RolapDimension {

    RolapCube cube;

    RolapDimension rolapDimension;
    int cubeOrdinal;
    MondrianDef.CubeDimension xmlDimension;

    /**
     * Creates a RolapCubeDimension.
     *
     * @param cube Cube
     * @param rolapDim Dimension wrapped by this dimension
     * @param cubeDim XML element definition
     * @param name Name of dimension
     * @param cubeOrdinal Ordinal of dimension within cube
     * @param hierarchyList List of hierarchies in cube
     * @param highCardinality Whether high cardinality dimension
     */
    public RolapCubeDimension(
        RolapCube cube,
        RolapDimension rolapDim,
        MondrianDef.CubeDimension cubeDim,
        String name,
        int cubeOrdinal,
        List<RolapHierarchy> hierarchyList,
        final boolean highCardinality)
    {
        super(
            null,
            name,
            cubeDim.caption != null
                ? cubeDim.caption
                : rolapDim.getCaption(),
            cubeDim.visible,
            cubeDim.caption != null
                ? cubeDim.description
                : rolapDim.getDescription(),
            null,
            highCardinality,
            RolapHierarchy.createAnnotationMap(cubeDim.annotations));
        this.xmlDimension = cubeDim;
        this.rolapDimension = rolapDim;
        this.cubeOrdinal = cubeOrdinal;
        this.cube = cube;
        this.caption = cubeDim.caption;

        // create new hierarchies
        hierarchies = new RolapCubeHierarchy[rolapDim.getHierarchies().length];

        for (int i = 0; i < rolapDim.getHierarchies().length; i++) {
            final RolapCubeHierarchy cubeHierarchy =
                new RolapCubeHierarchy(
                    this,
                    cubeDim,
                    (RolapHierarchy) rolapDim.getHierarchies()[i],
                    ((HierarchyBase) rolapDim.getHierarchies()[i]).getSubName(),
                    hierarchyList.size());
            hierarchies[i] = cubeHierarchy;
            hierarchyList.add(cubeHierarchy);
        }
    }

    public RolapCube getCube() {
        return cube;
    }

    public Schema getSchema() {
        return rolapDimension.getSchema();
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

    RolapCubeHierarchy newHierarchy(
        String subName, boolean hasAll, RolapHierarchy closureFor)
    {
        throw new UnsupportedOperationException();
    }

    public String getCaption() {
        if (caption != null) {
            return caption;
        }
        return rolapDimension.getCaption();
    }

    public void setCaption(String caption) {
        if (true) {
            throw new UnsupportedOperationException();
        }
        rolapDimension.setCaption(caption);
    }

    public DimensionType getDimensionType() {
        return rolapDimension.getDimensionType();
    }

}

// End RolapCubeDimension.java
