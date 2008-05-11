/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// wgorman, 19 October 2007
*/
package mondrian.rolap;

import mondrian.olap.Cube;
import mondrian.olap.DimensionType;
import mondrian.olap.HierarchyBase;
import mondrian.olap.MondrianDef;
import mondrian.olap.Schema;

/**
 * RolapCubeDimension wraps a RolapDimension for a specific Cube.
 *
 * @author Will Gorman (wgorman@pentaho.org)
 * @version $Id$
 */
public class RolapCubeDimension extends RolapDimension {

    RolapCube parent;

    RolapDimension rolapDimension;
    int cubeOrdinal;
    MondrianDef.CubeDimension xmlDimension;

    public RolapCubeDimension(RolapCube parent, RolapDimension rolapDim,
            MondrianDef.CubeDimension cubeDim, String name, int cubeOrdinal,
            final boolean highCardinality) {
        super(null, name, null, highCardinality);
        this.xmlDimension = cubeDim;
        this.rolapDimension = rolapDim;
        this.cubeOrdinal = cubeOrdinal;
        this.parent = parent;
        this.caption = cubeDim.caption;

        // create new hierarchies
        hierarchies = new RolapCubeHierarchy[rolapDim.getHierarchies().length];

        for (int i = 0; i < rolapDim.getHierarchies().length; i++) {
            hierarchies[i] = new RolapCubeHierarchy(this, cubeDim,
                    (RolapHierarchy)rolapDim.getHierarchies()[i],
                    ((HierarchyBase)rolapDim.getHierarchies()[i]).getSubName());
        }
    }

    public RolapCube getCube() {
        return parent;
    }

    public Schema getSchema() {
        return rolapDimension.getSchema();
    }

    // this method should eventually replace the call below
    public int getOrdinal() {
        return cubeOrdinal;
    }

    // note that the cube is not necessary here
    public int getOrdinal(Cube cube) {
        // this is temporary to validate that internals are consistant
        assert(cube == parent);
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
        if (!parent.equals(that.parent)) {
            return false;
        }
        return getUniqueName().equals(that.getUniqueName());
    }

    RolapCubeHierarchy newHierarchy(String subName, boolean hasAll) {
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
