/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// wgorman, 19 October 2007
*/
package mondrian.rolap;

import mondrian.olap.Cube;
import mondrian.olap.HierarchyBase;
import mondrian.olap.MondrianDef;

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
    
    public RolapCubeDimension(RolapCube parent, RolapDimension rolapDimension, 
            MondrianDef.CubeDimension cubeDim, int cubeOrdinal) {
        super(rolapDimension.getSchema(), cubeDim.name, 
                rolapDimension.getGlobalOrdinal(), 
                rolapDimension.getDimensionType());
        this.xmlDimension = cubeDim;
        this.rolapDimension = rolapDimension;
        this.cubeOrdinal = cubeOrdinal;
        this.parent = parent;
        // create new hierarchies
        hierarchies = new RolapCubeHierarchy[
                            rolapDimension.getHierarchies().length];
        
        for (int i = 0; i < rolapDimension.getHierarchies().length; i++) {
            hierarchies[i] = new RolapCubeHierarchy(this, cubeDim, 
                    (RolapHierarchy)rolapDimension.getHierarchies()[i], 
                    ((HierarchyBase)rolapDimension.getHierarchies()[i])
                                                        .getSubName());
        }
    }

    public RolapCube getCube() {
        return parent;
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
        if (!(o instanceof RolapCubeHierarchy)) {
            return false;
        }

        RolapCubeDimension that = (RolapCubeDimension)o;
        if (!parent.equals(that.parent)) {
            return false;
        } else {
            return getUniqueName().equals(that.getUniqueName());
        }
    }

    RolapCubeHierarchy newHierarchy(String subName, boolean hasAll) {
        throw new UnsupportedOperationException();
    }
    
    public String getCaption() {
        return rolapDimension.getCaption();
    }

    public void setCaption(String caption) {
        if (true) {
            throw new UnsupportedOperationException();
        }
        rolapDimension.setCaption(caption);
    }
}

// End RolapCubeDimension.java
