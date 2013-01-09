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

import java.util.*;


/**
 * RolapCubeDimension wraps a RolapDimension for a specific Cube.
 *
 * @author Will Gorman, 19 October 2007
 */
public class RolapCubeDimension extends RolapDimension {

    RolapCube cube;

    RolapDimension rolapDimension;
    int cubeOrdinal;

    /**
     * Creates a RolapCubeDimension.
     *
     * @param schemaLoader Schema loader
     * @param cube Cube
     * @param rolapDim Dimension wrapped by this dimension
     * @param name Name of dimension
     * @param dimSource Name of source dimension
     * @param cubeOrdinal Ordinal of dimension within cube
     * @param hierarchyList List of hierarchies in cube
     * @param larder Larder
     */
    public RolapCubeDimension(
        RolapSchemaLoader schemaLoader,
        RolapCube cube,
        RolapDimension rolapDim,
        String name,
        final String dimSource,
        int cubeOrdinal,
        List<RolapCubeHierarchy> hierarchyList,
        Larder larder)
    {
        super(
            cube.getSchema(),
            name,
            rolapDim.isVisible(),
            rolapDim.getDimensionType(),
            rolapDim.hanger,
            larder);
        this.rolapDimension = rolapDim;
        this.cubeOrdinal = cubeOrdinal;
        this.cube = cube;

        // create new hierarchies

        final int originalSize = hierarchyList.size();
        for (RolapHierarchy rolapHierarchy : rolapDim.getHierarchyList()) {
            final String uniqueName =
                rolapHierarchy.getDimension().isMeasures()
                    ? rolapHierarchy.getUniqueName()
                    : Util.makeFqName(this, rolapHierarchy.getName());
            final RolapCubeHierarchy hierarchy =
                new RolapCubeHierarchy(
                    schemaLoader,
                    this,
                    rolapHierarchy,
                    rolapHierarchy.getName(),
                    uniqueName,
                    hierarchyList.size(),
                    Larders.underride(
                        schemaLoader.createLarder(
                            cube + "." + uniqueName + ".hierarchy",
                            null,
                            null,
                            null),
                        Larders.prefix(
                            rolapHierarchy.getLarder(),
                            dimSource,
                            name)));
            hierarchyList.add(hierarchy);
            if (hierarchy.isScenario) {
                assert cube.scenarioHierarchy == null;
                cube.scenarioHierarchy = hierarchy;
            }
        }
        this.hierarchyList.addAll(
            hierarchyList.subList(originalSize, hierarchyList.size()));
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
}

// End RolapCubeDimension.java
