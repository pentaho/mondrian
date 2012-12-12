/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;

import org.olap4j.metadata.*;
import org.olap4j.metadata.Dimension;

import java.util.*;

import static mondrian.olap.Util.first;

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
     * @param caption Caption
     * @param description Description
     * @param cubeOrdinal Ordinal of dimension within cube
     * @param hierarchyList List of hierarchies in cube
     * @param annotationMap Annotation map
     */
    public RolapCubeDimension(
        RolapSchemaLoader schemaLoader,
        RolapCube cube,
        RolapDimension rolapDim,
        String name,
        final String dimSource,
        final String caption,
        final String description,
        int cubeOrdinal,
        List<RolapCubeHierarchy> hierarchyList,
        Map<String, Annotation> annotationMap)
    {
        super(
            cube.getSchema(),
            name,
            rolapDim.isVisible(),
            first(caption, rolapDim.getCaption()),
            first(description, rolapDim.getDescription()),
            rolapDim.getDimensionType(),
            rolapDim.hanger,
            annotationMap);
        this.rolapDimension = rolapDim;
        this.cubeOrdinal = cubeOrdinal;
        this.cube = cube;

        // create new hierarchies

        final int originalSize = hierarchyList.size();
        for (RolapHierarchy rolapHierarchy : rolapDim.getHierarchyList()) {
            final RolapCubeHierarchy hierarchy =
                new RolapCubeHierarchy(
                    schemaLoader,
                    this,
                    rolapHierarchy,
                    rolapHierarchy.getName(),
                    rolapHierarchy.getDimension().isMeasures()
                        ? rolapHierarchy.getUniqueName()
                        : Util.makeFqName(this, rolapHierarchy.getName()),
                    hierarchyList.size(),
                    applyPrefix(
                        rolapHierarchy.getCaption(),
                        dimSource, name, caption),
                    applyPrefix(
                        rolapHierarchy.getDescription(),
                        dimSource, name, description));
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

    public Dimension.Type getDimensionType() {
        return rolapDimension.getDimensionType();
    }

    @Override
    public RolapSchema.PhysPath getKeyPath(RolapSchema.PhysColumn column) {
        return rolapDimension.getKeyPath(column);
    }

    /**
     * Applies a prefix to a caption or description of a hierarchy in a shared
     * dimension. Ensures that if a dimension is used more than once in the same
     * cube then the hierarchies are distinguishable.
     *
     * <p>For example, if the [Time] dimension is imported as [Order Time] and
     * [Ship Time], then the [Time].[Weekly] hierarchy would have caption
     * "Order Time.Weekly caption" and description "Order Time.Weekly
     * description".
     *
     * <p>If the dimension usage has a caption, it overrides.
     *
     * <p>If the dimension usage has a null name, or the name is the same
     * as the dimension, and no caption, then no prefix is applied.
     *
     * @param caption Caption or description
     * @param source Name of source schema dimension
     * @param name Name of dimension
     * @param dimCaption Caption of dimension
     * @return Caption or description, possibly prefixed by dimension role name
     */
    static String applyPrefix(
        String caption,
        final String source,
        final String name,
        final String dimCaption)
    {
        if (caption != null
            && source != null
            && name != null
            && !source.equals(name))
        {
            return first(dimCaption, name) + "." + caption;
        }
        return caption;
    }
}

// End RolapCubeDimension.java
