/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2017 Pentaho and others
// All Rights Reserved.
//
// jhyde, 10 August, 2001
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.resource.MondrianResource;

import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Map;

/**
 * <code>RolapDimension</code> implements {@link Dimension}for a ROLAP
 * database.
 *
 * <h2><a name="topic_ordinals">Topic: Dimension ordinals </a></h2>
 *
 * {@link RolapEvaluator} needs each dimension to have an ordinal, so that it
 * can store the evaluation context as an array of members.
 *
 * <p>
 * A dimension may be either shared or private to a particular cube. The
 * dimension object doesn't actually know which; {@link Schema} has a list of
 * shared hierarchies ({@link Schema#getSharedHierarchies}), and {@link Cube}
 * has a list of dimensions ({@link Cube#getDimensions}).
 *
 * <p>
 * If a dimension is shared between several cubes, the {@link Dimension}objects
 * which represent them may (or may not be) the same. (That's why there's no
 * <code>getCube()</code> method.)
 *
 * <p>
 * Furthermore, since members are created by a {@link MemberReader}which
 * belongs to the {@link RolapHierarchy}, you will the members will be the same
 * too. For example, if you query <code>[Product].[Beer]</code> from the
 * <code>Sales</code> and <code>Warehouse</code> cubes, you will get the
 * same {@link RolapMember}object.
 * ({@link RolapSchema#mapSharedHierarchyToReader} holds the mapping. I don't
 * know whether it's still necessary.)
 *
 * @author jhyde
 * @since 10 August, 2001
 */
class RolapDimension extends DimensionBase {

    private static final Logger LOGGER = Logger.getLogger(RolapDimension.class);

    private final Schema schema;
    private final Map<String, Annotation> annotationMap;

    RolapDimension(
        Schema schema,
        String name,
        String caption,
        boolean visible,
        String description,
        DimensionType dimensionType,
        final boolean highCardinality,
        Map<String, Annotation> annotationMap)
    {
        // todo: recognition of a time dimension should be improved
        // allow multiple time dimensions
        super(
            name,
            caption,
            visible,
            description,
            dimensionType,
            highCardinality);
        assert annotationMap != null;
        this.schema = schema;
        this.annotationMap = annotationMap;
        this.hierarchies = new RolapHierarchy[0];
    }

    /**
     * Creates a dimension from an XML definition.
     *
     * @pre schema != null
     */
    RolapDimension(
        RolapSchema schema,
        RolapCube cube,
        MondrianDef.Dimension xmlDimension,
        MondrianDef.CubeDimension xmlCubeDimension)
    {
        this(
            schema,
            xmlDimension.name,
            xmlDimension.caption,
            xmlDimension.visible,
            xmlDimension.description,
            xmlDimension.getDimensionType(),
            xmlDimension.highCardinality,
            RolapHierarchy.createAnnotationMap(xmlDimension.annotations));

        Util.assertPrecondition(schema != null);

        if (cube != null) {
            Util.assertTrue(cube.getSchema() == schema);
        }

        if (!Util.isEmpty(xmlDimension.caption)) {
            setCaption(xmlDimension.caption);
        }
        this.hierarchies = new RolapHierarchy[xmlDimension.hierarchies.length];
        for (int i = 0; i < xmlDimension.hierarchies.length; i++) {
            RolapHierarchy hierarchy = new RolapHierarchy(
                cube, this, xmlDimension.hierarchies[i], xmlCubeDimension);
            hierarchies[i] = hierarchy;
        }

        // if there was no dimension type assigned, determine now.
        if (dimensionType == null) {
            for (int i = 0; i < hierarchies.length; i++) {
                Level[] levels = hierarchies[i].getLevels();
                LevLoop:
                for (int j = 0; j < levels.length; j++) {
                    Level lev = levels[j];
                    if (lev.isAll()) {
                        continue LevLoop;
                    }
                    if (dimensionType == null) {
                        // not set yet - set it according to current level
                        dimensionType = (lev.getLevelType().isTime())
                            ? DimensionType.TimeDimension
                            : isMeasures()
                            ? DimensionType.MeasuresDimension
                            : DimensionType.StandardDimension;

                    } else {
                        // Dimension type was set according to first level.
                        // Make sure that other levels fit to definition.
                        if (dimensionType == DimensionType.TimeDimension
                            && !lev.getLevelType().isTime()
                            && !lev.isAll())
                        {
                            throw MondrianResource.instance()
                                .NonTimeLevelInTimeHierarchy.ex(
                                    getUniqueName());
                        }
                        if (dimensionType != DimensionType.TimeDimension
                            && lev.getLevelType().isTime())
                        {
                            throw MondrianResource.instance()
                                .TimeLevelInNonTimeHierarchy.ex(
                                    getUniqueName());
                        }
                    }
                }
            }
        }
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    /**
     * Initializes a dimension within the context of a cube.
     */
    void init(MondrianDef.CubeDimension xmlDimension) {
        for (int i = 0; i < hierarchies.length; i++) {
            if (hierarchies[i] != null) {
                ((RolapHierarchy) hierarchies[i]).init(xmlDimension);
            }
        }
    }

    /**
     * Creates a hierarchy.
     *
     * @param subName Name of this hierarchy.
     * @param hasAll Whether hierarchy has an 'all' member
     * @param closureFor Hierarchy for which the new hierarchy is a closure;
     *     null for regular hierarchies
     * @return Hierarchy
     */
    RolapHierarchy newHierarchy(
        String subName,
        boolean hasAll,
        RolapHierarchy closureFor)
    {
        RolapHierarchy hierarchy =
            new RolapHierarchy(
                this, subName,
                caption, visible, description, hasAll, closureFor,
                Collections.<String, Annotation>emptyMap());
        this.hierarchies = Util.append(this.hierarchies, hierarchy);
        return hierarchy;
    }

    /**
     * Returns the hierarchy of an expression.
     *
     * <p>In this case, the expression is a dimension, so the hierarchy is the
     * dimension's default hierarchy (its first).
     */
    public Hierarchy getHierarchy() {
        return hierarchies[0];
    }

    public Schema getSchema() {
        return schema;
    }

    public Map<String, Annotation> getAnnotationMap() {
        return annotationMap;
    }

    @Override
    protected int computeHashCode() {
      if (isMeasuresDimension()) {
        return System.identityHashCode(this);
      }
      return super.computeHashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
    if (!(o instanceof RolapDimension)) {
        return false;
    }
      if (isMeasuresDimension()) {
        RolapDimension that = (RolapDimension) o;
        return this == that;
      }
      return super.equals(o);
    }

    private boolean isMeasuresDimension() {
      return this.getDimensionType() == DimensionType.MeasuresDimension;
    }

}

// End RolapDimension.java
