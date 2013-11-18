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
import mondrian.olap.Dimension;
import mondrian.util.Lazy;

import org.apache.log4j.Logger;

import org.olap4j.metadata.NamedList;

import java.util.*;

/**
 * <code>RolapDimension</code> implements {@link Dimension}for a ROLAP
 * database.
 *
 * <h2><a name="topic_ordinals">Topic: Dimension ordinals </a></h2>
 *
 * {@link RolapEvaluator} needs each dimension to have an ordinal, so that it
 * can store the evaluation context as an array of members.
 *
 * <p>A dimension may be either shared or private to a particular cube. The
 * dimension object doesn't actually know which; {@link Schema} has a list of
 * shared dimensions ({@link Schema#getSharedDimensions()}), and {@link Cube}
 * has a list of dimensions ({@link Cube#getDimensionList()}).</p>
 *
 * <p>If a dimension is shared between several cubes, the {@link Dimension}
 * objects which represent them may (or may not be) the same. (That's why
 * there's no <code>getCube()</code> method.)</p>
 *
 * <p>A {@link RolapDimension} cannot have members. Members belong to a
 * {@link RolapCubeDimension}, that is, a dimension in the context of a
 * particular cube, with a well-defined join path to each fact table in the
 * cube.</p>
 *
 * <p>NOTE: This class must not contain any references to XML (MondrianDef)
 * objects. Put those in {@link mondrian.rolap.RolapSchemaLoader}.</p>
 *
 * @author jhyde
 * @since 10 August, 2001
 */
public class RolapDimension extends DimensionBase {

    private static final Logger LOGGER = Logger.getLogger(RolapDimension.class);

    final RolapSchema schema;
    private final Larder larder;
    RolapAttribute keyAttribute;
    protected final Map<String, RolapAttribute> attributeMap =
        new HashMap<String, RolapAttribute>();

    /**
     * The key by which this dimension is to be linked to a fact table (or
     * fact tables). The key must contain the same columns as the key of the
     * key attribute. It must also be a key defined on its table.
     *
     * <p>NOTE: We currently assume that it is unique, but we do not enforce it.
     */
    Lazy<RolapSchema.PhysKey> key;

    final boolean hanger;

    RolapDimension(
        RolapSchema schema,
        String name,
        boolean visible,
        org.olap4j.metadata.Dimension.Type dimensionType,
        boolean hanger,
        Larder larder)
    {
        super(
            name,
            visible,
            dimensionType);
        assert larder != null;
        assert schema != null;
        this.schema = schema;
        this.hanger = hanger;
        this.larder = larder;
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public RolapHierarchy[] getHierarchies() {
        //noinspection SuspiciousToArrayCall
        return hierarchyList.toArray(new RolapHierarchy[hierarchyList.size()]);
    }

    public NamedList<? extends RolapHierarchy> getHierarchyList() {
        //noinspection unchecked
        return (NamedList) hierarchyList;
    }

    public RolapSchema getSchema() {
        return schema;
    }

    public Larder getLarder() {
        return larder;
    }

    /**
     * Returns the join path from a column to the key of this dimension.
     *
     * <p>Generally the column is a component of the key of an attribute.
     *
     * @param column Column
     * @return Join path, never null
     */
    public RolapSchema.PhysPath getKeyPath(RolapSchema.PhysColumn column) {
        final RolapSchema.PhysSchemaGraph graph =
            column.relation.getSchema().getGraph();
        try {
            final RolapSchema.PhysRelation relation = getKeyTable();
            return graph.findPath(relation, column.relation);
        } catch (RolapSchema.PhysSchemaException e) {
            // TODO: pre-compute path for all attributes, so this error could
            // never be the result of a user error in the schema definition
            Util.deprecated("TODO", false);
            throw Util.newInternal(
                e, "while finding path from attribute to dimension key");
        }
    }

    /**
     * Adds a hierarchy.
     *
     * <p>Called during load.
     *
     * @param hierarchy Hierarchy
     */
    void addHierarchy(RolapHierarchy hierarchy) {
        hierarchyList.add(hierarchy);
    }

    public RolapAttribute getKeyAttribute() {
        return keyAttribute;
    }

    public RolapSchema.PhysRelation getKeyTable() {
        return key.get().relation;
    }
}

// End RolapDimension.java
