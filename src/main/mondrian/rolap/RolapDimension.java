/*
 // $Id$
 // This software is subject to the terms of the Common Public License
 // Agreement, available at the following URL:
 // http://www.opensource.org/licenses/cpl.html.
 // Copyright (C) 2001-2005 Kana Software, Inc. and others.
 // All Rights Reserved.
 // You must accept the terms of that agreement to use this software.
 //
 // jhyde, 10 August, 2001
 */

package mondrian.rolap;

import org.apache.log4j.Logger;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;

/**
 * <code>RolapDimension</code> implements {@link Dimension}for a ROLAP
 * database.
 *
 * <h2><a name="topic_ordinals">Topic: Dimension ordinals </a></h2>
 *
 * {@link RolapEvaluator} needs each dimension to have an ordinal, so that it
 * can store the evaluation context as an array of members. When virtual cubes
 * and shared dimensions enter the picture, we find that dimensions' ordinals
 * must be unique within the whole schema, not just their cube.
 *
 * <p>
 * The ordinal of a dimension <em>within a particular cube</em> is found by
 * calling {@link #getOrdinal(Cube)}, which is implemented in terms of the
 * {@link RolapCube#localDimensionOrdinals} map. This map converts a
 * dimension's global ordinal into a local one within the cube. Local ordinals
 * are contiguous and zero-based. Zero is always the <code>[Measures]</code>
 * dimension.
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
 * @version $Id$
 */
class RolapDimension extends DimensionBase {

    private static final Logger LOGGER = Logger.getLogger(RolapDimension.class);

    /** Generator for {@link #globalOrdinal}. * */
    private static int nextOrdinal = 1; // 0 is reserved for [Measures]
    static synchronized int getNextOrdinal() {
        return RolapDimension.nextOrdinal++;
    }

    private final Schema schema;

    RolapDimension(Schema schema,
                   String name,
                   int globalOrdinal,
                   DimensionType dimensionType) {
        super(name,
            Util.makeFqName(name),
            null,
            globalOrdinal,
            // todo: recognition of a time dimension should be improved
            // allow multiple time dimensions
            dimensionType);

        this.schema = schema;
        Util.assertTrue((globalOrdinal == 0) == name.equals(MEASURES_NAME));

        this.hierarchies = new RolapHierarchy[0];
    }

    /**
     * Creates a dimension from an XML definition.
     *
     * @pre schema != null
     */
    RolapDimension(RolapSchema schema,
                   RolapCube cube,
                   MondrianDef.Dimension xmlDimension,
                   MondrianDef.CubeDimension xmlCubeDimension) {
        this(schema,
             xmlDimension.name,
             chooseOrdinal(cube, xmlCubeDimension),
             xmlDimension.getDimensionType());

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
                cube, this, xmlDimension.hierarchies[i],
                xmlCubeDimension);
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
                            : DimensionType.StandardDimension;

                    } else {
                        // Dimension type was set according to first level.
                        // Make sure that other levels fit to definition.
                        if (dimensionType == DimensionType.TimeDimension &&
                            !lev.getLevelType().isTime() &&
                            !lev.isAll()) {
                            throw MondrianResource.instance().NonTimeLevelInTimeHierarchy.ex(
                                    getUniqueName());
                        }
                        if (dimensionType != DimensionType.TimeDimension &&
                            lev.getLevelType().isTime()) {
                            throw MondrianResource.instance().TimeLevelInNonTimeHierarchy.ex(
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
     * Assigns an ordinal for a dimension usage; also assigns the join-level of
     * the usage.
     */
    private static synchronized int chooseOrdinal(RolapCube cube,
                            MondrianDef.CubeDimension xmlCubeDimension) {

        if (xmlCubeDimension.name.equals(MEASURES_NAME)) {
            return 0;
        }
        if (xmlCubeDimension instanceof MondrianDef.DimensionUsage) {
            RolapSchema schema = (RolapSchema) cube.getSchema();
            MondrianDef.DimensionUsage usage =
                (MondrianDef.DimensionUsage) xmlCubeDimension;
            RolapHierarchy hierarchy = schema.getSharedHierarchy(usage.source);
            if (hierarchy != null) {
                cube.createUsage(hierarchy, usage);

                RolapDimension dimension =
                    (RolapDimension) hierarchy.getDimension();
                return dimension.getGlobalOrdinal();
            }
        }
        return RolapDimension.getNextOrdinal();
    }

    /**
     * Initializes a dimension within the context of a cube.
     */
    void init(RolapCube cube, MondrianDef.CubeDimension xmlDimension) {
        for (int i = 0; i < hierarchies.length; i++) {
            if (hierarchies[i] != null) {
                ((RolapHierarchy) hierarchies[i]).init(cube, xmlDimension);
            }
        }
    }

    RolapHierarchy newHierarchy(String subName, boolean hasAll) {
        RolapHierarchy hierarchy = new RolapHierarchy(this, subName, hasAll);
        this.hierarchies = (RolapHierarchy[])
            RolapUtil.addElement(this.hierarchies, hierarchy);
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

    public int getOrdinal(Cube cube) {
        return ((RolapCube) cube).getOrdinal(this.globalOrdinal);
    }

    public Schema getSchema() {
        return schema;
    }

    /**
     * See {@link #nextOrdinal}.
     */
    int getGlobalOrdinal() {
        return globalOrdinal;
    }

    /**
     * Returns a copy of this dimension with a different name.
     *
     * @param cube
     * @param name Name for the new dimension.
     * @param xmlCubeDimension
     */
    public RolapDimension copy(RolapCube cube,
                               String name,
                               MondrianDef.CubeDimension xmlCubeDimension) {
        RolapDimension dimension = new RolapDimension(
            schema,
            name,
            RolapDimension.getNextOrdinal(),
            dimensionType);
        dimension.hierarchies = (Hierarchy[]) hierarchies.clone();
        for (int i = 0; i < hierarchies.length; i++) {
            final RolapHierarchy hierarchy = (RolapHierarchy) hierarchies[i];
            dimension.hierarchies[i] = new RolapHierarchy(cube, dimension,
                hierarchy.xmlHierarchy, xmlCubeDimension);
        }
        return dimension;
    }
}

// End RolapDimension.java
