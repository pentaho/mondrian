/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;

/**
 * <code>RolapDimension</code> implements {@link Dimension} for a ROLAP database.
 *
 * <h2><a name="topic_ordinals">Topic: Dimension ordinals</a></h2>
 *
 * {@link RolapEvaluator} needs each dimension to have an ordinal, so that it
 * can store the evaluation context as an array of members. When virtual cubes
 * and shared dimensions enter the picture, we find that dimensions' ordinals
 * must be unique within the whole schema, not just their cube.
 *
 * <p> The ordinal of a dimension <em>within a particular cube</em> is found
 * by calling {@link #getOrdinal(Cube)}, which is implemented in terms of
 * the {@link RolapCube#localDimensionOrdinals} map. This map converts a
 * dimension's global ordinal into a local one within the cube. Local ordinals
 * are contiguous and zero-based. Zero is always the <code>[Measures]</code>
 * dimension.
 *
 * <p> I'd like to clean up this model. Dimensions may be either shared or
 * private to a particular cube. A hierarchy always belongs to a dimension.
 * The link from a hierarchy to its dimension is clear, but the link from a
 * dimension to a cube depends upon the current context. We currently (I think)
 * create a {@link RolapDimension} for each <em>usage</em>, and this has a
 * {@link Dimension#getCube} method.
 *
 * <p> Shared hierarchies belong to a schema, and member readers belong to them
 * ({@link RolapSchema#mapSharedHierarchyToReader} holds the mapping).
 * This causes a problem with shared member readers. The same member
 * objects are returned regardless of which cube you are reading from, so
 * if you call a member's {@link #getCube} method you may receive the wrong
 * answer.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapDimension extends DimensionBase
{
	/** Generator for {@link #ordinal}. **/
	static int nextOrdinal = 1; // 0 is reserved for [Measures]
	RolapSchema schema;

	RolapDimension(RolapSchema schema, String name, int globalOrdinal) {
		this.schema = schema;
		Util.assertTrue((globalOrdinal == 0) == name.equals(MEASURES_NAME));
		this.globalOrdinal = globalOrdinal;
		this.name = name;
		this.uniqueName = Util.makeFqName(name);
		this.description = null;
		this.dimensionType = name.equals("Time") ? TIME : STANDARD;
		this.hierarchies = new RolapHierarchy[0];
	}

	/**
	 * @pre schema != null
	 */
	RolapDimension(
			RolapSchema schema, RolapCube cube, MondrianDef.Dimension xmlDimension,
		MondrianDef.CubeDimension xmlCubeDimension)
	{
		this(schema, xmlDimension.name, chooseOrdinal(schema, xmlCubeDimension));
		Util.assertPrecondition(schema != null);
		if (cube != null) {
			Util.assertTrue(cube.schema == schema);
		}
		this.hierarchies = new RolapHierarchy[xmlDimension.hierarchies.length];
		for (int i = 0; i < xmlDimension.hierarchies.length; i++) {
			hierarchies[i] = new RolapHierarchy(
					cube, this, xmlDimension.hierarchies[i], xmlCubeDimension);
		}
	}

	private static int chooseOrdinal(
			RolapSchema schema, MondrianDef.CubeDimension xmlCubeDimension) {
		if (xmlCubeDimension.name.equals(MEASURES_NAME)) {
			return 0;
		}
		if (xmlCubeDimension instanceof MondrianDef.DimensionUsage) {
			MondrianDef.DimensionUsage usage = (MondrianDef.DimensionUsage) xmlCubeDimension;
			RolapHierarchy hierarchy = schema.getSharedHierarchy(usage.source);
			if (hierarchy != null) {
				return ((RolapDimension) hierarchy.getDimension()).getGlobalOrdinal();
			}
		}
		return nextOrdinal++;
	}

	/**
	 * @param cube optional
	 */
	void init(RolapCube cube)
	{
		for (int i = 0; i < hierarchies.length; i++) {
			if (hierarchies[i] != null) {
				((RolapHierarchy) hierarchies[i]).init(cube);
			}
		}
	}

	RolapHierarchy newHierarchy(
		String subName, boolean hasAll, String sql, String primaryKey,
		String foreignKey)
	{
		RolapHierarchy hierarchy = new RolapHierarchy(
			this, subName, hasAll, sql, primaryKey, foreignKey);
		this.hierarchies = (RolapHierarchy[]) RolapUtil.addElement(
			this.hierarchies, hierarchy);
		return hierarchy;
	}

	// implement Exp
	public Hierarchy getHierarchy() {
		return hierarchies[0];
	}

	public int getOrdinal(Cube cube) {
		return ((RolapCube) cube).localDimensionOrdinals[globalOrdinal];
	}

	/**
	 * See #nextOrdinal
	 */
	int getGlobalOrdinal() {
		return globalOrdinal;
	}
}

// End RolapDimension.java
