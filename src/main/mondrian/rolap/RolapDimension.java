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
 * <p> A dimension may be either shared or private to a particular cube. The
 * dimension object doesn't actually know which; {@link Schema} has a list of
 * shared hierarchies ({@link Schema#getSharedHierarchies}), and {@link Cube}
 * has a list of dimensions ({@link Cube#getDimensions}).
 *
 * <p>If a dimension is shared between several cubes, the {@link Dimension}
 * objects which represent them may (or may not be) the same. (That's why
 * there's no <code>getCube()</code> method.)
 *
 * <p>Furthermore, since members are created by a
 * {@link MemberReader} which belongs to the {@link RolapHierarchy}, you will
 * the members will be the same too. For example, if you query
 * <code>[Product].[Beer]</code> from the <code>Sales</code> and
 * <code>Warehouse</code> cubes, you will get the same {@link RolapMember}
 * object. ({@link RolapSchema#mapSharedHierarchyToReader} holds the mapping. I
 * don't know whether it's still necessary.)
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapDimension extends DimensionBase
{
	/** Generator for {@link #globalOrdinal}. **/
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

	public Schema getSchema() {
		return schema;
	}

	/**
	 * See #nextOrdinal
	 */
	int getGlobalOrdinal() {
		return globalOrdinal;
	}
}

// End RolapDimension.java
