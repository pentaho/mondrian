/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

/**
 * <code>CubeBase</code> is an abstract implementation of {@link Cube}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public abstract class CubeBase extends OlapElementBase implements Cube {

	protected Schema schema;
	protected String name;
	protected DimensionBase[] dimensions;

	/** constraints indexes for adSchemaMembers
	 *
	 * http://msdn.microsoft.com/library/psdk/dasdk/mdx8h4k.htm
	 * check "Restrictions in the MEMBER Rowset" under MEMBER Rowset section
	 **/
 	public static final int CATALOG_NAME = 0;
	public static final int SCHEMA_NAME = 1;
	public static final int CUBE_NAME = 2;
	public static final int DIMENSION_UNIQUE_NAME = 3;
	public static final int HIERARCHY_UNIQUE_NAME = 4;
	public static final int LEVEL_UNIQUE_NAME = 5;
	public static final int LEVEL_NUMBER = 6;
	public static final int MEMBER_NAME = 7;
	public static final int MEMBER_UNIQUE_NAME = 8;
	public static final int MEMBER_CAPTION = 9;
	public static final int MEMBER_TYPE = 10;
	public static final int Tree_Operator = 11;
	public static final int maxNofConstraintsForAdSchemaMember = 12;
	public static final int MDTREEOP_SELF = 0;
	public static final int MDTREEOP_CHILDREN = 1;
	public static final int MDPROP_USERDEFINED0 = 19;

	// implement OlapElement
	public String getName() { return name; }
	public String getUniqueName() { return name; }
	public String getQualifiedName() {
		return Util.getRes().getMdxCubeName(getName());
	}
 	public Hierarchy getHierarchy() { return null; }
	public String getDescription() { return null; }
	public Cube getCube() { return this; }
	public int getType() {
		return Category.Cube;
	}
	public Schema getSchema() {
		return schema;
	}
	public boolean usesDimension(Dimension dimension) {
		return false;
	}
	public Dimension[] getDimensions() { return dimensions; }
	public Object[] getChildren() { return dimensions; }

	public Hierarchy lookupHierarchy(String s, boolean unique)
	{
		for (int i = 0; i < dimensions.length; i++) {
			DimensionBase dimension = dimensions[i];
			for (int j = 0; j < dimension.hierarchies.length; j++) {
				HierarchyBase hierarchy = dimension.hierarchies[j];
				String name = unique ? hierarchy.uniqueName : hierarchy.name;
				if (name.equals(s)) {
					return hierarchy;
				}
			}
		}
		return null;
	}
	public OlapElement lookupChild(SchemaReader schemaReader, String s)
	{
		Dimension mdxDimension = (Dimension)lookupDimension(s);
		if (mdxDimension != null) {
			return mdxDimension;
		}

		//maybe this is not a dimension - maybe it's hierarchy, level or name
		for (int i = 0; i < dimensions.length; i++) {
			OlapElement mdxElement = dimensions[i].lookupChild(schemaReader, s);
			if (mdxElement != null)
				return mdxElement;
		}
		return null;
	}

	public OlapElement lookupDimension(String s)
	{
		for (int i = 0; i < dimensions.length; i++) {
			if (dimensions[i].getName().equalsIgnoreCase(s))
				return dimensions[i];
		}
		return null;
	}

	protected Object[] getAllowedChildren( CubeAccess cubeAccess )
	{
		// cubeAccess sets permissions on hierarchies and members only
		return getChildren();
	}

	// ------------------------------------------------------------------------

	private Level getTimeLevel(int levelType)
	{
		for (int i = 0; i < dimensions.length; i++) {
			DimensionBase dimension = dimensions[i];
			if (dimension.dimensionType == Dimension.TIME) {
				for (int j = 0; j < dimension.hierarchies.length; j++) {
					HierarchyBase hierarchy = dimension.hierarchies[j];
					for (int k = 0; k < hierarchy.levels.length; k++) {
						LevelBase level = hierarchy.levels[k];
						if (level.levelType == levelType) {
							return level;
						}
					}
				}
			}
		}
		return null;
	}
	public Level getYearLevel()
	{
		return getTimeLevel(Level.YEARS);
	}
	public Level getQuarterLevel()
	{
		return getTimeLevel(Level.QUARTERS);
	}
	public Level getMonthLevel()
	{
		return getTimeLevel(Level.MONTHS);
	}
	public Level getWeekLevel()
	{
		return getTimeLevel(Level.WEEKS);
	}
	public void accept(Visitor visitor)
	{
		visitor.visit(this);
	}
	public void childrenAccept(Visitor visitor)
	{
		for (int i = 0; i < dimensions.length; i++) {
			dimensions[i].accept(visitor);
		}
	}
}


// End BaseCube.java
