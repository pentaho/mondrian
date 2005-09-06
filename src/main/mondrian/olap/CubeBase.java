/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

import mondrian.olap.type.Type;
import mondrian.olap.type.CubeType;

/**
 * <code>CubeBase</code> is an abstract implementation of {@link Cube}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public abstract class CubeBase extends OlapElementBase implements Cube {

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

    protected final String name;
    protected Dimension[] dimensions;

    protected CubeBase(String name, Dimension[] dimensions) {
        this.name = name;
        this.dimensions = dimensions;
    }

    // implement OlapElement
    public String getName() {
        return name;
    }

    public String getUniqueName() {
        return name;
    }

    public String getQualifiedName() {
        return Util.getRes().getMdxCubeName(getName());
    }

    public Dimension getDimension() {
        return null;
    }

    public Hierarchy getHierarchy() {
        return null;
    }

    public String getDescription() {
        return null;
    }

    public Cube getCube() {
        return this;
    }

    public int getCategory() {
        return Category.Cube;
    }

    public Type getTypeX() {
        return new CubeType(this);
    }

    public Dimension[] getDimensions() {
        return dimensions;
    }

    public Object[] getChildren() {
        return dimensions;
    }

    public Hierarchy lookupHierarchy(String s, boolean unique) {
        for (int i = 0; i < dimensions.length; i++) {
            Dimension dimension = dimensions[i];
            Hierarchy[] hierarchies = dimension.getHierarchies();
            for (int j = 0; j < hierarchies.length; j++) {
                Hierarchy hierarchy = hierarchies[j];
                String name = unique
                    ? hierarchy.getUniqueName() : hierarchy.getName();
                if (name.equals(s)) {
                    return hierarchy;
                }
            }
        }
        return null;
    }

    public OlapElement lookupChild(SchemaReader schemaReader, String s) {
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

    public Dimension getTimeDimension() {
        for (int i = 0; i < dimensions.length; i++) {
            if (dimensions[i].getDimensionType() ==
                    DimensionType.TimeDimension) {
                return dimensions[i];
            }
        }

        return null;
    }

    public OlapElement lookupDimension(String s) {
        for (int i = 0; i < dimensions.length; i++) {
            if (dimensions[i].getName().equalsIgnoreCase(s)) {
                return dimensions[i];
            }
        }
        return null;
    }

    protected Object[] getAllowedChildren(CubeAccess cubeAccess) {
        // cubeAccess sets permissions on hierarchies and members only
        return dimensions;
    }

    // ------------------------------------------------------------------------

    private Level getTimeLevel(LevelType levelType) {
        for (int i = 0; i < dimensions.length; i++) {
            Dimension dimension = dimensions[i];
            if (dimension.getDimensionType() == DimensionType.TimeDimension) {
                Hierarchy[] hierarchies = dimension.getHierarchies();
                for (int j = 0; j < hierarchies.length; j++) {
                    Hierarchy hierarchy = hierarchies[j];
                    Level[] levels = hierarchy.getLevels();
                    for (int k = 0; k < levels.length; k++) {
                        Level level = levels[k];
                        if (level.getLevelType() == levelType) {
                            return level;
                        }
                    }
                }
            }
        }
        return null;
    }

    public Level getYearLevel() {
        return getTimeLevel(LevelType.TimeYears);
    }

    public Level getQuarterLevel() {
        return getTimeLevel(LevelType.TimeQuarters);
    }

    public Level getMonthLevel() {
        return getTimeLevel(LevelType.TimeMonths);
    }

    public Level getWeekLevel() {
        return getTimeLevel(LevelType.TimeWeeks);
    }

    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    public void childrenAccept(Visitor visitor) {
        for (int i = 0; i < dimensions.length; i++) {
            dimensions[i].accept(visitor);
        }
    }

    public boolean dependsOn(Dimension dimension) {
        throw new UnsupportedOperationException();
    }
}


// End BaseCube.java
