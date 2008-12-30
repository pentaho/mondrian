/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

import mondrian.resource.MondrianResource;

/**
 * <code>CubeBase</code> is an abstract implementation of {@link Cube}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 */
public abstract class CubeBase extends OlapElementBase implements Cube {

    /** constraints indexes for adSchemaMembers
     *
     * http://msdn.microsoft.com/library/psdk/dasdk/mdx8h4k.htm
     * check "Restrictions in the MEMBER Rowset" under MEMBER Rowset section
     */
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
        // return e.g. '[Sales Ragged]'
        return Util.quoteMdxIdentifier(name);
    }

    public String getQualifiedName() {
        return MondrianResource.instance().MdxCubeName.str(getName());
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

    public Dimension[] getDimensions() {
        return dimensions;
    }

    public Hierarchy lookupHierarchy(Id.Segment s, boolean unique) {
        for (Dimension dimension : dimensions) {
            Hierarchy[] hierarchies = dimension.getHierarchies();
            for (Hierarchy hierarchy : hierarchies) {
                String name = unique
                    ? hierarchy.getUniqueName() : hierarchy.getName();
                if (name.equals(s.name)) {
                    return hierarchy;
                }
            }
        }
        return null;
    }

    public OlapElement lookupChild(SchemaReader schemaReader, Id.Segment s)
    {
        return lookupChild(schemaReader, s, MatchType.EXACT);
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, Id.Segment s, MatchType matchType)
    {
        Dimension mdxDimension = (Dimension)lookupDimension(s);
        if (mdxDimension != null ||
            MondrianProperties.instance().NeedDimensionPrefix.get()) {
            return mdxDimension;
        }

        //maybe this is not a dimension - maybe it's hierarchy, level or name
        for (Dimension dimension : dimensions) {
            OlapElement mdxElement = dimension.lookupChild(
                schemaReader, s, matchType);
            if (mdxElement != null) {
                return mdxElement;
            }
        }
        return null;
    }

    public Dimension getTimeDimension() {
        for (Dimension dimension : dimensions) {
            if (dimension.getDimensionType() ==
                DimensionType.TimeDimension) {
                return dimension;
            }
        }

        return null;
    }

    public OlapElement lookupDimension(Id.Segment s) {
        for (Dimension dimension : dimensions) {
            if (dimension.getName().equalsIgnoreCase(s.name)) {
                return dimension;
            }
        }
        return null;
    }

    // ------------------------------------------------------------------------

    private Level getTimeLevel(LevelType levelType) {
        for (Dimension dimension : dimensions) {
            if (dimension.getDimensionType() == DimensionType.TimeDimension) {
                Hierarchy[] hierarchies = dimension.getHierarchies();
                for (Hierarchy hierarchy : hierarchies) {
                    Level[] levels = hierarchy.getLevels();
                    for (Level level : levels) {
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

}


// End CubeBase.java
