/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2012 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/
package mondrian.olap;

import mondrian.resource.MondrianResource;

import org.olap4j.impl.ArrayNamedListImpl;
import org.olap4j.impl.NamedListImpl;
import org.olap4j.metadata.*;
import org.olap4j.type.LevelType;

import java.util.ArrayList;
import java.util.List;

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
    private final String uniqueName;
    private final String description;

    /**
     * Creates a CubeBase.
     *
     * @param name Name
     * @param caption Caption
     * @param description Description
     */
    protected CubeBase(
        String name,
        boolean visible,
        String caption,
        String description)
    {
        this.name = name;
        this.caption = caption;
        this.visible = visible;
        this.description = description;
        this.uniqueName = Util.quoteMdxIdentifier(name);
    }

    // implement OlapElement
    public String getName() {
        return name;
    }

    public String getUniqueName() {
        // return e.g. '[Sales Ragged]'
        return uniqueName;
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
        return description;
    }

    public Hierarchy lookupHierarchy(Id.Segment s, boolean unique) {
        for (Dimension dimension : getDimensionList()) {
            for (Hierarchy hierarchy : dimension.getHierarchyList()) {
                String name = unique
                    ? hierarchy.getUniqueName() : hierarchy.getName();
                if (name.equals(s.name)) {
                    return hierarchy;
                }
            }
        }
        return null;
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader,
        Id.Segment s,
        MatchType matchType)
    {
        Dimension mdxDimension = lookupDimension(s);
        if (mdxDimension != null) {
            return mdxDimension;
        }

        final List<Dimension> dimensions = schemaReader.getCubeDimensions(this);

        // Look for hierarchies named '[dimension.hierarchy]'.
        if (MondrianProperties.instance().SsasCompatibleNaming.get()
            && s.name.contains("."))
        {
            for (Dimension dimension : dimensions) {
                if (!s.name.startsWith(dimension.getName())) {
                    // Rough check to save time.
                    continue;
                }
                for (Hierarchy hierarchy
                    : schemaReader.getDimensionHierarchies(dimension))
                {
                    if (Util.equalName(
                            s.name,
                            dimension.getName()
                            + "."
                            + hierarchy.getName()))
                    {
                        return hierarchy;
                    }
                }
            }
        }

        // Try hierarchies, levels and members.
        for (Dimension dimension : dimensions) {
            OlapElement mdxElement = dimension.lookupChild(
                schemaReader, s, matchType);
            if (mdxElement != null) {
                if (mdxElement instanceof Member
                    && MondrianProperties.instance().NeedDimensionPrefix.get())
                {
                    // With this property setting, don't allow members to be
                    // referenced without at least a dimension prefix. We
                    // allow [Store].[USA].[CA] or even [Store].[CA] but not
                    // [USA].[CA].
                    continue;
                }
                return mdxElement;
            }
        }
        return null;
    }

    /**
     * Looks up a dimension in this cube based on a component of its name.
     *
     * @param s Name segment
     * @return Dimension, or null if not found
     */
    public Dimension lookupDimension(Id.Segment s) {
        for (Dimension dimension : getDimensionList()) {
            if (Util.equalName(dimension.getName(), s.name)) {
                return dimension;
            }
        }
        return null;
    }

    // ------------------------------------------------------------------------

    public Level getTimeLevel(org.olap4j.metadata.Level.Type levelType) {
        for (Dimension dimension : getDimensionList()) {
            if (dimension.getDimensionType() == DimensionType.TimeDimension) {
                for (Hierarchy hierarchy : dimension.getHierarchyList()) {
                    for (Level level : hierarchy.getLevelList()) {
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
        return getTimeLevel(org.olap4j.metadata.Level.Type.TIME_YEARS);
    }

    public Level getQuarterLevel() {
        return getTimeLevel(org.olap4j.metadata.Level.Type.TIME_QUARTERS);
    }

    public Level getMonthLevel() {
        return getTimeLevel(org.olap4j.metadata.Level.Type.TIME_MONTHS);
    }

    public Level getWeekLevel() {
        return getTimeLevel(org.olap4j.metadata.Level.Type.TIME_WEEKS);
    }
}

// End CubeBase.java
