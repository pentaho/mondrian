/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

import mondrian.resource.MondrianResource;

/**
 * Skeleton implementation of {@link Level}
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 */
public abstract class LevelBase
    extends OlapElementBase
    implements Level
{

    protected final Hierarchy hierarchy;
    protected final String name;
    protected final String uniqueName;
    protected String description;
    protected final int depth;
    protected final LevelType levelType;
    protected MemberFormatter memberFormatter;
    protected int  approxRowCount;

    protected LevelBase(
        Hierarchy hierarchy,
        String name,
        int depth,
        LevelType levelType)
    {
        this.hierarchy = hierarchy;
        this.name = name;
        this.uniqueName = Util.makeFqName(hierarchy, name);
        this.depth = depth;
        this.levelType = levelType;
    }

    /**
     * Sets the approximate number of members in this Level.
     * @see #getApproxRowCount()
     */
    public void setApproxRowCount(int approxRowCount) {
        this.approxRowCount = approxRowCount;
    }

    // from Element
    public String getQualifiedName() {
        return MondrianResource.instance().MdxLevelName.str(getUniqueName());
    }

    public LevelType getLevelType() {
        return levelType;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Hierarchy getHierarchy() {
        return hierarchy;
    }

    public Dimension getDimension() {
        return hierarchy.getDimension();
    }

    public int getDepth() {
        return depth;
    }

    public Level getChildLevel() {
        int childDepth = depth + 1;
        Level[] levels = hierarchy.getLevels();
        return (childDepth < levels.length)
            ? levels[childDepth]
            : null;
    }

    public Level getParentLevel() {
        int parentDepth = depth - 1;
        Level[] levels = hierarchy.getLevels();
        return (parentDepth >= 0)
            ? levels[parentDepth]
            : null;
    }

    public abstract boolean isAll();

    public boolean isMeasure() {
        return hierarchy.getName().equals("Measures");
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, Id.Segment s, MatchType matchType)
    {
        return areMembersUnique()
            ? Util.lookupHierarchyRootMember(
                schemaReader, hierarchy, s, matchType)
            : null;
    }

    /**
      * Returns the object which is used to format members of this level.
      */
    public MemberFormatter getMemberFormatter() {
        return memberFormatter;
    }
}


// End LevelBase.java
