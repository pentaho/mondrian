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
package mondrian.olap;

import mondrian.resource.MondrianResource;

import java.util.List;

/**
 * Skeleton implementation of {@link Level}.
 *
 * @author jhyde
 * @since 6 August, 2001
 */
public abstract class LevelBase
    extends OlapElementBase
    implements Level
{
    public final Hierarchy hierarchy;
    protected final String name;
    protected final String uniqueName;
    protected final int depth;
    protected int approxRowCount;

    protected LevelBase(
        Hierarchy hierarchy,
        String name,
        boolean visible,
        int depth)
    {
        assert hierarchy != null;
        assert name != null;
        assert depth >= 0;
        this.hierarchy = hierarchy;
        this.name = name;
        this.visible = visible;
        this.uniqueName = Util.makeFqName(hierarchy, name);
        this.depth = depth;
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

    public String getUniqueName() {
        return uniqueName;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return Larders.getDescription(getLarder());
    }

    public int getDepth() {
        return depth;
    }

    public Level getChildLevel() {
        int childDepth = depth + 1;
        List<? extends Level> levels = hierarchy.getLevelList();
        return (childDepth < levels.size())
            ? levels.get(childDepth)
            : null;
    }

    public Level getParentLevel() {
        int parentDepth = depth - 1;
        List<? extends Level> levels = hierarchy.getLevelList();
        return (parentDepth >= 0)
            ? levels.get(parentDepth)
            : null;
    }
}

// End LevelBase.java
