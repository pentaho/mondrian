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
 * Skeleton implementation for {@link Hierarchy}.
 *
 * @author jhyde
 * @since 6 August, 2001
 */
public abstract class HierarchyBase
    extends OlapElementBase
    implements Hierarchy
{
    protected final Dimension dimension;
    protected final String name;
    protected final String uniqueName;
    protected final boolean hasAll;

    protected HierarchyBase(
        Dimension dimension,
        String subName,
        String uniqueName,
        boolean visible,
        boolean hasAll)
    {
        this.dimension = dimension;
        this.hasAll = hasAll;
        this.visible = visible;

        assert subName != null;
        this.name = subName;
        this.uniqueName = uniqueName;
    }

    // implement MdxElement
    public String getUniqueName() {
        return uniqueName;
    }

    public String getUniqueNameSsas() {
        return Util.makeFqName(dimension, name);
    }

    public String getName() {
        return name;
    }

    public String getQualifiedName() {
        return MondrianResource.instance().MdxHierarchyName.str(
            getUniqueName());
    }

    public abstract boolean isRagged();

    public String getDescription() {
        return Larders.getDescription(getLarder());
    }

    public Dimension getDimension() {
        return dimension;
    }

    @Deprecated
    public Level[] getLevels() {
        final List<? extends Level> levelList = getLevelList();
        return levelList.toArray(new Level[levelList.size()]);
    }

    public Hierarchy getHierarchy() {
        return this;
    }

    public final boolean hasAll() {
        return hasAll;
    }

    public boolean equals(OlapElement mdxElement) {
        // Use object identity, because a private hierarchy can have the same
        // name as a public hierarchy.
        return (this == mdxElement);
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader,
        Id.Segment s,
        MatchType matchType)
    {
        OlapElement oe;
        if (s instanceof Id.NameSegment) {
            Id.NameSegment nameSegment = (Id.NameSegment) s;
            oe = Util.lookupHierarchyLevel(this, nameSegment.getName());
            if (oe == null) {
                oe = Util.lookupHierarchyRootMember(
                    schemaReader, this, nameSegment, matchType);
            }
        } else {
            // Key segment searches bottom level by default. For example,
            // [Products].&[1] is shorthand for [Products].[Product Name].&[1].
            final Id.KeySegment keySegment = (Id.KeySegment) s;
            final List<? extends Level> levelList = getLevelList();
            oe = Util.last(levelList)
                .lookupChild(schemaReader, keySegment, matchType);
        }

        if (getLogger().isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(64);
            buf.append("HierarchyBase.lookupChild: ");
            buf.append("name=");
            buf.append(getName());
            buf.append(", childname=");
            buf.append(s);
            if (oe == null) {
                buf.append(" returning null");
            } else {
                buf.append(" returning elementname=").append(oe.getName());
            }
            getLogger().debug(buf.toString());
        }
        return oe;
    }
}

// End HierarchyBase.java
