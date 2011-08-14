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

import java.util.ArrayList;
import java.util.List;

/**
 * Skeleton implementation for {@link Hierarchy}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 */
public abstract class HierarchyBase
    extends OlapElementBase
    implements Hierarchy
{

    protected final Dimension dimension;
    /**
     * <code>name</code> and <code>subName</code> are the name of the
     * hierarchy, respectively containing and not containing dimension
     * name. For example:
     * <table>
     * <tr> <th>uniqueName</th>    <th>name</th>        <th>subName</th></tr>
     * <tr> <td>[Time.Weekly]</td> <td>Time.Weekly</td> <td>Weekly</td></tr>
     * <tr> <td>[Customers]</td>   <td>Customers</td>   <td>null</td></tr>
     * </table>
     *
     * <p>If {@link mondrian.olap.MondrianProperties#SsasCompatibleNaming} is
     * true, name and subName have the same value.
     */
    protected final String subName;
    protected final String name;
    protected final String uniqueName;
    protected String description;
    protected final List<Level> levelList = new ArrayList<Level>();
    protected final boolean hasAll;
    protected String allMemberName;

    protected HierarchyBase(
        Dimension dimension,
        String subName,
        boolean visible,
        String caption,
        String description,
        boolean hasAll)
    {
        this.dimension = dimension;
        this.hasAll = hasAll;
        if (caption != null && !caption.equals("")) {
            this.caption = caption;
        } else if (subName == null) {
            this.caption = dimension.getCaption();
        } else {
            this.caption = subName;
        }
        this.description = description;
        this.visible = visible;

        String name = dimension.getName();
        if (true || MondrianProperties.instance().SsasCompatibleNaming.get()) {
            if (subName == null) {
                // e.g. "Time"
                subName = name;
            }
            this.subName = subName;
            this.name = subName;
            // e.g. "[Time].[Weekly]" for dimension "Time", hierarchy "Weekly";
            // "[Time]" for dimension "Time", hierarchy "Time".
            this.uniqueName =
                subName.equals(name)
                    ? dimension.getUniqueName()
                    : Util.makeFqName(dimension, this.name);
        } else {
            this.subName = subName;
            if (this.subName != null) {
                // e.g. "Time.Weekly"
                this.name = name + "." + subName;
                if (this.subName.equals(name)) {
                    this.uniqueName = dimension.getUniqueName();
                } else {
                    // e.g. "[Time.Weekly]"
                    this.uniqueName = Util.makeFqName(this.name);
                }
            } else {
                // e.g. "Time"
                this.name = name;
                // e.g. "[Time]"
                this.uniqueName = dimension.getUniqueName();
            }
        }
    }

    /**
     * Returns the name of the hierarchy sans dimension name.
     *
     * @return name of hierarchy sans dimension name
     */
    public String getSubName() {
        return subName;
    }

    // implement MdxElement
    public String getUniqueName() {
        return uniqueName;
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
        return description;
    }

    public Dimension getDimension() {
        return dimension;
    }

    @Deprecated
    public Level[] getLevels() {
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
        OlapElement oe = Util.lookupHierarchyLevel(this, s.name);
        if (oe == null) {
            oe = Util.lookupHierarchyRootMember(
                schemaReader, this, s, matchType);
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

    public String getAllMemberName() {
        return allMemberName;
    }

}

// End HierarchyBase.java
