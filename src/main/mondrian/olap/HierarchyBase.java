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

import mondrian.resource.MondrianResource;

/**
 * Skeleton implementation for {@link Hierarchy}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public abstract class HierarchyBase
    extends OlapElementBase
    implements Hierarchy {

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
     **/
    protected final String subName;
    protected final String name;
    protected final String uniqueName;
    protected String description;
    protected Level[] levels;
    protected final boolean hasAll;
    protected String allMemberName;
    protected String allLevelName;

    protected HierarchyBase(Dimension dimension,
                            String subName,
                            boolean hasAll) {
        this.dimension = dimension;
        this.hasAll = hasAll;
        setCaption(dimension.getCaption());

        this.subName = subName;
        String name = dimension.getName();
        if (this.subName != null) {
            // e.g. "Time.Weekly"
            this.name = name + "." + subName;
            // e.g. "[Time.Weekly]"
            this.uniqueName = Util.makeFqName(this.name);
        } else {
            // e.g. "Time"
            this.name = name;
            // e.g. "[Time]"
            this.uniqueName = dimension.getUniqueName();
        }
    }

    // implement MdxElement
    public String getUniqueName() {
        return uniqueName;
    }

    public String getName() {
        return name;
    }

    public String getQualifiedName() {
        return MondrianResource.instance().MdxHierarchyName.str(getUniqueName());
    }

    public String getDescription() {
        return description;
    }

    public Dimension getDimension() {
        return dimension;
    }

    public Level[] getLevels() {
        return levels;
    }

    public Hierarchy getHierarchy() {
        return this;
    }

    public boolean hasAll() {
        return hasAll;
    }

    public boolean equals(OlapElement mdxElement) {
        // Use object identity, because a private hierarchy can have the same
        // name as a public hierarchy.
        return (this == mdxElement);
    }

    public OlapElement lookupChild(SchemaReader schemaReader, String s) {
        OlapElement oe = Util.lookupHierarchyLevel(this, s);
        if (oe == null) {
            oe = Util.lookupHierarchyRootMember(schemaReader, this, s);
        }
        if (getLogger().isDebugEnabled()) {
            StringBuffer buf = new StringBuffer(64);
            buf.append("HierarchyBase.lookupChild: ");
            buf.append("name=");
            buf.append(getName());
            buf.append(", childname=");
            buf.append(s);
            if (oe == null) {
                buf.append(" returning null");
            } else {
                buf.append(" returning elementname="+oe.getName());
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
