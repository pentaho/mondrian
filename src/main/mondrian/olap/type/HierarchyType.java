/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

import mondrian.olap.Hierarchy;
import mondrian.olap.Dimension;
import mondrian.olap.Level;

/**
 * The type of an expression which represents a hierarchy.
 *
 * @author jhyde
 * @since Feb 17, 2005
 * @version $Id$
 */
public class HierarchyType implements Type {
    private final Dimension dimension;
    private final Hierarchy hierarchy;
    private final String digest;

    /**
     * Creates a type representing a hierarchy.
     *
     * @param dimension Dimension that values of this type must belong to, or
     *   null if the dimension is unknown
     * @param hierarchy Hierarchy that values of this type must belong to,
     *   null if the hierarchy is unknown
     */
    public HierarchyType(Dimension dimension, Hierarchy hierarchy) {
        this.dimension = dimension;
        this.hierarchy = hierarchy;
        StringBuilder buf = new StringBuilder("HierarchyType<");
        if (hierarchy != null) {
            buf.append("hierarchy=").append(hierarchy.getUniqueName());
        } else if (dimension != null) {
            buf.append("dimension=").append(dimension.getUniqueName());
        }
        buf.append(">");
        this.digest = buf.toString();
    }

    public static HierarchyType forHierarchy(Hierarchy hierarchy) {
        return new HierarchyType(hierarchy.getDimension(), hierarchy);
    }

    public static HierarchyType forType(Type type) {
        return new HierarchyType(type.getDimension(), type.getHierarchy());
    }

    public boolean usesDimension(Dimension dimension, boolean definitely) {
        return this.dimension == dimension ||
            (!definitely && this.dimension == null);
    }

    public Dimension getDimension() {
        return dimension;
    }

    public Hierarchy getHierarchy() {
        return hierarchy;
    }

    public Level getLevel() {
        return null;
    }

    public String toString() {
        return digest;
    }
}

// End HierarchyType.java
