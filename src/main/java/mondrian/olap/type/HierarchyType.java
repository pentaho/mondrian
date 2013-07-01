/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.type;

import mondrian.olap.*;

/**
 * The type of an expression which represents a hierarchy.
 *
 * @author jhyde
 * @since Feb 17, 2005
 */
public class HierarchyType implements Type {
    private final Dimension dimension;
    private final Hierarchy hierarchy;
    private final String digest;

    public static final HierarchyType Unknown = new HierarchyType(null, null);

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
        return this.dimension == dimension
            || (!definitely && this.dimension == null);
    }

    public boolean usesHierarchy(Hierarchy hierarchy, boolean definitely) {
        return this.hierarchy == hierarchy
            || (!definitely
                && this.hierarchy == null
                && (this.dimension == null
                    || this.dimension == hierarchy.getDimension()));
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

    public int hashCode() {
        return digest.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj instanceof HierarchyType) {
            HierarchyType that = (HierarchyType) obj;
            return Util.equals(this.hierarchy, that.hierarchy)
                && Util.equals(this.dimension, that.dimension);
        }
        return false;
    }

    public Type computeCommonType(Type type, int[] conversionCount) {
        if (!(type instanceof HierarchyType)) {
            return null;
        }
        HierarchyType that = (HierarchyType) type;
        if (this.getHierarchy() != null
            && this.getHierarchy().equals(that.getHierarchy()))
        {
            return this;
        }
        if (this.getDimension() != null
            && this.getDimension().equals(that.getDimension()))
        {
            return new HierarchyType(
                this.getDimension(),
                null);
        }
        return HierarchyType.Unknown;
    }

    public boolean isInstance(Object value) {
        return value instanceof Hierarchy
            && (hierarchy == null
                || value.equals(hierarchy))
            && (dimension == null
                || ((Hierarchy) value).getDimension().equals(dimension));
    }

    public int getArity() {
        return 1;
    }
}

// End HierarchyType.java
