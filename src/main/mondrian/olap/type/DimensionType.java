/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;

/**
 * The type of an expression which represents a Dimension.
 *
 * @author jhyde
 * @since Feb 17, 2005
 * @version $Id$
 */
public class DimensionType implements Type {
    private final Dimension dimension;

    public static final DimensionType Unknown = new DimensionType(null);

    /**
     * Creates a type representing a dimension.
     *
     * @param dimension Dimension that values of this type must belong to.
     *   Null if the dimension is unknown.
     */
    public DimensionType(Dimension dimension) {
        this.dimension = dimension;
    }

    public static DimensionType forDimension(Dimension dimension) {
        return new DimensionType(dimension);
    }

    public static DimensionType forType(Type type) {
        return new DimensionType(type.getDimension());
    }

    public boolean usesDimension(Dimension dimension, boolean maybe) {
        return this.dimension == dimension ||
                (maybe && this.dimension == null);
    }

    public Hierarchy getHierarchy() {
        return dimension == null ?
                null :
                dimension.getHierarchies().length > 1 ?
                null :
                dimension.getHierarchies()[0];
    }

    public Level getLevel() {
        return null;
    }

    public Dimension getDimension() {
        return dimension;
    }

}

// End DimensionType.java
