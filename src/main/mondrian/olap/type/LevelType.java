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

import mondrian.olap.Level;
import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Util;

/**
 * The type of an expression which represents a level.
 *
 * @author jhyde
 * @since Feb 17, 2005
 * @version $Id$
 */
public class LevelType implements Type {
    private final Dimension dimension;
    private final Hierarchy hierarchy;
    private final Level level;
    private final String digest;

    /**
     * Creates a type representing a level.
     *
     * @param dimension Dimension which values of this type must belong to, or
     *   null if not known
     * @param hierarchy Hierarchy which values of this type must belong to, or
     *   null if not known
     * @param level Level which values of this type must belong to, or null if
     */
    public LevelType(Dimension dimension, Hierarchy hierarchy, Level level) {
        this.dimension = dimension;
        this.hierarchy = hierarchy;
        this.level = level;
        if (level != null) {
            Util.assertPrecondition(hierarchy != null, "hierarchy != null");
            Util.assertPrecondition(level.getHierarchy() == hierarchy,
                    "level.getHierarchy() == hierarchy");
        }
        if (hierarchy != null) {
            Util.assertPrecondition(dimension != null, "dimension != null");
            Util.assertPrecondition(hierarchy.getDimension() == dimension,
                    "hierarchy.getDimension() == dimension");
        }
        StringBuilder buf = new StringBuilder("LevelType<");
        if (level != null) {
            buf.append("level=").append(level.getUniqueName());
        } else if (hierarchy != null) {
            buf.append("hierarchy=").append(hierarchy.getUniqueName());
        } else if (dimension != null) {
            buf.append("dimension=").append(dimension.getUniqueName());
        }
        buf.append(">");
        this.digest = buf.toString();
    }

    public static LevelType forType(Type type) {
        return new LevelType(
                type.getDimension(),
                type.getHierarchy(),
                type.getLevel());

    }

    public static LevelType forLevel(Level level) {
        return new LevelType(
                level.getDimension(),
                level.getHierarchy(),
                level);
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
        return level;
    }

    public String toString() {
        return digest;
    }
}

// End LevelType.java
