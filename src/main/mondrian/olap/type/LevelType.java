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
    private final Hierarchy hierarchy;
    private final Level level;

    /**
     * Creates a type representing a level.
     *
     * @param hierarchy Hierarchy which values of this type must belong to, or
     *   null if not known
     * @param level Level which values of this type must belong to, or null if
     *   not known
     */
    public LevelType(Hierarchy hierarchy, Level level) {
        this.hierarchy = hierarchy;
        this.level = level;
        if (level != null) {
            Util.assertPrecondition(hierarchy != null, "hierarchy != null");
            Util.assertPrecondition(level.getHierarchy() == hierarchy,
                    "level.getHierarchy() == hierarchy");
        }
    }

    public boolean usesDimension(Dimension dimension) {
        return hierarchy != null && hierarchy.getDimension() == dimension;
    }

    public Hierarchy getHierarchy() {
        return hierarchy;
    }

    public Level getLevel() {
        return level;
    }

}

// End LevelType.java
