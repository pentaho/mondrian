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
    private final Hierarchy hierarchy;

    /**
     * Creates a type representing a hierarchy.
     */
    public HierarchyType(Hierarchy hierarchy) {
        this.hierarchy = hierarchy;
    }

    public boolean usesDimension(Dimension dimension) {
        return hierarchy.getDimension() == dimension;
    }

    public Hierarchy getHierarchy() {
        return hierarchy;
    }

    public Level getLevel() {
        return null;
    }
}

// End HierarchyType.java
