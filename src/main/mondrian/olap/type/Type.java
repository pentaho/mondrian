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
 * Type of an MDX expression.
 *
 * @author jhyde
 * @since Feb 17, 2005
 * @version $Id$
 */
public interface Type {
    /**
     * Returns whether this type contains a given dimension.
     */
    boolean usesDimension(Dimension dimension);
    /**
     * Returns the hierarchy of this type. If not applicable, throws.
     */
    Hierarchy getHierarchy();

    /**
     * Returns the level of this type, or null if not known.
     */
    Level getLevel();
}

// End Type.java
