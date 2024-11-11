/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.gui.validate;

/**
 * A generalization of <code>javax.swing.tree.TreePath</code>.
 *
 * @author mlowery
 */
public interface TreeModelPath {
    /**
     * Returns the length of this path.
     */
    int getPathCount();

    /**
     * Returns the component of the path at the given index.
     */
    Object getPathComponent(int element);

    /**
     * Returns true if path has no components.
     */
    boolean isEmpty();
}

// End TreeModelPath.java
