/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.gui.validate;

/**
 * A generalization of a <code>javax.swing.tree.TreeModel</code>.
 *
 * @author mlowery
 */
public interface TreeModel {
    /**
     * Returns the number of children of <code>parent</code>.
     */
    int getChildCount(Object parent);

    /**
     * Returns the child at <code>index</code>.
     */
    Object getChild(Object parent, int index);

    /**
     * Returns the root object of this tree model.
     */
    Object getRoot();
}

// End TreeModel.java
