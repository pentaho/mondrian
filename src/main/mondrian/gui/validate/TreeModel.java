/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2009 Pentaho
// All Rights Reserved.
*/
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
