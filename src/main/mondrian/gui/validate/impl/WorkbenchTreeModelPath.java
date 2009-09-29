/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2009 Pentaho
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.gui.validate.impl;

import javax.swing.tree.TreePath;

import mondrian.gui.validate.TreeModelPath;

/**
 * Implementation of <code>TreeModelPath</code> for Workbench.
 *
 * @author mlowery
 */
public class WorkbenchTreeModelPath implements TreeModelPath {

    TreePath treePath;

    public WorkbenchTreeModelPath(TreePath treePath) {
        super();
        this.treePath = treePath;
    }

    public Object getPathComponent(int element) {
        return treePath.getPathComponent(element);
    }

    public int getPathCount() {
        return treePath.getPathCount();
    }

    public boolean isEmpty() {
        return treePath == null || treePath.getPathCount() == 0;
    }

}

// End WorkbenchTreeModelPath.java
