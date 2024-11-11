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


package mondrian.gui.validate.impl;

import mondrian.gui.validate.TreeModelPath;

import javax.swing.tree.TreePath;

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
