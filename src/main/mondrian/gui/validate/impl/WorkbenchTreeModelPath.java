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
