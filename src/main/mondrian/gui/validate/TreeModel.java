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
