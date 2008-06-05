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
