package mondrian.gui;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.swing.JTree;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeExpansionListener;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;
/**
 * @author erik
 *
 */
public class JTreeUpdater
	implements TreeExpansionListener, TreeSelectionListener {

	private JTree tree = null;
	private Set expandedTreePaths = new HashSet();
	private TreePath[] selectedTreePaths = new TreePath[0];
	private boolean nodeWasSelected = false;

	public JTreeUpdater(JTree p_tree) {
		tree = p_tree;
		tree.addTreeExpansionListener(this);
		tree.addTreeSelectionListener(this);
	}

	/**
	 * updates the tree
	 */
	public synchronized void update() {
		tree.removeTreeExpansionListener(this);
		tree.removeTreeSelectionListener(this);

		((DefaultTreeModel) tree.getModel()).reload();
		Iterator keys = expandedTreePaths.iterator();
		while (keys.hasNext()) {
			TreePath l_path = (TreePath) keys.next();
			tree.expandPath(l_path);
		}
		tree.getSelectionModel().setSelectionPaths(selectedTreePaths);
		tree.addTreeExpansionListener(this);
		tree.addTreeSelectionListener(this);
	}

	public void treeExpanded(TreeExpansionEvent treeExpansionEvent) {
		expandedTreePaths.add(treeExpansionEvent.getPath());
	}

	public void treeCollapsed(TreeExpansionEvent treeExpansionEvent) {
		expandedTreePaths.remove(treeExpansionEvent.getPath());
	}

	public void valueChanged(TreeSelectionEvent treeSelectionEvent) {
		if (tree.getSelectionPaths() != null
			&& tree.getSelectionPaths().length > 0) {
			selectedTreePaths = tree.getSelectionModel().getSelectionPaths();
		}
	}
}
