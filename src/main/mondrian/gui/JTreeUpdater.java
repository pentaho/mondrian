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
 * Helper to enable update the tree and keep expanded nodes expanded after reloading the tree
 *  
 * @author erik
 *
 */
public class JTreeUpdater implements TreeExpansionListener, TreeSelectionListener
{

	private JTree tree = null;
	private Set expandedTreePaths = new HashSet();
	private TreePath[] selectedTreePaths = new TreePath[0];

	/**
	 * Constructor
	 * 
	 * @param tree The tree to track 
	 */
	public JTreeUpdater(JTree tree)
	{
		this.tree = tree;
		this.tree.addTreeExpansionListener(this);
		this.tree.addTreeSelectionListener(this);
	}

	/**
	 * Call this method whenever you update the tree and needs it reloaded
	 */
	public synchronized void update()
	{
		synchronized(this.tree)
		{
			this.tree.removeTreeExpansionListener(this);
			this.tree.removeTreeSelectionListener(this);
	
			((DefaultTreeModel) this.tree.getModel()).reload();
			Iterator keys = expandedTreePaths.iterator();
			while (keys.hasNext())
			{
				TreePath path = (TreePath) keys.next();
				this.tree.expandPath(path);
			}
			this.tree.getSelectionModel().setSelectionPaths(selectedTreePaths);
			this.tree.addTreeExpansionListener(this);
			this.tree.addTreeSelectionListener(this);
		}
	}

	public void treeExpanded(TreeExpansionEvent treeExpansionEvent)
	{
		expandedTreePaths.add(treeExpansionEvent.getPath());
	}

	public void treeCollapsed(TreeExpansionEvent treeExpansionEvent)
	{
		expandedTreePaths.remove(treeExpansionEvent.getPath());
	}

	public void valueChanged(TreeSelectionEvent treeSelectionEvent)
	{
		if (this.tree.getSelectionPaths() != null && this.tree.getSelectionPaths().length > 0)
		{
			selectedTreePaths = this.tree.getSelectionModel().getSelectionPaths();
		}
	}
}
