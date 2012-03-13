/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2009 Pentaho
// Copyright (C) 2006-2007 Cincom Systems, Inc.
// Copyright (C) 2006-2007 JasperSoft
// All Rights Reserved.
*/
package mondrian.gui;

import java.util.HashSet;
import java.util.Set;
import javax.swing.*;
import javax.swing.event.*;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

/**
 * Helper to enable update the tree and keep expanded nodes expanded after
 * reloading the tree.
 *
 * @author erik
 */
public class JTreeUpdater
    implements TreeExpansionListener, TreeSelectionListener
{
    private JTree tree = null;
    private Set<TreePath> expandedTreePaths = new HashSet<TreePath>();
    private TreePath[] selectedTreePaths = new TreePath[0];

    /**
     * Constructor
     *
     * @param tree The tree to track
     */
    public JTreeUpdater(JTree tree) {
        this.tree = tree;
        this.tree.addTreeExpansionListener(this);
        this.tree.addTreeSelectionListener(this);
    }

    /**
     * Call this method whenever you update the tree and needs it reloaded
     */
    public synchronized void update() {
        synchronized (this.tree) {
            this.tree.removeTreeExpansionListener(this);
            this.tree.removeTreeSelectionListener(this);

            ((DefaultTreeModel) this.tree.getModel()).reload();
            for (TreePath treePath : expandedTreePaths) {
                this.tree.expandPath(treePath);
            }
            this.tree.getSelectionModel().setSelectionPaths(selectedTreePaths);
            this.tree.addTreeExpansionListener(this);
            this.tree.addTreeSelectionListener(this);
        }
    }

    public void treeExpanded(TreeExpansionEvent treeExpansionEvent) {
        TreePath expandedPath = treeExpansionEvent.getPath();

        // remove all ancestors of eventpath from expandedpaths set.
        Object[] paths = expandedTreePaths.toArray();
        for (int i = 0; i < paths.length; i++) {
            TreePath path = (TreePath) paths[i];

            // Path is a descendant of event path if path contains all
            // components that make eventpath. For example, if eventpath = [a,b]
            // path=[a,b,c] then path is descendant of eventpath.
            if (path.isDescendant(expandedPath)) {
                expandedTreePaths.remove(path);
            }
        }
        expandedTreePaths.add(expandedPath);
    }

    public void treeCollapsed(TreeExpansionEvent treeExpansionEvent) {
        TreePath collapsedPath = treeExpansionEvent.getPath();
        expandedTreePaths.remove(collapsedPath);

        // remove all descendants from expandedpaths set.
        Object[] paths = expandedTreePaths.toArray();
        for (int i = 0; i < paths.length; i++) {
            TreePath path = (TreePath) paths[i];

            // Path is a descendant of event path if path contains all
            // components that make eventpath. For example, if eventpath = [a,b]
            // path=[a,b,c] then path is descendant of eventpath.
            if (collapsedPath.isDescendant(path)) {
                expandedTreePaths.remove(path);
            }
        }
    }

    public void valueChanged(TreeSelectionEvent treeSelectionEvent) {
        if (this.tree.getSelectionPaths() != null
            && this.tree.getSelectionPaths().length > 0)
        {
            selectedTreePaths =
                this.tree.getSelectionModel().getSelectionPaths();
        }
    }
}

// End JTreeUpdater.java
