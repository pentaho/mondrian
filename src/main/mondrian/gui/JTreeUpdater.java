/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde, Cincom Systems, Inc.
// Copyright (C) 2006-2007 Cincom Systems, Inc.
// Copyright (C) 2006-2007 JasperSoft
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.gui;

import javax.swing.*;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeExpansionListener;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Helper to enable update the tree and keep expanded nodes expanded after
 * reloading the tree.
 *
 * @author erik
 * @version $Id$
 */
public class JTreeUpdater implements TreeExpansionListener, TreeSelectionListener {

    private JTree tree = null;
    private Set expandedTreePaths = new HashSet();
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
            Iterator keys = expandedTreePaths.iterator();
            while (keys.hasNext()) {
                TreePath path = (TreePath) keys.next();
                this.tree.expandPath(path);
            }
            this.tree.getSelectionModel().setSelectionPaths(selectedTreePaths);
            this.tree.addTreeExpansionListener(this);
            this.tree.addTreeSelectionListener(this);
        }
    }

    public void treeExpanded(TreeExpansionEvent treeExpansionEvent) {
        TreePath expandedPath = treeExpansionEvent.getPath();

        //System.out.println("expended ="+expandedTreePaths.size());

        // remove all ancestors of eventpath from expandedpaths set.
        Object[] paths = expandedTreePaths.toArray();
        for (int i = 0; i < paths.length; i++) {
            TreePath path = (TreePath) paths[i];

            // path is a descendant of event path if path contains all componennts that make eventpath
            // eventpath = [a,b]  path=[a,b,c] then path is descendant of eventpath
            if (path.isDescendant(expandedPath))   {
                expandedTreePaths.remove(path);
            }
        }
        //System.out.println("ancestor expended ="+expandedTreePaths.size());
        expandedTreePaths.add(expandedPath);

        //System.out.println("added expended ="+expandedTreePaths.size());
    }

    public void treeCollapsed(TreeExpansionEvent treeExpansionEvent) {
        TreePath collapsedPath = treeExpansionEvent.getPath();
        expandedTreePaths.remove(collapsedPath);
        //System.out.println("collapsed ="+expandedTreePaths.size());

        // remove all descendants from expandedpaths set.
        Object[] paths = expandedTreePaths.toArray();
        for (int i = 0; i < paths.length; i++) {
            TreePath path = (TreePath) paths[i];

            // path is a descendant of event path if path contains all componennts that make eventpath
            // eventpath = [a,b]  path=[a,b,c] then path is descendant of eventpath
            if (collapsedPath.isDescendant(path))   {
                expandedTreePaths.remove(path);
            }
        }
        //System.out.println("subtree collapsed ="+expandedTreePaths.size());

    }

    public void valueChanged(TreeSelectionEvent treeSelectionEvent) {
        if (this.tree.getSelectionPaths() != null && this.tree.getSelectionPaths().length > 0) {
            selectedTreePaths = this.tree.getSelectionModel().getSelectionPaths();
        }
    }
}

// End JTreeUpdater.java
