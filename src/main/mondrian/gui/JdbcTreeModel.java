/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.gui;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;
import javax.swing.event.TreeModelListener;
import javax.swing.tree.TreePath;

/**
 *
 * @author  sean
 */
public class JdbcTreeModel implements javax.swing.tree.TreeModel {

    private static final Logger LOGGER = Logger.getLogger(JdbcTreeModel.class);

    private Vector treeModelListeners = new Vector();
    Connection connection;
    DatabaseMetaData metadata;
    List catalogs;
    Node root;

    /** Creates a new instance of JDBCTreeModel */
    public JdbcTreeModel(Connection c) {
        connection = c;
        try {
            metadata = connection.getMetaData();
            catalogs = new ArrayList();
            String catalogName = connection.getCatalog();
            Node cat = new Node(catalogName, Node.CATALOG);

            ResultSet trs = metadata.getTables(cat.name, null, null, null);
            try {
                while (trs.next()) {
                    // Oracle 10g Driver returns bogus BIN$ tables that cause
                    // exceptions
                    String tbname = trs.getString("TABLE_NAME");
                    if (!tbname.matches("(?!BIN\\$).+")) {
                        continue;
                    }

                    Node table = new Node(trs.getString(3), Node.TABLE);
                    cat.children.add(table);
                    //get the tables for each catalog.
                    ResultSet crs =
                        metadata.getColumns(cat.name, null, table.name, null);
                    try {
                        while (crs.next()) {
                            Node column =
                                new Node(crs.getString(4), Node.COLUMN);
                            table.children.add(column);
                        }
                    } finally {
                        try {
                            if (crs != null) {
                                crs.close();
                            }
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                }
            } finally {
                try {
                    if (trs != null) {
                        trs.close();
                    }
                } catch (Exception e) {
                    // ignore
                }
            }
            root = cat;
        } catch (Exception ex) {
            LOGGER.error("JdbcTreeModel", ex);
        }
    }


    /** Adds a listener for the <code>TreeModelEvent</code>
     * posted after the tree changes.
     *
     * @param   l       the listener to add
     * @see     #removeTreeModelListener
     *
     */
    public void addTreeModelListener(TreeModelListener l) {
        treeModelListeners.add(l);
    }

    /** Returns the child of <code>parent</code> at index <code>index</code>
     * in the parent's
     * child array.  <code>parent</code> must be a node previously obtained
     * from this data source. This should not return <code>null</code>
     * if <code>index</code>
     * is a valid index for <code>parent</code> (that is <code>index >= 0 &&
     * index < getChildCount(parent</code>)).
     *
     * @param   parent  a node in the tree, obtained from this data source
     * @return  the child of <code>parent</code> at index <code>index</code>
     *
     */
    public Object getChild(Object parent, int index) {
        if (parent instanceof Node) {
            return ((Node)parent).children.get(index);
        }

        return null;
    }

    /** Returns the number of children of <code>parent</code>.
     * Returns 0 if the node
     * is a leaf or if it has no children.  <code>parent</code> must be a node
     * previously obtained from this data source.
     *
     * @param   parent  a node in the tree, obtained from this data source
     * @return  the number of children of the node <code>parent</code>
     *
     */
    public int getChildCount(Object parent) {
        if (parent instanceof Node) {
            return ((Node)parent).children.size();
        }
        return 0;
    }

    /** Returns the index of child in parent.  If <code>parent</code>
     * is <code>null</code> or <code>child</code> is <code>null</code>,
     * returns -1.
     *
     * @param parent a note in the tree, obtained from this data source
     * @param child the node we are interested in
     * @return the index of the child in the parent, or -1 if either
     *    <code>child</code> or <code>parent</code> are <code>null</code>
     *
     */
    public int getIndexOfChild(Object parent, Object child) {
        if (parent instanceof Node) {
            return ((Node)parent).children.indexOf(child);
        }

        return -1;
    }

    /** Returns the root of the tree.  Returns <code>null</code>
     * only if the tree has no nodes.
     *
     * @return  the root of the tree
     *
     */
    public Object getRoot() {
        return root;
    }

    /** Returns <code>true</code> if <code>node</code> is a leaf.
     * It is possible for this method to return <code>false</code>
     * even if <code>node</code> has no children.
     * A directory in a filesystem, for example,
     * may contain no files; the node representing
     * the directory is not a leaf, but it also has no children.
     *
     * @param   node  a node in the tree, obtained from this data source
     * @return  true if <code>node</code> is a leaf
     *
     */
    public boolean isLeaf(Object node) {
        return getChildCount(node) == 0;
    }

    /** Removes a listener previously added with
     * <code>addTreeModelListener</code>.
     *
     * @see     #addTreeModelListener
     * @param   l       the listener to remove
     *
     */
    public void removeTreeModelListener(TreeModelListener l) {
        treeModelListeners.remove(l);
    }

    /** Messaged when the user has altered the value for the item identified
     * by <code>path</code> to <code>newValue</code>.
     * If <code>newValue</code> signifies a truly new value
     * the model should post a <code>treeNodesChanged</code> event.
     *
     * @param path path to the node that the user has altered
     * @param newValue the new value from the TreeCellEditor
     *
     */
    public void valueForPathChanged(TreePath path, Object newValue) {
    }

    class Node {
        static final int CATALOG = 0;
        static final int TABLE = 1;
        static final int COLUMN = 2;
        String name;
        int type;
        ArrayList children;

        public Node(String n, int t) {
            name = n;
            type = t;
            children = new ArrayList();
        }

        public String toString() {
            return name;
        }
    }
}

// End JdbcTreeModel.java
