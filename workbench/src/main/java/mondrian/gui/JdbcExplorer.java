/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
*/

package mondrian.gui;

import mondrian.gui.JdbcMetaData.DbColumn;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.Enumeration;
import java.util.List;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeWillExpandListener;
import javax.swing.tree.*;

/**
 * @author sean
 */
public class JdbcExplorer
    extends javax.swing.JPanel
    implements TreeWillExpandListener
{
    private static final Logger LOGGER = LogManager.getLogger(JdbcExplorer.class);

    JdbcMetaData jdbcMetaData;
    JdbcTreeModel model;

    Workbench workbench;

    DefaultMutableTreeNode root;

    DefaultTreeModel treeModel;

    public JdbcExplorer(JdbcMetaData jdbcMetaData, Workbench wb) {
        workbench = wb;
        initComponents();
        setMetaData(jdbcMetaData);
    }

    public void setMetaData(JdbcMetaData jdbcMetaData) {
        try {
            this.jdbcMetaData = jdbcMetaData;

            Node rootNode = new Node(null, NodeType.ROOT, null);
            root = new DefaultMutableTreeNode(rootNode);

            for (String schemaName : jdbcMetaData.getAllSchemas()) {
                Node cat = new Node(schemaName, NodeType.CATALOG, null);

                DefaultMutableTreeNode catTreeNode =
                    new DefaultMutableTreeNode(cat);
                cat.treeNode = catTreeNode;
                root.add(catTreeNode);

                List<String> tables = jdbcMetaData.getAllTables(schemaName);
                for (String tableName : tables) {
                    Node table = new Node(tableName, NodeType.TABLE, null);
                    DefaultMutableTreeNode tableTreeNode =
                        new DefaultMutableTreeNode(table);
                    table.treeNode = tableTreeNode;
                    catTreeNode.add(tableTreeNode);
                }

                cat.gotChildren = true;
            }
            rootNode.gotChildren = true;

            treeModel = new DefaultTreeModel(root, true);
            tree.setModel(treeModel);
            tree.addTreeWillExpandListener(this);

            updater = new JTreeUpdater(tree);
        } catch (Exception ex) {
            LOGGER.error(ex);
        }
    }

    public void resetMetaData(JdbcMetaData jdbcMetaData) {
        setMetaData(jdbcMetaData);
    }

    public JTreeUpdater getTreeUpdater() {
        return updater;
    }

    public void treeWillExpand(TreeExpansionEvent event)
        throws ExpandVetoException
    {
        // The children are lazy loaded
        LOGGER.debug(
            "path = " + event.getPath() + ", last object is a "
            + event.getPath().getLastPathComponent().getClass().getName());

        DefaultMutableTreeNode theTreeNode =
            (DefaultMutableTreeNode) event.getPath().getLastPathComponent();
        Node theNode = (Node) theTreeNode.getUserObject();
        theNode.setChildren();

        logNode(theTreeNode, "will Expand");
    }

    private void logNode(DefaultMutableTreeNode theTreeNode, String message) {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }

        DefaultMutableTreeNode parentNode =
            (DefaultMutableTreeNode) theTreeNode.getParent();

        Node theNode = (Node) theTreeNode.getUserObject();
        Node theParentNode =
            parentNode == null
                ? null
                : (Node) parentNode.getUserObject();

        @SuppressWarnings({"unchecked"})
        Enumeration<TreeNode> children = theTreeNode.children();

        LOGGER.debug(
            message + ": " + theNode + ", " + theNode.type
            + ", parent " + theParentNode
            + (theParentNode == null
                ? ""
                : ", " + theParentNode.type));
        while (children.hasMoreElements()) {
            DefaultMutableTreeNode treeNode = ( DefaultMutableTreeNode ) children.nextElement();
            Node child = (Node) treeNode.getUserObject();
            LOGGER.debug("\t" + child.toString() + ", " + child.type);
        }
    }

    public void treeWillCollapse(TreeExpansionEvent arg0)
        throws ExpandVetoException
    {
    }

    enum NodeType {
        CATALOG,
        TABLE,
        COLUMN,
        ROOT
    }

    class Node {
        final String name;
        final NodeType type;
        boolean gotChildren = false;
        DefaultMutableTreeNode treeNode;
        final JdbcMetaData.DbColumn columnInfo;

        public Node(
            String name,
            NodeType type,
            DefaultMutableTreeNode treeNode)
        {
            this(name, type, treeNode, null);
        }

        public Node(
            String name,
            NodeType type,
            DefaultMutableTreeNode treeNode,
            JdbcMetaData.DbColumn columnInfo)
        {
            this.name = name;
            this.type = type;
            this.treeNode = treeNode;
            this.columnInfo = columnInfo;
        }

        public String toString() {
            if (type == NodeType.ROOT) {
                return workbench.getResourceConverter().getFormattedString(
                    "jdbcExplorer.root.name",
                    "All Schemas");
            }

            StringBuilder sb = new StringBuilder();
            if (name == null || name.trim().length() == 0) {
                switch (type) {
                case CATALOG:
                    sb.append(
                        workbench.getResourceConverter().getFormattedString(
                            "jdbcExplorer.default.name.catalog",
                            "Default Schema"));
                    break;
                case TABLE:
                    sb.append(
                        workbench.getResourceConverter().getFormattedString(
                            "jdbcExplorer.default.name.table", "Table"));
                    break;
                case COLUMN:
                    sb.append(
                        workbench.getResourceConverter().getFormattedString(
                            "jdbcExplorer.default.name.column",
                            "Column"));
                    break;
                }
            } else {
                sb.append(name);
            }

            if (type != NodeType.COLUMN) {
                return sb.toString();
            }

            // now for columns

            sb.append(" - ").append(columnInfo.displayType());

            return sb.toString();
        }

        public void setChildren() {
            if (!gotChildren) {
                if (type == NodeType.TABLE) {
                    DefaultMutableTreeNode theParentTreeNode =
                        (DefaultMutableTreeNode) treeNode.getParent();

                    Node theParentNode =
                        (Node) theParentTreeNode.getUserObject();

                    // This is a table, parent is a schema

                    List<DbColumn> columns =
                        jdbcMetaData.getAllDbColumns(
                            theParentNode.name, name);
                    for (DbColumn column : columns) {
                        Node columnNode = new Node(
                            column.name, NodeType.COLUMN, treeNode, column);
                        MutableTreeNode columnTreeNode =
                            new DefaultMutableTreeNode(columnNode, false);
                        treeNode.add(columnTreeNode);
                    }
                }
            }
            gotChildren = true;
        }
    }

    /**
     * This method is called from within the constructor to
     * initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is
     * always regenerated by the Form Editor.
     */
    private void initComponents() {//GEN-BEGIN:initComponents
        jSplitPane1 = new javax.swing.JSplitPane();
        jScrollPane1 = new javax.swing.JScrollPane();
        tree = new javax.swing.JTree();
        jScrollPane2 = new javax.swing.JScrollPane();

        setLayout(new java.awt.BorderLayout());

        jSplitPane1.setDividerLocation(200);
        jScrollPane1.setViewportView(tree);

        jSplitPane1.setLeftComponent(jScrollPane1);

        jSplitPane1.setRightComponent(jScrollPane2);

        add(jSplitPane1, java.awt.BorderLayout.CENTER);
    } //GEN-END:initComponents


    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JScrollPane jScrollPane2;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JTree tree;
    private javax.swing.JSplitPane jSplitPane1;
    // End of variables declaration//GEN-END:variables

    private JTreeUpdater updater;
}

// End JdbcExplorer.java
