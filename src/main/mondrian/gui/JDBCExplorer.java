/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.gui;

import java.util.Enumeration;
import java.util.Vector;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeWillExpandListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.ExpandVetoException;
import javax.swing.tree.MutableTreeNode;
import org.apache.log4j.Logger;

/**
 *
 * @author  sean
 * @version $Id$
 */
public class JDBCExplorer extends javax.swing.JPanel
        implements TreeWillExpandListener {

    private static final Logger LOGGER = Logger.getLogger(JDBCExplorer.class);

    JDBCMetaData jdbcMetaData;
    JDBCTreeModel model;

    Workbench workbench;

    DefaultMutableTreeNode root;

    DefaultTreeModel treeModel;

    /** Creates new form JDBCExplorer
    public JDBCExplorer() {
        initComponents();
    }
     */

    public JDBCExplorer(JDBCMetaData jdbcMetaData, Workbench wb) {
        workbench = wb;
        initComponents();
        setMetaData(jdbcMetaData);
    }

    public void setMetaData(JDBCMetaData jdbcMetaData) {
        try {
            this.jdbcMetaData = jdbcMetaData;

            Node rootNode = new Node(null, NodeType.ROOT, null);
            root = new DefaultMutableTreeNode(rootNode);

            for (String schemaName : jdbcMetaData.getAllSchemas()) {
                Node cat = new Node(schemaName, NodeType.CATALOG, null);

                DefaultMutableTreeNode catTreeNode = new DefaultMutableTreeNode(cat);
                cat.treeNode = catTreeNode;
                root.add(catTreeNode);

                Vector<String> tables = jdbcMetaData.getAllTables(schemaName);
                for (String tableName : tables) {
                    Node table = new Node(tableName, NodeType.TABLE, null);
                    DefaultMutableTreeNode tableTreeNode = new DefaultMutableTreeNode(table);
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

    public void resetMetaData(JDBCMetaData jdbcMetaData) {
        setMetaData(jdbcMetaData);
    }

    public JTreeUpdater getTreeUpdater() {
        return updater;
    }

    public void treeWillExpand(TreeExpansionEvent event) throws ExpandVetoException {
        // The children are lazy loaded
        LOGGER.debug("path = " + event.getPath()
                + ", last object is a "
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

        DefaultMutableTreeNode parentNode = (DefaultMutableTreeNode) theTreeNode.getParent();

        Node theNode = (Node) theTreeNode.getUserObject();
        Node theParentNode = parentNode == null ? null : (Node) parentNode.getUserObject();

        Enumeration children = theTreeNode.children();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(message + ": " + theNode + ", " + theNode.type
                    + ", parent " + theParentNode +
                    (theParentNode == null ? "" : ", " + theParentNode.type));
            while (children.hasMoreElements()) {
                Object o = children.nextElement();
                Node child = (Node) ((DefaultMutableTreeNode) o).getUserObject();
                LOGGER.debug("\t" + child.toString() + ", " + child.type);
            }
        }
    }

    public void treeWillCollapse(TreeExpansionEvent arg0)
            throws ExpandVetoException {}

    enum NodeType {
        CATALOG,
        TABLE,
        COLUMN,
        ROOT
    }

    class Node {
        String name;
        NodeType type;
        boolean gotChildren = false;
        DefaultMutableTreeNode treeNode;
        JDBCMetaData.DbColumn columnInfo;

        public Node(String n,
                NodeType t,
                DefaultMutableTreeNode tn) {
            name = n;
            type = t;
            treeNode = tn;
        }

        public Node(String n,
                NodeType t,
                DefaultMutableTreeNode tn,
                JDBCMetaData.DbColumn ci) {
            name = n;
            type = t;
            treeNode = tn;
            columnInfo = ci;
        }

        public String toString() {
            if (type == NodeType.ROOT) {
                return workbench.getResourceConverter().getFormattedString(
                        "jdbcExplorer.root.name", "All Schemas",  null);
            }

            StringBuffer sb = new StringBuffer();

            if (name == null || name.trim().length() == 0) {
                switch (type) {
                case CATALOG:
                    sb.append(workbench
                            .getResourceConverter()
                            .getFormattedString(
                                "jdbcExplorer.default.name.catalog",
                                "Default Schema",
                                null));
                    break;
                case TABLE:
                    sb.append(workbench
                            .getResourceConverter()
                            .getFormattedString(
                                "jdbcExplorer.default.name.table",
                                "Table",
                                null));
                    break;
                case COLUMN:
                    sb.append(workbench
                            .getResourceConverter()
                            .getFormattedString(
                                "jdbcExplorer.default.name.column",
                                "Column",
                                null));
                    break;
                }
            } else {
                sb.append(name);
            }

            if (type != NodeType.COLUMN) {
                return sb.toString();
            }

            // now for columns

            sb.append(" - ")
                .append(columnInfo.displayType());

            return sb.toString();
        }

        public Enumeration setChildren() {
            if (!gotChildren) {
                if (type == NodeType.TABLE) {
                    DefaultMutableTreeNode theParentTreeNode = (DefaultMutableTreeNode) treeNode.getParent();

                    Node theParentNode = (Node) theParentTreeNode.getUserObject();

                    // This is a table, parent is a schema

                    Vector<String> columnNames = jdbcMetaData.getAllColumns(theParentNode.name, name);
                    for (String columnName : columnNames) {
                        JDBCMetaData.DbColumn columnInfo = jdbcMetaData.getColumnDefinition(theParentNode.name, name, columnName);
                        Node column = new Node(columnName, NodeType.COLUMN, treeNode, columnInfo);
                        MutableTreeNode columnTreeNode = new DefaultMutableTreeNode(column, false);
                        treeNode.add(columnTreeNode);
                    }
                }
            }
            gotChildren = true;
            return treeNode.children();
        }
    }

    /** This method is called from within the constructor to
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
// End JDBCExplorer.java