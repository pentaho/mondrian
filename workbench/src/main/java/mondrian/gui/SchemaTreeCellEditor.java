/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2017 Hitachi Vantara and others
// Copyright (C) 2006-2007 CINCOM SYSTEMS, INC.
// All Rights Reserved.
*/

package mondrian.gui;

import java.awt.*;
import java.util.*;
import java.util.List;
import javax.swing.*;
import javax.swing.event.CellEditorListener;
import javax.swing.event.ChangeEvent;
import javax.swing.tree.*;

/**
 *
 * @author sarora
 */
public class SchemaTreeCellEditor
    extends javax.swing.tree.DefaultTreeCellEditor
{
    private final ClassLoader myClassLoader;
    JComboBox listEditor;
    final List<CellEditorListener> listeners =
        new ArrayList<CellEditorListener>();

    /** Creates a new instance of SchemaTreeCellEditor */
    public SchemaTreeCellEditor(
        Workbench workbench,
        JTree tree,
        DefaultTreeCellRenderer renderer,
        TreeCellEditor editor)
    {
        super(tree, renderer, editor);
        myClassLoader = this.getClass().getClassLoader();
    }

    public Component getTreeCellEditorComponent(
        JTree tree,
        Object value,
        boolean isSelected,
        boolean expanded,
        boolean leaf,
        int row)
    {
        if (value instanceof MondrianGuiDef.RelationOrJoin) {
            String valueClass = value.getClass().getName();
            String simpleName[] = valueClass.split("[$.]", 0);

            return super.getTreeCellEditorComponent(
                tree,
                simpleName[simpleName.length - 1],
                isSelected,
                expanded,
                leaf,
                row);
        } else {
            return null;
        }
    }

    public boolean isCellEditable(EventObject event) {
        return false;
    }

    protected void fireEditingStopped() {
        ChangeEvent ce = new ChangeEvent(this);
        for (int i = listeners.size() - 1; i >= 0; i--) {
            listeners.get(i).editingStopped(ce);
        }
    }

    public void addCellEditorListener(CellEditorListener l) {
        listeners.add(l);
    }

    public void removeCellEditorListener(CellEditorListener l) {
        listeners.remove(l);
    }

    public void setValueAt(JTree tree) {
        String retValue;
        MondrianGuiDef.RelationOrJoin relationObj = null;

        retValue = (String) getCellEditorValue();
        if (retValue.equals("Join")) {
            relationObj =
                new MondrianGuiDef.Join(
                    "", "",
                    new MondrianGuiDef.Table(
                        "", "Table 1", "", null),
                    "", "",
                    new MondrianGuiDef.Table(
                        "", "Table 2", "", null));
        } else if (retValue.equals("Table")) {
            relationObj = new MondrianGuiDef.Table("", "Table", "", null);
        }

        TreePath tpath = tree.getSelectionPath();
        if (tpath != null) {
            Object value = tpath.getLastPathComponent();
            TreePath parentpath = tpath.getParentPath();
            if (parentpath != null) {
                Object parent = parentpath.getLastPathComponent();
                if (parent instanceof MondrianGuiDef.Hierarchy) {
                    ((MondrianGuiDef.Hierarchy) parent).relation = relationObj;
                } else if (parent instanceof MondrianGuiDef.Closure) {
                    ((MondrianGuiDef.Closure) parent).table =
                        (MondrianGuiDef.Table)relationObj;
                } else if (parent instanceof MondrianGuiDef.Join) {
                    int indexOfChild =
                        tree.getModel().getIndexOfChild(parent, value);
                    switch (indexOfChild) {
                    case 0:
                        ((MondrianGuiDef.Join) parent).left = relationObj;
                        break;
                    case 1:
                        ((MondrianGuiDef.Join) parent).right = relationObj;
                        break;
                    }
                }
                tree.setSelectionPath(
                    parentpath.pathByAddingChild(relationObj));
            }
        }
    }

    public boolean stopCellEditing() {
        boolean retValue;

        setValueAt(super.tree);
        retValue = super.stopCellEditing();
        return retValue;
    }
}

// End SchemaTreeCellEditor.java
