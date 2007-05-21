/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde and others
// Copyright (C) 2006-2007 CINCOM SYSTEMS, INC.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.gui;

import java.awt.Component;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.EventObject;
import java.util.ResourceBundle;
import javax.swing.ImageIcon;
import javax.swing.JComboBox;
import javax.swing.JTree;
import javax.swing.event.CellEditorListener;
import javax.swing.event.ChangeEvent;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.TreeCellEditor;
import javax.swing.tree.TreePath;

/**
 *
 * @author sarora
 * @version $Id$
 */
public class SchemaTreeCellEditor extends javax.swing.tree.DefaultTreeCellEditor{

    private final ClassLoader myClassLoader;
    JComboBox listEditor;
    ArrayList listeners;
    //private final ResourceBundle resources;

    /** Creates a new instance of SchemaTreeCellEditor */
    public SchemaTreeCellEditor(Workbench workbench, JTree tree, DefaultTreeCellRenderer renderer, TreeCellEditor editor) {
        super(tree, renderer, editor);
        listeners = new ArrayList();
        myClassLoader = this.getClass().getClassLoader();
        //resources = ResourceBundle.getBundle("mondrian.gui.resources.gui");
        renderer.setLeafIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("table"))));
        renderer.setOpenIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("join"))));
        renderer.setClosedIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("join"))));

        /*
        listEditor = new JComboBox( new String[] {"Join", "Table"} );
        editor.addItemListener( new ItemListener() {
            public void itemStateChanged(ItemEvent e) {
                //System.out.println("item state changed ="+listEditor.getSelectedItem());
                //if (listEditor.isDisplayable()) listEditor.setPopupVisible(false);
                System.out.println("Item listener called");
                stopCellEditing();
            }
        });
         */

    }

    public Component getTreeCellEditorComponent(JTree tree, Object value, boolean isSelected, boolean expanded, boolean leaf, int row) {
        Component retValue=null;


        if (value instanceof MondrianGuiDef.Relation) {
            String valueClass = value.getClass().getName();
            String simpleName[] = valueClass.split("[$.]",0);

            retValue = super.getTreeCellEditorComponent(tree, simpleName[simpleName.length-1], isSelected, expanded, leaf, row);
            /*
            retValue.setPreferredSize(null);
            retValue.setPreferredSize(new java.awt.Dimension(retValue.getPreferredSize().width+1, 20)); //Do not remove this
            retValue.setMaximumSize(new java.awt.Dimension(retValue.getPreferredSize().width+1, 20)); //Do not remove this
             */
            /*
            if (listEditor.isDisplayable()) {
                listEditor.setPopupVisible(true);
            }
            return listEditor;
             */
        }

        return retValue;
    }

    public boolean isCellEditable(EventObject event) {
        boolean editable;
        //retValue = super.isCellEditable(event);

        if (event != null) {
            if (event.getSource() instanceof JTree) {
                if (event instanceof MouseEvent) {
                    TreePath path = tree.getPathForLocation(
                            ((MouseEvent)event).getX(),
                            ((MouseEvent)event).getY());
                    editable = (lastPath != null && path != null &&  lastPath.equals(path));
                    if (path!=null) {
                        Object value = path.getLastPathComponent();
                        TreePath parentPath = path.getParentPath();
                        Object parent = (parentPath==null?null:parentPath.getLastPathComponent());
                        if (value instanceof MondrianGuiDef.Relation && (parent instanceof MondrianGuiDef.Hierarchy || parent instanceof MondrianGuiDef.Relation)) {
                            // editing of relation(cube fact table is not allowed
                            //===System.out.println("Super iscelleditable="+ super.isCellEditable(event)); //editable;
                            if(((MouseEvent)event).getClickCount() == 2) {
                                return true;
                            }
                            return false;
                        } else {
                            return false;
                        }
                    }
                }
            }
        }
        return false;
    }

    public Object getCellEditorValue() {
        Object retValue;

        retValue = super.getCellEditorValue();
        /*
        System.out.println("Selected "+retValue);

        if (retValue.equals("Join")) {
            return new MondrianGuiDef.Join("","",new MondrianGuiDef.Table(), "", "", new MondrianGuiDef.Table());
        } else if (retValue.equals("Table")) {
            return new MondrianGuiDef.Table();

        }
        return null;
         */
        return retValue;
    }


    protected void fireEditingStopped() {
        ChangeEvent ce = new ChangeEvent(this);
        for (int i = listeners.size() - 1; i >= 0; i--) {
            ((CellEditorListener)listeners.get(i)).editingStopped(ce);
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
        MondrianGuiDef.Relation relationObj=null;

        retValue = (String) getCellEditorValue();
        if (retValue.equals("Join")) {
            relationObj= new MondrianGuiDef.Join("","",new MondrianGuiDef.Table("","Table 1",""), "", "", new MondrianGuiDef.Table("","Table 2",""));
        } else if (retValue.equals("Table")) {
            relationObj= new MondrianGuiDef.Table("","Table","");

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
                    ((MondrianGuiDef.Closure) parent).table = (MondrianGuiDef.Table)relationObj;
                } else if (parent instanceof MondrianGuiDef.Join) {
                    int indexOfChild = tree.getModel().getIndexOfChild(parent, value);
                    switch (indexOfChild) {
                        case 0: ((MondrianGuiDef.Join) parent).left = relationObj; break;
                        case 1: ((MondrianGuiDef.Join) parent).right = relationObj; break;
                    }
                }
                tree.setSelectionPath(parentpath.pathByAddingChild(relationObj));
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
