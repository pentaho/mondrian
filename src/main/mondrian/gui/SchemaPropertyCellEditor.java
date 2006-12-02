/*
 * SchemaPropertyCellEditor.java
 *
 * Created on October 3, 2002, 1:13 PM
 */

package mondrian.gui;

import mondrian.olap.MondrianDef;

import javax.swing.*;
import javax.swing.event.CellEditorListener;
import javax.swing.event.ChangeEvent;
import java.awt.*;
import java.util.ArrayList;
import java.util.EventObject;

/**
 *
 * @author  sean
 */
public class SchemaPropertyCellEditor implements javax.swing.table.TableCellEditor {
     ArrayList listeners;
     JTextField stringEditor;
     JCheckBox booleanEditor;
     JTextField integerEditor;
     JTable tableEditor;
     Component activeEditor;

    /** Creates a new instance of SchemaPropertyCellEditor */
    public SchemaPropertyCellEditor() {
        listeners = new ArrayList();
        stringEditor = new JTextField();
        stringEditor.setFont(Font.decode("Dialog"));
        stringEditor.setBorder(null);

        booleanEditor = new JCheckBox();
        booleanEditor.setBackground(Color.white);

        integerEditor = new JTextField();
        integerEditor.setBorder(null);
        integerEditor.setHorizontalAlignment(JTextField.RIGHT);
        integerEditor.setFont(Font.decode("Courier"));

        tableEditor = new JTable();

    }

    public Component getTableCellEditorComponent(JTable table, Object value, boolean isSelected, int row, int column) {
        if (value instanceof String) {
            activeEditor = stringEditor;
            stringEditor.setText((String)value);
        } else if (value instanceof Boolean) {
            activeEditor = booleanEditor;
            booleanEditor.setSelected((Boolean) value);
        } else if (value instanceof Integer) {
            activeEditor = integerEditor;
            integerEditor.setText((String)value);
        } else if (value == null ) {
            value = "";
            activeEditor = stringEditor;
            stringEditor.setText((String)value);
        } else if (value.getClass() == MondrianDef.Join.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor();
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer();
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(value,SchemaExplorer.DEF_JOIN);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianDef.NameExpression.class) {
            return null;
        } else if (value.getClass() == MondrianDef.Relation.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor();
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer();
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(value,SchemaExplorer.DEF_RELATION);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
            return null;
        } else if (value.getClass() == MondrianDef.OrdinalExpression.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor();
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer();
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(value,SchemaExplorer.DEF_SQL);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianDef.Table.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor();
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer();
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(value,SchemaExplorer.DEF_TABLE);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianDef.Property.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor();
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer();
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(value,SchemaExplorer.DEF_PROPERTY);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
        } else {
            value = "";
            activeEditor = stringEditor;
            stringEditor.setText((String)value);
        }
        activeEditor.setVisible(true);

        return activeEditor;
    }

    /** Adds a listener to the list that's notified when the editor
     * stops, or cancels editing.
     *
     * @param   l       the CellEditorListener
     *
     */
    public void addCellEditorListener(CellEditorListener l) {
        listeners.add(l);
    }

    /** Tells the editor to cancel editing and not accept any partially
     * edited value.
     *
     */
    public void cancelCellEditing() {
        if (activeEditor != null) {
            activeEditor.setVisible(false);
            fireEditingCancelled();
        }
    }

    /** Returns the value contained in the editor.
     * @return the value contained in the editor
     *
     */
    public Object getCellEditorValue() {
        if (activeEditor == stringEditor) {
            return stringEditor.getText();
        } else if (activeEditor == booleanEditor) {
            return booleanEditor.isSelected();
        } else if (activeEditor == tableEditor) {
            return ((PropertyTableModel) tableEditor.getModel()).getValue();
        }


        return null;
    }

    /** Asks the editor if it can start editing using <code>anEvent</code>.
     * <code>anEvent</code> is in the invoking component coordinate system.
     * The editor can not assume the Component returned by
     * <code>getCellEditorComponent</code> is installed.  This method
     * is intended for the use of client to avoid the cost of setting up
     * and installing the editor component if editing is not possible.
     * If editing can be started this method returns true.
     *
     * @param   anEvent     the event the editor should use to consider
     *              whether to begin editing or not
     * @return  true if editing can be started
     * @see #shouldSelectCell
     *
     */
    public boolean isCellEditable(EventObject anEvent) {
        return true;
    }

    /** Removes a listener from the list that's notified
     *
     * @param   l       the CellEditorListener
     *
     */
    public void removeCellEditorListener(CellEditorListener l) {
        listeners.remove(l);
    }

    /** Returns true if the editing cell should be selected, false otherwise.
     * Typically, the return value is true, because is most cases the editing
     * cell should be selected.  However, it is useful to return false to
     * keep the selection from changing for some types of edits.
     * eg. A table that contains a column of check boxes, the user might
     * want to be able to change those checkboxes without altering the
     * selection.  (See Netscape Communicator for just such an example)
     * Of course, it is up to the client of the editor to use the return
     * value, but it doesn't need to if it doesn't want to.
     *
     * @param   anEvent     the event the editor should use to start
     *              editing
     * @return  true if the editor would like the editing cell to be selected;
     *    otherwise returns false
     * @see #isCellEditable
     *
     */
    public boolean shouldSelectCell(EventObject anEvent) {
        return true;
    }

    /** Tells the editor to stop editing and accept any partially edited
     * value as the value of the editor.  The editor returns false if
     * editing was not stopped; this is useful for editors that validate
     * and can not accept invalid entries.
     *
     * @return  true if editing was stopped; false otherwise
     *
     */
    public boolean stopCellEditing() {
        if (activeEditor != null) {
            activeEditor.setVisible(false);
            fireEditingStopped();
        }
        return true;
    }

    protected void fireEditingStopped() {
        ChangeEvent ce = new ChangeEvent(this);
        for (int i = listeners.size() - 1; i >= 0; i--) {
        ((CellEditorListener)listeners.get(i)).editingStopped(ce);
        }
    }

    protected void fireEditingCancelled() {
        ChangeEvent ce = new ChangeEvent(this);
        for (int i = listeners.size() - 1; i >= 0; i--) {
        ((CellEditorListener)listeners.get(i)).editingCanceled(ce);
        }
    }

}
