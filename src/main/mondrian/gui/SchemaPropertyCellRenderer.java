/*
 * SchemaPropertyCellRenderer.java
 *
 * Created on October 3, 2002, 2:00 PM
 */

package mondrian.gui;

import mondrian.olap.MondrianDef;

import javax.swing.*;
import java.awt.*;

/**
 *
 * @author  sean
 */
public class SchemaPropertyCellRenderer extends javax.swing.table.DefaultTableCellRenderer {
    JLabel stringRenderer;
    JCheckBox booleanRenderer;
    JLabel integerRenderer;
    JTable tableRenderer;

    /** Creates a new instance of SchemaPropertyCellRenderer */
    public SchemaPropertyCellRenderer() {

        stringRenderer = new JLabel();
        stringRenderer.setFont(Font.decode("Dialog"));
        booleanRenderer = new JCheckBox();
        booleanRenderer.setBackground(Color.white);
        integerRenderer = new JLabel();
        integerRenderer.setHorizontalAlignment(JTextField.RIGHT);
        integerRenderer.setFont(Font.decode("Courier"));

        tableRenderer = new JTable();
    }

    public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
        if (column == 1) {
            if (value instanceof String) {
                stringRenderer.setText((String)value);
                return stringRenderer;
            } else if (value instanceof Boolean) {
                booleanRenderer.setSelected((Boolean) value);
                return booleanRenderer;
            } else if (value instanceof Integer) {
                integerRenderer.setText(value.toString());
                return integerRenderer;
            } else if (value == null) {
                return null;
            } else if (value.getClass() == MondrianDef.Join.class) {
                SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer();
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(value,SchemaExplorer.DEF_JOIN);
                tableRenderer.setModel(ptm);
                return tableRenderer;
            } else if (value.getClass() == MondrianDef.OrdinalExpression.class) {
                SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer();
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(value,SchemaExplorer.DEF_SQL);
                tableRenderer.setModel(ptm);
                return tableRenderer;

            } else if (value.getClass() == MondrianDef.Relation.class) {
                SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer();
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(value,SchemaExplorer.DEF_RELATION);
                tableRenderer.setModel(ptm);
                return tableRenderer;
            } else if (value.getClass() == MondrianDef.Table.class) {
                SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer();
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(value,SchemaExplorer.DEF_TABLE);
                tableRenderer.setModel(ptm);
                return tableRenderer;
            } else if (value.getClass() == MondrianDef.Property.class) {
                SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer();
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(value,SchemaExplorer.DEF_PROPERTY);
                tableRenderer.setModel(ptm);
                return tableRenderer;
            } else {
                return null;
            }

        }

        return super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);

    }
}
