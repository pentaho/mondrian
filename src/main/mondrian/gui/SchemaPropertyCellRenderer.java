/*
 * SchemaPropertyCellRenderer.java
 *
 * Created on October 3, 2002, 2:00 PM
 */

package mondrian.gui;

import javax.swing.*;
import javax.swing.table.*;
import java.awt.*;

/**
 *
 * @author  sean
 */
public class SchemaPropertyCellRenderer extends javax.swing.table.DefaultTableCellRenderer {
    JLabel stringRenderer;
    JCheckBox booleanRenderer;
    JLabel integerRenderer;

    /** Creates a new instance of SchemaPropertyCellRenderer */
    public SchemaPropertyCellRenderer() {
    
        stringRenderer = new JLabel();
        stringRenderer.setFont(Font.decode("Dialog"));
        booleanRenderer = new JCheckBox();
        booleanRenderer.setBackground(Color.white);
        integerRenderer = new JLabel();
        integerRenderer.setHorizontalAlignment(JTextField.RIGHT);
        integerRenderer.setFont(Font.decode("Courier"));
    }
    
    public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
        if (column == 1) {
            if (value instanceof String) {
                stringRenderer.setText((String)value);               
                return stringRenderer;
            } else if (value instanceof Boolean) {
                booleanRenderer.setSelected(((Boolean)value).booleanValue());
                return booleanRenderer;
            } else if (value instanceof Integer) {
                integerRenderer.setText((String)value);
                return integerRenderer;
            }            
        }
        
        return super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
        
    }
}
