/*
 * PropertyTableModel.java
 *
 * Created on October 2, 2002, 7:02 PM
 */

package mondrian.gui;

import java.lang.reflect.*;
import java.beans.*;
import java.util.*;

/**
 *
 * @author  sean
 */
public class PropertyTableModel extends javax.swing.table.AbstractTableModel {
    String[] propertyNames;
    Object target;
    
    public PropertyTableModel(Object t, String[] pNames) {
        super();
        
        propertyNames = pNames;
        target = t;
    }
    
    public String getColumnName(int i) {
        if (i == 0) {
            return "Property";
        } else if (i==1) {
            return "Value";
        } 
        
        return "?";        
    }
    
    /** Returns the number of columns in the model. A
     * <code>JTable</code> uses this method to determine how many columns it
     * should create and display by default.
     *
     * @return the number of columns in the model
     * @see #getRowCount
     *
     */
    public int getColumnCount() {
        return 2; //that's 'Property' and 'Value'
    }
    
    /** Returns the number of rows in the model. A
     * <code>JTable</code> uses this method to determine how many rows it
     * should display.  This method should be quick, as it
     * is called frequently during rendering.
     *
     * @return the number of rows in the model
     * @see #getColumnCount
     *
     */
    public int getRowCount() {
        return propertyNames.length;
    }
    
    /** Returns the value for the cell at <code>columnIndex</code> and
     * <code>rowIndex</code>.
     *
     * @param	rowIndex	the row whose value is to be queried
     * @param	columnIndex 	the column whose value is to be queried
     * @return	the value Object at the specified cell
     *
     */
    public Object getValueAt(int rowIndex, int columnIndex) {
        if (columnIndex == 0) {
            return propertyNames[rowIndex];
        } else {
            try {
                Field f = target.getClass().getField(propertyNames[rowIndex]);
                return f.get(target);
            } catch (Exception ex) {
                ex.printStackTrace();
                return "#ERROR";
            }            
        }        
    }
    
}
