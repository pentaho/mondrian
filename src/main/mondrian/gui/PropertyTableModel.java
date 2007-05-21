/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2007 Julian Hyde and others
// Copyright (C) 2006-2007 Cincom Systems, Inc.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.gui;

import java.lang.reflect.Field;
import java.util.*;

import org.apache.log4j.Logger;

/**
 *
 * @author  sean
 * @version $Id$
 */
public class PropertyTableModel extends javax.swing.table.AbstractTableModel {

    private static final Logger LOGGER = Logger.getLogger(PropertyTableModel.class);

    private Object parentTarget; // parent of target
    private String factTable;   // selected fact table
    private String factTableSchema;   // selected fact table schema
    private ArrayList names; // List of names  for this object's siblings already existing in parent'
    private String errorMsg = null; // error msg when property value could not be set.

    String[] propertyNames;
    Object target;
    Workbench workbench;

    public PropertyTableModel(Workbench wb, Object t, String[] pNames) {
        super();
        workbench = wb;
        propertyNames = pNames;
        target = t;
    }

    public String getColumnName(int i) {
        if (i == 0) {
            return workbench.getResourceConverter().getString("propertyTableModel.attribute","Attribute");
        } else if (i==1) {
            return workbench.getResourceConverter().getString("propertyTableModel.value","Value");
        }

        return workbench.getResourceConverter().getString("propertyTableModel.unknown","?");
    }

    // get property name for given row no.
    public String getRowName(int i) {
        String pName = propertyNames[i];
        int j=-1;
        if ((j=pName.indexOf('|')) != -1) {  //"|"
            return pName.substring(0,j).trim();
        } else {
            return propertyNames[i];   }
    }

    public boolean isCellEditable(int row, int col) {
        if (col == 1) {
            Object cellObj = getValueAt(row, col);
            if (cellObj instanceof MondrianGuiDef.Join) {
                return false;
            } else {
                return true;
            }
        }

        return false;
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
     * @param   rowIndex    the row whose value is to be queried
     * @param   columnIndex     the column whose value is to be queried
     * @return  the value Object at the specified cell
     *
     */
    public Object getValueAt(int rowIndex, int columnIndex) {
        if (columnIndex == 0) {
            return propertyNames[rowIndex];
        } else {
            try {
                String pName = propertyNames[rowIndex];
                if ((pName.indexOf('|')) != -1) {   //"formula | formulaElement.cdata"
                    /* This is for special cases where more than one field refers to the same value.
                     * For eg. calculated memeber's formula and formulaelement.cdata refers to the same formula string.
                     * These cases arise to handle xml standards where an attribute can also appear as an xml tag.
                     */
                    Object obj = null;
                    String[] pNames = pName.split("\\|",0); // split field names on | to form an array of property names strings that are optional.
                    for(int j=0; j<pNames.length; j++) {
                        if ((pNames[j].indexOf('.')) != -1) {
                            String[] pNamesField = pNames[j].trim().split("\\.",0); // split string on . to form an array of property name within the property name.
                            if(pNamesField.length >1) {
                                Field f = target.getClass().getField(pNamesField[0].trim());
                                obj = f.get(target);
                                if (obj != null) {
                                    Field f2 = obj.getClass().getField(pNamesField[1].trim());
                                    Object obj2 = f2.get(obj);
                                    return obj2;
                                }
                            }
                            return null;
                        } else {
                            Field f = target.getClass().getField(pNames[j].trim());
                            obj = f.get(target);
                            if (obj != null) {
                                return obj;
                            }
                        }
                    }
                    return obj;
                } else {
                    // default case where one field refers to one value.
                    Field f = target.getClass().getField(propertyNames[rowIndex]);

                    Object obj = f.get(target);
                    return obj;
                }
            } catch (Exception ex) {
                LOGGER.error("getValueAt(row, index)", ex);
                return "#ERROR";
            }
        }
    }

    public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
        setErrorMsg(null);
        try {
            String pName = propertyNames[rowIndex];
            int i=-1;
            if ((i=pName.indexOf('|')) != -1) {   //"formula | formulaElement.cdata"
                Field f = target.getClass().getField(propertyNames[rowIndex].substring(0,i).trim());    // save value in the first field name
                f.set(target,aValue);
                // delete the value from second and remaining field names
                String[] pNames = pName.split("\\|",0); // split field names on | to form an array of property names strings that are optional.
                for(int j=1; j<pNames.length; j++) {
                    String[] pNamesField = pNames[j].trim().split("\\.",0); // split string on . to form an array of property name within the property name.
                    Field f2 = target.getClass().getField(pNamesField[0].trim());
                    f2.set(target,null);
                }

            } else if ( (target instanceof MondrianGuiDef.Level) && (pName.equals("ordinalExp")) )  {
                ((MondrianGuiDef.Level) target).ordinalExp.expressions[0] = (MondrianGuiDef.SQL) aValue;
                    /*
                    Field f = target.getClass().getField(propertyNames[rowIndex]);
                    f.set(((MondrianGuiDef.Level) target).ordinalExp.expressions[0], aValue);
                     */
            } else if ( (target instanceof MondrianGuiDef.Table && pName.equals("name")) ||
                    (target instanceof MondrianGuiDef.Hierarchy && pName.equals("primaryKeyTable")) ||
                    (target instanceof MondrianGuiDef.Level && pName.equals("table"))
                    )  {
                // updating all table values
                if (aValue != null) {   // split and save only if value exists
                    String[] aValues = ((String) aValue).split("->");
                    if (aValues.length == 2)  {
                        if (target instanceof MondrianGuiDef.Table) {
                            ((MondrianGuiDef.Table) target).name = aValues[1];
                            ((MondrianGuiDef.Table) target).schema = aValues[0];
                            fireTableDataChanged(); // to refresh the value in schema field also alongwith table name
                        } else {
                            Field f = target.getClass().getField(propertyNames[rowIndex]);
                            f.set(target,aValues[1]);
                        }
                    } else {
                        Field f = target.getClass().getField(propertyNames[rowIndex]);
                        f.set(target,aValue);
                    }
                }

            } else if ( (target instanceof MondrianGuiDef.Dimension && pName.equals("foreignKey")) ||
                    (target instanceof MondrianGuiDef.DimensionUsage && pName.equals("foreignKey")) ||
                    (target instanceof MondrianGuiDef.Measure && pName.equals("column")) ||
                    (target instanceof MondrianGuiDef.Hierarchy && pName.equals("primaryKey")) ||
                    (target instanceof MondrianGuiDef.Level && pName.equals("column")) ||
                    (target instanceof MondrianGuiDef.Level && pName.equals("nameColumn")) ||
                    (target instanceof MondrianGuiDef.Level && pName.equals("ordinalColumn")) ||
                    (target instanceof MondrianGuiDef.Level && pName.equals("parentColumn")) ||
                    (target instanceof MondrianGuiDef.Level && pName.equals("captionColumn")) ||
                    (target instanceof MondrianGuiDef.Closure && pName.equals("parentColumn")) ||
                    (target instanceof MondrianGuiDef.Closure && pName.equals("childColumn")) ||                    
                    (target instanceof MondrianGuiDef.Property && pName.equals("column"))
                    )  {
                // updating all column values
                if (aValue != null) {   // split and save only if value exists
                    String[] aValues = ((String) aValue).split("->");
                    Field f = target.getClass().getField(propertyNames[rowIndex]);
                    f.set(target, aValues[aValues.length-1]);    // save the last value in the array from split
                }

            } else {
                if ( propertyNames[rowIndex].equals("name")
                && (! (target instanceof MondrianGuiDef.Table))
                && (! aValue.equals(target.getClass().getField(propertyNames[rowIndex]).get(target)))
                && duplicateName(aValue) ) {
                    setErrorMsg(workbench.getResourceConverter().getFormattedString("propertyTableModel.duplicateValue.error", 
                            "Error setting name property. {0} already exists", 
                                new String[] { aValue.toString() }));
                } else {
                    Field f = target.getClass().getField(propertyNames[rowIndex]);
                    f.set(target,aValue);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("setValueAt(aValue, row, index)", ex);
        }
    }

    public Object getValue() {
        return target;
    }

    public Object getParentTarget() {
        return parentTarget;
    }

    public void setParentTarget(Object parentTarget) {
        this.parentTarget = parentTarget;
    }

    public String getFactTable() {
        return factTable;
    }

    public void setFactTable(String factTable) {
        this.factTable = factTable;
    }

    public String getFactTableSchema() {
        return factTableSchema;
    }

    public void setFactTableSchema(String factTableSchema) {
        this.factTableSchema = factTableSchema;
    }

    private boolean duplicateName(Object aValue) {
        return (names!=null && names.contains(aValue));
    }

    public ArrayList getNames() {
        return names;
    }

    public void setNames(ArrayList names) {
        this.names = names;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }
}

// End PropertyTableModel.java
