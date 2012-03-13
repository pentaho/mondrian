/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// Copyright (C) 2006-2007 Cincom Systems, Inc.
// All Rights Reserved.
*/
package mondrian.gui;

import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.util.List;

/**
 * @author sean
 */
public class PropertyTableModel extends javax.swing.table.AbstractTableModel {

    private static final Logger LOGGER =
        Logger.getLogger(PropertyTableModel.class);

    private Object parentTarget; // parent of target
    private String factTable;   // selected fact table
    private String factTableSchema;   // selected fact table schema

    // List of names  for this object's siblings already existing in parent'
    private List<String> names;

    private String errorMsg = null;
        // error msg when property value could not be set.

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
            return workbench.getResourceConverter().getString(
                "propertyTableModel.attribute", "Attribute");
        } else if (i == 1) {
            return workbench.getResourceConverter().getString(
                "propertyTableModel.value", "Value");
        }

        return workbench.getResourceConverter().getString(
            "propertyTableModel.unknown", "?");
    }

    // get property name for given row no.
    public String getRowName(int i) {
        String pName = propertyNames[i];
        int j = -1;
        if ((j = pName.indexOf('|')) != -1) {  //"|"
            return pName.substring(0, j).trim();
        } else {
            return propertyNames[i];
        }
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

    /**
     * Returns the number of columns in the model. A
     * <code>JTable</code> uses this method to determine how many columns it
     * should create and display by default.
     *
     * @return the number of columns in the model
     * @see #getRowCount
     */
    public int getColumnCount() {
        return 2; //that's 'Property' and 'Value'
    }

    /**
     * Returns the number of rows in the model. A
     * <code>JTable</code> uses this method to determine how many rows it
     * should display.  This method should be quick, as it
     * is called frequently during rendering.
     *
     * @return the number of rows in the model
     * @see #getColumnCount
     */
    public int getRowCount() {
        return propertyNames.length;
    }

    /**
     * Returns the value for the cell at <code>columnIndex</code> and
     * <code>rowIndex</code>.
     *
     * @param rowIndex    the row whose value is to be queried
     * @param columnIndex the column whose value is to be queried
     * @return the value Object at the specified cell
     */
    public Object getValueAt(int rowIndex, int columnIndex) {
        if (columnIndex == 0) {
            return propertyNames[rowIndex];
        } else {
            try {
                String pName = propertyNames[rowIndex];
                if ((pName.indexOf('|')) != -1) {
                    //"formula | formulaElement.cdata" This is for special cases
                    // where more than one field refers to the same value.  For
                    // eg. calculated memeber's formula and formulaelement.cdata
                    // refers to the same formula string.  These cases arise to
                    // handle xml standards where an attribute can also appear
                    // as an xml tag.
                    Object obj = null;

                    // split field names on | to form an array of property names
                    // strings that are optional.
                    String[] pNames = pName.split(
                        "\\|",
                        0);
                    for (int j = 0; j < pNames.length; j++) {
                        if ((pNames[j].indexOf('.')) != -1) {
                            // Split string on . to form an array of property
                            // name within the property name.
                            String[] pNamesField = pNames[j].trim().split(
                                "\\.",
                                0);
                            if (pNamesField.length > 1) {
                                Field f =
                                    target.getClass().getField(
                                        pNamesField[0].trim());
                                obj = f.get(target);
                                if (obj != null) {
                                    Field f2 = obj.getClass().getField(
                                        pNamesField[1].trim());
                                    Object obj2 = f2.get(obj);
                                    return obj2;
                                }
                            }
                            return null;
                        } else {
                            Field f =
                                target.getClass().getField(pNames[j].trim());
                            obj = f.get(target);
                            if (obj != null) {
                                return obj;
                            }
                        }
                    }
                    return obj;
                } else {
                    // default case where one field refers to one value.
                    Field f =
                        target.getClass().getField(propertyNames[rowIndex]);

                    Object obj = f.get(target);
                    return obj;
                }
            } catch (Exception ex) {
                LOGGER.error("getValueAt(row, index)", ex);
                return "#ERROR";
            }
        }
    }

    public void setValueAt(Object value, int rowIndex, int columnIndex) {
        setErrorMsg(null);
        try {
            String pName = propertyNames[rowIndex];
            int i = -1;
            if ((i = pName.indexOf('|')) != -1) {
                //"formula | formulaElement.cdata"
                // save value in the first field name
                Field f =
                    target.getClass().getField(
                        propertyNames[rowIndex].substring(0, i).trim());
                f.set(target, value);
                // Delete the value from second and remaining field names. split
                // field names on | to form an array of property names strings
                // that are optional.
                String[] pNames =
                    pName.split(
                        "\\|",
                        0);
                for (int j = 1; j < pNames.length; j++) {
                    // Split string on . to form an array of property name
                    // within the property name.
                    String[] pNamesField =
                        pNames[j].trim().split(
                            "\\.",
                            0);
                    Field f2 =
                        target.getClass().getField(pNamesField[0].trim());
                    f2.set(target, null);
                }

//            } else if ((target instanceof MondrianGuiDef.UserDefinedFunction)
//                && (pName.equals("script")))
//            {
//                ((MondrianGuiDef.UserDefinedFunction) target).script =
//                    (MondrianGuiDef.Script) value;
//            } else if ((target instanceof MondrianGuiDef.MemberFormatter)
//                && (pName.equals("script")))
//            {
//                ((MondrianGuiDef.MemberFormatter) target).script =
//                    (MondrianGuiDef.Script) value;
//            } else if ((target instanceof MondrianGuiDef.CellFormatter)
//                && (pName.equals("script")))
//            {
//                ((MondrianGuiDef.CellFormatter) target).script =
//                    (MondrianGuiDef.Script) value;
//            } else if ((target instanceof MondrianGuiDef.PropertyFormatter)
//                && (pName.equals("script")))
//            {
//                ((MondrianGuiDef.PropertyFormatter) target).script =
//                    (MondrianGuiDef.Script) value;

            } else if ((target instanceof MondrianGuiDef.Level)
                && (pName.equals("ordinalExp")))
            {
                ((MondrianGuiDef.Level) target).ordinalExp.expressions[0] =
                    (MondrianGuiDef.SQL) value;
            } else if ((target instanceof MondrianGuiDef.Level)
                && (pName.equals("captionExp")))
            {
                ((MondrianGuiDef.Level) target).captionExp.expressions[0] =
                    (MondrianGuiDef.SQL) value;
            } else if ((target instanceof MondrianGuiDef.Table
                        && pName.equals("name"))
                       || (target instanceof MondrianGuiDef.Hierarchy
                           && pName.equals("primaryKeyTable"))
                       || (target instanceof MondrianGuiDef.Level
                           && pName.equals("table")))
            {
                // updating all table values
                if (value != null) {
                    // split and save only if value exists
                    String[] aValues =
                        ((String) value).split(JdbcMetaData.LEVEL_SEPARATOR);
                    if (aValues.length == 2) {
                        if (target instanceof MondrianGuiDef.Table) {
                            ((MondrianGuiDef.Table) target).name = aValues[1];
                            ((MondrianGuiDef.Table) target).schema = aValues[0];
                            // to refresh the value in schema field also
                            // alongwith table name
                            fireTableDataChanged();
                        } else {
                            Field f =
                                target.getClass().getField(
                                    propertyNames[rowIndex]);
                            f.set(target, aValues[1]);
                        }
                    } else {
                        // Avoids table="" to be set on schema
                        Field f =
                            target.getClass().getField(propertyNames[rowIndex]);
                        setFieldValue(f, value);
                    }
                }

            } else if ((target instanceof MondrianGuiDef.Dimension
                        && pName.equals("foreignKey"))
                       || (target instanceof MondrianGuiDef.DimensionUsage
                           && pName.equals("foreignKey"))
                       || (target instanceof MondrianGuiDef.Measure
                           && pName.equals("column"))
                       || (target instanceof MondrianGuiDef.Hierarchy
                           && pName.equals("primaryKey"))
                       || (target instanceof MondrianGuiDef.Level
                           && pName.equals("column"))
                       || (target instanceof MondrianGuiDef.Level
                           && pName.equals("nameColumn"))
                       || (target instanceof MondrianGuiDef.Level
                           && pName.equals("ordinalColumn"))
                       || (target instanceof MondrianGuiDef.Level
                           && pName.equals("parentColumn"))
                       || (target instanceof MondrianGuiDef.Level
                           && pName.equals("captionColumn"))
                       || (target instanceof MondrianGuiDef.Closure
                           && pName.equals("parentColumn"))
                       || (target instanceof MondrianGuiDef.Closure
                           && pName.equals("childColumn"))
                       || (target instanceof MondrianGuiDef.Property
                           && pName.equals("column")))
            {
                // updating all column values
                if (value != null) {
                    // split and save only if value exists
                    String[] aValues =
                        ((String) value).split(JdbcMetaData.LEVEL_SEPARATOR);
                    Field f =
                        target.getClass().getField(propertyNames[rowIndex]);
                    // Avoids *Column="" to be set on schema.  Also remove
                    // column data type with the final split on a dash.
                    value = aValues[aValues.length - 1].split(" - ")[0];
                    setFieldValue(f, value);
                }

            } else {
                if (propertyNames[rowIndex].equals("name")
                    && (!(target instanceof MondrianGuiDef.Table))
                    && (!value.equals(
                        target.getClass().getField(
                            propertyNames[rowIndex]).get(target)))
                    && duplicateName(value))
                {
                    setErrorMsg(
                        workbench.getResourceConverter().getFormattedString(
                            "propertyTableModel.duplicateValue.error",
                            "Error setting name property. {0} already exists",
                            value.toString()));
                } else {
                    Field f =
                        target.getClass().getField(propertyNames[rowIndex]);
                    // Avoids property to be set on schema with an empty value.
                    setFieldValue(f, value);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("setValueAt(aValue, row, index)", ex);
        }
    }

    private void setFieldValue(Field aField, Object aValue)
        throws IllegalAccessException
    {
        if (aValue != null && aValue.toString().trim().length() == 0) {
            aField.set(target, null);
        } else {
            aField.set(target, aValue);
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
        return (names != null && names.contains(aValue));
    }

    public List<String> getNames() {
        return names;
    }

    public void setNames(List<String> names) {
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
