/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2003 Julian Hyde <jhyde@users.sf.net>
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, May 2, 2003
*/
package mondrian.xmla;

import mondrian.olap.Util;
import mondrian.olap.EnumeratedValues;
import mondrian.util.SAXHandler;
import org.xml.sax.SAXException;

import java.util.*;

/**
 * Base class for an XML for Analysis schema rowset. A concrete derived class
 * should implement {@link #unparse}, calling {@link #emit} for each row.
 *
 * @see RowsetDefinition
 *
 * @author jhyde
 * @since May 2, 2003
 * @version $Id$
 **/
abstract class Rowset {
    private final RowsetDefinition rowsetDefinition;
    protected final HashMap restrictions;
    protected final Properties properties;
    private final RowsetDefinition.Column[] restrictedColumns;

    Rowset(RowsetDefinition definition, HashMap restrictions, Properties properties) {
        this.rowsetDefinition = definition;
        this.restrictions = restrictions;
        this.properties = properties;
        ArrayList list = new ArrayList();
        for (Iterator restrictionsIter = restrictions.keySet().iterator();
             restrictionsIter.hasNext();) {
            String restrictedColumn = (String) restrictionsIter.next();
            final RowsetDefinition.Column column = definition.lookupColumn(
                    restrictedColumn);
            if (column == null) {
                throw Util.newError("Rowset '" + definition.name_ +
                        "' does not contain column '" + restrictedColumn + "'");
            }
            if (!column.restriction) {
                throw Util.newError("Rowset '" + definition.name_ +
                        "' column '" + restrictedColumn +
                        "' does not allow restrictions");
            }
            // Check that the value is of the right type.
            final Object requiredValue = restrictions.get(column.name);
            if (requiredValue instanceof String) {
                // OK
            } else if (requiredValue instanceof String[]) {
                final RowsetDefinition.Type type = column.type;
                switch (type.ordinal_) {
                case RowsetDefinition.Type.StringArray_ORDINAL:
                case RowsetDefinition.Type.EnumerationArray_ORDINAL:
                case RowsetDefinition.Type.StringSometimesArray_ORDINAL:
                    break; // OK
                default:
                    throw Util.newError("Rowset '" + definition.name_ +
                            "' column '" + restrictedColumn +
                            "' can only be restricted on one value at a time");
                }
            } else {
                throw Util.newInternal("Bad type of restricted value" +
                        requiredValue);
            }
            list.add(column);
        }
        list = pruneRestrictions(list);
        this.restrictedColumns = (RowsetDefinition.Column[]) list.toArray(
                new RowsetDefinition.Column[0]);
        for (Iterator propertiesIter = properties.keySet().iterator();
                propertiesIter.hasNext();) {
            String propertyName = (String) propertiesIter.next();
            final PropertyDefinition propertyDef = PropertyDefinition.getValue(propertyName);
            if (propertyDef == null) {
                throw Util.newError("Rowset '" + definition.name_ +
                        "' does not support property '" + propertyName + "'");
            }
            final String propertyValue = properties.getProperty(propertyName);
            setProperty(propertyDef, propertyValue);
        }
    }

    protected ArrayList pruneRestrictions(ArrayList list) {
        return list;
    }

    /**
     * Sets a property for this rowset. Called by the constructor for each
     * supplied property.
     *
     * <p>A derived class should override this method and intercept each
     * property it supports. Any property it does not support, it should forward
     * to the base class method, which will probably throw an error.
     */
    protected void setProperty(PropertyDefinition propertyDef, String value) {
        switch (propertyDef.ordinal_) {
        case PropertyDefinition.Format_ORDINAL:
            final Enumeration.Format format = Enumeration.Format.getValue(value);
            if (format != Enumeration.Format.Tabular) {
                throw Util.newError("<Format> value '" + format + "' not supported");
            }
            break;
        case PropertyDefinition.DataSourceInfo_ORDINAL:
            break;
        case PropertyDefinition.Catalog_ORDINAL:
            break;
        default:
            System.out.println("Warning: Rowset '" + rowsetDefinition.name_ +
                    "' does not support property '" + propertyDef.name_ +
                    "' (value is '" + value + "')");
        }
    }

    /**
     * Writes the contents of this rowset as a series of SAX events.
     * @param saxHandler Handler to write to
     */
    public abstract void unparse(SAXHandler saxHandler) throws SAXException;

    private static boolean haveCommonMember(String[] a, String[] b) {
        for (int i = 0; i < a.length; i++) {
            for (int j = 0; j < b.length; j++) {
                if (a[i].equals(b[j])) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Emits a row for this rowset, reading fields from a {@link Row} object.
     *
     * @param row
     * @param saxHandler
     * @throws SAXException
     */
    protected void emit(Row row, SAXHandler saxHandler) throws SAXException {
        for (int i = 0; i < restrictedColumns.length; i++) {
            RowsetDefinition.Column column = restrictedColumns[i];
            Object value = row.get(column.name);
            if (value == null) {
                return;
            }
            final Object requiredValue = restrictions.get(column.name);
            String[] requiredValues, values;
            if (requiredValue instanceof String) {
                requiredValues = new String[] {(String) requiredValue};
            } else if (requiredValue instanceof String[]) {
                requiredValues = (String[]) requiredValue;
            } else {
                throw Util.newInternal("Restriction value is of wrong type: " + requiredValue);
            }
            if (value instanceof String) {
                values = new String[] {(String) value};
            } else if (value instanceof String[]) {
                values = (String[]) value;
            } else {
                throw Util.newInternal("Unexpected value type: " + value);
            }
            if (!haveCommonMember(requiredValues, values)) {
                return;
            }
        }
        saxHandler.startElement("row");
        for (int i = 0; i < rowsetDefinition.columnDefinitions.length; i++) {
            RowsetDefinition.Column column = rowsetDefinition.columnDefinitions[i];
            saxHandler.startElement(column.name);
            Object value = row.get(column.name);
            if (value == null) {
                if (!column.nullable) {
                    throw Util.newInternal("Value required for column " +
                            column.name + " of rowset " + rowsetDefinition.name_);
                }
            } else if (value instanceof XmlElement[]) {
                XmlElement[] elements = (XmlElement[]) value;
                for (int j = 0; j < elements.length; j++) {
                    final XmlElement element = elements[j];
                    saxHandler.element(element.tag, element.attributes);
                }
            } else if (value instanceof Object[]) {
                Object[] values = (Object[]) value;
                for (int j = 0; j < values.length; j++) {
                    saxHandler.element(values[j].toString(), new String[0]);
                }
            } else {
                saxHandler.characters(value.toString());
            }
            saxHandler.endElement();
        }
        saxHandler.endElement();
    }

    private static boolean contains(String[] strings, String value) {
        for (int i = 0; i < strings.length; i++) {
            if (strings[i].equals(value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Emits a row for this rowset, reading field values from an object using
     * reflection.
     *
     * @param row
     * @param saxHandler
     * @throws SAXException
     */
    protected void emit(Object row, SAXHandler saxHandler) throws SAXException {
        for (int i = 0; i < restrictedColumns.length; i++) {
            RowsetDefinition.Column column = restrictedColumns[i];
            Object value = column.get(row);
            if (value == null) {
                return;
            }
            final Object requiredValue = restrictions.get(column.name);
            if (requiredValue instanceof String) {
                if (!value.equals(requiredValue)) {
                    return;
                }
            } else if (requiredValue instanceof String[]) {
                if (!contains((String[]) requiredValue, value.toString())) {
                    return;
                }
            } else {
                throw Util.newInternal("Restriction value is of wrong type: " + requiredValue);
            }
        }
        saxHandler.startElement("row");
        for (int i = 0; i < rowsetDefinition.columnDefinitions.length; i++) {
            RowsetDefinition.Column column = rowsetDefinition.columnDefinitions[i];
            Object value = column.get(row);
            if (value != null) {
                saxHandler.startElement(column.name);
                saxHandler.characters(value.toString());
                saxHandler.endElement();
            }
        }
        saxHandler.endElement();
    }

    /**
     * Emits all of the values in an enumeration.
     */
    protected void emit(EnumeratedValues enumeration, SAXHandler saxHandler) throws SAXException {
        final List valuesSortedByName = enumeration.getValuesSortedByName();
        for (int i = 0; i < valuesSortedByName.size(); i++) {
            EnumeratedValues.Value value = (EnumeratedValues.Value)
                    valuesSortedByName.get(i);
            emit(value, saxHandler);
        }
    }

    /**
     * Returns whether a column value matches any restrictions placed on it.
     * If there are no restrictions, always returns true.
     */
    protected boolean passesRestriction(RowsetDefinition.Column column,
            String value) {
        final Object requiredValue = restrictions.get(column.name);
        if (requiredValue == null) {
            return true;
        }
        return requiredValue.equals(value);
    }

    protected boolean isRestricted(RowsetDefinition.Column column) {
        return restrictions.get(column.name) != null;
    }


    /**
     * A set of name/value pairs, which can be output using {@link #emit}.
     */
    protected class Row {
        private final ArrayList names = new ArrayList();
        private final ArrayList values = new ArrayList();

        void set(String name, Object value) {
            if (value != null) {
                names.add(name);
                values.add(value);
            }
        }
        void set(String name, int value) {
            set(name, Integer.toString(value));
        }
        void set(String name, boolean value) {
            set(name, value ? "true" : "false");
        }
        /**
         * Retrieves the value of a field with a given name, or null if the
         * field's value is not defined.
         */
        public Object get(String name) {
            int i = names.indexOf(name);
            if (i < 0) {
                return null;
            } else {
                return values.get(i);
            }
        }
    }

    /**
     * Holder for non-scalar column values of a {@link Row}.
     */
    protected class XmlElement {
        private final String tag;
        private final String[] attributes;

        XmlElement(String tag, String[] attributes) {
            this.tag = tag;
            this.attributes = attributes;
        }
    }
}

// End Rowset.java
