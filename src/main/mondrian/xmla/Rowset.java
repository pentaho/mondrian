/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, May 2, 2003
*/
package mondrian.xmla;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mondrian.olap.EnumeratedValues;
import mondrian.olap.Util;

import org.apache.log4j.Logger;

/**
 * Base class for an XML for Analysis schema rowset. A concrete derived class
 * should implement {@link #unparse}, calling {@link #emit} for each row.
 *
 * @author jhyde
 * @see mondrian.xmla.RowsetDefinition
 * @since May 2, 2003
 * @version $Id$
 */
abstract class Rowset {
    private static final Logger LOGGER = Logger.getLogger(Rowset.class);

    protected final RowsetDefinition rowsetDefinition;
    protected final Map restrictions;
    protected final Map properties;
    protected final XmlaRequest request;
    protected final XmlaHandler handler;
    private final RowsetDefinition.Column[] restrictedColumns;

    Rowset(RowsetDefinition definition, XmlaRequest request, XmlaHandler handler) {
        this.rowsetDefinition = definition;
        this.restrictions = request.getRestrictions();
        this.properties = request.getProperties();
        this.request = request;
        this.handler = handler;
        ArrayList list = new ArrayList();
        for (Iterator restrictionsIter = restrictions.keySet().iterator();
             restrictionsIter.hasNext();) {
            String restrictedColumn = (String) restrictionsIter.next();
            final RowsetDefinition.Column column = definition.lookupColumn(
                    restrictedColumn);
            if (column == null) {
                throw Util.newError("Rowset '" + definition.name +
                        "' does not contain column '" + restrictedColumn + "'");
            }
            if (!column.restriction) {
                throw Util.newError("Rowset '" + definition.name +
                        "' column '" + restrictedColumn +
                        "' does not allow restrictions");
            }
            // Check that the value is of the right type.
            final Object requiredValue = restrictions.get(column.name);
            if (requiredValue instanceof String) {
                // OK
            } else if (requiredValue instanceof String[]) {
                final RowsetDefinition.Type type = column.type;
                switch (type.ordinal) {
                case RowsetDefinition.Type.StringArray_ORDINAL:
                case RowsetDefinition.Type.EnumerationArray_ORDINAL:
                case RowsetDefinition.Type.StringSometimesArray_ORDINAL:
                    break; // OK
                default:
                    throw Util.newError("Rowset '" + definition.name +
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
                throw Util.newError("Rowset '" + definition.name +
                        "' does not support property '" + propertyName + "'");
            }
            final String propertyValue = (String) properties.get(propertyName);
            setProperty(propertyDef, propertyValue);
        }
    }

    protected ArrayList pruneRestrictions(ArrayList list) {
        return list;
    }

    /**
     * Sets a property for this rowset. Called by the constructor for each
     * supplied property.<p/>
     *
     * A derived class should override this method and intercept each
     * property it supports. Any property it does not support, it should forward
     * to the base class method, which will probably throw an error.<p/>
     */
    protected void setProperty(PropertyDefinition propertyDef, String value) {
        switch (propertyDef.ordinal) {
        case PropertyDefinition.Format_ORDINAL:
            break;
        case PropertyDefinition.DataSourceInfo_ORDINAL:
            break;
        case PropertyDefinition.Catalog_ORDINAL:
            break;
        default:
            LOGGER.warn("Warning: Rowset '" + rowsetDefinition.name +
                    "' does not support property '" + propertyDef.name +
                    "' (value is '" + value + "')");
        }
    }

    /**
     * Writes the contents of this rowset as a series of SAX events.
     */
    public abstract void unparse(XmlaResponse response);

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
     * Emits a row for this rowset, reading fields from a {@link mondrian.xmla.Rowset.Row} object.
     *
     * @param row
     * @param response
     */
    protected void emit(Row row, XmlaResponse response) {
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

        SaxWriter writer = response.getWriter();

        writer.startElement("row");
        for (int i = 0; i < rowsetDefinition.columnDefinitions.length; i++) {
            RowsetDefinition.Column column = rowsetDefinition.columnDefinitions[i];
            Object value = row.get(column.name);
            if (value == null) {
                if (!column.nullable) {
                    throw Util.newInternal("Value required for column " +
                            column.name + " of rowset " + rowsetDefinition.name);
                }
            } else if (value instanceof XmlElement[]) {
                XmlElement[] elements = (XmlElement[]) value;
                for (int j = 0; j < elements.length; j++) {
                    emitXmlElement(writer, elements[j]);
                }
            } else if (value instanceof Object[]) {
                Object[] values = (Object[]) value;
                for (int j = 0; j < values.length; j++) {
                    writer.startElement(column.name);
                    writer.characters(values[j].toString());
                    writer.endElement();
                }
            } else {
                writer.startElement(column.name);
                writer.characters(value.toString());
                writer.endElement();
            }
        }
        writer.endElement();
    }

    private void emitXmlElement(SaxWriter writer, XmlElement element) {
        if (element.attributes == null) {
            writer.startElement(element.tag);
        } else {
            writer.startElement(element.tag, element.attributes);
        }

        if (element.text == null) {
            for (int i = 0; i < element.children.length; i++) {
                emitXmlElement(writer, element.children[i]);
            }
        } else {
            writer.characters(element.text);
        }

        writer.endElement();
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
     * @param response
     */
    protected void emit(Object row, XmlaResponse response) {
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

        SaxWriter writer = response.getWriter();

        writer.startElement("row");
        for (int i = 0; i < rowsetDefinition.columnDefinitions.length; i++) {
            RowsetDefinition.Column column = rowsetDefinition.columnDefinitions[i];
            Object value = column.get(row);
            if (value != null) {
                writer.startElement(column.name);
                writer.characters(value.toString());
                writer.endElement();
            }
        }
        writer.endElement();
    }

    /**
     * Emits all of the values in an enumeration.
     */
    protected void emit(EnumeratedValues enumeration, XmlaResponse response) {
        final List valuesSortedByName = enumeration.getValuesSortedByName();
        for (int i = 0; i < valuesSortedByName.size(); i++) {
            EnumeratedValues.Value value = (EnumeratedValues.Value)
                    valuesSortedByName.get(i);
            emit(value, response);
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
     * A set of name/value pairs, which can be output using
     * {@link Rowset#emit}.
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
     * Holder for non-scalar column values of a {@link mondrian.xmla.Rowset.Row}.
     */
    protected class XmlElement {
        private final String tag;
        private final String[] attributes;
        private final String text;
        private final XmlElement[] children;

        XmlElement(String tag, String[] attributes) {
            this(tag, attributes, null, null);
        }

        XmlElement(String tag, String[] attributes, String text) {
            this(tag, attributes, text, null);
        }

        XmlElement(String tag, String[] attributes, XmlElement[] children) {
            this(tag, attributes, null, children);
        }

        private XmlElement(String tag, String[] attributes, String text, XmlElement[] children) {
            this.tag = tag;
            this.attributes = attributes;
            this.text = text;
            this.children = children;
        }
    }
}

// End Rowset.java
