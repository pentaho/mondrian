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

import java.util.*;

import mondrian.olap.EnumeratedValues;
import mondrian.olap.Util;

import org.apache.log4j.Logger;

/**
 * Base class for an XML for Analysis schema rowset. A concrete derived class
 * should implement {@link #populate}, calling {@link #addRow} for each row.
 *
 * @author jhyde
 * @see mondrian.xmla.RowsetDefinition
 * @since May 2, 2003
 * @version $Id$
 */
abstract class Rowset implements XmlaConstants {
    protected static final Logger LOGGER = Logger.getLogger(Rowset.class);

    protected final RowsetDefinition rowsetDefinition;
    protected final Map restrictions;
    protected final Map properties;
    protected final XmlaRequest request;
    protected final XmlaHandler handler;
    private final RowsetDefinition.Column[] restrictedColumns;

    /**
     * The exceptions thrown in this constructor are not produced during
     * the execution of an XMLA request and so can be ordinary exceptions and
     * not XmlaException (which are  specifically for generating SOAP Fault
     * xml).
     *
     */
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
LOGGER.debug("Rowset<init>: restrictedColumn=\""+restrictedColumn+"\"");
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
        case PropertyDefinition.LocaleIdentifier_ORDINAL:
            // locale ids:
            // http://krafft.com/scripts/deluxe-calendar/lcid_chart.htm
            // 1033 is US English
            if ((value != null) && (value.equals("1033"))) {
                return;
            }
            // fall through
        default:
            LOGGER.warn("Warning: Rowset '" + rowsetDefinition.name +
                    "' does not support property '" + propertyDef.name +
                    "' (value is '" + value + "')");
        }
    }

    /**
     * Writes the contents of this rowset as a series of SAX events.
     */
    public final void unparse(XmlaResponse response) throws XmlaException
    {
        ArrayList rows = new ArrayList();
        populate(response, rows);
        Comparator comparator = rowsetDefinition.getComparator();
        if (comparator != null) {
            Collections.sort(rows, comparator);
        }
        for (int i = 0; i < rows.size(); i++) {
            Row row = (Row) rows.get(i);
            emit(row, response);
        }
    }

    /**
     * Gathers the set of rows which match a given set of the criteria.
     */
    public abstract void populate(XmlaResponse response, List rows) throws XmlaException;

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
     * Adds a {@link Row} to a result, provided that it meets the necessary
     * criteria. Returns whether the row was added.
     *
     * @param row Row
     * @param rows List of result rows
     */
    protected final boolean addRow(Row row, List rows) throws XmlaException {
        return rows.add(row);
    }

    /**
     * Emits a row for this rowset, reading fields from a
     * {@link mondrian.xmla.Rowset.Row} object.
     *
     * @param row Row
     * @param response XMLA response writer
     */
    protected void emit(Row row, XmlaResponse response) throws XmlaException {

        SaxWriter writer = response.getWriter();

        writer.startElement("row");
        for (int i = 0; i < rowsetDefinition.columnDefinitions.length; i++) {
            RowsetDefinition.Column column = rowsetDefinition.columnDefinitions[i];
            Object value = row.get(column.name);
            if (value == null) {
                if (!column.nullable) {
                    throw new XmlaException(
                        CLIENT_FAULT_FC,
                        HSB_BAD_NON_NULLABLE_COLUMN_CODE,
                        HSB_BAD_NON_NULLABLE_COLUMN_FAULT_FS,
                        Util.newInternal("Value required for column " +
                            column.name +
                            " of rowset " +
                            rowsetDefinition.name));
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
     */
    protected void emit(Object row, XmlaResponse response)
            throws XmlaException {
        SaxWriter writer = response.getWriter();

        writer.startElement("row");
        for (int i = 0; i < rowsetDefinition.columnDefinitions.length; i++) {
            RowsetDefinition.Column column = rowsetDefinition.columnDefinitions[i];
            Object value = column.get(row);
            if (value != null) {
                writer.startElement(column.name);
                writer.characters(value.toString());
                writer.endElement();
            } else {
                writer.startElement(column.name);
                writer.endElement();
            }
        }
        writer.endElement();
    }

    /**
     * Emits all of the values in an enumeration.
     */
    protected void emit(EnumeratedValues enumeration, XmlaResponse response)
            throws XmlaException {
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
    protected boolean passesRestriction(RowsetDefinition.Column column,
            Object value) {
        final Object requiredValue = restrictions.get(column.name);

        if (requiredValue == null) {
            return true;
        } else if (requiredValue instanceof String[]) {
            return contains((String[]) requiredValue, value.toString());
        } else {
            return requiredValue.equals(value);
        }
    }
     */

    /** 
     * Extensions to this abstract class implement a restriction test
     * for each Rowset's discovery request. If there is no restriction 
     * then the passes method always returns true.
     * Since whether the restriction is not specified (null), a single
     * value (String) or an array of values (String[]) is known at
     * the beginning of a Rowset's populate() method, creating these
     * just once at the beginning is faster than having to determine
     * the restriction status each time it is needed.
     */
    static abstract class RistrictionTest {
        public boolean passes(int ival) {
            return passes(new Integer(ival));
        }
        public abstract boolean passes(Object value);
    }

    RistrictionTest getRistrictionTest(RowsetDefinition.Column column) {
        final Object requiredValue = restrictions.get(column.name);
/*
System.out.println("Rowset.getRistrictionTest: column=" +column.name);
System.out.println("Rowset.getRistrictionTest: requiredValue=" +requiredValue);
*/

        if (requiredValue == null) {
            return new RistrictionTest() {
                public boolean passes(int ival) {
                    return true;
                }
                public boolean passes(Object value) {
                    return true;
                }
            };
        } else if (requiredValue instanceof String[]) {
            final String[] requiredValueArray = (String[]) requiredValue;
            return new RistrictionTest() {
                public boolean passes(Object value) {
                    return contains(requiredValueArray, value.toString());
                }
            };
        } else {
            return new RistrictionTest() {
                public boolean passes(Object value) {
                    return requiredValue.equals(value);
                }
            };
        }
    }
    
    /** 
     * This returns either null, a String or String[] depending upon
     * what was in the XMLA request.
     * 
     */
    Object getRestrictionValue(RowsetDefinition.Column column) {
        return restrictions.get(column.name);
    }
    
    /** 
     * This returns the restriction if its a String or null. This does not
     * attempt two determine if the restriction is an array of Strings
     * if all members of the array have the same value (in which case
     * one could return, again, simply a single String).
     * 
     */
    String getRestrictionValueAsString(RowsetDefinition.Column column) {
        Object rval = getRestrictionValue(column);
        return (rval instanceof String) ? (String) rval : null;
    }
    
    /** 
     * This returns the restriction as an integer (not an Integer) if it
     * exists and returns -1 otherwise.
     * 
     */
    int getRestrictionValueAsInt(RowsetDefinition.Column column) {
        Object rval = getRestrictionValue(column);
        if (rval instanceof String) {
            try {
                return Integer.parseInt((String)rval);
            } catch (NumberFormatException ex) {
                LOGGER.info("Rowset.getRestrictionValue: "+
                    "bad integer restriction \""+
                    rval+
                    "\"");
                return -1;
            }
        } else if (rval instanceof Integer) {
            return ((Integer) rval).intValue();
        } else {
            return -1;
        }
    }



    /** 
     * Returns true if there is a restriction for the given column 
     * definition.
     * 
     */
    protected boolean isRestricted(RowsetDefinition.Column column) {
        return (restrictions.get(column.name) != null);
    }

    /**
     * A set of name/value pairs, which can be output using
     * {@link Rowset#addRow}. This uses less memory than simply
     * using a HashMap and for very big data sets memory is
     * a concern.
     */
    protected class Row {
        private final ArrayList names;
        private final ArrayList values;
        Row() {
            this.names = new ArrayList();
            this.values = new ArrayList();
        }
        void set(String name, Object value) {
            this.names.add(name);
            this.values.add(value);
        }

        void set(String name, int value) {
            set(name, new Integer(value));
        }

        void set(String name, boolean value) {
            set(name, value ? "true" : "false");
        }

        /**
         * Retrieves the value of a field with a given name, or null if the
         * field's value is not defined.
         */
        public Object get(String name) {
            int i = this.names.indexOf(name);
            return (i < 0) ? null : this.values.get(i);
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
