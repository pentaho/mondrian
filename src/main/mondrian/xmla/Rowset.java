/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, May 2, 2003
*/
package mondrian.xmla;

import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

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
    protected final Map<String, Object> restrictions;
    protected final Map<String, String> properties;
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
        ArrayList<RowsetDefinition.Column> list =
            new ArrayList<RowsetDefinition.Column>();
        for (Map.Entry<String, Object> restrictionEntry :
            restrictions.entrySet())
        {
            String restrictedColumn = restrictionEntry.getKey();
            LOGGER.debug(
                "Rowset<init>: restrictedColumn=\"" + restrictedColumn + "\"");
            final RowsetDefinition.Column column = definition.lookupColumn(
                restrictedColumn);
            if (column == null) {
                throw Util.newError("Rowset '" + definition.name() +
                    "' does not contain column '" + restrictedColumn + "'");
            }
            if (!column.restriction) {
                throw Util.newError("Rowset '" + definition.name() +
                    "' column '" + restrictedColumn +
                    "' does not allow restrictions");
            }
            // Check that the value is of the right type.
            final Object restriction = restrictionEntry.getValue();
            if (restriction instanceof List
                && ((List) restriction).size() > 1)
            {
                final RowsetDefinition.Type type = column.type;
                switch (type) {
                case StringArray:
                case EnumerationArray:
                case StringSometimesArray:
                    break; // OK
                default:
                    throw Util.newError("Rowset '" + definition.name() +
                        "' column '" + restrictedColumn +
                        "' can only be restricted on one value at a time");
                }
            }
            list.add(column);
        }
        list = pruneRestrictions(list);
        this.restrictedColumns =
            list.toArray(
                new RowsetDefinition.Column[list.size()]);
        for (Map.Entry<String, String> propertyEntry : properties.entrySet()) {
            String propertyName = propertyEntry.getKey();
            final PropertyDefinition propertyDef =
                Util.lookup(PropertyDefinition.class, propertyName);
            if (propertyDef == null) {
                throw Util.newError("Rowset '" + definition.name() +
                    "' does not support property '" + propertyName + "'");
            }
            final String propertyValue = propertyEntry.getValue();
            setProperty(propertyDef, propertyValue);
        }
    }

    protected ArrayList<RowsetDefinition.Column> pruneRestrictions(
        ArrayList<RowsetDefinition.Column> list)
    {
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
        switch (propertyDef) {
        case Format:
            break;
        case DataSourceInfo:
            break;
        case Catalog:
            break;
        case LocaleIdentifier:
            // locale ids:
            // http://krafft.com/scripts/deluxe-calendar/lcid_chart.htm
            // 1033 is US English
            if ((value != null) && (value.equals("1033"))) {
                return;
            }
            // fall through
        default:
            LOGGER.warn("Warning: Rowset '" + rowsetDefinition.name() +
                    "' does not support property '" + propertyDef.name() +
                    "' (value is '" + value + "')");
        }
    }

    /**
     * Writes the contents of this rowset as a series of SAX events.
     */
    public final void unparse(XmlaResponse response) throws XmlaException
    {
        List<Row> rows = new ArrayList<Row>();
        populate(response, rows);
        Comparator<Row> comparator = rowsetDefinition.getComparator();
        if (comparator != null) {
            Collections.sort(rows, comparator);
        }
        for (Row row : rows) {
            emit(row, response);
        }
    }

    /**
     * Gathers the set of rows which match a given set of the criteria.
     */
    public abstract void populate(XmlaResponse response, List<Row> rows) throws XmlaException;

    /**
     * Adds a {@link Row} to a result, provided that it meets the necessary
     * criteria. Returns whether the row was added.
     *
     * @param row Row
     * @param rows List of result rows
     */
    protected final boolean addRow(Row row, List<Row> rows) throws XmlaException {
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
        for (RowsetDefinition.Column column : rowsetDefinition.columnDefinitions) {
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
                            rowsetDefinition.name()));
                }
            } else if (value instanceof XmlElement[]) {
                XmlElement[] elements = (XmlElement[]) value;
                for (XmlElement element : elements) {
                    emitXmlElement(writer, element);
                }
            } else if (value instanceof Object[]) {
                Object[] values = (Object[]) value;
                for (Object value1 : values) {
                    writer.startElement(column.name);
                    writer.characters(value1.toString());
                    writer.endElement();
                }
            } else if (value instanceof List) {
                List values = (List) value;
                for (Object value1 : values) {
                    if (value1 instanceof XmlElement) {
                        XmlElement xmlElement = (XmlElement) value1;
                        emitXmlElement(writer, xmlElement);
                    } else {
                        writer.startElement(column.name);
                        writer.characters(value1.toString());
                        writer.endElement();
                    }
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
            for (XmlElement aChildren : element.children) {
                emitXmlElement(writer, aChildren);
            }
        } else {
            writer.characters(element.text);
        }

        writer.endElement();
    }

    /**
     * Populates all of the values in an enumeration into a list of rows.
     */
    protected <E extends Enum<E>> void populate(
        Class<E> clazz,
        List<Row> rows)
        throws XmlaException
    {
        final E[] enumsSortedByName = clazz.getEnumConstants().clone();
        Arrays.sort(
            enumsSortedByName,
            new Comparator<E>() {
                public int compare(E o1, E o2) {
                    return o1.name().compareTo(o2.name());
                }
            });
        for (E anEnum : enumsSortedByName) {
            Row row = new Row();
            for (RowsetDefinition.Column column : rowsetDefinition.columnDefinitions) {
                row.names.add(column.name);
                row.values.add(column.get(anEnum));
            }
            rows.add(row);
        }
    }

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
    static abstract class RestrictionTest {
        public abstract boolean passes(Object value);
    }

    RestrictionTest getRestrictionTest(RowsetDefinition.Column column) {
        final Object restriction = restrictions.get(column.name);

        if (restriction == null) {
            return new RestrictionTest() {
                public boolean passes(Object value) {
                    return true;
                }
            };
        } else if (restriction instanceof XmlaUtil.Wildcard) {
            XmlaUtil.Wildcard wildcard = (XmlaUtil.Wildcard) restriction;
            String regexp =
                Util.wildcardToRegexp(
                    Collections.singletonList(wildcard.pattern));
            final Matcher matcher = Pattern.compile(regexp).matcher("");
            return new RestrictionTest() {
                public boolean passes(Object value) {
                    return matcher.reset(String.valueOf(value)).matches();
                }
            };
        } else if (restriction instanceof List) {
            final List<String> requiredValues = (List<String>) restriction;
            return new RestrictionTest() {
                public boolean passes(Object value) {
                    return requiredValues.contains(value);
                }
            };
        } else {
            throw Util.newInternal(
                "unexpected restriction type: " + restriction.getClass());
        }
    }

    /**
     * Returns the restriction if it is a String, or null otherwise. Does not
     * attempt two determine if the restriction is an array of Strings
     * if all members of the array have the same value (in which case
     * one could return, again, simply a single String).
     */
    String getRestrictionValueAsString(RowsetDefinition.Column column) {
        final Object restriction = restrictions.get(column.name);
        if (restriction instanceof List) {
            List<String> rval = (List<String>) restriction;
            if (rval.size() == 1) {
                return rval.get(0);
            }
        }
        return null;
    }

    /**
     * Returns a column's restriction as an <code>int</code> if it
     * exists, -1 otherwise.
     */
    int getRestrictionValueAsInt(RowsetDefinition.Column column) {
        final Object restriction = restrictions.get(column.name);
        if (restriction instanceof List) {
            List<String> rval = (List<String>) restriction;
            if (rval.size() == 1) {
                try {
                    return Integer.parseInt(rval.get(0));
                } catch (NumberFormatException ex) {
                    LOGGER.info("Rowset.getRestrictionValue: "+
                        "bad integer restriction \""+
                        rval+
                        "\"");
                    return -1;
                }
            }
        }
        return -1;
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
    protected static class Row {
        private final ArrayList<String> names;
        private final ArrayList<Object> values;
        Row() {
            this.names = new ArrayList<String>();
            this.values = new ArrayList<Object>();
        }

        void set(String name, Object value) {
            this.names.add(name);
            this.values.add(value);
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
    protected static class XmlElement {
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
