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
import mondrian.util.SAXHandler;
import org.xml.sax.SAXException;

import java.util.ArrayList;
import java.util.Properties;

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
    protected final Properties restrictions;
    protected final Properties properties;

    Rowset(Properties restrictions, Properties properties) {
        this.restrictions = restrictions;
        this.properties = properties;
    }
    /**
     * Writes the contents of this rowset as a series of SAX events.
     * @param saxHandler Handler to write to
     */
    public abstract void unparse(SAXHandler saxHandler) throws SAXException;

    /**
     * Emits a row for this rowset, reading fields from a {@link Row} object.
     *
     * @param rowsetDefinition
     * @param row
     * @param saxHandler
     * @throws SAXException
     */
    protected void emit(RowsetDefinition rowsetDefinition, Row row, SAXHandler saxHandler) throws SAXException {
        saxHandler.startElement("row");
        for (int i = 0; i < rowsetDefinition.columnDefinitions.length; i++) {
            RowsetDefinition.Column columnDefinition = rowsetDefinition.columnDefinitions[i];
            saxHandler.startElement(columnDefinition.name);
            String value = row.getString(columnDefinition.name);
            if (value == null) {
                if (!columnDefinition.nullable) {
                    throw Util.newInternal("Value required for column " + columnDefinition.name + " of rowset " + rowsetDefinition.name);
                }
            } else {
                saxHandler.characters(value);
            }
            saxHandler.endElement();
        }
        saxHandler.endElement();
    }

    /**
     * Emits a row for this rowset, reading field values from an object using
     * reflection.
     * @param rowsetDefinition
     * @param row
     * @param saxHandler
     * @throws SAXException
     */
    protected void emit(RowsetDefinition rowsetDefinition, Object row, SAXHandler saxHandler) throws SAXException {
        for (int i = 0; i < rowsetDefinition.columnDefinitions.length; i++) {
            RowsetDefinition.Column columnDefinition = rowsetDefinition.columnDefinitions[i];
            saxHandler.startElement(columnDefinition.name);
            String value = columnDefinition.get(row);
            saxHandler.characters(value);
            saxHandler.endElement();
        }
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
                values.add(value.toString());
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
        public String getString(String name) {
            int i = names.indexOf(name);
            if (i < 0) {
                return null;
            } else {
                return (String) values.get(i);
            }
        }

    }
}

// End Rowset.java
