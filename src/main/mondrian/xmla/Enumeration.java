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

import mondrian.olap.EnumeratedValues;

/**
 * Contains inner classes which define enumerations used in XML for Analysis.
 *
 * @author jhyde
 * @since May 2, 2003
 * @version $Id$
 **/
class Enumeration {
    public static final class Methods extends EnumeratedValues.BasicValue {
        public static final Methods discover = new Methods("Discover", 1);
        public static final Methods execute = new Methods("Execute", 2);
        public static final Methods discoverAndExecute = new Methods("Discover/Execute", 3);

        private Methods(String name, int ordinal) {
            super(name, ordinal, null);
        }
    }

    public static final class Access extends EnumeratedValues.BasicValue {
        public static final Access read = new Access("R", 1);
        public static final Access write = new Access("W", 2);
        public static final Access readWrite = new Access("R/W", 3);

        private Access(String name, int ordinal) {
            super(name, ordinal, null);
        }
    }

    public static final class Format extends EnumeratedValues.BasicValue {
        public static final Format Tabular = new Format("Tabular", 0, "a flat or hierarchical rowset. Similar to the XML RAW format in SQL. The Format property should be set to Tabular for OLE DB for Data Mining commands.");
        public static final Format Multidimensional = new Format("Multidimensional", 1, "Indicates that the result set will use the MDDataSet format (Execute method only).");
        public static final Format Native = new Format("Native", 2, "The client does not request a specific format, so the provider may return the format  appropriate to the query. (The actual result type is identified by namespace of the result.)");
        public Format(String name, int ordinal, String description) {
            super(name, ordinal, description);
        }
        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new Format[] {Tabular, Multidimensional, Native}
        );

        public static Format getValue(String name) {
            return (Format) enumeration.getValue(name);
        }
    }

    public static final class AxisFormat extends EnumeratedValues.BasicValue {
        public AxisFormat(String name, int ordinal) {
            super(name, ordinal, null);
        }
        public static final AxisFormat TupleFormat = new AxisFormat("TupleFormat", 0);
        public static final AxisFormat ClusterFormat = new AxisFormat("ClusterFormat", 1);
        public static final AxisFormat CustomFormat = new AxisFormat("CustomFormat", 2);
        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new AxisFormat[] {TupleFormat, ClusterFormat, CustomFormat});
        public static AxisFormat getValue(String name) {
            return (AxisFormat) enumeration.getValue(name);
        }
    }

    public static class Content extends EnumeratedValues.BasicValue {
        public static final EnumeratedValues enumeration = new EnumeratedValues();

        public Content(String name, int ordinal, String description) {
            super(name, ordinal, description);
        }
    }

    public static class MDXSupport extends EnumeratedValues.BasicValue {
        public static final EnumeratedValues enumeration = new EnumeratedValues();

        public MDXSupport(String name, int ordinal, String description) {
            super(name, ordinal, description);
        }
    }

    public static class StateSupport extends EnumeratedValues.BasicValue {
        public static final EnumeratedValues enumeration = new EnumeratedValues();

        public StateSupport(String name, int ordinal, String description) {
            super(name, ordinal, description);
        }
    }
}

// End Enumeration.java