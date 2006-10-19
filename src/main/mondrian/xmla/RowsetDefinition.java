/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.olap.*;
import mondrian.olap.Category;
import mondrian.olap.FunTable;
import mondrian.olap.fun.FunInfo;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapSchema;
import mondrian.rolap.RolapAggregator;
import mondrian.rolap.RolapMember;

import java.lang.reflect.Field;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.Format;

/**
 * <code>RowsetDefinition</code> defines a rowset, including the columns it
 * should contain.
 *
 * <p>See "XML for Analysis Rowsets", page 38 of the XML for Analysis
 * Specification, version 1.1.
 *
 * @author jhyde
 * @version $Id$
 */
abstract class RowsetDefinition extends EnumeratedValues.BasicValue {
    final Column[] columnDefinitions;
    final Column[] sortColumnDefinitions;

    /**
     * Date the schema was last modified.
     *
     * <p>TODO: currently schema grammar does not support modify date
     * so we return just some date for now.
     */
    private static final String dateModified = "2005-01-25T17:35:32";

    /**
     * Returns a list of XML for Analysis data sources
     * available on the server or Web Service. (For an
     * example of how these may be published, see
     * "XML for Analysis Implementation Walkthrough"
     * in the XML for Analysis specification.)
     */
    public static final int DISCOVER_DATASOURCES = 0;
    public static final int DISCOVER_PROPERTIES = 1;
    public static final int DISCOVER_SCHEMA_ROWSETS = 2;
    public static final int DISCOVER_ENUMERATORS = 3;
    public static final int DISCOVER_KEYWORDS = 4;
    public static final int DISCOVER_LITERALS = 5;
    public static final int DBSCHEMA_CATALOGS = 6;
    public static final int DBSCHEMA_COLUMNS = 7;
    public static final int DBSCHEMA_PROVIDER_TYPES = 8;
    public static final int DBSCHEMA_TABLES = 9;
    public static final int DBSCHEMA_TABLES_INFO = 10;
    public static final int MDSCHEMA_ACTIONS = 11;
    public static final int MDSCHEMA_CUBES = 12;
    public static final int MDSCHEMA_DIMENSIONS = 13;
    public static final int MDSCHEMA_FUNCTIONS = 14;
    public static final int MDSCHEMA_HIERARCHIES = 15;
    public static final int MDSCHEMA_LEVELS = 16;
    public static final int MDSCHEMA_MEASURES = 17;
    public static final int MDSCHEMA_MEMBERS = 18;
    public static final int MDSCHEMA_PROPERTIES = 19;
    public static final int MDSCHEMA_SETS = 20;
    public static final int OTHER = 21;
    public static final EnumeratedValues enumeration = new EnumeratedValues(
            new RowsetDefinition[] {
                DiscoverDatasourcesRowset.definition,
                DiscoverEnumeratorsRowset.definition,
                DiscoverPropertiesRowset.definition,
                DiscoverSchemaRowsetsRowset.definition,
                DiscoverKeywordsRowset.definition,
                DiscoverLiteralsRowset.definition,
                DbschemaCatalogsRowset.definition,
                DbschemaColumnsRowset.definition,
                DbschemaProviderTypesRowset.definition,
                DbschemaTablesRowset.definition,
                DbschemaTablesInfoRowset.definition,
                MdschemaActionsRowset.definition,
                MdschemaCubesRowset.definition,
                MdschemaDimensionsRowset.definition,
                MdschemaFunctionsRowset.definition,
                MdschemaHierarchiesRowset.definition,
                MdschemaLevelsRowset.definition,
                MdschemaMeasuresRowset.definition,
                MdschemaMembersRowset.definition,
                MdschemaPropertiesRowset.definition,
                MdschemaSetsRowset.definition,
            }
    );

    /**
     * Creates a rowset definition.
     *
     * @param name
     * @param ordinal
     * @param description
     * @param columnDefinitions List of column definitions
     * @param sortColumnDefinitions List of column definitions to sort on,
     *   or null if the rowset is not sorted
     */
    RowsetDefinition(
        String name,
        int ordinal,
        String description,
        Column[] columnDefinitions,
        Column[] sortColumnDefinitions)
    {
        super(name, ordinal, description);
        this.columnDefinitions = columnDefinitions;
        this.sortColumnDefinitions = sortColumnDefinitions;
    }

    public static RowsetDefinition getValue(String name) {
        return (RowsetDefinition) enumeration.getValue(name, true);
    }

    public abstract Rowset getRowset(XmlaRequest request, XmlaHandler handler);

    public Column lookupColumn(String name) {
        for (int i = 0; i < columnDefinitions.length; i++) {
            Column columnDefinition = columnDefinitions[i];
            if (columnDefinition.name.equals(name)) {
                return columnDefinition;
            }
        }
        return null;
    }

    /**
     * Returns a comparator with which to sort rows of this rowset definition.
     * The sort order is defined by the {@link #sortColumnDefinitions} field.
     * If the rowset is not sorted, returns null.
     */
    Comparator getComparator() {
        if (sortColumnDefinitions == null) {
            return null;
        }
        return new Comparator() {
            public int compare(Object o1, Object o2) {
                return compare((Rowset.Row) o1, (Rowset.Row) o2);
            }

            int compare(Rowset.Row row1, Rowset.Row row2) {
                // A faster implementation is welcome.
                for (int i = 0; i < sortColumnDefinitions.length; i++) {
                    RowsetDefinition.Column sortColumn = sortColumnDefinitions[i];
                    Comparable val1 = (Comparable) row1.get(sortColumn.name);
                    Comparable val2 = (Comparable) row2.get(sortColumn.name);
                    if ((val1 == null) && (val2 == null)) {
                        // columns can be optional, compare next column
                        continue;
                    } else if (val1 == null) {
                        return -1;
                    } else if (val2 == null) {
                        return 1;
                    } else if (val1 instanceof String &&
                            val2 instanceof String) {
                        int v = ((String) val1).compareToIgnoreCase((String) val2);
                        // if equal (= 0), compare next column
                        if (v != 0) {
                            return v;
                        }
                    } else {
                        int v = val1.compareTo(val2);
                        // if equal (= 0), compare next column
                        if (v != 0) {
                            return v;
                        }
                    }
                }
                return 0;
            }
        };
    }

    /**
     * Generates an XML schema description to the writer.
     * This is broken into top, Row definition and bottom so that on a
     * case by case basis a RowsetDefinition can redefine the Row
     * definition output. The default assumes a flat set of elements, but
     * for example, SchemaRowsets has a element with child elements.
     *
     * @param writer SAX writer
     * @see XmlaHandler#writeDatasetXmlSchema(SaxWriter, int)
     */
    void writeRowsetXmlSchema(SaxWriter writer) {
        writeRowsetXmlSchemaTop(writer);
        writeRowsetXmlSchemaRowDef(writer);
        writeRowsetXmlSchemaBottom(writer);
    }
    protected void writeRowsetXmlSchemaTop(SaxWriter writer) {
        writer.startElement("xsd:schema", new String[] {
            "xmlns:xsd", XmlaConstants.NS_XSD,
            "xmlns", XmlaConstants.NS_XMLA_ROWSET,
            "xmlns:xsi", XmlaConstants.NS_XSI,
            "xmlns:sql", "urn:schemas-microsoft-com:xml-sql",
            "targetNamespace", XmlaConstants.NS_XMLA_ROWSET,
            "elementFormDefault", "qualified"
        });

        writer.startElement("xsd:element", new String[] {
            "name", "root"
        });
        writer.startElement("xsd:complexType");
        writer.startElement("xsd:sequence");
        writer.element("xsd:element", new String[] {
            "name", "row",
            "type", "row",
            "minOccurs", "0",
            "maxOccurs", "unbounded"
        });
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType
        writer.endElement(); // xsd:element

        // MS SQL includes this in its schema section even thought
        // its not need for most queries.
        writer.startElement("xsd:simpleType", new String[] {
            "name", "uuid"
        });
        writer.startElement("xsd:restriction", new String[] {
            "base", "xsd:string"
        });
        writer.element("xsd:pattern", new String[] {
            "value", "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
        });

        writer.endElement(); // xsd:restriction
        writer.endElement(); // xsd:simpleType

    }
    protected void writeRowsetXmlSchemaRowDef(SaxWriter writer) {
        writer.startElement("xsd:complexType", new String[] {
            "name", "row"
        });
        writer.startElement("xsd:sequence");
        for (int i = 0; i < columnDefinitions.length; i++) {
            RowsetDefinition.Column column = columnDefinitions[i];
            final String name = XmlaUtil.encodeElementName(column.name);
            final String xsdType = column.type.columnType;

            String[] attrs = null;
            if (column.nullable) {
                if (column.unbounded) {
                    attrs = new String[] {
                        "sql:field", column.name,
                        "name", name,
                        "type", xsdType,
                        "minOccurs", "0",
                        "maxOccurs", "unbounded"
                    };
                } else {
                    attrs = new String[] {
                        "sql:field", column.name,
                        "name", name,
                        "type", xsdType,
                        "minOccurs", "0"
                    };
                }
            } else {
                if (column.unbounded) {
                    attrs = new String[] {
                        "sql:field", column.name,
                        "name", name,
                        "type", xsdType,
                        "maxOccurs", "unbounded"
                    };
                } else {
                    attrs = new String[] {
                        "sql:field", column.name,
                        "name", name,
                        "type", xsdType
                    };
                }
            }
            writer.element("xsd:element", attrs);
        }
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType
    }
    protected void writeRowsetXmlSchemaBottom(SaxWriter writer) {
        writer.endElement(); // xsd:schema
    }

    static class Type extends EnumeratedValues.BasicValue {
        public static final int String_ORDINAL = 0;
        public static final Type String =
            new Type("string", String_ORDINAL, "xsd:string");

        public static final int StringArray_ORDINAL = 1;
        public static final Type StringArray =
            new Type("StringArray", StringArray_ORDINAL, "xsd:string");

        public static final int Array_ORDINAL = 2;
        public static final Type Array =
            new Type("Array", Array_ORDINAL, "xsd:string");

        public static final int Enumeration_ORDINAL = 3;
        public static final Type Enumeration =
            new Type("Enumeration", Enumeration_ORDINAL, "xsd:string");

        public static final int EnumerationArray_ORDINAL = 4;
        public static final Type EnumerationArray =
            new Type("EnumerationArray", EnumerationArray_ORDINAL, "xsd:string");

        public static final int EnumString_ORDINAL = 5;
        public static final Type EnumString =
            new Type("EnumString", EnumString_ORDINAL, "xsd:string");

        public static final int Boolean_ORDINAL = 6;
        public static final Type Boolean =
            new Type("Boolean", Boolean_ORDINAL, "xsd:boolean");

        public static final int StringSometimesArray_ORDINAL = 7;
        public static final Type StringSometimesArray =
            new Type("StringSometimesArray", StringSometimesArray_ORDINAL, "xsd:string");

        public static final int Integer_ORDINAL = 8;
        public static final Type Integer =
            new Type("Integer", Integer_ORDINAL, "xsd:int");

        public static final int UnsignedInteger_ORDINAL = 9;
        public static final Type UnsignedInteger =
            new Type("UnsignedInteger", UnsignedInteger_ORDINAL, "xsd:unsignedInt");

        public static final int DataTime_ORDINAL = 10;
        public static final Type DateTime =
            new Type("DateTime", DataTime_ORDINAL, "xsd:dateTime");

        public static final int Short_ORDINAL = 11;
        public static final Type Short =
            new Type("Short", Short_ORDINAL, "xsd:short");

        public static final int UUID_ORDINAL = 12;
        public static final Type UUID =
            new Type("UUID", UUID_ORDINAL, "uuid");

        public static final int UnsignedShort_ORDINAL = 13;
        public static final Type UnsignedShort =
            new Type("UnsignedShort", UnsignedShort_ORDINAL, "xsd:unsignedShort");

        public static final int Long_ORDINAL = 14;
        public static final Type Long =
            new Type("Long", Long_ORDINAL, "xsd:long");

        public static final int UnsignedLong_ORDINAL = 15;
        public static final Type UnsignedLong =
            new Type("UnsignedLong", UnsignedLong_ORDINAL, "xsd:unsignedLong");

        public final String columnType;

        public Type(String name, int ordinal, String columnType) {
            super(name, ordinal, null);
            this.columnType = columnType;
        }

        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new Type[] {
                    String,
                    StringArray,
                    Array,
                    Enumeration,
                    EnumerationArray,
                    EnumString,
                });

        boolean isEnum() {
            switch (ordinal) {
            case Enumeration_ORDINAL:
            case EnumerationArray_ORDINAL:
            case EnumString_ORDINAL:
                return true;
            }
            return false;
        }
    }

    private static DBType getDBTypeFromProperty(Property prop) {
        DBType dbType = DBType.WSTR;
        switch (prop.getType()) {
        case Property.TYPE_STRING :
            dbType = DBType.WSTR;
            break;
        case Property.TYPE_NUMERIC :
            dbType = DBType.R8;
            break;
        case Property.TYPE_BOOLEAN :
            dbType = DBType.BOOL;
            break;
        case Property.TYPE_OTHER :
            // TODO: what type is it really, its
            // not a string
            break;
        }
        return dbType;
    }

    /**
     * The only OLE DB Types Indicators returned by SQL Server are thoses coded
     * below.
     * http://msdn.microsoft.com/library/default.asp?url=/library/en-us/oledb/htm/oledbtype_indicators.asp
     */

    static class DBType extends EnumeratedValues.BasicValue {
        /*
        * The following values exactly match VARENUM
        * in Automation and may be used in VARIANT.
        */
        public static final int I4_ORDINAL        = 3;
        public static final DBType I4 = new DBType("INTEGER", I4_ORDINAL,
            "A four-byte, signed integer: INTEGER",
            "DBTYPE_I4"
            );

        public static final int R8_ORDINAL        = 5;
        public static final DBType R8 = new DBType("DOUBLE", R8_ORDINAL,
            "A double-precision floating-point value: Double",
            "DBTYPE_R8"
            );

        public static final int CY_ORDINAL        = 6;
        public static final DBType CY = new DBType("CURRENCY", CY_ORDINAL,
            "A currency value: LARGE_INTEGER, Currency is a fixed-point number with four digits to the right of the decimal point. It is stored in an eight-byte signed integer, scaled by 10,000.",
            "DBTYPE_CY"
            );

        public static final int BOOL_ORDINAL        = 11;
        public static final DBType BOOL = new DBType("BOOLEAN", BOOL_ORDINAL,
            "A Boolean value stored in the same way as in Automation: VARIANT_BOOL; 0 means false and ~0 (bitwise, the value is not 0; that is, all bits are set to 1) means true.",
            "DBTYPE_BOOL"
            );

        /**
         * Used by SQL Server for value.
         */
        public static final int VARIANT_ORDINAL      = 12;
        public static final DBType VARIANT = new DBType("VARIANT",
            VARIANT_ORDINAL,
            "An Automation VARIANT",
            "DBTYPE_VARIANT"
            );

        /**
         * Used by SQL Server for font size.
         */
        public static final int UI2_ORDINAL      = 18;
        public static final DBType UI2 = new DBType("UNSIGNED_SHORT",
            UI2_ORDINAL,
            "A two-byte, unsigned integer",
            "DBTYPE_UI2"
            );

        /**
         * Used by SQL Server for colors, font flags and cell ordinal.
         */
        public static final int UI4_ORDINAL      = 19;
        public static final DBType UI4 = new DBType("UNSIGNED_INTEGER",
            UI4_ORDINAL,
            "A four-byte, unsigned integer",
            "DBTYPE_UI4"
            );

        /*
        * The following values exactly match VARENUM
        * in Automation but cannot be used in VARIANT.
        */
        public static final int I8_ORDINAL        = 20;
        public static final DBType I8 = new DBType("LARGE_INTEGER", I8_ORDINAL,
            "An eight-byte, signed integer: LARGE_INTEGER",
            "DBTYPE_I8"
            );
        /*
        * The following values are not in VARENUM in OLE.
        */
        public static final int WSTR_ORDINAL        = 130;
        public static final DBType WSTR = new DBType("STRING", WSTR_ORDINAL,
            "A null-terminated Unicode character string: wchar_t[length]; If DBTYPE_WSTR is used by itself, the number of bytes allocated for the string, including the null-termination character, is specified by cbMaxLen in the DBBINDING structure. If DBTYPE_WSTR is combined with DBTYPE_BYREF, the number of bytes allocated for the string, including the null-termination character, is at least the length of the string plus two. In either case, the actual length of the string is determined from the bound length value. The maximum length of the string is the number of allocated bytes divided by sizeof(wchar_t) and truncated to the nearest integer.",
            "DBTYPE_WSTR"
            );


        /**
         * The length of a non-numeric column or parameter that refers to either
         * the maximum or the length defined for this type by the provider. For
         * character data, this is the maximum or defined length in characters.
         * For DateTime data types, this is the length of the string
         * representation (assuming the maximum allowed precision of the
         * fractional seconds component).
         *
         * If the data type is numeric, this is the upper bound on the maximum
         * precision of the data type.
        int columnSize;
         */

        /**
         *  A Boolean that indicates whether the data type is nullable.
         *  VARIANT_TRUE indicates that the data type is nullable.
         *  VARIANT_FALSE indicates that the data type is not nullable.
         *  NULL-- indicates that it is not known whether the data type is
         *  nullable.
        boolean isNullable;
         */

        String dbTypeIndicator;

        public DBType(String name, int ordinal, String description,
                String dbTypeIndicator) {
            super(name, ordinal, description);
            this.dbTypeIndicator = dbTypeIndicator;
        }
    }
    static class Column {

        /**
         * This is used as the true value for the restriction parameter.
         */
        static final boolean RESTRICTION = true;
        /**
         * This is used as the false value for the restriction parameter.
         */
        static final boolean NOT_RESTRICTION = false;

        /**
         * This is used as the false value for the nullable parameter.
         */
        static final boolean REQUIRED = false;
        /**
         * This is used as the true value for the nullable parameter.
         */
        static final boolean OPTIONAL = true;

        /**
         * This is used as the false value for the unbounded parameter.
         */
        static final boolean ONE_MAX = false;
        /**
         * This is used as the true value for the unbounded parameter.
         */
        static final boolean UNBOUNDED = true;

        final String name;
        final Type type;
        final Enumeration enumeration;
        final String description;
        final boolean restriction;
        final boolean nullable;
        final boolean unbounded;

        /**
         * Creates a column.
         *
         * @param name
         * @param type           A {@link mondrian.xmla.RowsetDefinition.Type} value
         * @param enumeratedType Must be specified for enumeration or array
         *                       of enumerations
         * @param description
         * @param restriction
         * @param nullable
         * @pre type != null
         * @pre (type == Type.Enumeration || type == Type.EnumerationArray || type == Type.EnumString) == (enumeratedType != null)
         * @pre description == null || description.indexOf('\r') == -1
         */
        Column(String name, Type type, Enumeration enumeratedType,
                boolean restriction, boolean nullable, String description) {
            this(name, type, enumeratedType,
                restriction, nullable, ONE_MAX, description);
        }

        Column(String name, Type type, Enumeration enumeratedType,
                boolean restriction, boolean nullable, boolean unbounded,
                String description) {
            Util.assertPrecondition(type != null, "Type.instance.isValid(type)");
            Util.assertPrecondition(
                    (type == Type.Enumeration || type == Type.EnumerationArray || type == Type.EnumString) == (enumeratedType != null),
                    "(type == Type.Enumeration || type == Type.EnumerationArray || type == Type.EnumString) == (enumeratedType != null)");
            // Line endings must be UNIX style (LF) not Windows style (LF+CR).
            // Thus the client will receive the same XML, regardless
            // of the server O/S.
            Util.assertPrecondition(
                    description == null || description.indexOf('\r') == -1,
                    "description == null || description.indexOf('\r') == -1");
            this.name = name;
            this.type = type;
            this.enumeration = enumeratedType;
            this.description = description;
            this.restriction = restriction;
            this.nullable = nullable;
            this.unbounded = unbounded;
        }

        /**
         * Retrieves a value of this column from a row. The base implementation
         * uses reflection; a derived class may provide a different
         * implementation.
         */
        Object get(Object row) {
            try {
                String javaFieldName = name.substring(0, 1).toLowerCase() +
                    name.substring(1);
                Field field = row.getClass().getField(javaFieldName);
                return field.get(row);
            } catch (NoSuchFieldException e) {
                throw Util.newInternal(e, "Error while accessing rowset column " + name);
            } catch (SecurityException e) {
                throw Util.newInternal(e, "Error while accessing rowset column " + name);
            } catch (IllegalAccessException e) {
                throw Util.newInternal(e, "Error while accessing rowset column " + name);
            }
        }

        public String getColumnType() {
            if (type.isEnum()) {
                return enumeration.type.columnType;
            }
            return type.columnType;
        }
    }

    // -------------------------------------------------------------------------
    // From this point on, just rowset classess.

    static class DiscoverDatasourcesRowset extends Rowset {
        private static final Column DataSourceName =
            new Column(
                "DataSourceName",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the data source, such as FoodMart 2000.");
        private static final Column DataSourceDescription =
            new Column(
                "DataSourceDescription",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A description of the data source, as entered by the publisher.");
        private static final Column URL =
            new Column(
                "URL",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The unique path that shows where to invoke the XML for Analysis methods for that data source.");
        private static final Column DataSourceInfo =
            new Column(
                "DataSourceInfo",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A string containing any additional information required to connect to the data source. This can include the Initial Catalog property or other information for the provider.\n" +
                "Example: \"Provider=MSOLAP;Data Source=Local;\"");
        private static final Column ProviderName =
            new Column(
                "ProviderName",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the provider behind the data source. \n" +
                "Example: \"MSDASQL\"");
        private static final Column ProviderType =
            new Column(
                "ProviderType",
                Type.EnumerationArray,
                Enumeration.ProviderType.enumeration,
                Column.RESTRICTION,
                Column.REQUIRED,
                Column.UNBOUNDED,
                "The types of data supported by the provider. May include one or more of the following types. Example follows this table.\n" +
                "TDP: tabular data provider.\n" +
                "MDP: multidimensional data provider.\n" +
                "DMP: data mining provider. A DMP provider implements the OLE DB for Data Mining specification.");
        private static final Column AuthenticationMode =
            new Column(
                "AuthenticationMode",
                Type.EnumString,
                Enumeration.AuthenticationMode.enumeration,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Specification of what type of security mode the data source uses. Values can be one of the following:\n" +
                "Unauthenticated: no user ID or password needs to be sent.\n" +
                "Authenticated: User ID and Password must be included in the information required for the connection.\n" +
                "Integrated: the data source uses the underlying security to determine authorization, such as Integrated Security provided by Microsoft Internet Information Services (IIS).");
        /*
         *  http://msdn2.microsoft.com/en-us/library/ms126129(SQL.90).aspx
         *
         *
         * restrictions
         *
         * Not supported
         */
        static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_DATASOURCES", DISCOVER_DATASOURCES,
                "Returns a list of XML for Analysis data sources available on the server or Web Service.",
                new Column[] {
                    DataSourceName,
                    DataSourceDescription,
                    URL,
                    DataSourceInfo,
                    ProviderName,
                    ProviderType,
                    AuthenticationMode,
                },
            null /* not sorted */) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DiscoverDatasourcesRowset(request, handler);
            }
        };

        public DiscoverDatasourcesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            for (Iterator it = handler.getDataSourceEntries().values().iterator(); it.hasNext();) {
                DataSourcesConfig.DataSource ds = (DataSourcesConfig.DataSource) it.next();
                Row row = new Row();
                row.set(DataSourceName.name, ds.getDataSourceName());
                row.set(DataSourceDescription.name, ds.getDataSourceDescription());
                row.set(URL.name, ds.getURL());
                row.set(DataSourceInfo.name, ds.getDataSourceName());
                row.set(ProviderName.name, ds.getProviderName());
                row.set(ProviderType.name, ds.getProviderType());
                row.set(AuthenticationMode.name, ds.getAuthenticationMode());
                addRow(row, rows);
            }
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverSchemaRowsetsRowset extends Rowset {
        private static final Column SchemaName =
            new Column(
                "SchemaName",
                Type.StringArray,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the schema/request. This returns the values in the RequestTypes enumeration, plus any additional types supported by the provider. The provider defines rowset structures for the additional types");
        private static final Column SchemaGuid =
            new Column(
                "SchemaGuid",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The GUID of the schema.");
        private static final Column Restrictions =
            new Column(
                "Restrictions",
                Type.Array,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "An array of the restrictions suppoted by provider. An example follows this table.");
        private static final Column Description =
            new Column(
                "Description",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A localizable description of the schema");
        /*
         * Note that SQL Server also returns the data-mining columns.
         *
         *
         * restrictions
         *
         * Not supported
         */
        private static RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_SCHEMA_ROWSETS", DISCOVER_SCHEMA_ROWSETS,
                "Returns the names, values, and other information of all supported RequestType enumeration values.",
                new Column[] {
                    SchemaName,
                    SchemaGuid,
                    Restrictions,
                    Description,
                },
                null /* not sorted */) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DiscoverSchemaRowsetsRowset(request, handler);
            }
            protected void writeRowsetXmlSchemaRowDef(SaxWriter writer) {
                writer.startElement("xsd:complexType", new String[] {
                    "name", "row"
                });
                writer.startElement("xsd:sequence");
                for (int i = 0; i < columnDefinitions.length; i++) {
                    RowsetDefinition.Column column = columnDefinitions[i];
                    final String name = XmlaUtil.encodeElementName(column.name);

                    if (column == Restrictions) {
                        writer.startElement("xsd:element", new String[] {
                                    "sql:field", column.name,
                                    "name", name,
                                    "minOccurs", "0",
                                    "maxOccurs", "unbounded"
                        });
                        writer.startElement("xsd:complexType");
                        writer.startElement("xsd:sequence");
                        writer.element("xsd:element", new String[] {
                                    "name", "Name",
                                    "type", "xsd:string",
                                    "sql:field", "Name"
                        });
                        writer.element("xsd:element", new String[] {
                                    "name", "Type",
                                    "type", "xsd:string",
                                    "sql:field", "Type"
                        });

                        writer.endElement(); // xsd:sequence
                        writer.endElement(); // xsd:complexType
                        writer.endElement(); // xsd:element

                    } else {
                        final String xsdType = column.type.columnType;

                        String[] attrs = null;
                        if (column.nullable) {
                            if (column.unbounded) {
                                attrs = new String[] {
                                    "sql:field", column.name,
                                    "name", name,
                                    "type", xsdType,
                                    "minOccurs", "0",
                                    "maxOccurs", "unbounded"
                                };
                            } else {
                                attrs = new String[] {
                                    "sql:field", column.name,
                                    "name", name,
                                    "type", xsdType,
                                    "minOccurs", "0"
                                };
                            }
                        } else {
                            if (column.unbounded) {
                                attrs = new String[] {
                                    "sql:field", column.name,
                                    "name", name,
                                    "type", xsdType,
                                    "maxOccurs", "unbounded"
                                };
                            } else {
                                attrs = new String[] {
                                    "sql:field", column.name,
                                    "name", name,
                                    "type", xsdType
                                };
                            }
                        }
                        writer.element("xsd:element", attrs);
                    }
                }
                writer.endElement(); // xsd:sequence
                writer.endElement(); // xsd:complexType
            }
        };

        public DiscoverSchemaRowsetsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            final RowsetDefinition[] rowsetDefinitions = (RowsetDefinition[])
                    enumeration.getValuesSortedByName().
                    toArray(new RowsetDefinition[0]);
            for (int i = 0; i < rowsetDefinitions.length; i++) {
                RowsetDefinition rowsetDefinition = rowsetDefinitions[i];
                Row row = new Row();
                row.set(SchemaName.name, rowsetDefinition.name);

                // TODO: If we have a SchemaGuid output here
                //row.set(SchemaGuid.name, "");

                row.set(Restrictions.name, getRestrictions(rowsetDefinition));

                String desc = rowsetDefinition.description;
                row.set(Description.name, (desc == null) ? "" : desc);
                addRow(row, rows);
            }
        }

        private XmlElement[] getRestrictions(RowsetDefinition rowsetDefinition) {
            ArrayList restrictionList = new ArrayList();
            final Column[] columns = rowsetDefinition.columnDefinitions;
            for (int j = 0; j < columns.length; j++) {
                Column column = columns[j];
                if (column.restriction) {
                    restrictionList.add(
                            new XmlElement(Restrictions.name, null, new XmlElement[] {
                                new XmlElement("Name", null, column.name),
                                new XmlElement("Type", null, column.getColumnType())
                            }));
                }
            }
            final XmlElement[] restrictions = (XmlElement[])
                    restrictionList.toArray(
                            new XmlElement[restrictionList.size()]);
            return restrictions;
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverPropertiesRowset extends Rowset {
        private final RistrictionTest propertyNameRT;
        DiscoverPropertiesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
            propertyNameRT = getRistrictionTest(PropertyName);
        }

        private static final Column PropertyName =
            new Column(
                "PropertyName",
                Type.StringSometimesArray,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the property.");
        private static final Column PropertyDescription =
            new Column(
                "PropertyDescription",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A localizable text description of the property.");
        private static final Column PropertyType =
            new Column(
                "PropertyType",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The XML data type of the property.");
        private static final Column PropertyAccessType =
            new Column(
                "PropertyAccessType",
                Type.EnumString,
                Enumeration.Access.enumeration,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Access for the property. The value can be Read, Write, or ReadWrite.");
        private static final Column IsRequired =
            new Column(
                "IsRequired",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "True if a property is required, false if it is not required.");
        private static final Column Value =
            new Column(
                "Value",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The current value of the property.");
        /*
         *
         *
         *
         * restrictions
         *
         * Not supported
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_PROPERTIES", DISCOVER_PROPERTIES,
                "Returns a list of information and values about the requested properties that are supported by the specified data source provider.",
                new Column[] {
                    PropertyName,
                    PropertyDescription,
                    PropertyType,
                    PropertyAccessType,
                    IsRequired,
                    Value,
                },
                null /* not sorted */) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DiscoverPropertiesRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            final String[] propertyNames = PropertyDefinition.enumeration.getNames();
            for (int i = 0; i < propertyNames.length; i++) {
                String pname = propertyNames[i];
                if (! propertyNameRT.passes(pname)) {
                    continue;
                }
                PropertyDefinition propertyDefinition = 
                    PropertyDefinition.getValue(pname);
                Row row = new Row();
                row.set(PropertyName.name, propertyDefinition.name);
                row.set(PropertyDescription.name, propertyDefinition.description);
                row.set(PropertyType.name, propertyDefinition.type);
                row.set(PropertyAccessType.name, propertyDefinition.access);
                row.set(IsRequired.name, false);
                row.set(Value.name, propertyDefinition.value);
                addRow(row, rows);
            }
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverEnumeratorsRowset extends Rowset {
        DiscoverEnumeratorsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column EnumName =
            new Column(
                "EnumName",
                Type.StringArray,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the enumerator that contains a set of values.");
        private static final Column EnumDescription =
            new Column(
                "EnumDescription",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A localizable description of the enumerator.");
        private static final Column EnumType =
            new Column(
                "EnumType",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The data type of the Enum values.");
        private static final Column ElementName =
            new Column(
                "ElementName",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The name of one of the value elements in the enumerator set.\n" + "Example: TDP");
        private static final Column ElementDescription =
            new Column(
                "ElementDescription",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A localizable description of the element (optional).");
        private static final Column ElementValue =
            new Column(
                "ElementValue",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The value of the element.\n" + "Example: 01");
        /*
         *
         *
         *
         * restrictions
         *
         * Not supported
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_ENUMERATORS", DISCOVER_ENUMERATORS,
                "Returns a list of names, data types, and enumeration values for enumerators supported by the provider of a specific data source.",
                new Column[] {
                    EnumName,
                    EnumDescription,
                    EnumType,
                    ElementName,
                    ElementDescription,
                    ElementValue,
                },
                null /* not sorted */) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DiscoverEnumeratorsRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            Enumeration[] enumerators = getEnumerators();
            for (int i = 0; i < enumerators.length; i++) {
                Enumeration enumerator = enumerators[i];
                final String[] valueNames = enumerator.getNames();
                for (int j = 0; j < valueNames.length; j++) {
                    String valueName = valueNames[j];
                    final EnumeratedValues.Value value = enumerator.getValue(valueName, true);
                    Row row = new Row();
                    row.set(EnumName.name, enumerator.name);
                    row.set(EnumDescription.name, enumerator.description);

                    // Note: SQL Server always has EnumType string
                    // Need type of element of array, not the array
                    // it self.
                    row.set(EnumType.name, "string");

                    row.set(ElementName.name, value.getName());
                    row.set(ElementDescription.name, value.getDescription());
                    switch (enumerator.type.ordinal) {
                    case Type.String_ORDINAL:
                    case Type.StringArray_ORDINAL:
                        // these don't have ordinals
                        break;
                    default:
                        row.set(ElementValue.name, value.getOrdinal());
                        break;
                    }
                    addRow(row, rows);
                }
            }
        }

        private static Enumeration[] getEnumerators() {
            HashSet enumeratorSet = new HashSet();
            final String[] rowsetNames = RowsetDefinition.enumeration.getNames();
            for (int i = 0; i < rowsetNames.length; i++) {
                String rowsetName = rowsetNames[i];
                final RowsetDefinition rowsetDefinition = (RowsetDefinition)
                        RowsetDefinition.enumeration.getValue(rowsetName, true);
                for (int j = 0; j < rowsetDefinition.columnDefinitions.length; j++) {
                    Column column = rowsetDefinition.columnDefinitions[j];
                    if (column.enumeration != null) {
                        enumeratorSet.add(column.enumeration);
                    }
                }
            }
            final Enumeration[] enumerators = (Enumeration[])
                    enumeratorSet.toArray(new Enumeration[enumeratorSet.size()]);
            Arrays.sort(enumerators, new Comparator() {
                public int compare(Object o1, Object o2) {
                    return ((Enumeration) o1).name.compareTo(
                            ((Enumeration) o2).name);
                }
            });
            return enumerators;
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverKeywordsRowset extends Rowset {
        DiscoverKeywordsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column Keyword =
            new Column(
                "Keyword",
                Type.StringSometimesArray,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "A list of all the keywords reserved by a provider.\n" +
                "Example: AND");
        /*
         *
         *
         *
         * restrictions
         *
         * Not supported
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_KEYWORDS", DISCOVER_KEYWORDS,
                "Returns an XML list of keywords reserved by the provider.",
                new Column[] {
                    Keyword,
                },
                null /* not sorted */) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DiscoverKeywordsRowset(request, handler);
            }
        };

        private static final String[] keywords = new String[] {
            "$AdjustedProbability", "$Distance", "$Probability",
            "$ProbabilityStDev", "$ProbabilityStdDeV", "$ProbabilityVariance",
            "$StDev", "$StdDeV", "$Support", "$Variance",
            "AddCalculatedMembers", "Action", "After", "Aggregate", "All",
            "Alter", "Ancestor", "And", "Append", "As", "ASC", "Axis",
            "Automatic", "Back_Color", "BASC", "BDESC", "Before",
            "Before_And_After", "Before_And_Self", "Before_Self_After",
            "BottomCount", "BottomPercent", "BottomSum", "Break", "Boolean",
            "Cache", "Calculated", "Call", "Case", "Catalog_Name", "Cell",
            "Cell_Ordinal", "Cells", "Chapters", "Children",
            "Children_Cardinality", "ClosingPeriod", "Cluster",
            "ClusterDistance", "ClusterProbability", "Clusters",
            "CoalesceEmpty", "Column_Values", "Columns", "Content",
            "Contingent", "Continuous", "Correlation", "Cousin", "Covariance",
            "CovarianceN", "Create", "CreatePropertySet", "CrossJoin", "Cube",
            "Cube_Name", "CurrentMember", "CurrentCube", "Custom", "Cyclical",
            "DefaultMember", "Default_Member", "DESC", "Descendents",
            "Description", "Dimension", "Dimension_Unique_Name", "Dimensions",
            "Discrete", "Discretized", "DrillDownLevel",
            "DrillDownLevelBottom", "DrillDownLevelTop", "DrillDownMember",
            "DrillDownMemberBottom", "DrillDownMemberTop", "DrillTrough",
            "DrillUpLevel", "DrillUpMember", "Drop", "Else", "Empty", "End",
            "Equal_Areas", "Exclude_Null", "ExcludeEmpty", "Exclusive",
            "Expression", "Filter", "FirstChild", "FirstRowset",
            "FirstSibling", "Flattened", "Font_Flags", "Font_Name",
            "Font_size", "Fore_Color", "Format_String", "Formatted_Value",
            "Formula", "From", "Generate", "Global", "Head", "Hierarchize",
            "Hierarchy", "Hierary_Unique_name", "IIF", "IsEmpty",
            "Include_Null", "Include_Statistics", "Inclusive", "Input_Only",
            "IsDescendant", "Item", "Lag", "LastChild", "LastPeriods",
            "LastSibling", "Lead", "Level", "Level_Unique_Name", "Levels",
            "LinRegIntercept", "LinRegR2", "LinRegPoint", "LinRegSlope",
            "LinRegVariance", "Long", "MaxRows", "Median", "Member",
            "Member_Caption", "Member_Guid", "Member_Name", "Member_Ordinal",
            "Member_Type", "Member_Unique_Name", "Members",
            "Microsoft_Clustering", "Microsoft_Decision_Trees", "Mining",
            "Model", "Model_Existence_Only", "Models", "Move", "MTD", "Name",
            "Nest", "NextMember", "Non", "Normal", "Not", "Ntext", "Nvarchar",
            "OLAP", "On", "OpeningPeriod", "OpenQuery", "Or", "Ordered",
            "Ordinal", "Pages", "Pages", "ParallelPeriod", "Parent",
            "Parent_Level", "Parent_Unique_Name", "PeriodsToDate", "PMML",
            "Predict", "Predict_Only", "PredictAdjustedProbability",
            "PredictHistogram", "Prediction", "PredictionScore",
            "PredictProbability", "PredictProbabilityStDev",
            "PredictProbabilityVariance", "PredictStDev", "PredictSupport",
            "PredictVariance", "PrevMember", "Probability",
            "Probability_StDev", "Probability_StdDev", "Probability_Variance",
            "Properties", "Property", "QTD", "RangeMax", "RangeMid",
            "RangeMin", "Rank", "Recursive", "Refresh", "Related", "Rename",
            "Rollup", "Rows", "Schema_Name", "Sections", "Select", "Self",
            "Self_And_After", "Sequence_Time", "Server", "Session", "Set",
            "SetToArray", "SetToStr", "Shape", "Skip", "Solve_Order", "Sort",
            "StdDev", "Stdev", "StripCalculatedMembers", "StrToSet",
            "StrToTuple", "SubSet", "Support", "Tail", "Text", "Thresholds",
            "ToggleDrillState", "TopCount", "TopPercent", "TopSum",
            "TupleToStr", "Under", "Uniform", "UniqueName", "Use", "Value",
            "Value", "Var", "Variance", "VarP", "VarianceP", "VisualTotals",
            "When", "Where", "With", "WTD", "Xor",
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            for (int i = 0; i < keywords.length; i++) {
                String keyword = keywords[i];
                Row row = new Row();
                row.set(Keyword.name, keyword);
                addRow(row, rows);
            }
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverLiteralsRowset extends Rowset {
        DiscoverLiteralsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column LiteralName = new Column(
            "LiteralName",
            Type.StringSometimesArray,
            null,
            Column.RESTRICTION,
            Column.REQUIRED,
            "The name of the literal described in the row.\n" + "Example: DBLITERAL_LIKE_PERCENT");

        private static final Column LiteralValue = new Column(
            "LiteralValue",
            Type.String,
            null,
            Column.NOT_RESTRICTION,
            Column.REQUIRED,
            "Contains the actual literal value.\n" + "Example, if LiteralName is DBLITERAL_LIKE_PERCENT and the percent character (%) is used to match zero or more characters in a LIKE clause, this column's value would be \"%\".");

        private static final Column LiteralInvalidChars = new Column(
            "LiteralInvalidChars",
            Type.String,
            null,
            Column.NOT_RESTRICTION,
            Column.REQUIRED,
            "The characters, in the literal, that are not valid.\n" + "For example, if table names can contain anything other than a numeric character, this string would be \"0123456789\".");

        private static final Column LiteralInvalidStartingChars = new Column(
            "LiteralInvalidStartingChars",
            Type.String,
            null,
            Column.NOT_RESTRICTION,
            Column.REQUIRED,
            "The characters that are not valid as the first character of the literal. If the literal can start with any valid character, this is null.");

        private static final Column LiteralMaxLength = new Column(
            "LiteralMaxLength",
            Type.Integer,
            null,
            Column.NOT_RESTRICTION,
            Column.REQUIRED,
            "The maximum number of characters in the literal. If there is no maximum or the maximum is unknown, the value is ?1.");

        /*
        *
        *
        *
        * restrictions
        *
        * Not supported
        */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_LITERALS", DISCOVER_LITERALS,
                "Returns information about literals supported by the provider.",
                new Column[] {
                    LiteralName,
                    LiteralValue,
                    LiteralInvalidChars,
                    LiteralInvalidStartingChars,
                    LiteralMaxLength,
                },
                null /* not sorted */) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DiscoverLiteralsRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            emit(Enumeration.Literal.enumeration, response);
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }

    }

    static class DbschemaCatalogsRowset extends Rowset {
        private final RistrictionTest catalogNameRT;
        DbschemaCatalogsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
            catalogNameRT = getRistrictionTest(CatalogName);
        }

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Catalog name. Cannot be NULL.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Human-readable description of the catalog.");
        private static final Column Roles =
            new Column(
                "ROLES",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A comma delimited list of roles to which the current user belongs. An asterisk (*) is included as a role if the current user is a server or database administrator. Username is appended to ROLES if one of the roles uses dynamic security.");
        private static final Column DateModified =
            new Column(
                "DATE_MODIFIED",
                Type.DateTime,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The date that the catalog was last modified.");

        /*
         *
         *
         *
         * restrictions
         *
         * Not supported
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_CATALOGS", DBSCHEMA_CATALOGS,
                "Returns information about literals supported by the provider.",
                new Column[] {
                    CatalogName,
                    Description,
                    Roles,
                    DateModified,
                },
                new Column[] {
                    CatalogName,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DbschemaCatalogsRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs = ds.catalogs.catalogs;
            String role = request.getRole();
            for (int i = 0; i < catalogs.length; i++) {
                DataSourcesConfig.Catalog dsCatalog = catalogs[i];
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                Connection connection = handler.getConnection(dsCatalog, role);
                if (connection == null) {
                    continue;
                }
                if (! catalogNameRT.passes(dsCatalog.name)) {
                    continue;
                }
                final RolapSchema schema = (RolapSchema) connection.getSchema();

                Row row = new Row();
                row.set(CatalogName.name, dsCatalog.name);

                // TODO: currently schema grammar does not support a description
                row.set(Description.name, "No description available");

                // get Role names
                // TODO: this returns ALL roles, no the current user's roles
                StringBuffer buf = new StringBuffer(100);
                Iterator roleNamesIt = schema.roleNames().iterator();
                while (roleNamesIt.hasNext()) {
                    String roleName = (String) roleNamesIt.next();
                    buf.append(roleName);
                    if (roleNamesIt.hasNext()) {
                        buf.append(',');
                    }
                }
                row.set(Roles.name, buf.toString());

                // TODO: currently schema grammar does not support modify date
                // so we return just some date for now.
                if (false) row.set(DateModified.name, dateModified);
                addRow(row, rows);
            }
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DbschemaColumnsRowset extends Rowset {
        private final RistrictionTest tableCatalogRT;
        private final RistrictionTest tableNameRT;
        private final RistrictionTest columnNameRT;
        DbschemaColumnsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
            tableCatalogRT = getRistrictionTest(TableCatalog);
            tableNameRT = getRistrictionTest(TableName);
            columnNameRT = getRistrictionTest(ColumnName);
        }

        private static final Column TableCatalog =
            new Column(
                "TABLE_CATALOG",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the Database.");
        private static final Column TableSchema =
            new Column(
                "TABLE_SCHEMA",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                null);
        private static final Column TableName =
            new Column(
                "TABLE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the cube.");
        private static final Column ColumnName =
            new Column(
                "COLUMN_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the attribute hierarchy or measure.");
        private static final Column OrdinalPosition =
            new Column(
                "ORDINAL_POSITION",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The position of the column, beginning with 1.");
        private static final Column ColumnHasDefault =
            new Column(
                "COLUMN_HAS_DEFAULT",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Not supported.");
        /*
         *  A bitmask indicating the information stored in
         *      DBCOLUMNFLAGS in OLE DB.
         *  1 = Bookmark
         *  2 = Fixed length
         *  4 = Nullable
         *  8 = Row versioning
         *  16 = Updateable column
         *
         * And, of course, MS SQL Server sometimes has the value of 80!!
        */
        private static final Column ColumnFlags =
            new Column(
                "COLUMN_FLAGS",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A DBCOLUMNFLAGS bitmask indicating column properties.");
        private static final Column IsNullable =
            new Column(
                "IS_NULLABLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Always returns false.");
        private static final Column DataType =
            new Column(
                "DATA_TYPE",
                Type.UnsignedShort,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The data type of the column. Returns a string for dimension columns and a variant for measures.");
        private static final Column CharacterMaximumLength =
            new Column(
                "CHARACTER_MAXIMUM_LENGTH",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The maximum possible length of a value within the column.");
        private static final Column CharacterOctetLength =
            new Column(
                "CHARACTER_OCTET_LENGTH",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The maximum possible length of a value within the column, in bytes, for character or binary columns.");
        private static final Column NumericPrecision =
            new Column(
                "NUMERIC_PRECISION",
                Type.UnsignedShort,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The maximum precision of the column for numeric data types other than DBTYPE_VARNUMERIC.");
        private static final Column NumericScale =
            new Column(
                "NUMERIC_SCALE",
                Type.Short,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The number of digits to the right of the decimal point for DBTYPE_DECIMAL, DBTYPE_NUMERIC, DBTYPE_VARNUMERIC. Otherwise, this is NULL.");
        /*
         *
         *
         *
         * restrictions
         *
         * Not supported
         *    COLUMN_OLAP_TYPE
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_COLUMNS", DBSCHEMA_COLUMNS, null,
                new Column[] {
                    TableCatalog,
                    TableSchema,
                    TableName,
                    ColumnName,
                    OrdinalPosition,
                    ColumnHasDefault,
                    ColumnFlags,
                    IsNullable,
                    DataType,
                    CharacterMaximumLength,
                    CharacterOctetLength,
                    NumericPrecision,
                    NumericScale,
                },
                new Column[] {
                    TableCatalog,
                    TableSchema,
                    TableName,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DbschemaColumnsRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs = ds.catalogs.catalogs;
            String roleStr = request.getRole();
            for (int g = 0; g < catalogs.length; g++) {
                DataSourcesConfig.Catalog dsCatalog = catalogs[g];
                if (dsCatalog == null || dsCatalog.definition == null) {
                     continue;
                }
                Connection connection = 
                    handler.getConnection(dsCatalog, roleStr);
                if (connection == null) {
                    continue;
                }
                Role role = connection.getSchema().lookupRole(roleStr);
                final RolapSchema schema = (RolapSchema) connection.getSchema();
                String catalogName = dsCatalog.name;
                if (! tableCatalogRT.passes(catalogName)) {
                    continue;
                }
                //final String schemaName = schema.getName();
                Cube[] cubes = schema.getCubes();

                int ordinalPosition = 1;
                Row row = null;

                for (int i = 0; i < cubes.length; i++) {
                    RolapCube cube = (RolapCube) cubes[i];
                    SchemaReader schemaReader = cube.getSchemaReader(role);
                    String cubeName = cube.getName();
                    if (! tableNameRT.passes(cubeName)) {
                        continue;
                    }
                    Dimension[] dims = cube.getDimensions();
                    for (int j = 1; j < dims.length; j++) {
                        Dimension dimension = dims[j];

                        Hierarchy[] hierarchies = dimension.getHierarchies();
                        for (int h = 0; h < hierarchies.length; h++) {
                            HierarchyBase hierarchy = (HierarchyBase) hierarchies[h];
                            ordinalPosition = populateHierarchy(
                                schemaReader, cube, hierarchy,
                                ordinalPosition, rows);
                        }
                    }

                    RolapMember[] rms = cube.getMeasuresMembers();
                    for (int k = 1; k < rms.length; k++) {
                        RolapMember member = rms[k];

                        // null == true for regular cubes
                        // virtual cubes do not set the visible property
                        // on its measures so it might be null.
                        Boolean isVisible = (Boolean)
                                member.getPropertyValue(Property.VISIBLE.name);
                        if ((isVisible != null) && (! isVisible.booleanValue())) {
                            continue;
                        }

                        String memberName = member.getName();
                        if (! columnNameRT.passes("Measures:" + memberName)) {
                            continue;
                        }

                        row = new Row();
                        row.set(TableCatalog.name, catalogName);
                        row.set(TableName.name, cubeName);
                        row.set(ColumnName.name, "Measures:" + memberName);
                        row.set(OrdinalPosition.name, ordinalPosition++);
                        row.set(ColumnHasDefault.name, false);
                        row.set(ColumnFlags.name, 0);
                        row.set(IsNullable.name, false);
                        // TODO: here is where one tries to determine the
                        // type of the column - since these are all
                        // Measures, aggregate Measures??, maybe they
                        // are all numeric? (or currency)
                        row.set(DataType.name, DBType.R8_ORDINAL);
                        // TODO: 16/255 seems to be what MS SQL Server
                        // always returns.
                        row.set(NumericPrecision.name, 16);
                        row.set(NumericScale.name, 255);
                        addRow(row, rows);
                    }
                }
            }
        }

        private int populateHierarchy(
            SchemaReader schemaReader,
            RolapCube cube,
            HierarchyBase hierarchy,
            int ordinalPosition,
            List rows) {

            // Access control
            if (!canAccess(schemaReader, hierarchy)) {
                return ordinalPosition;
            }
            String schemaName = cube.getSchema().getName();
            String cubeName = cube.getName();
            String hierarchyName = hierarchy.getName();

            if (hierarchy.hasAll()) {
                Row row = new Row();
                row.set(TableCatalog.name, schemaName);
                row.set(TableName.name, cubeName);
                row.set(ColumnName.name, hierarchyName + ":(All)!NAME");
                row.set(OrdinalPosition.name, ordinalPosition++);
                row.set(ColumnHasDefault.name, false);
                row.set(ColumnFlags.name, 0);
                row.set(IsNullable.name, false);
                // names are always WSTR
                row.set(DataType.name, DBType.WSTR_ORDINAL);
                row.set(CharacterMaximumLength.name, 0);
                row.set(CharacterOctetLength.name, 0);
                addRow(row, rows);

                row = new Row();
                row.set(TableCatalog.name, schemaName);
                row.set(TableName.name, cubeName);
                row.set(ColumnName.name, hierarchyName + ":(All)!UNIQUE_NAME");
                row.set(OrdinalPosition.name, ordinalPosition++);
                row.set(ColumnHasDefault.name, false);
                row.set(ColumnFlags.name, 0);
                row.set(IsNullable.name, false);
                // names are always WSTR
                row.set(DataType.name, DBType.WSTR_ORDINAL);
                row.set(CharacterMaximumLength.name, 0);
                row.set(CharacterOctetLength.name, 0);
                addRow(row, rows);

                if (false) {
                    // TODO: SQLServer outputs this hasall KEY column name -
                    // don't know what it's for
                    row = new Row();
                    row.set(TableCatalog.name, schemaName);
                    row.set(TableName.name, cubeName);
                    row.set(ColumnName.name, hierarchyName + ":(All)!KEY");
                    row.set(OrdinalPosition.name, ordinalPosition++);
                    row.set(ColumnHasDefault.name, false);
                    row.set(ColumnFlags.name, 0);
                    row.set(IsNullable.name, false);
                    // names are always BOOL
                    row.set(DataType.name, DBType.BOOL_ORDINAL);
                    row.set(NumericPrecision.name, 255);
                    row.set(NumericScale.name, 255);
                    addRow(row, rows);
                }
            }

            Level[] levels = hierarchy.getLevels();
            for (int k = 0; k < levels.length; k++) {
                Level level = levels[k];
                ordinalPosition = populateLevel(
                    cube, hierarchy, level, ordinalPosition, rows);
            }
            return ordinalPosition;
        }

        private int populateLevel(
            Cube cube,
            HierarchyBase hierarchy,
            Level level,
            int ordinalPosition, List rows) {

            String schemaName = cube.getSchema().getName();
            String cubeName = cube.getName();
            String hierarchyName = hierarchy.getName();
            String levelName = level.getName();

            Row row = new Row();
            row.set(TableCatalog.name, schemaName);
            row.set(TableName.name, cubeName);
            row.set(ColumnName.name,
                hierarchyName + ':' + levelName + "!NAME");
            row.set(OrdinalPosition.name, ordinalPosition++);
            row.set(ColumnHasDefault.name, false);
            row.set(ColumnFlags.name, 0);
            row.set(IsNullable.name, false);
            // names are always WSTR
            row.set(DataType.name, DBType.WSTR_ORDINAL);
            row.set(CharacterMaximumLength.name, 0);
            row.set(CharacterOctetLength.name, 0);
            addRow(row, rows);

            row = new Row();
            row.set(TableCatalog.name, schemaName);
            row.set(TableName.name, cubeName);
            row.set(ColumnName.name,
                hierarchyName + ':' + levelName + "!UNIQUE_NAME");
            row.set(OrdinalPosition.name, ordinalPosition++);
            row.set(ColumnHasDefault.name, false);
            row.set(ColumnFlags.name, 0);
            row.set(IsNullable.name, false);
            // names are always WSTR
            row.set(DataType.name, DBType.WSTR_ORDINAL);
            row.set(CharacterMaximumLength.name, 0);
            row.set(CharacterOctetLength.name, 0);
            addRow(row, rows);

/*
TODO: see above
            row = new Row();
            row.set(TableCatalog.name, schemaName);
            row.set(TableName.name, cubeName);
            row.set(ColumnName.name,
                hierarchyName + ":" + levelName + "!KEY");
            row.set(OrdinalPosition.name, ordinalPosition++);
            row.set(ColumnHasDefault.name, false);
            row.set(ColumnFlags.name, 0);
            row.set(IsNullable.name, false);
            // names are always BOOL
            row.set(DataType.name, DBType.BOOL_ORDINAL);
            row.set(NumericPrecision.name, 255);
            row.set(NumericScale.name, 255);
            addRow(row, rows);
*/
            Property[] props = level.getProperties();
            for (int m = 0; m < props.length; m++) {
                Property prop = props[m];
                String propName = prop.getName();

                row = new Row();
                row.set(TableCatalog.name, schemaName);
                row.set(TableName.name, cubeName);
                row.set(ColumnName.name,
                    hierarchyName + ':' + levelName + '!' + propName);
                row.set(OrdinalPosition.name, ordinalPosition++);
                row.set(ColumnHasDefault.name, false);
                row.set(ColumnFlags.name, 0);
                row.set(IsNullable.name, false);

                DBType dbType = getDBTypeFromProperty(prop);
                row.set(DataType.name, dbType.getOrdinal());

                switch (prop.getType()) {
                case Property.TYPE_STRING :
                    row.set(CharacterMaximumLength.name, 0);
                    row.set(CharacterOctetLength.name, 0);
                    break;
                case Property.TYPE_NUMERIC :
                    // TODO: 16/255 seems to be what MS SQL Server
                    // always returns.
                    row.set(NumericPrecision.name, 16);
                    row.set(NumericScale.name, 255);
                    break;
                case Property.TYPE_BOOLEAN :
                    row.set(NumericPrecision.name, 255);
                    row.set(NumericScale.name, 255);
                    break;
                case Property.TYPE_OTHER :
                    // TODO: what type is it really, its
                    // not a string
                    row.set(CharacterMaximumLength.name, 0);
                    row.set(CharacterOctetLength.name, 0);
                    break;
                }
                addRow(row, rows);
            }
            return ordinalPosition;
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DbschemaProviderTypesRowset extends Rowset {
        private final RistrictionTest dataTypeRT;
        DbschemaProviderTypesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
            dataTypeRT = getRistrictionTest(DataType);
        }

/*
DATA_TYPE DBTYPE_UI2
BEST_MATCH DBTYPE_BOOL
Column(String name, Type type, Enumeration enumeratedType,
boolean restriction, boolean nullable, String description)
*/
        /*
         * These are the columns returned by SQL Server.
         */
        private static final Column TypeName =
            new Column(
                "TYPE_NAME",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The provider-specific data type name.");
        private static final Column DataType =
            new Column(
                "DATA_TYPE",
                Type.UnsignedShort,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The indicator of the data type.");
        private static final Column ColumnSize =
            new Column(
                "COLUMN_SIZE",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The length of a non-numeric column. If the data type is numeric, this is the upper bound on the maximum precision of the data type.");
        private static final Column LiteralPrefix =
            new Column(
                "LITERAL_PREFIX",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The character or characters used to prefix a literal of this type in a text command.");
        private static final Column LiteralSuffix =
            new Column(
                "LITERAL_SUFFIX",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The character or characters used to suffix a literal of this type in a text command.");
        private static final Column IsNullable =
            new Column(
                "IS_NULLABLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type is nullable. NULL-- indicates that it is not known whether the data type is nullable.");
        private static final Column CaseSensitive =
            new Column(
                "CASE_SENSITIVE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type is a characters type and case-sensitive.");
        private static final Column Searchable =
            new Column(
                "SEARCHABLE",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "An integer indicating how the data type can be used in searches if the provider supports ICommandText; otherwise, NULL.");
        private static final Column UnsignedAttribute =
            new Column(
                "UNSIGNED_ATTRIBUTE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type is unsigned.");
        private static final Column FixedPrecScale =
            new Column(
                "FIXED_PREC_SCALE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type has a fixed precision and scale.");
        private static final Column AutoUniqueValue =
            new Column(
                "AUTO_UNIQUE_VALUE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type is autoincrementing.");
        private static final Column IsLong =
            new Column(
                "IS_LONG",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type is a binary large object (BLOB) and has very long data.");
        private static final Column BestMatch =
            new Column(
                "BEST_MATCH",
                Type.Boolean,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type is a best match.");
        /*
         *
         *
         *
         * restrictions
         *
         * Not supported
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_PROVIDER_TYPES", DBSCHEMA_PROVIDER_TYPES, null,
                new Column[] {
                    TypeName,
                    DataType,
                    ColumnSize,
                    LiteralPrefix,
                    LiteralSuffix,
                    IsNullable,
                    CaseSensitive,
                    Searchable,
                    UnsignedAttribute,
                    FixedPrecScale,
                    AutoUniqueValue,
                    IsLong,
                    BestMatch,
                },
                new Column[] {
                    DataType,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DbschemaProviderTypesRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            // Identifies the (base) data types supported by the data provider.
            Row row = null;

            // i4
            Integer dt = new Integer(DBType.I4_ORDINAL);
            if (dataTypeRT.passes(dt)) {
                row = new Row();
                row.set(TypeName.name, DBType.I4.name);
                row.set(DataType.name, dt);
                row.set(ColumnSize.name, 8);
                row.set(IsNullable.name, true);
                row.set(Searchable.name, null);
                row.set(UnsignedAttribute.name, false);
                row.set(FixedPrecScale.name, false);
                row.set(AutoUniqueValue.name, false);
                row.set(IsLong.name, false);
                row.set(BestMatch.name, true);
                addRow(row, rows);
            }

            // R8
            dt = new Integer(DBType.R8_ORDINAL);
            if (dataTypeRT.passes(dt)) {
                row = new Row();
                row.set(TypeName.name, DBType.R8.name);
                row.set(DataType.name, dt);
                row.set(ColumnSize.name, 16);
                row.set(IsNullable.name, true);
                row.set(Searchable.name, null);
                row.set(UnsignedAttribute.name, false);
                row.set(FixedPrecScale.name, false);
                row.set(AutoUniqueValue.name, false);
                row.set(IsLong.name, false);
                row.set(BestMatch.name, true);
                addRow(row, rows);
            }

            // CY
            dt = new Integer(DBType.CY_ORDINAL);
            if (dataTypeRT.passes(dt)) {
                row = new Row();
                row.set(TypeName.name, DBType.CY.name);
                row.set(DataType.name, dt);
                row.set(ColumnSize.name, 8);
                row.set(IsNullable.name, true);
                row.set(Searchable.name, null);
                row.set(UnsignedAttribute.name, false);
                row.set(FixedPrecScale.name, false);
                row.set(AutoUniqueValue.name, false);
                row.set(IsLong.name, false);
                row.set(BestMatch.name, true);
                addRow(row, rows);
            }

            // BOOL
            dt = new Integer(DBType.BOOL_ORDINAL);
            if (dataTypeRT.passes(dt)) {
                row = new Row();
                row.set(TypeName.name, DBType.BOOL.name);
                row.set(DataType.name, dt);
                row.set(ColumnSize.name, 1);
                row.set(IsNullable.name, true);
                row.set(Searchable.name, null);
                row.set(UnsignedAttribute.name, false);
                row.set(FixedPrecScale.name, false);
                row.set(AutoUniqueValue.name, false);
                row.set(IsLong.name, false);
                row.set(BestMatch.name, true);
                addRow(row, rows);
            }

            // I8
            dt = new Integer(DBType.I8_ORDINAL);
            if (dataTypeRT.passes(dt)) {
                row = new Row();
                row.set(TypeName.name, DBType.I8.name);
                row.set(DataType.name, dt);
                row.set(ColumnSize.name, 16);
                row.set(IsNullable.name, true);
                row.set(Searchable.name, null);
                row.set(UnsignedAttribute.name, false);
                row.set(FixedPrecScale.name, false);
                row.set(AutoUniqueValue.name, false);
                row.set(IsLong.name, false);
                row.set(BestMatch.name, true);
                addRow(row, rows);
            }

            // WSTR
            dt = new Integer(DBType.WSTR_ORDINAL);
            if (dataTypeRT.passes(dt)) {
                row = new Row();
                row.set(TypeName.name, DBType.WSTR.name);
                row.set(DataType.name, dt);
                // how big are the string columns in the db
                row.set(ColumnSize.name, 255);
                row.set(LiteralPrefix.name, "\"");
                row.set(LiteralSuffix.name, "\"");
                row.set(IsNullable.name, true);
                row.set(CaseSensitive.name, false);
                row.set(Searchable.name, null);
                row.set(FixedPrecScale.name, false);
                row.set(AutoUniqueValue.name, false);
                row.set(IsLong.name, false);
                row.set(BestMatch.name, true);
                addRow(row, rows);
            }
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DbschemaTablesRowset extends Rowset {
        private final RistrictionTest tableCatalogRT;
        private final RistrictionTest tableNameRT;
        private final RistrictionTest tableTypeRT;
        DbschemaTablesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
            tableCatalogRT = getRistrictionTest(TableCatalog);
            tableNameRT = getRistrictionTest(TableName);
            tableTypeRT = getRistrictionTest(TableType);
        }

        private static final Column TableCatalog =
            new Column(
                "TABLE_CATALOG",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the catalog to which this object belongs.");
        private static final Column TableSchema =
            new Column(
                "TABLE_SCHEMA",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the cube to which this object belongs.");
        private static final Column TableName =
            new Column(
                "TABLE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the object, if TABLE_TYPE is TABLE.");
        private static final Column TableType =
            new Column(
                "TABLE_TYPE",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The type of the table. TABLE indicates the object is a measure group. SYSTEM TABLE indicates the object is a dimension.");

        private static final Column TableGuid =
            new Column(
                "TABLE_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Not supported.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A human-readable description of the object.");
        private static final Column TablePropId =
            new Column(
                "TABLE_PROPID",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Not supported.");
        private static final Column DateCreated =
            new Column(
                "DATE_CREATED",
                Type.DateTime,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Not supported.");
        private static final Column DateModified =
            new Column(
                "DATE_MODIFIED",
                Type.DateTime,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The date the object was last modified.");
/*
        private static final Column TableOlapType =
            new Column(
                "TABLE_OLAP_TYPE",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The OLAP type of the object.  MEASURE_GROUP indicates the object is a measure group.  CUBE_DIMENSION indicated the object is a dimension.");

*/
        /*
         * http://msdn2.microsoft.com/en-us/library/ms126299(SQL.90).aspx
         *
         * restrictions:
         *   TABLE_CATALOG Optional
         *   TABLE_SCHEMA Optional
         *   TABLE_NAME Optional
         *   TABLE_TYPE Optional
         *   TABLE_OLAP_TYPE Optional
         *
         * Not supported
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_TABLES", DBSCHEMA_TABLES, null,
                new Column[] {
                    TableCatalog,
                    TableSchema,
                    TableName,
                    TableType,
                    TableGuid,
                    Description,
                    TablePropId,
                    DateCreated,
                    DateModified,
                    //TableOlapType,
                },
                new Column[] {
                    TableType,
                    TableCatalog,
                    TableSchema,
                    TableName,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DbschemaTablesRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                            handler.getCatalogs(request, ds);
            String roleStr = request.getRole();

            for (int i = 0; i < catalogs.length; i++) {
                DataSourcesConfig.Catalog dsCatalog = catalogs[i];
                if (dsCatalog == null || dsCatalog.definition == null) {
                     continue;
                }
                Connection connection = 
                        handler.getConnection(dsCatalog, roleStr);
                if (connection == null) {
                    continue;
                }
                Role role = connection.getSchema().lookupRole(roleStr);
                final RolapSchema schema = (RolapSchema) connection.getSchema();
                String catalogName = dsCatalog.name;
                if (! tableCatalogRT.passes(catalogName)) {
                    continue;
                }

                //final String schemaName = schema.getName();
                Cube[] cubes = schema.getCubes();

                Row row = null;
                for (int j = 0; j < cubes.length; j++) {
                    RolapCube cube = (RolapCube) cubes[j];
                    String cubeName = cube.getName();

                    String tableName = cubeName;
                    if (! tableNameRT.passes(tableName)) {
                        continue;
                    }
                    SchemaReader schemaReader = cube.getSchemaReader(role);

                    String desc = cube.getDescription();
                    if (desc == null) {
                        //TODO: currently this is always null
                        desc = catalogName + " - " + cubeName + " Cube";
                    }


                    if (tableTypeRT.passes("TABLE")) {
                        row = new Row();
                        row.set(TableCatalog.name, catalogName);
                        row.set(TableName.name, tableName);
                        row.set(TableType.name, "TABLE");
                        row.set(Description.name, desc);
                        if (false) row.set(DateModified.name, dateModified);
                        addRow(row, rows);
                    }


                    if (tableTypeRT.passes("SYSTEM TABLE")) {
                        Dimension[] dims = cube.getDimensions();
                        for (int k = 1; k < dims.length; k++) {
                            Dimension dimension = dims[k];

                            Hierarchy[] hierarchies = dimension.getHierarchies();
                            for (int h = 0; h < hierarchies.length; h++) {
                                HierarchyBase hierarchy = 
                                    (HierarchyBase) hierarchies[h];
                                populateHierarchy(schemaReader, cube, 
                                        hierarchy, rows);
                            }
                        }
                    }
                }
            }
        }

        private void populateHierarchy(
            SchemaReader schemaReader,
            RolapCube cube,
            HierarchyBase hierarchy,
            List rows) {

            // Access control
            if (!canAccess(schemaReader, hierarchy)) {
                return;
            }
/*
            String schemaName = cube.getSchema().getName();
            String cubeName = cube.getName();
            String hierarchyName = hierarchy.getName();

            String desc = hierarchy.getDescription();
            if (desc == null) {
                //TODO: currently this is always null
                desc = schemaName +
                    " - " +
                    cubeName +
                    " Cube - " +
                    hierarchyName +
                    " Hierarchy";
            }

            if (hierarchy.hasAll()) {
                String tableName = cubeName +
                    ':' + hierarchyName + ':' + "(All)";

                Row row = new Row();
                row.set(TableCatalog.name, schemaName);
                row.set(TableName.name, tableName);
                row.set(TableType.name, "SYSTEM TABLE");
                row.set(Description.name, desc);
                row.set(DateModified.name, dateModified);
                addRow(row, rows);
            }
*/
            Level[] levels = hierarchy.getLevels();
            for (int k = 0; k < levels.length; k++) {
                Level level = levels[k];
                populateLevel(cube, hierarchy, level, rows);

            }
        }

        private void populateLevel(
            RolapCube cube,
            HierarchyBase hierarchy,
            Level level,
            List rows) {

            String schemaName = cube.getSchema().getName();
            String cubeName = cube.getName();
            String hierarchyName = hierarchy.getName();
            String levelName = level.getName();

            String tableName = cubeName +
                ':' + hierarchyName + ':' + levelName;

            String desc = level.getDescription();
            if (desc == null) {
                //TODO: currently this is always null
                desc = schemaName +
                    " - " +
                    cubeName +
                    " Cube - " +
                    hierarchyName +
                    " Hierarchy - " +
                    levelName +
                    " Level";
            }

            Row row = new Row();
            row.set(TableCatalog.name, schemaName);
            row.set(TableName.name, tableName);
            row.set(TableType.name, "SYSTEM TABLE");
            row.set(Description.name, desc);
            if (false) row.set(DateModified.name, dateModified);
            addRow(row, rows);
        }

        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    // TODO: Is this needed????
    static class DbschemaTablesInfoRowset extends Rowset {
        DbschemaTablesInfoRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column TableCatalog =
            new Column(
                "TABLE_CATALOG",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "Catalog name. NULL if the provider does not support catalogs.");
        private static final Column TableSchema =
            new Column(
                "TABLE_SCHEMA",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "Unqualified schema name. NULL if the provider does not support schemas.");
        private static final Column TableName =
            new Column(
                "TABLE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Table name.");
        private static final Column TableType =
            new Column(
                "TABLE_TYPE",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Table type. One of the following or a provider-specific value: ALIAS, TABLE, SYNONYM, SYSTEM TABLE, VIEW, GLOBAL TEMPORARY, LOCAL TEMPORARY, EXTERNAL TABLE, SYSTEM VIEW");
        private static final Column TableGuid =
            new Column(
                "TABLE_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "GUID that uniquely identifies the table. Providers that do not use GUIDs to identify tables should return NULL in this column.");

        private static final Column Bookmarks =
            new Column(
                "BOOKMARKS",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Whether this table supports bookmarks. Allways is false.");
        private static final Column BookmarkType =
            new Column(
                "BOOKMARK_TYPE",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Default bookmark type supported on this table.");
        private static final Column BookmarkDataType =
            new Column(
                "BOOKMARK_DATATYPE",
                Type.UnsignedShort,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The indicator of the bookmark's native data type.");
        private static final Column BookmarkMaximumLength =
            new Column(
                "BOOKMARK_MAXIMUM_LENGTH",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Maximum length of the bookmark in bytes.");
        private static final Column BookmarkInformation =
            new Column(
                "BOOKMARK_INFORMATION",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A bitmask specifying additional information about bookmarks over the rowset. ");
        private static final Column TableVersion =
            new Column(
                "TABLE_VERSION",
                Type.Long,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Version number for this table or NULL if the provider does not support returning table version information.");
        private static final Column Cardinality =
            new Column(
                "CARDINALITY",
                Type.UnsignedLong,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Cardinality (number of rows) of the table.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Human-readable description of the table.");
        private static final Column TablePropId =
            new Column(
                "TABLE_PROPID",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Property ID of the table. Return null.");

        /*
         * http://msdn.microsoft.com/library/default.asp?url=/library/en-us/oledb/htm/oledbtables_info_rowset.asp
         *
         *
         * restrictions
         *
         * Not supported
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_TABLES_INFO", DBSCHEMA_TABLES_INFO, null,
                new Column[] {
                    TableCatalog,
                    TableSchema,
                    TableName,
                    TableType,
                    TableGuid,
                    Bookmarks,
                    BookmarkType,
                    BookmarkDataType,
                    BookmarkMaximumLength,
                    BookmarkInformation,
                    TableVersion,
                    Cardinality,
                    Description,
                    TablePropId,
                },
                null /* cannot find doc -- presume unsorted */) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DbschemaTablesInfoRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                            handler.getCatalogs(request, ds);
            String role = request.getRole();

            for (int h = 0; h < catalogs.length; h++) {
                DataSourcesConfig.Catalog dsCatalog = catalogs[h];
                if (dsCatalog == null || dsCatalog.definition == null) {
                     continue;
                }
                Connection connection = handler.getConnection(dsCatalog, role);
                if (connection == null) {
                    continue;
                }
                final RolapSchema schema = (RolapSchema) connection.getSchema();
                String catalogName = dsCatalog.name;
                //final String catalogName = schema.getName();
                Cube[] cubes = schema.getCubes();

                //TODO: Is this cubes or tables? SQL Server returns what
                // in foodmart are cube names for TABLE_NAME
                // http://msdn.microsoft.com/library/default.asp?url=/library/en-us/oledb/htm/oledbtables_info_rowset.asp
                for (int i = 0; i < cubes.length; i++) {
                    RolapCube cube = (RolapCube) cubes[i];
                    String cubeName = cube.getName();
                    String desc = cube.getDescription();
                    if (desc == null) {
                        //TODO: currently this is always null
                        desc = catalogName + " - " + cubeName + " Cube";
                    }
                    //TODO: SQL Server returns 1000000 for all tables
                    int cardinality = 1000000;
                    String version = "null";

                    Row row = new Row();
                    row.set(TableCatalog.name, catalogName);
                    row.set(TableName.name, cubeName);
                    row.set(TableType.name, "TABLE");
                    row.set(Bookmarks.name, false);
                    row.set(TableVersion.name, version);
                    row.set(Cardinality.name, cardinality);
                    row.set(Description.name, desc);
                    addRow(row, rows);
                }
            }
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaActionsRowset extends Rowset {
        MdschemaActionsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                null);
        private static final Column Coordinate =
            new Column(
                "COORDINATE",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                null);
        private static final Column CoordinateType =
            new Column(
                "COORDINATE_TYPE",
                Type.Integer,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                null);
        /*
            TODO: optional columns
            SCHEMA_NAME
            ACTION_NAME
            ACTION_TYPE
            INVOCATION
            CUBE_SOURCE
        */
        /*
         * http://msdn2.microsoft.com/en-us/library/ms126032(SQL.90).aspx
         * http://msdn.microsoft.com/library/default.asp?url=/library/en-us/oledb/htm/olapactions_rowset.asp
         *
         * restrictions
         *   CATALOG_NAME Optional
         *   SCHEMA_NAME Optional
         *   CUBE_NAME Mandatory
         *   ACTION_NAME Optional
         *   ACTION_TYPE Optional
         *   COORDINATE Mandatory
         *   COORDINATE_TYPE Mandatory
         *   INVOCATION
         *      (Optional) The INVOCATION restriction column defaults to the
         *      value of MDACTION_INVOCATION_INTERACTIVE. To retrieve all
         *      actions, use the MDACTION_INVOCATION_ALL value in the
         *      INVOCATION restriction column.
         *   CUBE_SOURCE
         *      (Optional) A bitmap with one of the following valid values:
         *
         *      1 CUBE
         *      2 DIMENSION
         *
         *      Default restriction is a value of 1.
         *
         * Not supported
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_ACTIONS", MDSCHEMA_ACTIONS, null, new Column[] {
                    CubeName,
                    Coordinate,
                    CoordinateType,
                }, new Column[] {
                    // Spec says sort on CATALOG_NAME, SCHEMA_NAME, CUBE_NAME,
                    // ACTION_NAME.
                    CubeName,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaActionsRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_UNSUPPORTED_OPERATION_CODE,
                HSB_UNSUPPORTED_OPERATION_FAULT_FS,
                new UnsupportedOperationException("MDSCHEMA_ACTIONS"));
        }
    }

    // REF http://msdn.microsoft.com/library/en-us/oledb/htm/olapcubes_rowset.asp
    static class MdschemaCubesRowset extends Rowset {
        private final RistrictionTest catalogNameRT;
        private final RistrictionTest cubeNameRT;
        MdschemaCubesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
            catalogNameRT = getRistrictionTest(CatalogName);
            cubeNameRT = getRistrictionTest(CubeName);
        }

        private static final String MD_CUBTYPE_CUBE = "CUBE";
        private static final String MD_CUBTYPE_VIRTUAL_CUBE = "VIRTUAL CUBE";

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the catalog to which this cube belongs.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the schema to which this cube belongs.");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Name of the cube.");
        private static final Column CubeType =
            new Column(
                "CUBE_TYPE",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Cube type.");
        private static final Column CubeGuid =
            new Column(
                "CUBE_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Cube type.");
        private static final Column CreatedOn =
            new Column(
                "CREATED_ON",
                Type.DateTime,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Date and time of cube creation.");
        private static final Column LastSchemaUpdate =
            new Column(
                "LAST_SCHEMA_UPDATE",
                Type.DateTime,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Date and time of last schema update.");
        private static final Column SchemaUpdatedBy =
            new Column(
                "SCHEMA_UPDATED_BY",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "User ID of the person who last updated the schema.");
        private static final Column LastDataUpdate =
            new Column(
                "LAST_DATA_UPDATE",
                Type.DateTime,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Date and time of last data update.");
        private static final Column DataUpdatedBy =
            new Column(
                "DATA_UPDATED_BY",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "User ID of the person who last updated the data. ");
        private static final Column IsDrillthroughEnabled =
            new Column(
                "IS_DRILLTHROUGH_ENABLED",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Describes whether DRILLTHROUGH can be performed on the members of a cube");
        private static final Column IsWriteEnabled =
            new Column(
                "IS_WRITE_ENABLED",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Describes whether a cube is write-enabled");
        private static final Column IsLinkable =
            new Column(
                "IS_LINKABLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Describes whether a cube can be used in a linked cube");
        private static final Column IsSqlEnabled =
            new Column(
                "IS_SQL_ENABLED",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Describes whether or not SQL can be used on the cube");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A user-friendly description of the dimension.");

        /*
         * http://msdn2.microsoft.com/en-us/library/ms126271(SQL.90).aspx
         * http://msdn.microsoft.com/library/default.asp?url=/library/en-us/oledb/htm/olapproperties_rowset.asp
         *
         * restrictions
         *   CATALOG_NAME Optional.
         *   SCHEMA_NAME Optional.
         *   CUBE_NAME Optional.
         *   CUBE_TYPE
         *      (Optional) A bitmap with one of these valid values:
         *      1 CUBE
         *      2 DIMENSION
         *     Default restriction is a value of 1.
         *   BASE_CUBE_NAME Optional.
         *
         * Not supported
         *   CREATED_ON
         *   LAST_SCHEMA_UPDATE
         *   SCHEMA_UPDATED_BY
         *   LAST_DATA_UPDATE
         *   DATA_UPDATED_BY
         *   ANNOTATIONS
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_CUBES", MDSCHEMA_CUBES, null,
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    CubeType,
                    CubeGuid,
                    CreatedOn,
                    LastSchemaUpdate,
                    SchemaUpdatedBy,
                    LastDataUpdate,
                    DataUpdatedBy,
                    IsDrillthroughEnabled,
                    IsWriteEnabled,
                    IsLinkable,
                    IsSqlEnabled,
                    Description
                },
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaCubesRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                            handler.getCatalogs(request, ds);
            String roleStr = request.getRole();

            for (int h = 0; h < catalogs.length; h++) {
                DataSourcesConfig.Catalog dsCatalog = catalogs[h];
                if (dsCatalog == null || dsCatalog.definition == null) {
                     continue;
                }
                Connection connection = 
                        handler.getConnection(dsCatalog, roleStr);
                if (connection == null) {
                    continue;
                }
                Role role = connection.getSchema().lookupRole(roleStr);
                String catalogName = dsCatalog.name;
                if (! catalogNameRT.passes(catalogName)) {
                    continue;
                }

                final RolapSchema schema = (RolapSchema) connection.getSchema();

                final Cube[] cubes = schema.getCubes();
                for (int i = 0; i < cubes.length; i++) {
                    Cube cube = cubes[i];

                    SchemaReader schemaReader = cube.getSchemaReader(role);
                    // Access control
                    if (!canAccess(schemaReader, cube)) {
                        continue;
                    }
                    if (! cubeNameRT.passes(cube.getName())) {
                        continue;
                    }

                    String desc = cube.getDescription();
                    if (desc == null) {
                        desc = catalogName +
                            " Schema - " +
                            cube.getName() +
                            " Cube";
                    }

                    Row row = new Row();
                    row.set(CatalogName.name, catalogName);
                    //row.set(SchemaName.name, catalogName);
                    row.set(CubeName.name, cube.getName());
                    row.set(CubeType.name, 
                        ((RolapCube) cube).isVirtual() 
                            ? MD_CUBTYPE_VIRTUAL_CUBE : MD_CUBTYPE_CUBE);
                    //row.set(CubeGuid.name, "");
                    //row.set(CreatedOn.name, "");
                    //row.set(LastSchemaUpdate.name, "");
                    //row.set(SchemaUpdatedBy.name, "");
                    //row.set(LastDataUpdate.name, "");
                    //row.set(DataUpdatedBy.name, "");
                    row.set(IsDrillthroughEnabled.name, true);
                    row.set(IsWriteEnabled.name, false);
                    row.set(IsLinkable.name, false);
                    row.set(IsSqlEnabled.name, false);
                    row.set(Description.name, desc);
                    row.set(LastSchemaUpdate.name,lastUpdateDate(schema));     
                    addRow(row, rows);
                }
            }
        }

	    private String lastUpdateDate(RolapSchema schema) {
		    Format formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		    String formattedDate = formatter.format(schema.getSchemaLoadDate());
		    return formattedDate;
	    }

        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    // REF http://msdn.microsoft.com/library/en-us/oledb/htm/olapdimensions_rowset.asp
    static class MdschemaDimensionsRowset extends Rowset {
        private final RistrictionTest schemaNameRT;
        private final RistrictionTest cubeNameRT;
        private final RistrictionTest dimensionUniqueNameRT;
        private final RistrictionTest dimensionNameRT;
        MdschemaDimensionsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
            schemaNameRT = getRistrictionTest(SchemaName);
            cubeNameRT = getRistrictionTest(CubeName);
            dimensionUniqueNameRT = getRistrictionTest(DimensionUniqueName);
            dimensionNameRT = getRistrictionTest(DimensionName);
        }

        public static final int MD_DIMTYPE_OTHER = 3;
        public static final int MD_DIMTYPE_MEASURE = 2;
        public static final int MD_DIMTYPE_TIME = 1;

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the database.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "Not supported.");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the cube.");
        private static final Column DimensionName =
            new Column(
                "DIMENSION_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the dimension. ");
        private static final Column DimensionUniqueName =
            new Column(
                "DIMENSION_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The unique name of the dimension.");
        private static final Column DimensionGuid =
            new Column(
                "DIMENSION_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Not supported.");
        private static final Column DimensionCaption =
            new Column(
                "DIMENSION_CAPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The caption of the dimension.");
        private static final Column DimensionOrdinal =
            new Column(
                "DIMENSION_ORDINAL",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The position of the dimension within the cube.");
        /*
         * SQL Server returns values:
         *   MD_DIMTYPE_TIME (1)
         *   MD_DIMTYPE_MEASURE (2)
         *   MD_DIMTYPE_OTHER (3)
         */
        private static final Column DimensionType =
            new Column(
                "DIMENSION_TYPE",
                Type.Short,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The type of the dimension.");
        private static final Column DimensionCardinality =
            new Column(
                "DIMENSION_CARDINALITY",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The number of members in the key attribute.");
        private static final Column DefaultHierarchy =
            new Column(
                "DEFAULT_HIERARCHY",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A hierarchy from the dimension. Preserved for backwards compatibility.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A user-friendly description of the dimension.");
        private static final Column IsVirtual =
            new Column(
                "IS_VIRTUAL",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Always FALSE.");
        private static final Column IsReadWrite =
            new Column("IS_READWRITE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the dimension is write-enabled.");
        /*
         * SQL Server returns values: 0 or 1
         */
        private static final Column DimensionUniqueSettings =
            new Column(
                "DIMENSION_UNIQUE_SETTINGS",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A bitmap that specifies which columns contain unique values if the dimension contains only members with unique names.");
        private static final Column DimensionMasterUniqueName =
            new Column(
                "DIMENSION_MASTER_UNIQUE_NAME",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Always NULL.");
        private static final Column DimensionIsVisible =
            new Column(
                "DIMENSION_IS_VISIBLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Always TRUE.");
        /*
         * http://msdn2.microsoft.com/en-us/library/ms126180(SQL.90).aspx
         * http://msdn2.microsoft.com/en-us/library/ms126180.aspx
         *
         * restrictions
         *    CATALOG_NAME Optional.
         *    SCHEMA_NAME Optional.
         *    CUBE_NAME Optional.
         *    DIMENSION_NAME Optional.
         *    DIMENSION_UNIQUE_NAME Optional.
         *    CUBE_SOURCE (Optional) A bitmap with one of the following valid values:
         *      1 CUBE
         *      2 DIMENSION
         *    Default restriction is a value of 1.
         *
         *    DIMENSION_VISIBILITY (Optional) A bitmap with one of the following valid values:
         *      1 Visible
         *      2 Not visible
         *    Default restriction is a value of 1.
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_DIMENSIONS", MDSCHEMA_DIMENSIONS, null,
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionName,
                    DimensionUniqueName,
                    DimensionGuid,
                    DimensionCaption,
                    DimensionOrdinal,
                    DimensionType,
                    DimensionCardinality,
                    DefaultHierarchy,
                    Description,
                    IsVirtual,
                    IsReadWrite,
                    DimensionUniqueSettings,
                    DimensionMasterUniqueName,
                    DimensionIsVisible,
                },
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionName,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaDimensionsRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            DataSourcesConfig.DataSource ds = 
                handler.getDataSource(request);
            String roleStr = request.getRole();
            DataSourcesConfig.Catalog[] catalogs = 
                    handler.getCatalogs(request, ds);

            for (int i = 0; i < catalogs.length; i++) {
                DataSourcesConfig.Catalog dsCatalog = catalogs[i];
                if (dsCatalog == null || dsCatalog.definition == null) {
                     continue;
                }
                String catalogName = dsCatalog.name;
                if (! schemaNameRT.passes(catalogName)) {
                    continue;
                }

                Connection connection = 
                    handler.getConnection(dsCatalog, roleStr);
                if (connection == null) {
                    continue;
                }
                Role role = connection.getSchema().lookupRole(roleStr);
                populateCatalog(connection, role, catalogName, rows);
            }
        }
        protected void populateCatalog(Connection connection, 
                Role role, 
                String catalogName, 
                List rows) 
                throws XmlaException {
            
            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];
                if (! cubeNameRT.passes(cube.getName())) {
                    continue;
                }
                SchemaReader schemaReader = cube.getSchemaReader(role);
                populateCube(schemaReader, catalogName, cube, rows);
            }
        }
        protected void populateCube(SchemaReader schemaReader,
                String catalogName,
                Cube cube,
                List rows) 
                throws XmlaException {

            final Dimension[] dimensions = cube.getDimensions();
            for (int j = 0; j < dimensions.length; j++) {
                Dimension dimension = dimensions[j];
                String name = dimension.getName();
                String unique = dimension.getUniqueName();
                if (dimensionNameRT.passes(name) &&
                        dimensionUniqueNameRT.passes(unique)) {
                    populateDimension(schemaReader, catalogName,
                            cube, dimension, rows);
                }
            }
        }
        protected void populateDimension(SchemaReader schemaReader, 
                String catalogName, 
                Cube cube, 
                Dimension dimension, 
                List rows) 
                throws XmlaException {

            // Access control
            if (!canAccess(schemaReader, dimension)) {
                return;
            }
            String desc = dimension.getDescription();
            if (desc == null) {
                desc = cube.getName() +
                    " Cube - " +
                    dimension.getName() +
                    " Dimension";
            }

            Row row = new Row();
            row.set(CatalogName.name, catalogName);
            // NOTE: SQL Server does not return this
            //row.set(SchemaName.name, cube.getSchema().getName());
            row.set(CubeName.name, cube.getName());
            row.set(DimensionName.name, dimension.getName());
            row.set(DimensionUniqueName.name, dimension.getUniqueName());
            row.set(DimensionCaption.name, dimension.getCaption());
            row.set(DimensionOrdinal.name, dimension.getOrdinal(cube));
            row.set(DimensionType.name, getDimensionType(dimension));

            //Is this the number of primaryKey members there are??
            // According to microsoft this is:
            //    "The number of members in the key attribute."
            // There may be a better way of doing this but
            // this is what I came up with. Note that I need to
            // add '1' to the number inorder for it to match
            // match what microsoft SQL Server is producing.
            // The '1' might have to do with whether or not the
            // hierarchy has a 'all' member or not - don't know yet.
            // large data set total for Orders cube 0m42.923s
            Hierarchy firstHierarchy = dimension.getHierarchies()[0];
            Level[] levels = firstHierarchy.getLevels();
            Level lastLevel = levels[levels.length-1];



            /*
            if override config setting is set
                if approxRowCount has a value
                    use it
            else
                                    do default
            */

            // Added by TWI to returned cached row numbers
            int n=schemaReader.getLevelCardinality(lastLevel, true, true);
            row.set(DimensionCardinality.name, n+1);

            // TODO: I think that this is just the dimension name
            row.set(DefaultHierarchy.name, dimension.getUniqueName());
            row.set(Description.name, desc);
            row.set(IsVirtual.name, false);
            // SQL Server always returns false
            row.set(IsReadWrite.name, false);
            // TODO: don't know what to do here
            // Are these the levels with uniqueMembers == true?
            // How are they mapped to specific column numbers?
            row.set(DimensionUniqueSettings.name, 0);
            row.set(DimensionIsVisible.name, true);

            addRow(row, rows);
        }

        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static int getDimensionType(Dimension dim) {
        if (dim.isMeasures())
            return MdschemaDimensionsRowset.MD_DIMTYPE_MEASURE;
        else if (DimensionType.TimeDimension.equals(dim.getDimensionType())) {
            return MdschemaDimensionsRowset.MD_DIMTYPE_TIME;
        } else {
            return MdschemaDimensionsRowset.MD_DIMTYPE_OTHER;
        }
    }

    static class MdschemaFunctionsRowset extends Rowset {
        /**
         * http://www.csidata.com/custserv/onlinehelp/VBSdocs/vbs57.htm
         */
        static class VarType extends EnumeratedValues.BasicValue {
            static int convertCategory(int category) {
                switch (category) {
                case Category.Unknown:
                // expression == unknown ???
                // case Category.Expression:
                    return EMPTY_ORDINAL;
                case Category.Array:
                    return ARRAY_ORDINAL;
                case Category.Dimension:
                case Category.Hierarchy:
                case Category.Level:
                case Category.Member:
                case Category.Set:
                case Category.Tuple:
                case Category.Cube:
                case Category.Value:
                    return VARIANT_ORDINAL;
                case Category.Logical:
                    return BOOLEAN_ORDINAL;
                case Category.Numeric:
                    return DOUBLE_ORDINAL;
                case Category.String:
                case Category.Symbol:
                case Category.Constant:
                    return STRING_ORDINAL;
                case Category.Integer:
                case Category.Mask:
                    return INTEGERL_ORDINAL;
                }
                // NOTE: this should never happen
                return EMPTY_ORDINAL;
            }

            public static final int EMPTY_ORDINAL        = 0;
            public static final VarType Empty =
                new VarType("Empty", EMPTY_ORDINAL, "Uninitialized (default)");

            public static final int NULL_ORDINAL        = 1;
            public static final VarType Null =
                new VarType("Null", NULL_ORDINAL, "Contains no valid data");

            public static final int INTEGERL_ORDINAL        = 2;
            public static final VarType Integer =
                new VarType("Integer", INTEGERL_ORDINAL, "Integer subtype");

            public static final int LONG_ORDINAL        = 3;
            public static final VarType Long =
                new VarType("Long", LONG_ORDINAL, "Long subtype");

            public static final int SINGLE_ORDINAL        = 4;
            public static final VarType Single =
                new VarType("Single", SINGLE_ORDINAL, "Single subtype");

            public static final int DOUBLE_ORDINAL        = 5;
            public static final VarType Double =
                new VarType("Double", DOUBLE_ORDINAL, "Double subtype");

            public static final int CURRENCY_ORDINAL        = 6;
            public static final VarType Currency =
                new VarType("Currency", CURRENCY_ORDINAL, "Currency subtype");

            public static final int DATE_ORDINAL        = 7;
            public static final VarType Date =
                new VarType("Date", DATE_ORDINAL, "Date subtype");

            public static final int STRING_ORDINAL        = 8;
            public static final VarType String =
                new VarType("String", STRING_ORDINAL, "String subtype");

            public static final int OBJECT_ORDINAL        = 9;
            public static final VarType Object =
                new VarType("Object", OBJECT_ORDINAL, "Object subtype");

            public static final int ERROR_ORDINAL        = 10;
            public static final VarType Error =
                new VarType("Error", ERROR_ORDINAL, "Error subtype");

            public static final int BOOLEAN_ORDINAL        = 11;
            public static final VarType Boolean =
                new VarType("Boolean", BOOLEAN_ORDINAL, "Boolean subtype");

            public static final int VARIANT_ORDINAL        = 12;
            public static final VarType Variant =
                new VarType("Variant", VARIANT_ORDINAL, "Variant subtype");

            public static final int DATA_OBJECT_ORDINAL        = 13;
            public static final VarType DataObject =
                new VarType("DataObject", DATA_OBJECT_ORDINAL, "DataObject subtype");
            public static final int DECIMAL_ORDINAL        = 14;
            public static final VarType Decimal =
                new VarType("Decimal", DECIMAL_ORDINAL, "Decimal subtype");

            public static final int BYTE_ORDINAL        = 17;
            public static final VarType Byte =
                new VarType("Byte", BYTE_ORDINAL, "Byte subtype");

            public static final int ARRAY_ORDINAL        = 17;
            public static final VarType Array =
                new VarType("Array", ARRAY_ORDINAL, "Array subtype");

            public VarType(String name, int ordinal, String description) {
                super(name, ordinal, description);
            }
        }

        private final RistrictionTest functionNameRT;
        MdschemaFunctionsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
            functionNameRT = getRistrictionTest(FunctionName);
        }

        private static final Column FunctionName =
            new Column(
                "FUNCTION_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the function.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A description of the function.");
        private static final Column ParameterList =
            new Column(
                "PARAMETER_LIST",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A comma delimited list of parameters.");
        private static final Column ReturnType =
            new Column(
                "RETURN_TYPE",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The VARTYPE of the return data type of the function.");
        private static final Column Origin =
            new Column(
                "ORIGIN",
                Type.Integer,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The origin of the function:  1 for MDX functions.  2 for user-defined functions.");
        private static final Column InterfaceName =
            new Column(
                "INTERFACE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the interface for user-defined functions");
        private static final Column LibraryName =
            new Column(
                "LIBRARY_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the type library for user-defined functions. NULL for MDX functions.");
        private static final Column Caption =
            new Column(
                "CAPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The display caption for the function.");
        /*
         * http://msdn2.microsoft.com/en-us/library/ms126257(SQL.90).aspx
         * http://msdn.microsoft.com/library/en-us/oledb/htm/olapfunctions_rowset.asp
         *
         * restrictions
         *   LIBRARY_NAME Optional.
         *   INTERFACE_NAME Optional.
         *   FUNCTION_NAME Optional.
         *   ORIGIN Optional.
         *
         * Not supported
         *  DLL_NAME
         *    Optional
         *  HELP_FILE
         *    Optional
         *  HELP_CONTEXT
         *    Optional
         *    - SQL Server xml schema says that this must be present
         *  OBJECT
         *    Optional
         *  CAPTION The display caption for the function.
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_FUNCTIONS", MDSCHEMA_FUNCTIONS, null,
                new Column[] {
                    FunctionName,
                    Description,
                    ParameterList,
                    ReturnType,
                    Origin,
                    InterfaceName,
                    LibraryName,
                    Caption,
                },
                new Column[] {
                    LibraryName,
                    InterfaceName,
                    FunctionName,
                    Origin,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaFunctionsRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                            handler.getCatalogs(request, ds);
            String role = request.getRole();

            for (int h = 0; h < catalogs.length; h++) {
                DataSourcesConfig.Catalog dsCatalog = catalogs[h];
                if (dsCatalog == null || dsCatalog.definition == null) {
                     continue;
                }
                Connection connection = handler.getConnection(dsCatalog, role);
                if (connection == null) {
                    continue;
                }
                final RolapSchema schema = (RolapSchema) connection.getSchema();
                FunTable funTable = schema.getFunTable();

                StringBuffer buf = new StringBuffer(50);
                List functions = funTable.getFunInfoList();
                for (Iterator it  = functions.iterator(); it.hasNext(); ) {
                    FunInfo fi = (FunInfo) it.next();
                    if (! functionNameRT.passes(fi.getName())) {
                        continue;
                    }

                    int[][] paramCategories = fi.getParameterCategories();
                    int[] returnCategories = fi.getReturnCategories();

                    // Convert Windows newlines in 'description' to UNIX format.
                    String description = fi.getDescription();
                    if (description != null) {
                        description = Util.replace(fi.getDescription(), "\r", "");
                    }
                    if ((paramCategories == null) || (paramCategories.length == 0)) {
                        Row row = new Row();
                        row.set(FunctionName.name, fi.getName());
                        row.set(Description.name, description);
                        row.set(ParameterList.name, "(none)");
                        row.set(ReturnType.name, 1);
                        row.set(Origin.name, 1);
                        //row.set(LibraryName.name, "");
                        // TODO WHAT VALUE should this have
                        row.set(InterfaceName.name, "");
                        row.set(Caption.name, fi.getName());
                        addRow(row, rows);

                    } else {
                        for (int i = 0; i < paramCategories.length; i++) {
                            int[] pc = paramCategories[i];
                            int returnCategory = returnCategories[i];

                            Row row = new Row();
                            row.set(FunctionName.name, fi.getName());
                            row.set(Description.name, description);

                            buf.setLength(0);
                            for (int j = 0; j < pc.length; j++) {
                                int v = pc[j];
                                if (j > 0) {
                                    buf.append(", ");
                                }
                                buf.append(Category.instance.getDescription(v & Category.Mask));
                            }
                            row.set(ParameterList.name, buf.toString());

                            int varType = VarType.convertCategory(returnCategory);
                            row.set(ReturnType.name, varType);

                            //TODO: currently FunInfo can not tell us which
                            // functions are MDX and which are UDFs.
                            row.set(Origin.name, 1);

                            // TODO: Name of the type library for UDFs. NULL for MDX
                            // functions.
                            //row.set(LibraryName.name, "");

                            // TODO: Name of the interface for UDF and Group name
                            // for the MDX functions.
                            // TODO WHAT VALUE should this have
                            row.set(InterfaceName.name, "");

                            row.set(Caption.name, fi.getName());
                            addRow(row, rows);
                        }
                    }
                }
            }
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }


    static class MdschemaHierarchiesRowset extends Rowset {
        private final RistrictionTest schemaNameRT;
        private final RistrictionTest cubeNameRT;
        private final RistrictionTest dimensionUniqueNameRT;
        private final RistrictionTest hierarchyUniqueNameRT;
        private final RistrictionTest hierarchyNameRT;
        MdschemaHierarchiesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
            schemaNameRT = getRistrictionTest(SchemaName);
            cubeNameRT = getRistrictionTest(CubeName);
            dimensionUniqueNameRT = getRistrictionTest(DimensionUniqueName);
            hierarchyUniqueNameRT = getRistrictionTest(HierarchyUniqueName);
            hierarchyNameRT = getRistrictionTest(HierarchyName);
        }

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the catalog to which this hierarchy belongs.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "Not supported");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the cube to which this hierarchy belongs.");
        private static final Column DimensionUniqueName =
            new Column(
                "DIMENSION_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The unique name of the dimension to which this hierarchy belongs. ");
        private static final Column HierarchyName =
            new Column(
                "HIERARCHY_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the hierarchy. Blank if there is only a single hierarchy in the dimension.");
        private static final Column HierarchyUniqueName =
            new Column(
                "HIERARCHY_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The unique name of the hierarchy.");

        private static final Column HierarchyGuid =
            new Column(
                "HIERARCHY_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Hierarchy GUID.");

        private static final Column HierarchyCaption =
            new Column(
                "HIERARCHY_CAPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A label or a caption associated with the hierarchy.");
        private static final Column DimensionType =
            new Column(
                "DIMENSION_TYPE",
                Type.Short,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The type of the dimension. ");
        private static final Column HierarchyCardinality =
            new Column(
                "HIERARCHY_CARDINALITY",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The number of members in the hierarchy.");
        private static final Column DefaultMember =
            new Column(
                "DEFAULT_MEMBER",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The default member for this hierarchy. ");
        private static final Column AllMember =
            new Column(
                "ALL_MEMBER",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The member at the highest level of rollup in the hierarchy.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A human-readable description of the hierarchy. NULL if no description exists.");
        private static final Column Structure =
            new Column(
                "STRUCTURE",
                Type.Short,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The structure of the hierarchy.");
        private static final Column IsVirtual =
            new Column(
                "IS_VIRTUAL",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Always returns False.");
        private static final Column IsReadWrite =
            new Column(
                "IS_READWRITE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A Boolean that indicates whether the Write Back to dimension column is enabled.");
        private static final Column DimensionUniqueSettings =
            new Column(
                "DIMENSION_UNIQUE_SETTINGS",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Always returns MDDIMENSIONS_MEMBER_KEY_UNIQUE (1).");
        private static final Column DimensionIsVisible =
            new Column(
                "DIMENSION_IS_VISIBLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Always returns true.");
        private static final Column HierarchyOrdinal =
            new Column(
                "HIERARCHY_ORDINAL",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The ordinal number of the hierarchy across all hierarchies of the cube.");
        private static final Column DimensionIsShared =
            new Column(
                "DIMENSION_IS_SHARED",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Always returns true.");


        /*
         * NOTE: This is non-standard, where did it come from?
         */
        private static final Column ParentChild =
            new Column(
                "PARENT_CHILD",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Is hierarcy a parent.");

        /*
         * http://msdn2.microsoft.com/en-us/library/ms126062(SQL.90).aspx
         * http://msdn.microsoft.com/library/default.asp?url=/library/en-us/oledb/htm/olapproperties_rowset.asp
         *
         * restrictions
         *    CATALOG_NAME Optional.
         *    SCHEMA_NAME Optional.
         *    CUBE_NAME Optional.
         *    DIMENSION_UNIQUE_NAME Optional.
         *    HIERARCHY_NAME Optional.
         *    HIERARCHY_UNIQUE_NAME Optional.
         *    HIERARCHY_ORIGIN
         *       (Optional) A default restriction is in effect
         *       on MD_USER_DEFINED and MD_SYSTEM_ENABLED.
         *    CUBE_SOURCE
         *      (Optional) A bitmap with one of the following valid values:
         *      1 CUBE
         *      2 DIMENSION
         *      Default restriction is a value of 1.
         *    HIERARCHY_VISIBILITY
         *      (Optional) A bitmap with one of the following valid values:
         *      1 Visible
         *      2 Not visible
         *      Default restriction is a value of 1.
         *
         * Not supported
         *  HIERARCHY_IS_VISIBLE
         *  HIERARCHY_ORIGIN
         *  HIERARCHY_DISPLAY_FOLDER
         *  INSTANCE_SELECTION
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_HIERARCHIES", MDSCHEMA_HIERARCHIES, null,
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionUniqueName,
                    HierarchyName,
                    HierarchyUniqueName,
                    HierarchyGuid,
                    HierarchyCaption,
                    DimensionType,
                    HierarchyCardinality,
                    DefaultMember,
                    AllMember,
                    Description,
                    Structure,
                    IsVirtual,
                    IsReadWrite,
                    DimensionUniqueSettings,
                    DimensionIsVisible,
                    HierarchyOrdinal,
                    DimensionIsShared,
                    ParentChild,
                },
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionUniqueName,
                    HierarchyName,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaHierarchiesRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            DataSourcesConfig.DataSource ds = 
                handler.getDataSource(request);
            String roleStr = request.getRole();
            DataSourcesConfig.Catalog[] catalogs = 
                    handler.getCatalogs(request, ds);

            for (int i = 0; i < catalogs.length; i++) {
                DataSourcesConfig.Catalog dsCatalog = catalogs[i];
                if (dsCatalog == null || dsCatalog.definition == null) {
                     continue;
                }
                String catalogName = dsCatalog.name;
                if (! schemaNameRT.passes(catalogName)) {
                    continue;
                }

                Connection connection = 
                    handler.getConnection(dsCatalog, roleStr);
                if (connection == null) {
                    continue;
                }
                Role role = connection.getSchema().lookupRole(roleStr);
                populateCatalog(connection, role, catalogName, rows);
            }
        }
        protected void populateCatalog(Connection connection, 
                Role role, 
                String catalogName, 
                List rows) 
                throws XmlaException {
            try {
                final Cube[] cubes = connection.getSchema().getCubes();
                for (int i = 0; i < cubes.length; i++) {
                    Cube cube = cubes[i];
                    if (! cubeNameRT.passes(cube.getName())) {
                        continue;
                    }
                    SchemaReader schemaReader = cube.getSchemaReader(role);
                    populateCube(schemaReader, catalogName, cube, rows);
                }
            } finally {
            }
        }
        protected void populateCube(SchemaReader schemaReader,
                String catalogName, 
                Cube cube, 
                List rows) 
                throws XmlaException {
            try {
                int ordinal = 0;
                final Dimension[] dimensions = cube.getDimensions();
                for (int j = 0; j < dimensions.length; j++) {
                    Dimension dimension = dimensions[j];
                    String unique = dimension.getUniqueName();
                    // Must increment ordinal for all dimensions but
                    // only output some of them.
                    boolean genOutput = dimensionUniqueNameRT.passes(unique);
                    ordinal = populateDimension(
                            genOutput,
                            schemaReader, catalogName, 
                            cube, dimension, ordinal, rows);
                }
            } finally {
            }
        }
        protected int populateDimension(
                boolean genOutput, 
                SchemaReader schemaReader, 
                String catalogName, 
                Cube cube, 
                Dimension dimension, 
                int ordinal,
                List rows) 
                throws XmlaException {
            try {
                Hierarchy[] hierarchies = dimension.getHierarchies();
                for (int i = 0; i < hierarchies.length; i++) {
                    Hierarchy hierarchy = hierarchies[i];
                    if (genOutput) {
                        String name = hierarchy.getName();
                        String unique = hierarchy.getUniqueName();
                        if (hierarchyNameRT.passes(hierarchy.getName()) &&
                            hierarchyUniqueNameRT.passes(unique)) {
                            populateHierarchy(schemaReader, catalogName, 
                                cube, dimension, (HierarchyBase) hierarchy, 
                                ordinal++, rows);
                        } else {
                            ordinal++;
                        }
                    } else {
                        ordinal++;
                    }
                }
                return ordinal;
            } finally {
            }
        }
        protected void populateHierarchy(SchemaReader schemaReader, 
                String catalogName, 
                Cube cube, 
                Dimension dimension, 
                HierarchyBase hierarchy, 
                int ordinal,
                List rows) 
                throws XmlaException {

            // Access control
            if (!canAccess(schemaReader, hierarchy)) {
                return;
            }

            String desc = hierarchy.getDescription();
            if (desc == null) {
                desc = cube.getName() +
                    " Cube - " +
                    hierarchy.getName() +
                    " Hierarchy";
            }

            Row row = new Row();
            row.set(CatalogName.name, catalogName);

            // SQL Server does not return Schema name
            //row.set(SchemaName.name, cube.getSchema().getName());

            row.set(CubeName.name, cube.getName());
            row.set(DimensionUniqueName.name, dimension.getUniqueName());
            row.set(HierarchyName.name, hierarchy.getName());
            row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
            //row.set(HierarchyGuid.name, "");

            row.set(HierarchyCaption.name, hierarchy.getCaption());
            row.set(DimensionType.name, getDimensionType(dimension));
            // The number of members in the hierarchy. Because
            // of the presence of multiple hierarchies, this number
            // might not be the same as DIMENSION_CARDINALITY. This
            // value can be an approximation of the real
            // cardinality. Consumers should not assume that this
            // value is accurate.
            int cardinality = 
                RolapMember.getHierarchyCardinality(schemaReader, hierarchy);
            row.set(HierarchyCardinality.name, cardinality);

            row.set(DefaultMember.name, hierarchy.getDefaultMember());
            if (hierarchy.hasAll()) {
                row.set(AllMember.name,
                    Util.makeFqName(hierarchy, hierarchy.getAllMemberName()));
            }
            row.set(Description.name, desc);

            //TODO: only support:
            // MD_STRUCTURE_FULLYBALANCED (0)
            // MD_STRUCTURE_RAGGEDBALANCED (1)
            row.set(Structure.name, hierarchy.isRagged() ? 1 : 0);

            row.set(IsVirtual.name, false);
            row.set(IsReadWrite.name, false);

            // NOTE that SQL Server returns '0' not '1'.
            row.set(DimensionUniqueSettings.name, 0);

            // always true
            row.set(DimensionIsVisible.name, true);

            row.set(HierarchyOrdinal.name, ordinal);

            // always true
            row.set(DimensionIsShared.name, true);

            RolapLevel nonAllFirstLevel =
                    (RolapLevel) hierarchy.getLevels()[
                    (hierarchy.hasAll() ? 1 : 0)];
            row.set(ParentChild.name, nonAllFirstLevel.isParentChild());
            addRow(row, rows);
        }



        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaLevelsRowset extends Rowset {
        private final RistrictionTest schemaNameRT;
        private final RistrictionTest cubeNameRT;
        private final RistrictionTest dimensionUniqueNameRT;
        private final RistrictionTest hierarchyUniqueNameRT;
        private final RistrictionTest levelUniqueNameRT;
        private final RistrictionTest levelNameRT;
        MdschemaLevelsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
            schemaNameRT = getRistrictionTest(SchemaName);
            cubeNameRT = getRistrictionTest(CubeName);
            dimensionUniqueNameRT = getRistrictionTest(DimensionUniqueName);
            hierarchyUniqueNameRT = getRistrictionTest(HierarchyUniqueName);
            levelUniqueNameRT = getRistrictionTest(LevelUniqueName);
            levelNameRT = getRistrictionTest(LevelName);
        }

        public static final int MDLEVEL_TYPE_UNKNOWN = 0x0000;
        public static final int MDLEVEL_TYPE_REGULAR = 0x0000;
        public static final int MDLEVEL_TYPE_ALL = 0x0001;
        public static final int MDLEVEL_TYPE_CALCULATED = 0x0002;
        public static final int MDLEVEL_TYPE_TIME = 0x0004;
        public static final int MDLEVEL_TYPE_RESERVED1 = 0x0008;
        public static final int MDLEVEL_TYPE_TIME_YEARS = 0x0014;
        public static final int MDLEVEL_TYPE_TIME_HALF_YEAR = 0x0024;
        public static final int MDLEVEL_TYPE_TIME_QUARTERS = 0x0044;
        public static final int MDLEVEL_TYPE_TIME_MONTHS = 0x0084;
        public static final int MDLEVEL_TYPE_TIME_WEEKS = 0x0104;
        public static final int MDLEVEL_TYPE_TIME_DAYS = 0x0204;
        public static final int MDLEVEL_TYPE_TIME_HOURS = 0x0304;
        public static final int MDLEVEL_TYPE_TIME_MINUTES = 0x0404;
        public static final int MDLEVEL_TYPE_TIME_SECONDS = 0x0804;
        public static final int MDLEVEL_TYPE_TIME_UNDEFINED = 0x1004;

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the catalog to which this level belongs.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the schema to which this level belongs.");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the cube to which this level belongs.");
        private static final Column DimensionUniqueName =
            new Column(
                "DIMENSION_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The unique name of the dimension to which this level belongs.");
        private static final Column HierarchyUniqueName =
            new Column(
                "HIERARCHY_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The unique name of the hierarchy.");
        private static final Column LevelName =
            new Column(
                "LEVEL_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the level.");
        private static final Column LevelUniqueName =
            new Column(
                "LEVEL_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The properly escaped unique name of the level.");
        private static final Column LevelGuid =
            new Column(
                "LEVEL_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Level GUID.");
        private static final Column LevelCaption =
            new Column(
                "LEVEL_CAPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A label or caption associated with the hierarchy.");
        private static final Column LevelNumber =
            new Column(
                "LEVEL_NUMBER",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The distance of the level from the root of the hierarchy. Root level is zero (0).");
        private static final Column LevelCardinality =
            new Column(
                "LEVEL_CARDINALITY",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The number of members in the level. This value can be an approximation of the real cardinality.");
        private static final Column LevelType =
            new Column(
                "LEVEL_TYPE",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Type of the level");
        private static final Column CustomRollupSettings =
            new Column(
                "CUSTOM_ROLLUP_SETTINGS",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A bitmap that specifies the custom rollup options.");
        private static final Column LevelUniqueSettings =
            new Column(
                "LEVEL_UNIQUE_SETTINGS",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A bitmap that specifies which columns contain unique values, if the level only has members with unique names or keys.");
        private static final Column LevelIsVisible =
            new Column(
                "LEVEL_IS_VISIBLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A Boolean that indicates whether the level is visible.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A human-readable description of the level. NULL if no description exists.");
        /*
         * http://msdn2.microsoft.com/en-us/library/ms126038(SQL.90).aspx
         * http://msdn.microsoft.com/library/en-us/oledb/htm/olaplevels_rowset.asp
         *
         * restriction
         *   CATALOG_NAME Optional.
         *   SCHEMA_NAME Optional.
         *   CUBE_NAME Optional.
         *   DIMENSION_UNIQUE_NAME Optional.
         *   HIERARCHY_UNIQUE_NAME Optional.
         *   LEVEL_NAME Optional.
         *   LEVEL_UNIQUE_NAME Optional.
         *   LEVEL_ORIGIN
         *       (Optional) A default restriction is in effect
         *       on MD_USER_DEFINED and MD_SYSTEM_ENABLED
         *   CUBE_SOURCE
         *       (Optional) A bitmap with one of the following valid values:
         *       1 CUBE
         *       2 DIMENSION
         *       Default restriction is a value of 1.
         *   LEVEL_VISIBILITY
         *       (Optional) A bitmap with one of the following values:
         *       1 Visible
         *       2 Not visible
         *       Default restriction is a value of 1.
         *
         * Not supported
         *  CUSTOM_ROLLUP_SETTINGS
         *  LEVEL_UNIQUE_SETTINGS
         *  LEVEL_ORDERING_PROPERTY
         *  LEVEL_DBTYPE
         *  LEVEL_MASTER_UNIQUE_NAME
         *  LEVEL_NAME_SQL_COLUMN_NAME Customers:(All)!NAME
         *  LEVEL_KEY_SQL_COLUMN_NAME Customers:(All)!KEY
         *  LEVEL_UNIQUE_NAME_SQL_COLUMN_NAME Customers:(All)!UNIQUE_NAME
         *  LEVEL_ATTRIBUTE_HIERARCHY_NAME
         *  LEVEL_KEY_CARDINALITY
         *  LEVEL_ORIGIN
         *
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_LEVELS", MDSCHEMA_LEVELS, null,
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionUniqueName,
                    HierarchyUniqueName,
                    LevelName,
                    LevelUniqueName,
                    LevelGuid,
                    LevelCaption,
                    LevelNumber,
                    LevelCardinality,
                    LevelType,
                    CustomRollupSettings,
                    LevelUniqueSettings,
                    LevelIsVisible,
                    Description,
                },
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionUniqueName,
                    HierarchyUniqueName,
                    LevelNumber,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaLevelsRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            String roleStr = request.getRole();
            DataSourcesConfig.Catalog[] catalogs = 
                    handler.getCatalogs(request, ds);

            for (int i = 0; i < catalogs.length; i++) {
                DataSourcesConfig.Catalog dsCatalog = catalogs[i];
                if (dsCatalog == null || dsCatalog.definition == null) {
                     continue;
                }
                String catalogName = dsCatalog.name;
                if (! schemaNameRT.passes(catalogName)) {
                    continue;
                }

                Connection connection = 
                    handler.getConnection(dsCatalog, roleStr);
                if (connection == null) {
                    continue;
                }
                Role role = connection.getSchema().lookupRole(roleStr);
                populateCatalog(connection, role, catalogName, rows);
            }
        }
        protected void populateCatalog(Connection connection, 
                Role role, 
                String catalogName, 
                List rows) 
                throws XmlaException {

            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];
                if (! cubeNameRT.passes(cube.getName())) {
                    continue;
                }
                SchemaReader schemaReader = cube.getSchemaReader(role);
                populateCube(schemaReader, catalogName, cube, rows);
            }
        }

        protected void populateCube(SchemaReader schemaReader,
                String catalogName, 
                Cube cube, 
                List rows) 
                throws XmlaException {
            try {
                final Dimension[] dimensions = cube.getDimensions();
                for (int j = 0; j < dimensions.length; j++) {
                    Dimension dimension = dimensions[j];
                    String uniqueName = dimension.getUniqueName();
                    if (dimensionUniqueNameRT.passes(uniqueName)) {
                        populateDimension(schemaReader, catalogName,
                                    cube, dimension, rows);
                    }
                }
            } finally {
            }
        }
        protected void populateDimension(SchemaReader schemaReader, 
                String catalogName, 
                Cube cube, 
                Dimension dimension, 
                List rows) 
                throws XmlaException {
            try {
                Hierarchy[] hierarchies = dimension.getHierarchies();
                for (int i = 0; i < hierarchies.length; i++) {
                    Hierarchy hierarchy = hierarchies[i];
                    String uniqueName = hierarchy.getUniqueName();
                    if (hierarchyUniqueNameRT.passes(uniqueName)) {
                        populateHierarchy(schemaReader, catalogName, 
                            cube, hierarchy, rows);
                    }
                }
            } finally {
            }
        }
        protected void populateHierarchy(SchemaReader schemaReader, 
                String catalogName, 
                Cube cube, 
                Hierarchy hierarchy, 
                List rows) 
                throws XmlaException {

                final Level[] levels = hierarchy.getLevels();
                for (int i = 0; i < levels.length; i++) {
                    Level level = levels[i];
                    String uniqueName = level.getUniqueName();
                    String name = level.getName();
                    if (levelUniqueNameRT.passes(uniqueName) &&
                            levelNameRT.passes(name)) {
                        outputLevel(schemaReader,
                                catalogName, cube, hierarchy, level, rows);
                    }
                }
        }
        protected void outputLevel(SchemaReader schemaReader, 
                String catalogName, 
                Cube cube, 
                Hierarchy hierarchy, 
                Level level,
                List rows) 
                throws XmlaException {

            // Access control
            if (!canAccess(schemaReader, level)) {
                return;
            }
            String desc = level.getDescription();
            if (desc == null) {
                desc = cube.getName() +
                    " Cube - " +
                    hierarchy.getName() +
                    " Hierarchy" +
                    level.getName() +
                    " Level";
            }

            Row row = new Row();
            row.set(CatalogName.name, catalogName);
            row.set(SchemaName.name, catalogName);
            row.set(CubeName.name, cube.getName());
            row.set(DimensionUniqueName.name, 
                hierarchy.getDimension().getUniqueName());
            row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
            row.set(LevelName.name, level.getName());
            row.set(LevelUniqueName.name, level.getUniqueName());
            //row.set(LevelGuid.name, "");
            row.set(LevelCaption.name, level.getCaption());
            // see notes on this #getDepth()
            row.set(LevelNumber.name, new Integer(level.getDepth()));

            // Get level cardinality
            // According to microsoft this is:
            //   "The number of members in the level."
            int n = schemaReader.getLevelCardinality(level, true, true);
            row.set(LevelCardinality.name, n);

            row.set(LevelType.name, getLevelType(level));

            // TODO: most of the time this is correct
            row.set(CustomRollupSettings.name, 0);

            if (level instanceof RolapLevel) {
                RolapLevel rl = (RolapLevel) level;
                row.set(LevelUniqueSettings.name, 
                    (rl.isUnique() ? 1 : 0) +
                    (rl.isAll() ? 2 : 0)
                );
            } else {
                // can not access unique member attribute
                // this is the best we can do.
                row.set(LevelUniqueSettings.name, 
                    (level.isAll() ? 2 : 0)
                );
            }
            row.set(LevelIsVisible.name, true);
            row.set(Description.name, desc);
            addRow(row, rows);
        }


        private int getLevelType(Level lev) {
            int ret = 0;

            if (lev.isAll()) {
                ret |= MDLEVEL_TYPE_ALL;
            }

            mondrian.olap.LevelType type = lev.getLevelType();
            switch (type.getOrdinal()) {
            case mondrian.olap.LevelType.RegularORDINAL:
                ret |= MDLEVEL_TYPE_REGULAR;
                break;
            case mondrian.olap.LevelType.TimeDaysORDINAL:
                ret |= MDLEVEL_TYPE_TIME_DAYS;
                break;
            case mondrian.olap.LevelType.TimeMonthsORDINAL:
                ret |= MDLEVEL_TYPE_TIME_MONTHS;
                break;
            case mondrian.olap.LevelType.TimeQuartersORDINAL:
                ret |= MDLEVEL_TYPE_TIME_QUARTERS;
                break;
            case mondrian.olap.LevelType.TimeWeeksORDINAL:
                ret |= MDLEVEL_TYPE_TIME_WEEKS;
                break;
            case mondrian.olap.LevelType.TimeYearsORDINAL:
                ret |= MDLEVEL_TYPE_TIME_YEARS;
                break;
            default:
                ret |= MDLEVEL_TYPE_UNKNOWN;
            }

            return ret;
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }


    // REF http://msdn.microsoft.com/library/en-us/oledb/htm/olapmeasures_rowset.asp
    static class MdschemaMeasuresRowset extends Rowset {
        public static final int MDMEASURE_AGGR_UNKNOWN = 0;
        public static final int MDMEASURE_AGGR_SUM = 1;
        public static final int MDMEASURE_AGGR_COUNT = 2;
        public static final int MDMEASURE_AGGR_MIN = 3;
        public static final int MDMEASURE_AGGR_MAX = 4;
        public static final int MDMEASURE_AGGR_AVG = 5;
        public static final int MDMEASURE_AGGR_VAR = 6;
        public static final int MDMEASURE_AGGR_STD = 7;
        public static final int MDMEASURE_AGGR_CALCULATED = 127;

        private final RistrictionTest schemaNameRT;
        private final RistrictionTest cubeNameRT;
        private final RistrictionTest measureUniqueNameRT;
        private final RistrictionTest measureNameRT;
        MdschemaMeasuresRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
            schemaNameRT = getRistrictionTest(SchemaName);
            cubeNameRT = getRistrictionTest(CubeName);
            measureNameRT = getRistrictionTest(MeasureName);
            measureUniqueNameRT = getRistrictionTest(MeasureUniqueName);
        }

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the catalog to which this measure belongs. ");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the schema to which this measure belongs.");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the cube to which this measure belongs.");
        private static final Column MeasureName =
            new Column(
                "MEASURE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the measure.");
        private static final Column MeasureUniqueName =
            new Column(
                "MEASURE_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The Unique name of the measure.");
        private static final Column MeasureCaption =
            new Column(
                "MEASURE_CAPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A label or caption associated with the measure. ");
        private static final Column MeasureGuid =
            new Column(
                "MEASURE_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Measure GUID.");
        private static final Column MeasureAggregator =
            new Column(
                "MEASURE_AGGREGATOR",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "How a measure was derived. ");
        private static final Column DataType =
            new Column(
                "DATA_TYPE",
                Type.UnsignedShort,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Data type of the measure.");
        private static final Column MeasureIsVisible =
            new Column(
                "MEASURE_IS_VISIBLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A Boolean that always returns True. If the measure is not visible, it will not be included in the schema rowset.");
        private static final Column LevelsList =
            new Column(
                "LEVELS_LIST",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A string that always returns NULL. EXCEPT that SQL Server returns non-null values!!!");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A human-readable description of the measure. ");
        /*
         * http://msdn2.microsoft.com/en-us/library/ms126250(SQL.90).aspx
         * http://msdn.microsoft.com/library/default.asp?url=/library/en-us/oledb/htm/olapmeasures_rowset.asp
         *
         * restrictions
         *   CATALOG_NAME Optional.
         *   SCHEMA_NAME Optional.
         *   CUBE_NAME Optional.
         *   MEASURE_NAME Optional.
         *   MEASURE_UNIQUE_NAME Optional.
         *   CUBE_SOURCE
         *     (Optional) A bitmap with one of the following valid values:
         *     1 CUBE
         *     2 DIMENSION
         *     Default restriction is a value of 1.
         *   MEASURE_VISIBILITY
         *     (Optional) A bitmap with one of the following valid values:
         *     1 Visible
         *     2 Not Visible
         *     Default restriction is a value of 1.
         *
         * Not supported
         *  MEASURE_GUID
         *  NUMERIC_PRECISION
         *  NUMERIC_SCALE
         *  MEASURE_UNITS
         *  EXPRESSION
         *  MEASURE_NAME_SQL_COLUMN_NAME
         *  MEASURE_UNQUALIFIED_CAPTION
         *  MEASUREGROUP_NAME
         *  MEASURE_DISPLAY_FOLDER
         *  DEFAULT_FORMAT_STRING
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_MEASURES", MDSCHEMA_MEASURES, null,
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    MeasureName,
                    MeasureUniqueName,
                    MeasureCaption,
                    MeasureGuid,
                    MeasureAggregator,
                    DataType,
                    MeasureIsVisible,
                    LevelsList,
                    Description,
                },
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    MeasureName,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaMeasuresRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            String roleStr = request.getRole();
            DataSourcesConfig.Catalog[] catalogs = 
                    handler.getCatalogs(request, ds);

            for (int i = 0; i < catalogs.length; i++) {
                DataSourcesConfig.Catalog dsCatalog = catalogs[i];
                if (dsCatalog == null || dsCatalog.definition == null) {
                     continue;
                }
                Connection connection = 
                    handler.getConnection(dsCatalog, roleStr);
                if (connection == null) {
                    continue;
                }

                String catalogName = dsCatalog.name;
                if (schemaNameRT.passes(catalogName)) {
                    Role role = connection.getSchema().lookupRole(roleStr);
                    populateCatalog(connection, role, catalogName, rows);
                }
            }
        }
        protected void populateCatalog(Connection connection, 
                Role role, 
                String catalogName, 
                List rows) 
                throws XmlaException {

            // SQL Server actually includes the LEVELS_LIST row
            StringBuffer buf = new StringBuffer(100);

            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];
                if (cubeNameRT.passes(cube.getName())) {
                    SchemaReader schemaReader = cube.getSchemaReader(role);
                    Dimension measuresDimension = cube.getDimensions()[0];
                    Hierarchy measuresHierarchy = 
                        measuresDimension.getHierarchies()[0];
                    Level measuresLevel = 
                        measuresHierarchy.getLevels()[0];

                    buf.setLength(0);

                    Dimension[] dims = cube.getDimensions();
                    for (int j = 1; j < dims.length; j++) {
                        Hierarchy[] hierarchies = dims[j].getHierarchies();
                        for (int k = 0; k < hierarchies.length; k++) {
                            Level[] levels = hierarchies[k].getLevels();
                            Level lastLevel = levels[levels.length-1];
                            buf.append(lastLevel.getUniqueName());
                            if (k +1 < hierarchies.length) {
                                buf.append(',');
                            }
                        }
                        if (j +1 < dims.length) {
                            buf.append(',');
                        }
                    }
                    String levelListStr = buf.toString();

                    Member[] storedMembers =
                            schemaReader.getLevelMembers(measuresLevel, false);
                    for (int j = 0; j < storedMembers.length; j++) {
                        Member member = storedMembers[j];
                        String name = member.getName();
                        String unique = member.getUniqueName();
                        if (measureNameRT.passes(name) &&
                                measureUniqueNameRT.passes(unique)) {
                            populateMember(schemaReader, catalogName,
                                member, cube, levelListStr, rows);
                        }
                    }

                    List calcMembers =
                        schemaReader.getCalculatedMembers(measuresHierarchy);
                    for (Iterator it = calcMembers.iterator(); it.hasNext();) {
                        Member member = (Member) it.next();
                        String name = member.getName();
                        String unique = member.getUniqueName();
                        if (measureNameRT.passes(name) &&
                                measureUniqueNameRT.passes(unique)) {
                            populateMember(schemaReader, catalogName,
                                member, cube, null, rows);
                        }
                    }
                }
            }
        }

        private void populateMember(
            SchemaReader schemaReader,
            String catalogName,
            Member member,
            Cube cube,
            String levelListStr,
            List rows) {

            // Access control
            if (!canAccess(schemaReader, member)) {
                return;
            }

            if (member instanceof MemberBase) {
                MemberBase mb = (MemberBase) member;
                Boolean isVisible = (Boolean)
                       mb.getPropertyValue(Property.VISIBLE.name);
                if ((isVisible != null) && (! isVisible.booleanValue())) {
                    return;
                }
            }

            //TODO: currently this is always null
            String desc = member.getDescription();
            if (desc == null) {
                desc = cube.getName() +
                    " Cube - " +
                    member.getName() +
                    " Member";
            }

            Row row = new Row();
            row.set(CatalogName.name, catalogName);

            // SQL Server does not return this
            //row.set(SchemaName.name, cube.getSchema().getName());

            row.set(CubeName.name, cube.getName());
            row.set(MeasureName.name, member.getName());
            row.set(MeasureUniqueName.name, member.getUniqueName());
            row.set(MeasureCaption.name, member.getCaption());
            //row.set(MeasureGuid.name, "");

            Object aggProp =
                member.getPropertyValue(Property.AGGREGATION_TYPE.getName());
            int aggNumber = MDMEASURE_AGGR_UNKNOWN;
            if (aggProp != null) {
                RolapAggregator agg = (RolapAggregator) aggProp;
                if (agg == RolapAggregator.Sum) {
                    aggNumber = MDMEASURE_AGGR_SUM;
                } else if (agg == RolapAggregator.Count) {
                    aggNumber = MDMEASURE_AGGR_COUNT;
                } else if (agg == RolapAggregator.Min) {
                    aggNumber = MDMEASURE_AGGR_MIN;
                } else if (agg == RolapAggregator.Max) {
                    aggNumber = MDMEASURE_AGGR_MAX;
                } else if (agg == RolapAggregator.Avg) {
                    aggNumber = MDMEASURE_AGGR_AVG;
                }
                //TODO: what are VAR and STD
            } else {
                aggNumber = MDMEASURE_AGGR_CALCULATED;
            }
            row.set(MeasureAggregator.name, aggNumber);

            // DATA_TYPE DBType best guess is string
            int dbType = DBType.WSTR_ORDINAL;
            String datatype = (String)
                        member.getPropertyValue(Property.DATATYPE.getName());
            if (datatype != null) {
                if (datatype.equals("Integer")) {
                    dbType = DBType.I4_ORDINAL;
                } else if (datatype.equals("Numeric")) {
                    dbType = DBType.R8_ORDINAL;
                } else {
                    dbType = DBType.WSTR_ORDINAL;
                }
            }
            row.set(DataType.name, dbType);
            row.set(MeasureIsVisible.name, true);

            if (levelListStr != null) {
                row.set(LevelsList.name, levelListStr);
            }

            row.set(Description.name, desc);
            addRow(row, rows);
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaMembersRowset extends Rowset {
        private final RistrictionTest schemaNameRT;
        private final RistrictionTest cubeNameRT;
        private final RistrictionTest dimensionUniqueNameRT;
        private final RistrictionTest hierarchyUniqueNameRT;
        private final RistrictionTest memberNameRT;
        private final RistrictionTest memberUniqueNameRT;
        private final RistrictionTest memberTypeRT;

        MdschemaMembersRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
            schemaNameRT = getRistrictionTest(SchemaName);
            cubeNameRT = getRistrictionTest(CubeName);
            dimensionUniqueNameRT = getRistrictionTest(DimensionUniqueName);
            hierarchyUniqueNameRT = getRistrictionTest(HierarchyUniqueName);
            memberNameRT = getRistrictionTest(MemberName);
            memberUniqueNameRT = getRistrictionTest(MemberUniqueName);
            memberTypeRT = getRistrictionTest(MemberType);
        }

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the catalog to which this member belongs. ");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the schema to which this member belongs. ");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Name of the cube to which this member belongs.");
        private static final Column DimensionUniqueName =
            new Column(
                "DIMENSION_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Unique name of the dimension to which this member belongs. ");
        private static final Column HierarchyUniqueName =
            new Column(
                "HIERARCHY_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Unique name of the hierarchy. If the member belongs to more than one hierarchy, there is one row for each hierarchy to which it belongs.");
        private static final Column LevelUniqueName =
            new Column(
                "LEVEL_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                " Unique name of the level to which the member belongs.");
        private static final Column LevelNumber =
            new Column(
                "LEVEL_NUMBER",
                Type.UnsignedInteger,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The distance of the member from the root of the hierarchy.");
        private static final Column MemberOrdinal =
            new Column(
                "MEMBER_ORDINAL",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Ordinal number of the member. Sort rank of the member when members of this dimension are sorted in their natural sort order. If providers do not have the concept of natural ordering, this should be the rank when sorted by MEMBER_NAME.");
        private static final Column MemberName =
            new Column(
                "MEMBER_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Name of the member.");
        private static final Column MemberUniqueName =
            new Column(
                "MEMBER_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                " Unique name of the member.");
        private static final Column MemberType =
            new Column(
                "MEMBER_TYPE",
                Type.Integer,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Type of the member.");
        private static final Column MemberGuid =
            new Column(
                "MEMBER_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Memeber GUID.");
        private static final Column MemberCaption =
            new Column(
                "MEMBER_CAPTION",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "A label or caption associated with the member.");
        private static final Column ChildrenCardinality =
            new Column(
                "CHILDREN_CARDINALITY",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Number of children that the member has.");
        private static final Column ParentLevel =
            new Column(
                "PARENT_LEVEL",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The distance of the member's parent from the root level of the hierarchy. ");
        private static final Column ParentUniqueName =
            new Column(
                "PARENT_UNIQUE_NAME",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Unique name of the member's parent.");
        private static final Column ParentCount =
            new Column(
                "PARENT_COUNT",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Number of parents that this member has.");
        private static final Column TreeOp =
            new Column(
                "TREE_OP",
                Type.Enumeration,
                Enumeration.TreeOp.enumeration,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "Tree Operation");
        /* Mondrian specified member properties. */
        private static final Column Depth =
            new Column(
                "DEPTH",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "depth");

        /*
         *
         * http://msdn2.microsoft.com/es-es/library/ms126046.aspx
         *
         *
         * restrictions
         *   CATALOG_NAME Optional.
         *   SCHEMA_NAME Optional.
         *   CUBE_NAME Optional.
         *   DIMENSION_UNIQUE_NAME Optional.
         *   HIERARCHY_UNIQUE_NAME Optional.
         *   LEVEL_UNIQUE_NAME Optional.
         *   LEVEL_NUMBER Optional.
         *   MEMBER_NAME Optional.
         *   MEMBER_UNIQUE_NAME Optional.
         *   MEMBER_CAPTION Optional.
         *   MEMBER_TYPE Optional.
         *   TREE_OP (Optional) Only applies to a single member:
         *      MDTREEOP_ANCESTORS (0x20) returns all of the ancestors.
         *      MDTREEOP_CHILDREN (0x01) returns only the immediate children.
         *      MDTREEOP_SIBLINGS (0x02) returns members on the same level.
         *      MDTREEOP_PARENT (0x04) returns only the immediate parent.
         *      MDTREEOP_SELF (0x08) returns itself in the list of
         *                 returned rows.
         *      MDTREEOP_DESCENDANTS (0x10) returns all of the descendants.
         *   CUBE_SOURCE (Optional) A bitmap with one of the
         *      following valid values:
         *        1 CUBE
         *        2 DIMENSION
         *      Default restriction is a value of 1.         
         *
         * Not supported
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_MEMBERS", MDSCHEMA_MEMBERS, null,
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionUniqueName,
                    HierarchyUniqueName,
                    LevelUniqueName,
                    LevelNumber,
                    MemberOrdinal,
                    MemberName,
                    MemberUniqueName,
                    MemberType,
                    MemberGuid,
                    MemberCaption,
                    ChildrenCardinality,
                    ParentLevel,
                    ParentUniqueName,
                    ParentCount,
                    TreeOp,
                    Depth,
                },
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionUniqueName,
                    HierarchyUniqueName,
                    LevelUniqueName,
                    LevelNumber,
                    MemberOrdinal,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaMembersRowset(request, handler);
            }
        };
        public void populate(XmlaResponse response, List rows) 
                throws XmlaException {

            DataSourcesConfig.DataSource ds = 
                handler.getDataSource(request);
            String roleStr = request.getRole();
            DataSourcesConfig.Catalog[] catalogs = 
                    handler.getCatalogs(request, ds);

            for (int i = 0; i < catalogs.length; i++) {
                DataSourcesConfig.Catalog dsCatalog = catalogs[i];
                if (dsCatalog == null || dsCatalog.definition == null) {
                     continue;
                }
                Connection connection = 
                    handler.getConnection(dsCatalog, roleStr);
                if (connection == null) {
                    continue;
                }

                String catalogName = dsCatalog.name;
                if (schemaNameRT.passes(catalogName)) {
                    Role role = connection.getSchema().lookupRole(roleStr);
                    populateCatalog(connection, role, catalogName, rows);
                }
            }
        }
        protected void populateCatalog(Connection connection, 
                Role role, 
                String catalogName, 
                List rows) 
                throws XmlaException {
            
            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];
                if (cubeNameRT.passes(cube.getName())) {
                    SchemaReader schemaReader = cube.getSchemaReader(role);
                    populateCube(schemaReader, catalogName, cube, rows);
                }
            }
        }
        protected void populateCube(SchemaReader schemaReader,
                String catalogName, 
                Cube cube, 
                List rows) 
                throws XmlaException {

            if (isRestricted(MemberUniqueName)) {
                // NOTE: it is believed that if MEMBER_UNIQUE_NAME is
                // a restriction, then none of the remaining possible
                // restrictions other than TREE_OP are relevant 
                // (or allowed??).
                outputUniqueMemberName(schemaReader,
                                catalogName, cube, rows);
            } else if (isRestricted(LevelUniqueName)) {
                // Note: If the LEVEL_UNIQUE_NAME has been specified, then
                // the dimension and hierarchy are specified implicitly.
                String levelUniqueName = 
                    getRestrictionValueAsString(LevelUniqueName);
                if (levelUniqueName == null) {
                    // The query specified two or more unique names
                    // which means that nothing will match.
                    return;
                }
                final String[] nameParts = Util.explode(levelUniqueName);
                Hierarchy hier = cube.lookupHierarchy(nameParts[0], false);
                if (hier == null) {
                    return;
                }
                Level[] levels = hier.getLevels();
                for (int i = 0; i < levels.length; i++) {
                    Level level = levels[i];
                    if (! level.getUniqueName().equals(levelUniqueName)) {
                        continue;
                    }
                    Member[] members = 
                        cube.getSchemaReader(null).getLevelMembers(level, false);
                    outputMembers(schemaReader, members,
                        catalogName, cube, rows);
                }
                // finished
                return;

            } else {
                final Dimension[] dimensions = cube.getDimensions();
                for (int j = 0; j < dimensions.length; j++) {
                    Dimension dimension = dimensions[j];
                    String uniqueName = dimension.getUniqueName();
                    if (dimensionUniqueNameRT.passes(uniqueName)) {
                        populateDimension(schemaReader, catalogName,
                                    cube, dimension, rows);
                    }
                }
            }
        }
        protected void populateDimension(SchemaReader schemaReader, 
                String catalogName, 
                Cube cube, 
                Dimension dimension, 
                List rows) 
                throws XmlaException {
            
            Hierarchy[] hierarchies = dimension.getHierarchies();
            for (int i = 0; i < hierarchies.length; i++) {
                Hierarchy hierarchy = hierarchies[i];
                String uniqueName = hierarchy.getUniqueName();
                if (hierarchyUniqueNameRT.passes(uniqueName)) {
                    populateHierarchy(schemaReader, catalogName, 
                        cube, hierarchy, rows);
                }
            }
        }
        protected void populateHierarchy(SchemaReader schemaReader, 
                String catalogName, 
                Cube cube, 
                Hierarchy hierarchy, 
                List rows) 
                throws XmlaException {

             
             if (isRestricted(LevelNumber)) {
                int levelNumber = getRestrictionValueAsInt(LevelNumber);
                if (levelNumber == -1) {
                    LOGGER.warn("RowsetDefinition.populateHierarchy: " +
                        "LevelNumber invalid"
                        );
                    return;
                }
                Level[] levels = hierarchy.getLevels();
                if (levelNumber >= levels.length) {
                    LOGGER.warn("RowsetDefinition.populateHierarchy: " +
                        "LevelNumber (" +
                        levelNumber +
                        ") is greater than number of levels (" +
                        levels.length +
                        ") for hierarchy \"" +
                        hierarchy.getUniqueName() +
                        "\""
                        );
                    return;
                }

                Level level = levels[levelNumber];
                Member[] members = 
                    schemaReader.getLevelMembers(level, false);
                outputMembers(schemaReader, members, catalogName, cube, rows);
             } else {
                // At this point we get ALL of the members associated with
                // the Hierarchy (rather than getting them one at a time).
                // The value returned is not used at this point but they are
                // now cached in the SchemaReader.
                Member[][] membersArray =
                    RolapMember.getAllMembers(schemaReader, hierarchy);
                for (int i = 0; i < membersArray.length; i++) {
                    Member[] members = membersArray[i];
                        outputMembers(schemaReader, members,
                            catalogName, cube, rows);
                }
            }
        }

        /**
         * Returns whether a value contains all of the bits in a mask.
         */
        private static boolean mask(int value, int mask) {
            return (value & mask) == mask;
        }

        /**
         * Adds a member to a result list and, depending upon the
         * <code>treeOp</code> parameter, other relatives of the member. This
         * method recursively invokes itself to walk up, down, or across the
         * hierarchy.
         */
        private void populateMember(
            final SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            Member member,
            int treeOp,
            List rows) {

            // Visit node itself.
            if (mask(treeOp, Enumeration.TreeOp.Self.ordinal)) {
                outputMember(schemaReader, member, catalogName, cube, rows);
            }
            // Visit node's siblings (not including itself).
            if (mask(treeOp, Enumeration.TreeOp.Siblings.ordinal)) {
                final Member parent =
                        schemaReader.getMemberParent(member);
                final Member[] siblings = (parent == null)
                    ?  schemaReader.getHierarchyRootMembers(member.getHierarchy())
                    : schemaReader.getMemberChildren(parent);

                for (int i = 0; i < siblings.length; i++) {
                    Member sibling = siblings[i];
                    if (sibling == member) {
                        continue;
                    }
                    populateMember(schemaReader, catalogName,
                            cube, sibling,
                        Enumeration.TreeOp.Self.ordinal, rows);
                }
            }
            // Visit node's descendants or its immediate children, but not both.
            if (mask(treeOp, Enumeration.TreeOp.Descendants.ordinal)) {
                final Member[] children = schemaReader.getMemberChildren(member);
                for (int i = 0; i < children.length; i++) {
                    Member child = children[i];
                    populateMember(schemaReader, catalogName,
                            cube, child,
                        Enumeration.TreeOp.Self.ordinal |
                        Enumeration.TreeOp.Descendants.ordinal, rows);
                }
            } else if (mask(treeOp, Enumeration.TreeOp.Children.ordinal)) {
                final Member[] children =
                        schemaReader.getMemberChildren(member);
                for (int i = 0; i < children.length; i++) {
                    Member child = children[i];
                    populateMember(schemaReader, catalogName,
                            cube, child,
                        Enumeration.TreeOp.Self.ordinal, rows);
                }
            }
            // Visit node's ancestors or its immediate parent, but not both.
            if (mask(treeOp, Enumeration.TreeOp.Ancestors.ordinal)) {
                final Member parent = schemaReader.getMemberParent(member);
                if (parent != null) {
                    populateMember(schemaReader, catalogName,
                            cube, parent,
                        Enumeration.TreeOp.Self.ordinal |
                        Enumeration.TreeOp.Ancestors.ordinal, rows);
                }
            } else if (mask(treeOp, Enumeration.TreeOp.Parent.ordinal)) {
                final Member parent = schemaReader.getMemberParent(member);
                if (parent != null) {
                    populateMember(schemaReader, catalogName,
                            cube, parent,
                        Enumeration.TreeOp.Self.ordinal, rows);
                }
            }
        }

        protected ArrayList pruneRestrictions(ArrayList list) {
            // If they've restricted TreeOp, we don't want to literally filter
            // the result on TreeOp (because it's not an output column) or
            // on MemberUniqueName (because TreeOp will have caused us to
            // generate other members than the one asked for).
            if (list.contains(TreeOp)) {
                list.remove(TreeOp);
                list.remove(MemberUniqueName);
            }
            return list;
        }

        private void outputMembers(
            final SchemaReader schemaReader,
            Member[] members,
            final String catalogName,
            Cube cube, List rows) {

            for (int i = 0; i < members.length; i++) {
                Member member = members[i];
                outputMember(schemaReader, member, catalogName, cube, rows);
            }
        }
        private void outputUniqueMemberName(
            final SchemaReader schemaReader,
            final String catalogName,
            Cube cube, List rows) {

            String memberUniqueName = 
                            getRestrictionValueAsString(MemberUniqueName);
            if (memberUniqueName == null) {
                // The query specified two or more unique names
                // which means that nothing will match.
                return;
            }
            final String[] nameParts = Util.explode(memberUniqueName);

            Member member = schemaReader.getMemberByUniqueName(
                                nameParts, false);

            if (member == null) {
                return;
            }
            if (isRestricted(TreeOp)) {
                int treeOp = getRestrictionValueAsInt(TreeOp);
                if (treeOp == -1) {
                    return;
                }
                populateMember(schemaReader, catalogName,
                            cube, member, treeOp, rows);
            } else {
                outputMember(schemaReader, member, catalogName, cube, rows);
            }
        }
         
        private void outputMember(
            final SchemaReader schemaReader,
            Member member,
            final String catalogName,
            Cube cube, List rows) {

            // Access control
            if (!canAccess(schemaReader, member)) {
                return;
            }
            if (! memberNameRT.passes(member.getName())) {
                return;
            }
            if (! memberTypeRT.passes(member.getMemberType())) {
                return;
            }

            if (member.getOrdinal() == -1) {
                RolapMember.setOrdinals(schemaReader, member);
            }

            final Level level = member.getLevel();
            final Hierarchy hierarchy = level.getHierarchy();
            final Dimension dimension = hierarchy.getDimension();
            Row row = new Row();
            row.set(CatalogName.name, catalogName);
            row.set(SchemaName.name, catalogName);
            row.set(CubeName.name, cube.getName());
            row.set(DimensionUniqueName.name, dimension.getUniqueName());
            row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
            row.set(LevelUniqueName.name, level.getUniqueName());
            row.set(LevelNumber.name, level.getDepth());
            row.set(MemberOrdinal.name, member.getOrdinal());
            row.set(MemberName.name, member.getName());
            row.set(MemberUniqueName.name, member.getUniqueName());
            row.set(MemberType.name, member.getMemberType());
            //row.set(MemberGuid.name, "");
            row.set(MemberCaption.name, member.getCaption());
            row.set(ChildrenCardinality.name, 
                member.getPropertyValue(Property.CHILDREN_CARDINALITY.name));
            row.set(ChildrenCardinality.name, 100);

            row.set(ParentLevel.name, 
                (member.getParentMember() == null) 
                    ? 0 : member.getParentMember().getDepth());

            String parentUniqueName = member.getParentUniqueName();
            if (parentUniqueName != null) {
                row.set(ParentUniqueName.name, parentUniqueName);
            } else {
                // row.set(ParentUniqueName.name, "");
            }

            row.set(ParentCount.name, member.getParentMember() == null ? 0 : 1);

            row.set(Depth.name, member.getDepth());
            addRow(row, rows);
        }
        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }

/*
RME: this code was used to test various algorithms to set member ordinals.
        void setOrdinals(SchemaReader schemaReader, Member startMember) {
            //int v = 0;
            //int v = 1;
            //int v = 2;
            //int v = 3;
            int v = 4;
LOGGER.debug("RowsetDefinition.setOrdinals: v==" +v);
            if (v == 0) {
                // foodmart time for Sales: time=63
                // For very big data set, it takes time= 651865ms
                RolapMember.setOrdinals(schemaReader, startMember);
            } else if (v == 1) {
                // foodmart time for Sales: time=32
                // result is same as v == 0
                // For very big data set, it takes time= 73880ms
                Hierarchy hierarchy = startMember.getHierarchy();
                Member[][] membersArray = 
                    RolapMember.getAllMembers(schemaReader, hierarchy);
                RolapMember.setOrdinals(schemaReader, startMember);
            } else if (v == 2) {
                // foodmart time for Sales: time=18
                // result is NOT same as v == 0
                int ordinal = 0;
                Hierarchy hierarchy = startMember.getHierarchy();
                Member[][] membersArray = 
                    RolapMember.getAllMembers(schemaReader, hierarchy);

                // RME: this does a breath first setting of ordinals
                // for very big data set, it takes time= 4197ms
                for (int i = 0; i < membersArray.length; i++) {
                    Member[] members = membersArray[i];
                    for (int j = 0; j < members.length; j++) {
                        Member member = members[j];
                         ((RolapMember) member).setOrdinal(ordinal++);
                    }
                }
            } else if (v == 3) {
                // foodmart time for Sales: time=19
                // result is same as v == 0
                int ordinal = 1;
                int depth = 1;
                Hierarchy hierarchy = startMember.getHierarchy();
                Member[][] membersArray = 
                    RolapMember.getAllMembers(schemaReader, hierarchy);
                Member[] rootMembers = membersArray[0];
LOGGER.debug("RowsetDefinition.setOrdinals: rootMembers.length=" +rootMembers.length);
                // RME: this does a depth first setting of ordinals
                // for very big data set, it takes time= 97310ms
                for (int i = 0; i < rootMembers.length; i++) {
                    Member member = rootMembers[i];
                    if (member.getOrdinal() == -1) {
LOGGER.debug("RowsetDefinition.setOrdinals: member=" +member.getName());
                        ((RolapMember) member).setOrdinal(ordinal++);
                    } else {
LOGGER.debug("RowsetDefinition.setOrdinals: NO member=" +member.getName());
                    }
                    ordinal = setOrdinals(ordinal, member, membersArray, depth);
                }
            } else {
                // foodmart time for Sales: time=17
                // result is same as v == 0
                // bottom up depth first
                // For very big data set, it takes time= 4241ms
                int ordinal = 1;
                Hierarchy hierarchy = startMember.getHierarchy();
                Member[][] membersArray = 
                    RolapMember.getAllMembers(schemaReader, hierarchy);
                Member[] leafMembers = membersArray[membersArray.length-1];

                for (int i = 0; i < leafMembers.length; i++) {
                    Member child = leafMembers[i];
                    ordinal = bottomUpSetOrdinals(ordinal, child);
                    ((RolapMember) child).setOrdinal(ordinal++);
                }
                boolean needsFullTopDown = false;
                for (int i = 0; i < membersArray.length-1; i++) {
                    Member[] members = membersArray[i];
                    for (int j = 0; j < members.length; j++) {
                        Member member = members[j];
                        if (member.getOrdinal() == -1) {
                            needsFullTopDown = true;
                            break;
                        }
                    }
                }
LOGGER.debug("RowsetDefinition.setOrdinals: needsFullTopDown=" +needsFullTopDown);
            }
        }
        int bottomUpSetOrdinals(int ordinal, Member child) {
            Member parent = child.getParentMember();
            if (parent.getOrdinal() == -1) {
                ordinal = bottomUpSetOrdinals(ordinal, parent);
                ((RolapMember) parent).setOrdinal(ordinal++);
            }
            return ordinal;
        }


        // RME: this is used as part of the depth first setting of ordinals
        int setOrdinals(int ordinal, Member parent, Member[][] membersArray, int depth) {
            boolean nextLevelExists = (depth+1 < membersArray.length);
            Member[] members = membersArray[depth];
            for (int i = 0; i < members.length; i++) {
                Member member = members[i];
                if (member.getParentMember() == parent) {
                    ((RolapMember) member).setOrdinal(ordinal++);
                    if (nextLevelExists) {
                        ordinal = 
                            setOrdinals(ordinal, member, membersArray, depth+1);
                    }
                }
            }
            return ordinal;
        }
*/

    }

    static class MdschemaSetsRowset extends Rowset {
        MdschemaSetsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, true, null);
        private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, true, true, null);
        private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
        private static final Column SetName = new Column("SET_NAME", Type.String, null, true, false, null);
        private static final Column SetCaption = new Column("SET_CAPTION", Type.String, null, true, true, null);
        private static final Column Scope = new Column("SCOPE", Type.Integer, null, true, false, null);
        private static final Column Description = new Column("DESCRIPTION", Type.String, null, false, true, "A human-readable description of the measure. ");
        /*
         * http://msdn2.microsoft.com/en-us/library/ms126290(SQL.90).aspx
         * http://msdn.microsoft.com/library/default.asp?url=/library/en-us/oledb/htm/olapproperties_rowset.asp
         *
         * restrictions
         *    CATALOG_NAME Optional.
         *    SCHEMA_NAME Optional.
         *    CUBE_NAME Optional.
         *    SET_NAME Optional.
         *    SCOPE Optional.
         *    HIERARCHY_UNIQUE_NAME Optional.
         *    CUBE_SOURCE Optional.
         *        Note: Only one hierarchy can be included, and only those named
         *        sets whose hierarchies exactly match the restriction are
         *        returned.
         *
         * Not supported
         *    EXPRESSION
         *    DIMENSIONS
         *    SET_DISPLAY_FOLDER
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_SETS", MDSCHEMA_SETS, null,
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    SetName,
                    Scope,
                },
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaSetsRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_UNSUPPORTED_OPERATION_CODE,
                HSB_UNSUPPORTED_OPERATION_FAULT_FS,
                new UnsupportedOperationException("MDSCHEMA_SETS"));
        }
    }

    static class MdschemaPropertiesRowset extends Rowset {
        private final RistrictionTest schemaNameRT;
        private final RistrictionTest cubeNameRT;
        private final RistrictionTest dimensionUniqueNameRT;
        private final RistrictionTest hierarchyUniqueNameRT;
        private final RistrictionTest propertyNameRT;
        MdschemaPropertiesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
            schemaNameRT = getRistrictionTest(SchemaName);
            cubeNameRT = getRistrictionTest(CubeName);
            dimensionUniqueNameRT = getRistrictionTest(DimensionUniqueName);
            hierarchyUniqueNameRT = getRistrictionTest(HierarchyUniqueName);
            propertyNameRT = getRistrictionTest(PropertyName);
        }

        private static final int MDPROP_MEMBER = 0x01;
        private static final int MDPROP_CELL = 0x02;
        private static final int MDPROP_SYSTEM = 0x04;
        private static final int MDPROP_BLOB = 0x08;

        private static final int MD_PROPTYPE_REGULAR = 0x00;

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the database.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the schema to which this property belongs.");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the cube.");
        private static final Column DimensionUniqueName =
            new Column(
                "DIMENSION_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The unique name of the dimension.");
        private static final Column HierarchyUniqueName =
            new Column(
                "HIERARCHY_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The unique name of the hierarchy.");
        private static final Column LevelUniqueName =
            new Column(
                "LEVEL_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The unique name of the level to which this property belongs.");
        // According to MS this should not be nullable
        private static final Column MemberUniqueName =
            new Column(
                "MEMBER_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The unique name of the member to which the property belongs.");
        private static final Column PropertyName =
            new Column(
                "PROPERTY_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Name of the property.");
        private static final Column PropertyType =
            new Column(
                "PROPERTY_TYPE",
                Type.Short,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "A bitmap that specifies the type of the property");
        private static final Column PropertyCaption =
            new Column(
                "PROPERTY_CAPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A label or caption associated with the property, used primarily for display purposes.");
        private static final Column DataType =
            new Column(
                "DATA_TYPE",
                Type.UnsignedShort,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Data type of the property.");
        private static final Column PropertyContentType =
            new Column(
                "PROPERTY_CONTENT_TYPE",
                Type.Short,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The type of the property. ");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A human-readable description of the measure. ");
        /*
         * http://msdn2.microsoft.com/en-us/library/ms126309(SQL.90).aspx
         * http://msdn.microsoft.com/library/default.asp?url=/library/en-us/oledb/htm/olapproperties_rowset.asp
         *
         * restrictions
         *    CATALOG_NAME Mandatory
         *    SCHEMA_NAME Optional
         *    CUBE_NAME Optional
         *    DIMENSION_UNIQUE_NAME Optional
         *    HIERARCHY_UNIQUE_NAME Optional
         *    LEVEL_UNIQUE_NAME Optional
         *
         *    MEMBER_UNIQUE_NAME Optional
         *    PROPERTY_NAME Optional
         *    PROPERTY_TYPE Optional
         *    PROPERTY_CONTENT_TYPE
         *       (Optional) A default restriction is in place on MDPROP_MEMBER
         *       OR MDPROP_CELL.
         *    PROPERTY_ORIGIN
         *       (Optional) A default restriction is in place on MD_USER_DEFINED
         *       OR MD_SYSTEM_ENABLED
         *    CUBE_SOURCE
         *       (Optional) A bitmap with one of the following valid values:
         *       1 CUBE
         *       2 DIMENSION
         *       Default restriction is a value of 1.
         *    PROPERTY_VISIBILITY
         *       (Optional) A bitmap with one of the following valid values:
         *       1 Visible
         *       2 Not visible
         *       Default restriction is a value of 1.
         *
         * Not supported
         *    PROPERTY_ORIGIN
         *    CUBE_SOURCE
         *    PROPERTY_VISIBILITY
         *    CHARACTER_MAXIMUM_LENGTH
         *    CHARACTER_OCTET_LENGTH
         *    NUMERIC_PRECISION
         *    NUMERIC_SCALE
         *    DESCRIPTION
         *    SQL_COLUMN_NAME
         *    LANGUAGE
         *    PROPERTY_ATTRIBUTE_HIERARCHY_NAME
         *    PROPERTY_CARDINALITY
         *    MIME_TYPE
         *    PROPERTY_IS_VISIBLE
         */
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_PROPERTIES", MDSCHEMA_PROPERTIES, null,
                new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionUniqueName,
                    HierarchyUniqueName,
                    LevelUniqueName,
                    MemberUniqueName,
                    PropertyName,
                    PropertyCaption,
                    PropertyType,
                    DataType,
                    PropertyContentType,
                    Description
                },
                null /* not sorted */) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaPropertiesRowset(request, handler);
            }
        };

        public void populate(XmlaResponse response, List rows) throws XmlaException {
            DataSourcesConfig.DataSource ds = 
                handler.getDataSource(request);
            String roleStr = request.getRole();
            DataSourcesConfig.Catalog[] catalogs = 
                    handler.getCatalogs(request, ds);

            for (int i = 0; i < catalogs.length; i++) {
                DataSourcesConfig.Catalog dsCatalog = catalogs[i];
                if (dsCatalog == null || dsCatalog.definition == null) {
                     continue;
                }
                Connection connection = 
                    handler.getConnection(dsCatalog, roleStr);
                if (connection == null) {
                    continue;
                }

                String catalogName = dsCatalog.name;
                if (schemaNameRT.passes(catalogName)) {
                    Role role = connection.getSchema().lookupRole(roleStr);
                    populateCatalog(connection, role, catalogName, rows);
                }
            }
        }
        protected void populateCatalog(Connection connection, 
                Role role, 
                String catalogName, 
                List rows) 
                throws XmlaException {

            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];
                if (cubeNameRT.passes(cube.getName())) {
                    SchemaReader schemaReader = cube.getSchemaReader(role);
                    populateCube(schemaReader, catalogName, cube, rows);
                }
            }
        }
        protected void populateCube(SchemaReader schemaReader,
                String catalogName, 
                Cube cube, 
                List rows) 
                throws XmlaException {
            if (isRestricted(LevelUniqueName)) {
                // Note: If the LEVEL_UNIQUE_NAME has been specified, then
                // the dimension and hierarchy are specified implicitly.
                String levelUniqueName = 
                    getRestrictionValueAsString(LevelUniqueName);
                if (levelUniqueName == null) {
                    // The query specified two or more unique names
                    // which means that nothing will match.
                    return;
                }
                final String[] nameParts = Util.explode(levelUniqueName);
                Hierarchy hier = cube.lookupHierarchy(nameParts[0], false);
                if (hier == null) {
                    return;
                }
                Level[] levels = hier.getLevels();
                for (int i = 0; i < levels.length; i++) {
                    Level level = levels[i];
                    if (level.getUniqueName().equals(levelUniqueName)) {
                        populateLevel(schemaReader, catalogName,
                                cube, level, rows);
                        break;
                    }
                }
                // finished
                return;

            } else {
                final Dimension[] dimensions = cube.getDimensions();
                for (int j = 0; j < dimensions.length; j++) {
                    Dimension dimension = dimensions[j];
                    String uniqueName = dimension.getUniqueName();
                    if (dimensionUniqueNameRT.passes(uniqueName)) {
                        populateDimension(schemaReader, catalogName,
                                    cube, dimension, rows);
                    }
                }
            }
        }
        private void populateDimension(
            final SchemaReader schemaReader,
            final String catalogName,
            Cube cube, Dimension dimension, List rows) {

            Hierarchy[] hierarchies = dimension.getHierarchies();
            for (int i = 0; i < hierarchies.length; i++) {
                Hierarchy hierarchy = hierarchies[i];
                String unique = hierarchy.getUniqueName();
                if (hierarchyUniqueNameRT.passes(unique)) {
                    populateHierarchy(schemaReader, catalogName,
                                    cube, hierarchy, rows);
                }
            }
        }
        private void populateHierarchy(
            final SchemaReader schemaReader,
            final String catalogName,
            Cube cube, Hierarchy hierarchy, List rows) {

            Level[] levels = hierarchy.getLevels();
            for (int i = 0; i < levels.length; i++) {
                Level level = levels[i];
                populateLevel(schemaReader, catalogName,
                              cube, level, rows);
            }
        }
        private void populateLevel(
            final SchemaReader schemaReader,
            final String catalogName,
            Cube cube, Level level, List rows) {

            Property[] properties = level.getProperties();
            for (int i = 0; i < properties.length; i++) {
                Property property = properties[i];
                if (propertyNameRT.passes(property.getName())) {
                    outputProperty(schemaReader, property,
                            catalogName, cube, level, rows);
                }
            }
        }

        private void outputProperty(
            final SchemaReader schemaReader,
            Property property,
            final String catalogName,
            Cube cube, Level level, List rows) {

            Hierarchy hierarchy = level.getHierarchy();
            Dimension dimension = hierarchy.getDimension();

            String propertyName = property.getName();

            Row row = new Row();
            row.set(CatalogName.name, catalogName);
            row.set(SchemaName.name, catalogName);
            row.set(CubeName.name, cube.getName());
            row.set(DimensionUniqueName.name, dimension.getUniqueName());
            row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
            row.set(LevelUniqueName.name, level.getUniqueName());
            //TODO: what is the correct value here
            //row.set(MemberUniqueName.name, "");

            row.set(PropertyName.name, propertyName);
            // Only member properties now
            row.set(PropertyType.name, MDPROP_MEMBER);
            row.set(PropertyContentType.name, MD_PROPTYPE_REGULAR);
            row.set(PropertyCaption.name, property.getCaption());
            DBType dbType = getDBTypeFromProperty(property);
            row.set(DataType.name, dbType.getOrdinal());

            String desc = cube.getName() +
                " Cube - " +
                hierarchy.getName() +
                " Hierarchy - " +
                level.getName() +
                " Level - " +
                property.getName() +
                " Property";
            row.set(Description.name, desc);

            addRow(row, rows);
        }


        protected void setProperty(PropertyDefinition propertyDef, String value) {
            switch (propertyDef.ordinal) {
            case PropertyDefinition.Content_ORDINAL:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    private static boolean canAccess(SchemaReader schemaReader, OlapElement elem) {
        Role role = schemaReader.getRole();
        return role.canAccess(elem);
    }
}

// End RowsetDefinition.java
