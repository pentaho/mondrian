package mondrian.xmla;

import mondrian.olap.*;
import mondrian.util.SAXHandler;
import org.xml.sax.SAXException;

import java.lang.reflect.Field;
import java.util.Properties;
import java.util.HashMap;

/**
 * <code>RowsetDefinition</code> defines a rowset, including the columns it
 * should contain.
 *
 * <p>See "XML for Analysis Rowsets", page 38 of the XML for Analysis
 * Specification, version 1.1.
 */
abstract class RowsetDefinition extends EnumeratedValues.BasicValue {
    final Column[] columnDefinitions;
    private static final String nl = System.getProperty("line.separator");
    /** Returns a list of XML for Analysis data sources
     * available on the server or Web Service. (For an
     * example of how these may be published, see
     * "XML for Analysis Implementation Walkthrough"
     * in the XML for Analysis specification.) */
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
    public static final int MDSCHEMA_MEASURES = 16;
    public static final int MDSCHEMA_MEMBERS = 17;
    public static final int MDSCHEMA_PROPERTIES = 18;
    public static final int MDSCHEMA_SETS = 19;
    public static final int OTHER = 20;
    public static final EnumeratedValues enumeration = new EnumeratedValues(
            new RowsetDefinition[] {
                DatasourcesRowset.definition,
                DiscoverPropertiesRowset.definition,
                DiscoverSchemaRowsetsRowset.definition,
                DiscoverEnumeratorsRowset.definition,
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
                MdschemaMeasuresRowset.definition,
                MdschemaMembersRowset.definition,
                MdschemaPropertiesRowset.definition,
                MdschemaSetsRowset.definition,
            }
    );

    RowsetDefinition(String name, int ordinal, Column[] columnDefinitions) {
        super(name, ordinal, null);
        this.columnDefinitions = columnDefinitions;
    }

    static class Type extends EnumeratedValues.BasicValue {
        public static final int String_ORDINAL = 0;
        public static final Type String = new Type("String", String_ORDINAL);
        public static final int StringArray_ORDINAL = 1;
        public static final Type StringArray = new Type("StringArray", StringArray_ORDINAL);
        public static final int Array_ORDINAL = 2;
        public static final Type Array = new Type("Array", Array_ORDINAL);
        public static final int Enumeration_ORDINAL = 3;
        public static final Type Enumeration = new Type("Enumeration", Enumeration_ORDINAL);
        public static final int EnumerationArray_ORDINAL = 4;
        public static final Type EnumerationArray = new Type("EnumerationArray", EnumerationArray_ORDINAL);
        public static final int EnumString_ORDINAL = 5;
        public static final Type EnumString = new Type("EnumString", EnumString_ORDINAL);
        public static final int Boolean_ORDINAL = 6;
        public static final Type Boolean = new Type("Boolean", Boolean_ORDINAL);
        public static final int StringSometimesArray_ORDINAL = 7;
        public static final Type StringSometimesArray = new Type("StringSometimesArray", StringSometimesArray_ORDINAL);
        public static final int Integer_ORDINAL = 8;
        public static final Type Integer = new Type("Integer", Integer_ORDINAL);
        public static final int UnsignedInteger_ORDINAL = 9;
        public static final Type UnsignedInteger = new Type("UnsignedInteger", UnsignedInteger_ORDINAL);

        public Type(String name, int ordinal) {
            super(name, ordinal, null);
        }

        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new Type[] {
                    String, StringArray, Array, Enumeration, EnumerationArray, EnumString,
                });

        boolean isEnum() {
            switch (ordinal_) {
            case Enumeration_ORDINAL:
            case EnumerationArray_ORDINAL:
            case EnumString_ORDINAL:
                return true;
            }
            return false;
        }
    }

    static class Column {
        final String name;
        final Type type;
        final EnumeratedValues enumeratedType;
        final String description;
        final boolean restriction;
        final boolean nullable;

        /**
         * Creates a column.
         * @param name
         * @param type A {@link Type} value
         * @param enumeratedType Must be specified for enumeration or array
         *   of enumerations
         * @param description
         * @param restriction
         * @param nullable
         * @pre type != null
         * @pre (type == Type.Enumeration || type == Type.EnumerationArray || type == Type.EnumString) == (enumeratedType != null)
         */
        Column(String name, Type type, EnumeratedValues enumeratedType,
                boolean restriction, boolean nullable, String description) {
            Util.assertPrecondition(type != null, "Type.instance.isValid(type)");
            Util.assertPrecondition((type == Type.Enumeration || type == Type.EnumerationArray || type == Type.EnumString) == (enumeratedType != null), "(type == Type.Enumeration || type == Type.EnumerationArray || type == Type.EnumString) == (enumeratedType != null)");
            this.name = name;
            this.type = type;
            this.enumeratedType = enumeratedType;
            this.description = description;
            this.restriction = restriction;
            this.nullable = nullable;
        }

        /**
         * Retrieves a value of this column from a row. The base implementation
         * uses reflection; a derived class may provide a different
         * implementation.
         */
        Object get(Object row) {
            try {
                String javaFieldName = name.substring(0, 1).toLowerCase() + name.substring(1);
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
    }

    static class DatasourcesRowset extends Rowset {
        private static final Column DataSourceName = new Column("DataSourceName", Type.String, null, true, false,
                                    "The name of the data source, such as FoodMart 2000.");
        private static final Column DataSourceDescription = new Column("DataSourceDescription", Type.String, null, false, true,
                                    "A description of the data source, as entered by the publisher.");
        private static final Column URL = new Column("URL", Type.String, null, true, true,
                                    "The unique path that shows where to invoke the XML for Analysis methods for that data source.");
        private static final Column DataSourceInfo = new Column("DataSourceInfo", Type.String, null, false, true,
                                    "A string containing any additional information required to connect to the data source. This can include the Initial Catalog property or other information for the provider." + nl +
                        "Example: \"Provider=MSOLAP;Data Source=Local;\"");
        private static final Column ProviderName = new Column("ProviderName", Type.String, null, true, true,
                                    "The name of the provider behind the data source. " + nl +
                        "Example: \"MSDASQL\"");
        private static final Column ProviderType = new Column("ProviderType", Type.EnumerationArray, Enumeration.ProviderType.enumeration, true, false,
                                    "The types of data supported by the provider. May include one or more of the following types. Example follows this table." + nl +
                        "TDP: tabular data provider." + nl +
                        "MDP: multidimensional data provider." + nl +
                        "DMP: data mining provider. A DMP provider implements the OLE DB for Data Mining specification.");
        private static final Column AuthenticationMode = new Column("AuthenticationMode", Type.EnumString, Enumeration.AuthenticationMode.enumeration, true, false,
                                    "Specification of what type of security mode the data source uses. Values can be one of the following:" + nl +
                        "Unauthenticated: no user ID or password needs to be sent." + nl +
                        "Authenticated: User ID and Password must be included in the information required for the connection." + nl +
                        "Integrated: the data source uses the underlying security to determine authorization, such as Integrated Security provided by Microsoft Internet Information Services (IIS).");
        static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_DATASOURCES", DISCOVER_DATASOURCES, new Column[] {
                    DataSourceName,
                    DataSourceDescription,
                    URL,
                    DataSourceInfo,
                    ProviderName,
                    ProviderType,
                    AuthenticationMode,
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new DatasourcesRowset(restrictions, properties);
            }
        };

        public DatasourcesRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public void unparse(SAXHandler saxHandler) throws SAXException {
//            Connection connection = XmlaMediator.getConnection(properties);
            Row row = new Row();
            row.set(DataSourceName.name, null);
            row.set(DataSourceDescription.name, null);
            row.set(URL.name, null);
            row.set(DataSourceInfo.name, null);
            row.set(ProviderType.name, null);
            row.set(AuthenticationMode.name, null);
            emit(row, saxHandler);
        }
    }

    static class SchemaRowsetsRowset extends Rowset {
        private static RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_DATASOURCES", DISCOVER_DATASOURCES, new Column[] {
                    new Column(
                            "SchemaName",
                            Type.EnumerationArray,
                            null, true, false, "The name of the schema/request. This returns the values in the RequestTypes enumeration, plus any additional types suppoted by the provider. The provider defines rowset structures for the additional types"
                    ),
                    new Column(
                            "Restrictions",
                            Type.Array,
                            null, false, true, "An array of the restrictions suppoted by provider. An example follows this table."
                    ),
                    new Column(
                            "Description",
                            Type.String,
                            null, false, true, "A localizable description of the schema"
                    ),
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new SchemaRowsetsRowset(restrictions, properties);
            }

        };

        public SchemaRowsetsRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public void unparse(SAXHandler saxHandler) throws SAXException {
            emit(enumeration, saxHandler);
        }
    }

    public static RowsetDefinition getValue(String name) {
        return (RowsetDefinition) enumeration.getValue(name);
    }

    public abstract Rowset getRowset(HashMap restrictions, Properties properties);

    public Column lookupColumn(String name) {
        for (int i = 0; i < columnDefinitions.length; i++) {
            Column columnDefinition = columnDefinitions[i];
            if (columnDefinition.name.equals(name)) {
                return columnDefinition;
            }
        }
        return null;
    }

    static class DiscoverPropertiesRowset extends Rowset {
        DiscoverPropertiesRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final Column PropertyName = new Column("PropertyName", Type.StringSometimesArray, null, true, false,
                "The name of the property.");
        public static final Column PropertyDescription = new Column("PropertyDescription", Type.String, null, false, true,
                "A localizable text description of the property.");
        public static final Column PropertyType = new Column("PropertyType", Type.String, null, false, true,
                "The XML data type of the property.");
        public static final Column PropertyAccessType = new Column("PropertyAccessType", Type.EnumString, Enumeration.Access.enumeration, false, false,
                "Access for the property. The value can be Read, Write, or ReadWrite.");
        public static final Column IsRequired = new Column("IsRequired", Type.Boolean, null, false, true,
                "True if a property is required, false if it is not required.");
        public static final Column Value = new Column("Value", Type.String, null, false, true,
                "The current value of the property.");
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_PROPERTIES", DISCOVER_PROPERTIES, new Column[] {
                    PropertyName,
                    PropertyDescription,
                    PropertyType,
                    PropertyAccessType,
                    IsRequired,
                    Value,
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new DiscoverPropertiesRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            final String[] propertyNames = PropertyDefinition.enumeration.getNames();
            for (int i = 0; i < propertyNames.length; i++) {
                PropertyDefinition propertyDefinition = PropertyDefinition.getValue(propertyNames[i]);
                Row row = new Row();
                row.set(PropertyName.name, propertyDefinition.name_);
                row.set(PropertyDescription.name, propertyDefinition.description_);
                row.set(PropertyType.name, propertyDefinition.type);
                row.set(PropertyAccessType.name, propertyDefinition.access);
                //row.set(IsRequired.name, false);
                //row.set(Value.name, null);
                emit(row, saxHandler);
            }
        }
    }

    static class DiscoverSchemaRowsetsRowset extends Rowset {
        DiscoverSchemaRowsetsRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_SCHEMA_ROWSETS", DISCOVER_SCHEMA_ROWSETS, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new DiscoverSchemaRowsetsRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DiscoverEnumeratorsRowset extends Rowset {
        DiscoverEnumeratorsRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_ENUMERATORS", DISCOVER_ENUMERATORS, new Column[] {
                    new Column("EnumName", Type.StringArray, null, true, false,
                            "The name of the enumerator that contains a set of values."),
                    new Column("EnumDescription", Type.String, null, false, true,
                            "A localizable description of the enumerator."),
                    new Column("EnumType", Type.String, null, false, false,
                            "The data type of the Enum values."),
                    new Column("ElementName", Type.String, null, false, false,
                            "The name of one of the value elements in the enumerator set." + nl +
                "Example: TDP"),
                    new Column("ElementDescription", Type.String, null, false, true,
                            "A localizable description of the element (optional)."),
                    new Column(
                            "ElementValue", Type.String, null, false, true, "The value of the element." + nl +
                "Example: 01"),
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new DiscoverEnumeratorsRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DiscoverKeywordsRowset extends Rowset {
        DiscoverKeywordsRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_KEYWORDS", DISCOVER_KEYWORDS, new Column[] {
                    new Column("Keyword", Type.StringSometimesArray, null, true, false,
                            "A list of all the keywords reserved by a provider." + nl +
                "Example: AND"),
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new DiscoverKeywordsRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DiscoverLiteralsRowset extends Rowset {
        DiscoverLiteralsRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_LITERALS", DISCOVER_LITERALS, new Column[] {
                    new Column("LiteralName", Type.StringSometimesArray, null, true, false,
                            "The name of the literal described in the row." + nl +
                "Example: DBLITERAL_LIKE_PERCENT"),
                    new Column("LiteralValue", Type.String, null, false, true,
                            "Contains the actual literal value." + nl +
                "Example, if LiteralName is DBLITERAL_LIKE_PERCENT and the percent character (%) is used to match zero or more characters in a LIKE clause, this column’s value would be \"%\"."),
                    new Column("LiteralInvalidChars", Type.String, null, false, true,
                            "The characters, in the literal, that are not valid." + nl +
                "For example, if table names can contain anything other than a numeric character, this string would be \"0123456789\"."),
                    new Column("LiteralInvalidStartingChars", Type.String, null, false, true,
                            "The characters that are not valid as the first character of the literal. If the literal can start with any valid character, this is null."),
                    new Column("LiteralMaxLength", Type.Integer, null, false, true,
                            "The maximum number of characters in the literal. If there is no maximum or the maximum is unknown, the value is –1."),
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new DiscoverLiteralsRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            emit(Enumeration.Literal.enumeration, saxHandler);
        }

    }

    static class DbschemaCatalogsRowset extends Rowset {
        DbschemaCatalogsRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_CATALOGS", DBSCHEMA_CATALOGS, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new DbschemaCatalogsRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DbschemaColumnsRowset extends Rowset {
        DbschemaColumnsRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_COLUMNS", DBSCHEMA_COLUMNS, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new DbschemaColumnsRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DbschemaProviderTypesRowset extends Rowset {
        DbschemaProviderTypesRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_PROVIDER_TYPES", DBSCHEMA_PROVIDER_TYPES, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new DbschemaProviderTypesRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DbschemaTablesRowset extends Rowset {
        DbschemaTablesRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_TABLES", DBSCHEMA_TABLES, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new DbschemaTablesRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DbschemaTablesInfoRowset extends Rowset {
        DbschemaTablesInfoRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_TABLES_INFO", DBSCHEMA_TABLES_INFO, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new DbschemaTablesInfoRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaActionsRowset extends Rowset {
        MdschemaActionsRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_ACTIONS", MDSCHEMA_ACTIONS, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new MdschemaActionsRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaCubesRowset extends Rowset {
        MdschemaCubesRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        private static final String CATALOG_NAME = "CATALOG_NAME";
        private static final String SCHEMA_NAME = "SCHEMA_NAME";
        private static final String CUBE_NAME = "CUBE_NAME";
        private static final String IS_DRILLTHROUGH_ENABLED = "IS_DRILLTHROUGH_ENABLED";
        private static final String IS_WRITE_ENABLED = "IS_WRITE_ENABLED";
        private static final String IS_LINKABLE = "IS_LINKABLE";
        private static final String IS_SQL_ALLOWED = "IS_SQL_ALLOWED";
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_CUBES", MDSCHEMA_CUBES, new Column[] {
                    new Column(CATALOG_NAME, Type.String, null, true, false, null),
                    new Column(SCHEMA_NAME, Type.String, null, true, true, null),
                    new Column(CUBE_NAME, Type.String, null, true, false, null),
                    new Column(IS_DRILLTHROUGH_ENABLED, Type.Boolean, null, false, false,
                            "Describes whether DRILLTHROUGH can be performed on the members of a cube"),
                    new Column(IS_WRITE_ENABLED, Type.Boolean, null, false, false,
                            "Describes whether a cube is write-enabled"),
                    new Column(IS_LINKABLE, Type.Boolean, null, false, false,
                            "Describes whether a cube can be used in a linked cube"),
                    new Column(IS_SQL_ALLOWED, Type.Boolean, null, false, false,
                            "Describes whether or not SQL can be used on the cube"),
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new MdschemaCubesRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            final Connection connection = XmlaMediator.getConnection(properties);
            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];
                Row row = new Row();
                row.set(CATALOG_NAME, connection.getCatalogName());
                row.set(SCHEMA_NAME, cube.getSchema().getName());
                row.set(CUBE_NAME, cube.getName());
                row.set(IS_DRILLTHROUGH_ENABLED, true);
                row.set(IS_WRITE_ENABLED, false);
                row.set(IS_LINKABLE, false);
                row.set(IS_SQL_ALLOWED, false);
                emit(row, saxHandler);
            }
        }
    }

    static class MdschemaDimensionsRowset extends Rowset {
        MdschemaDimensionsRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_DIMENSIONS", MDSCHEMA_DIMENSIONS, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new MdschemaDimensionsRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaFunctionsRowset extends Rowset {
        MdschemaFunctionsRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_FUNCTIONS", MDSCHEMA_FUNCTIONS, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new MdschemaFunctionsRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaHierarchiesRowset extends Rowset {
        MdschemaHierarchiesRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_HIERARCHIES", MDSCHEMA_HIERARCHIES, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new MdschemaHierarchiesRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaMeasuresRowset extends Rowset {
        MdschemaMeasuresRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_MEASURES", MDSCHEMA_MEASURES, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new MdschemaMeasuresRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaMembersRowset extends Rowset {
        MdschemaMembersRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_MEMBERS", MDSCHEMA_MEMBERS, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new MdschemaMembersRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaSetsRowset extends Rowset {
        MdschemaSetsRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_SETS", MDSCHEMA_SETS, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new MdschemaSetsRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaPropertiesRowset extends Rowset {
        MdschemaPropertiesRowset(HashMap restrictions, Properties properties) {
            super(definition, restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_PROPERTIES", MDSCHEMA_PROPERTIES, new Column[] {
                }) {
            public Rowset getRowset(HashMap restrictions, Properties properties) {
                return new MdschemaPropertiesRowset(restrictions, properties);
            }
        };

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }
}

// End XmlaTest.java
