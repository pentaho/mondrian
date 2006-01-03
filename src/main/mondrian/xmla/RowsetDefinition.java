/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.olap.*;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapLevel;

import java.lang.reflect.Field;
import java.util.*;

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
    private static final String nl = Util.nl;
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

    RowsetDefinition(String name, int ordinal, String description, Column[] columnDefinitions) {
        super(name, ordinal, description);
        this.columnDefinitions = columnDefinitions;
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

    static class Type extends EnumeratedValues.BasicValue {
        public static final int String_ORDINAL = 0;
        public static final Type String = new Type("string", String_ORDINAL, "string");
        public static final int StringArray_ORDINAL = 1;
        public static final Type StringArray = new Type("StringArray", StringArray_ORDINAL, "string");
        public static final int Array_ORDINAL = 2;
        public static final Type Array = new Type("Array", Array_ORDINAL, "string");
        public static final int Enumeration_ORDINAL = 3;
        public static final Type Enumeration = new Type("Enumeration", Enumeration_ORDINAL, "string");
        public static final int EnumerationArray_ORDINAL = 4;
        public static final Type EnumerationArray = new Type("EnumerationArray", EnumerationArray_ORDINAL, "string");
        public static final int EnumString_ORDINAL = 5;
        public static final Type EnumString = new Type("EnumString", EnumString_ORDINAL, "string");
        public static final int Boolean_ORDINAL = 6;
        public static final Type Boolean = new Type("Boolean", Boolean_ORDINAL, "boolean");
        public static final int StringSometimesArray_ORDINAL = 7;
        public static final Type StringSometimesArray = new Type("StringSometimesArray", StringSometimesArray_ORDINAL, "string");
        public static final int Integer_ORDINAL = 8;
        public static final Type Integer = new Type("Integer", Integer_ORDINAL, "integer");
        public static final int UnsignedInteger_ORDINAL = 9;
        public static final Type UnsignedInteger = new Type("UnsignedInteger", UnsignedInteger_ORDINAL, "unsignedInteger");
        public final String columnType;

        public Type(String name, int ordinal, String columnType) {
            super(name, ordinal, null);
            this.columnType = columnType;
        }

        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new Type[] {
                    String, StringArray, Array, Enumeration, EnumerationArray, EnumString,
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

    static class Column {
        final String name;
        final Type type;
        final Enumeration enumeration;
        final String description;
        final boolean restriction;
        final boolean nullable;

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
         */
        Column(String name, Type type, Enumeration enumeratedType,
                boolean restriction, boolean nullable, String description) {
            Util.assertPrecondition(type != null, "Type.instance.isValid(type)");
            Util.assertPrecondition((type == Type.Enumeration || type == Type.EnumerationArray || type == Type.EnumString) == (enumeratedType != null), "(type == Type.Enumeration || type == Type.EnumerationArray || type == Type.EnumString) == (enumeratedType != null)");
            this.name = name;
            this.type = type;
            this.enumeration = enumeratedType;
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
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DiscoverDatasourcesRowset(request, handler);
            }
        };

        public DiscoverDatasourcesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        public void unparse(XmlaResponse response) {
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
                emit(row, response);
            }
        }
    }

    static class DiscoverSchemaRowsetsRowset extends Rowset {
        private static final Column SchemaName = new Column("SchemaName", Type.StringArray, null, true, false, "The name of the schema/request. This returns the values in the RequestTypes enumeration, plus any additional types supported by the provider. The provider defines rowset structures for the additional types");
        private static final Column Restrictions = new Column("Restrictions", Type.Array, null, false, true, "An array of the restrictions suppoted by provider. An example follows this table.");
        private static final Column Description = new Column("Description", Type.String, null, false, true, "A localizable description of the schema");
        private static RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_SCHEMA_ROWSETS", DISCOVER_SCHEMA_ROWSETS,
                "Returns the names, values, and other information of all supported RequestType enumeration values.",
                new Column[] {
                    SchemaName,
                    Restrictions,
                    Description,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DiscoverSchemaRowsetsRowset(request, handler);
            }
        };

        public DiscoverSchemaRowsetsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        public void unparse(XmlaResponse response) {
            final RowsetDefinition[] rowsetDefinitions = (RowsetDefinition[])
                    enumeration.getValuesSortedByName().
                    toArray(new RowsetDefinition[0]);
            for (int i = 0; i < rowsetDefinitions.length; i++) {
                RowsetDefinition rowsetDefinition = rowsetDefinitions[i];
                Row row = new Row();
                row.set(SchemaName.name, rowsetDefinition.name);
                row.set(Restrictions.name, getRestrictions(rowsetDefinition));
                row.set(Description.name, rowsetDefinition.description);
                emit(row, response);
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
    }

    static class DiscoverPropertiesRowset extends Rowset {
        DiscoverPropertiesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column PropertyName = new Column("PropertyName", Type.StringSometimesArray, null, true, false,
                "The name of the property.");
        private static final Column PropertyDescription = new Column("PropertyDescription", Type.String, null, false, true,
                "A localizable text description of the property.");
        private static final Column PropertyType = new Column("PropertyType", Type.String, null, false, true,
                "The XML data type of the property.");
        private static final Column PropertyAccessType = new Column("PropertyAccessType", Type.EnumString, Enumeration.Access.enumeration, false, false,
                "Access for the property. The value can be Read, Write, or ReadWrite.");
        private static final Column IsRequired = new Column("IsRequired", Type.Boolean, null, false, true,
                "True if a property is required, false if it is not required.");
        private static final Column Value = new Column("Value", Type.String, null, false, true,
                "The current value of the property.");
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
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DiscoverPropertiesRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            final String[] propertyNames = PropertyDefinition.enumeration.getNames();
            for (int i = 0; i < propertyNames.length; i++) {
                PropertyDefinition propertyDefinition = PropertyDefinition.getValue(propertyNames[i]);
                Row row = new Row();
                row.set(PropertyName.name, propertyDefinition.name);
                row.set(PropertyDescription.name, propertyDefinition.description);
                row.set(PropertyType.name, propertyDefinition.type);
                row.set(PropertyAccessType.name, propertyDefinition.access);
                //row.set(IsRequired.name, false);
                //row.set(Value.name, null);
                emit(row, response);
            }
        }
    }

    static class DiscoverEnumeratorsRowset extends Rowset {
        DiscoverEnumeratorsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column EnumName = new Column("EnumName", Type.StringArray, null, true, false,
                "The name of the enumerator that contains a set of values.");
        private static final Column EnumDescription = new Column("EnumDescription", Type.String, null, false, true,
                "A localizable description of the enumerator.");
        private static final Column EnumType = new Column("EnumType", Type.String, null, false, false,
                "The data type of the Enum values.");
        private static final Column ElementName = new Column("ElementName", Type.String, null, false, false,
                "The name of one of the value elements in the enumerator set." + nl +
                "Example: TDP");
        private static final Column ElementDescription = new Column("ElementDescription", Type.String, null, false, true,
                "A localizable description of the element (optional).");
        private static final Column ElementValue = new Column("ElementValue", Type.String, null, false, true, "The value of the element." + nl +
                "Example: 01");
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
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DiscoverEnumeratorsRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
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
                    row.set(EnumType.name, enumerator.type);
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
                    emit(row, response);
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
    }

    static class DiscoverKeywordsRowset extends Rowset {
        DiscoverKeywordsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column Keyword = new Column("Keyword", Type.StringSometimesArray, null, true, false,
                "A list of all the keywords reserved by a provider." + nl +
                "Example: AND");
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_KEYWORDS", DISCOVER_KEYWORDS,
                "Returns an XML list of keywords reserved by the provider.",
                new Column[] {
                    Keyword,
                }) {
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

        public void unparse(XmlaResponse response) {
            for (int i = 0; i < keywords.length; i++) {
                String keyword = keywords[i];
                Row row = new Row();
                row.set(Keyword.name, keyword);
                emit(row, response);
            }
        }
    }

    static class DiscoverLiteralsRowset extends Rowset {
        DiscoverLiteralsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_LITERALS", DISCOVER_LITERALS,
                "Returns information about literals supported by the provider.",
                new Column[] {
                    new Column("LiteralName", Type.StringSometimesArray, null, true, false,
                            "The name of the literal described in the row." + nl +
                "Example: DBLITERAL_LIKE_PERCENT"),
                    new Column("LiteralValue", Type.String, null, false, true,
                            "Contains the actual literal value." + nl +
                "Example, if LiteralName is DBLITERAL_LIKE_PERCENT and the percent character (%) is used to match zero or more characters in a LIKE clause, this column's value would be \"%\"."),
                    new Column("LiteralInvalidChars", Type.String, null, false, true,
                            "The characters, in the literal, that are not valid." + nl +
                "For example, if table names can contain anything other than a numeric character, this string would be \"0123456789\"."),
                    new Column("LiteralInvalidStartingChars", Type.String, null, false, true,
                            "The characters that are not valid as the first character of the literal. If the literal can start with any valid character, this is null."),
                    new Column("LiteralMaxLength", Type.Integer, null, false, true,
                            "The maximum number of characters in the literal. If there is no maximum or the maximum is unknown, the value is ?1."),
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DiscoverLiteralsRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            emit(Enumeration.Literal.enumeration, response);
        }

    }

    static class DbschemaCatalogsRowset extends Rowset {
        DbschemaCatalogsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, false, "Catalog name. Cannot be NULL.");
        private static final Column Description = new Column("DESCRIPTION", Type.String, null, false, true, "Human-readable description of the catalog.");

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_CATALOGS", DBSCHEMA_CATALOGS,
                "Returns information about literals supported by the provider.",
                new Column[] {
                    CatalogName,
                    Description,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DbschemaCatalogsRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            Connection connection = handler.getConnection(request);
            if (connection == null) {
                return;
            }
            Row row = new Row();
            final Schema schema = connection.getSchema();
            row.set(CatalogName.name, schema.getName());
            //row.set(Description.name, "No description");
            emit(row, response);
        }
    }

    static class DbschemaColumnsRowset extends Rowset {
        DbschemaColumnsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column TableCatalog = new Column("TABLE_CATALOG", Type.String, null, true, false, null);
        private static final Column TableSchema = new Column("TABLE_SCHEMA", Type.String, null, true, false, null);
        private static final Column TableName = new Column("TABLE_NAME", Type.String, null, true, false, null);
        private static final Column ColumnName = new Column("COLUMN_NAME", Type.String, null, true, false, null);
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_COLUMNS", DBSCHEMA_COLUMNS, null, new Column[] {
                    TableCatalog,
                    TableSchema,
                    TableName,
                    ColumnName,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DbschemaColumnsRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            //TODO
            throw new UnsupportedOperationException();
        }
    }

    static class DbschemaProviderTypesRowset extends Rowset {
        DbschemaProviderTypesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column DataType = new Column("DATA_TYPE", Type.UnsignedInteger, null, true, false, null);
        private static final Column BestMatch = new Column("BEST_MATCH", Type.Boolean, null, true, false, null);
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_PROVIDER_TYPES", DBSCHEMA_PROVIDER_TYPES, null, new Column[] {
                    DataType,
                    BestMatch,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DbschemaProviderTypesRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            //TODO
            throw new UnsupportedOperationException();
        }
    }

    static class DbschemaTablesRowset extends Rowset {
        DbschemaTablesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column TableCatalog = new Column("TABLE_CATALOG", Type.String, null, true, false, null);
        private static final Column TableSchema = new Column("TABLE_SCHEMA", Type.String, null, true, false, null);
        private static final Column TableName = new Column("TABLE_NAME", Type.String, null, true, false, null);
        private static final Column TableType = new Column("TABLE_TYPE", Type.String, null, true, false, null);
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_TABLES", DBSCHEMA_TABLES, null, new Column[] {
                    TableCatalog,
                    TableSchema,
                    TableName,
                    TableType,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DbschemaTablesRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            //TODO
            throw new UnsupportedOperationException();
        }
    }

    static class DbschemaTablesInfoRowset extends Rowset {
        DbschemaTablesInfoRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column TableCatalog = new Column("TABLE_CATALOG", Type.String, null, true, false, null);
        private static final Column TableSchema = new Column("TABLE_SCHEMA", Type.String, null, true, false, null);
        private static final Column TableName = new Column("TABLE_NAME", Type.String, null, true, false, null);
        private static final Column TableType = new Column("TABLE_TYPE", Type.String, null, true, false, null);
        public static final RowsetDefinition definition = new RowsetDefinition(
                "DBSCHEMA_TABLES_INFO", DBSCHEMA_TABLES_INFO, null, new Column[] {
                    TableCatalog,
                    TableSchema,
                    TableName,
                    TableType,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new DbschemaTablesInfoRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            //TODO
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaActionsRowset extends Rowset {
        MdschemaActionsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
        private static final Column Coordinate = new Column("COORDINATE", Type.String, null, true, false, null);
        private static final Column CoordinateType = new Column("COORDINATE_TYPE", Type.String, null, true, false, null);
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_ACTIONS", MDSCHEMA_ACTIONS, null, new Column[] {
                    CubeName,
                    Coordinate,
                    CoordinateType,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaActionsRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            //TODO
            throw new UnsupportedOperationException();
        }
    }

    // REF http://msdn.microsoft.com/library/en-us/oledb/htm/olapcubes_rowset.asp
    static class MdschemaCubesRowset extends Rowset {
        MdschemaCubesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final String MD_CUBTYPE_CUBE = "CUBE";
        private static final String MD_CUBTYPE_VIRTUAL_CUBE = "VIRTUAL CUBE";


        private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, false, null);
        private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, true, true, null);
        private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
        private static final Column CubeType = new Column("CUBE_TYPE", Type.String, null, true, false, "Cube type.");
        private static final Column IsDrillthroughEnabled = new Column("IS_DRILLTHROUGH_ENABLED", Type.Boolean, null, false, false,
                "Describes whether DRILLTHROUGH can be performed on the members of a cube");
        private static final Column IsWriteEnabled = new Column("IS_WRITE_ENABLED", Type.Boolean, null, false, false,
                "Describes whether a cube is write-enabled");
        private static final Column IsLinkable = new Column("IS_LINKABLE", Type.Boolean, null, false, false,
                "Describes whether a cube can be used in a linked cube");
        private static final Column IsSqlAllowed = new Column("IS_SQL_ALLOWED", Type.Boolean, null, false, false,
                "Describes whether or not SQL can be used on the cube");
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_CUBES", MDSCHEMA_CUBES, null, new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    CubeType,
                    IsDrillthroughEnabled,
                    IsWriteEnabled,
                    IsLinkable,
                    IsSqlAllowed,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaCubesRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            final Connection connection = handler.getConnection(request);
            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];

                // Access control
                if (!canAccess(connection, cube)) {
                    continue;
                }

                Row row = new Row();
                row.set(CatalogName.name, cube.getSchema().getName());
                row.set(SchemaName.name, cube.getSchema().getName());
                row.set(CubeName.name, cube.getName());
                row.set(CubeType.name, ((RolapCube) cube).isVirtual() ? MD_CUBTYPE_VIRTUAL_CUBE : MD_CUBTYPE_CUBE);
                row.set(IsDrillthroughEnabled.name, true);
                row.set(IsWriteEnabled.name, false);
                row.set(IsLinkable.name, false);
                row.set(IsSqlAllowed.name, false);
                emit(row, response);
            }
        }
    }

    // REF http://msdn.microsoft.com/library/en-us/oledb/htm/olapdimensions_rowset.asp
    static class MdschemaDimensionsRowset extends Rowset {
        MdschemaDimensionsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        public static final int MD_DIMTYPE_OTHER = 3;
        public static final int MD_DIMTYPE_MEASURE = 2;
        public static final int MD_DIMTYPE_TIME = 1;

        private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, false, null);
        private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, true, true, null);
        private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
        private static final Column DimensionName = new Column("DIMENSION_NAME", Type.String, null, true, false, null);
        private static final Column DimensionUniqueName = new Column("DIMENSION_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column DimensionCaption = new Column("DIMENSION_CAPTION", Type.String, null, true, false, null);
        private static final Column DimensionOrdinal = new Column("DIMENSION_ORDINAL", Type.Integer, null, true, false, null);
        private static final Column DimensionType = new Column("DIMENSION_TYPE", Type.Integer, null, true, false, null);
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_DIMENSIONS", MDSCHEMA_DIMENSIONS, null, new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionName,
                    DimensionUniqueName,
                    DimensionCaption,
                    DimensionOrdinal,
                    DimensionType,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaDimensionsRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            final Connection connection = handler.getConnection(request);
            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];
                final Dimension[] dimensions = cube.getDimensions();
                for (int j = 0; j < dimensions.length; j++) {
                    Dimension dimension = dimensions[j];

                    // Access control
                    if (!canAccess(connection, dimension)) {
                        continue;
                    }

                    Row row = new Row();
                    row.set(CatalogName.name, cube.getSchema().getName());
                    row.set(SchemaName.name, cube.getSchema().getName());
                    row.set(CubeName.name, cube.getName());
                    row.set(DimensionName.name, dimension.getName());
                    row.set(DimensionUniqueName.name, dimension.getUniqueName());
                    row.set(DimensionCaption.name, dimension.getCaption());
                    row.set(DimensionOrdinal.name, dimension.getOrdinal(cube));
                    row.set(DimensionType.name, getDimensionType(dimension));
                    emit(row, response);
                }
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

    // REF http://msdn.microsoft.com/library/en-us/oledb/htm/olapfunctions_rowset.asp
    static class MdschemaFunctionsRowset extends Rowset {
        MdschemaFunctionsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column LibraryName = new Column("LIBRARY_NAME", Type.String, null, true, true, null);
        private static final Column InterfaceName = new Column("INTERFACE_NAME", Type.String, null, true, true, null);
        private static final Column FunctionName = new Column("FUNCTION_NAME", Type.String, null, true, true, null);
        private static final Column Origin = new Column("ORIGIN", Type.Integer, null, true, true, null);
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_FUNCTIONS", MDSCHEMA_FUNCTIONS, null, new Column[] {
                    LibraryName,
                    InterfaceName,
                    FunctionName,
                    Origin,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaFunctionsRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            //TODO
            throw new UnsupportedOperationException();
        }
    }


    // REF http://msdn.microsoft.com/library/en-us/oledb/htm/olaphierarchies_rowset.asp
    static class MdschemaHierarchiesRowset extends Rowset {
        MdschemaHierarchiesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, false, null);
        private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, true, true, null);
        private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
        private static final Column DimensionUniqueName = new Column("DIMENSION_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column HierarchyName = new Column("HIERARCHY_NAME", Type.String, null, true, false, null);
        private static final Column HierarchyUniqueName = new Column("HIERARCHY_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column HierarchyCaption = new Column("HIERARCHY_CAPTION", Type.String, null, true, false, null);
        private static final Column DimensionType = new Column("DIMENSION_TYPE", Type.Integer, null, true, false, null);
        private static final Column DefaultMember = new Column("DEFAULT_MEMBER", Type.String, null, true, true, null);
        private static final Column AllMember = new Column("ALL_MEMBER", Type.String, null, true, true, null);
        private static final Column ParentChild = new Column("PARENT_CHILD", Type.Boolean, null, false, false, null);

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_HIERARCHIES", MDSCHEMA_HIERARCHIES, null, new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionUniqueName,
                    HierarchyName,
                    HierarchyUniqueName,
                    HierarchyCaption,
                    DimensionType,
                    DefaultMember,
                    AllMember,
                    ParentChild,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaHierarchiesRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            final Connection connection = handler.getConnection(request);
            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];
                final Dimension[] dimensions = cube.getDimensions();
                for (int j = 0; j < dimensions.length; j++) {
                    Dimension dimension = dimensions[j];
                    final Hierarchy[] hierarchies = dimension.getHierarchies();
                    for (int k = 0; k < hierarchies.length; k++) {
                        HierarchyBase hierarchy = (HierarchyBase) hierarchies[k];

                        // Access control
                        if (!canAccess(connection, hierarchy)) {
                            continue;
                        }

                        Row row = new Row();
                        row.set(CatalogName.name, cube.getSchema().getName());
                        row.set(SchemaName.name, cube.getSchema().getName());
                        row.set(CubeName.name, cube.getName());
                        row.set(DimensionUniqueName.name, dimension.getUniqueName());
                        row.set(HierarchyName.name, hierarchy.getName());
                        row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
                        row.set(HierarchyCaption.name, hierarchy.getCaption());
                        row.set(DimensionType.name, getDimensionType(dimension));
                        row.set(DefaultMember.name, hierarchy.getDefaultMember());
                        if (hierarchy.hasAll()) {
                            row.set(AllMember.name, Util.makeFqName(hierarchy, hierarchy.getAllMemberName()));
                        }
                        RolapLevel nonAllFirstLevel =
                                (RolapLevel) hierarchy.getLevels()[
                                (hierarchy.hasAll() ? 1 : 0)];
                        row.set(ParentChild.name, nonAllFirstLevel.isParentChild());
                        emit(row, response);
                    }
                }
            }
        }
    }

    // REF http://msdn.microsoft.com/library/en-us/oledb/htm/olaplevels_rowset.asp
    static class MdschemaLevelsRowset extends Rowset {
        MdschemaLevelsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
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

        private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, false, null);
        private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, true, true, null);
        private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
        private static final Column DimensionUniqueName = new Column("DIMENSION_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column HierarchyUniqueName = new Column("HIERARCHY_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column LevelName = new Column("LEVEL_NAME", Type.String, null, true, false, null);
        private static final Column LevelUniqueName = new Column("LEVEL_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column LevelCaption = new Column("LEVEL_CAPTION", Type.String, null, true, false, null);
        private static final Column LevelNumber = new Column("LEVEL_NUMBER", Type.Integer, null, true, false, null);
        private static final Column LevelType = new Column("LEVEL_TYPE", Type.Integer, null, true, false, null);
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_LEVELS", MDSCHEMA_LEVELS, null, new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionUniqueName,
                    HierarchyUniqueName,
                    LevelName,
                    LevelUniqueName,
                    LevelCaption,
                    LevelNumber,
                    LevelType,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaLevelsRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            final Connection connection = handler.getConnection(request);
            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];
                final Dimension[] dimensions = cube.getDimensions();
                for (int j = 0; j < dimensions.length; j++) {
                    Dimension dimension = dimensions[j];
                    final Hierarchy[] hierarchies = dimension.getHierarchies();
                    for (int k = 0; k < hierarchies.length; k++) {
                        Hierarchy hierarchy = hierarchies[k];
                        final Level[] levels = hierarchy.getLevels();
                        for (int m = 0; m < levels.length; m++) {
                            Level level = levels[m];

                            // Access control
                            if (!canAccess(connection, level)) {
                                continue;
                            }

                            Row row = new Row();
                            row.set(CatalogName.name, cube.getSchema().getName());
                            row.set(SchemaName.name, cube.getSchema().getName());
                            row.set(CubeName.name, cube.getName());
                            row.set(DimensionUniqueName.name, dimension.getUniqueName());
                            row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
                            row.set(LevelName.name, level.getName());
                            row.set(LevelUniqueName.name, level.getUniqueName());
                            row.set(LevelCaption.name, level.getCaption());
                            row.set(LevelNumber.name, level.getDepth()); // see notes on this #getDepth()
                            row.set(LevelType.name, getLevelType(level));
                            emit(row, response);
                        }
                    }
                }
            }
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
    }


    // REF http://msdn.microsoft.com/library/en-us/oledb/htm/olapmeasures_rowset.asp
    static class MdschemaMeasuresRowset extends Rowset {
        MdschemaMeasuresRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, false, null);
        private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, true, true, null);
        private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
        private static final Column MeasureName = new Column("MEASURE_NAME", Type.String, null, true, false, null);
        private static final Column MeasureUniqueName = new Column("MEASURE_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column MeasureCaption = new Column("MEASURE_CAPTION", Type.String, null, true, false, null);
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_MEASURES", MDSCHEMA_MEASURES, null, new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    MeasureName,
                    MeasureUniqueName,
                    MeasureCaption,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaMeasuresRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            // return both stored and calculated members on hierarchy [Measures]
            final Connection connection = handler.getConnection(request);
            final Role role = connection.getRole();
            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];
                SchemaReader schemaReader = cube.getSchemaReader(role);
                final Dimension measuresDimension = cube.getDimensions()[0];
                final Hierarchy measuresHierarchy = measuresDimension.getHierarchies()[0];
                // #getLevelMembers() already returns both stored and calculated members
                final Level measuresLevel = measuresHierarchy.getLevels()[0];
                Member[] storedMembers =
                        schemaReader.getLevelMembers(measuresLevel, false);
                for (int j = 0; j < storedMembers.length; j++) {
                    emitMember(response, connection, storedMembers[j], cube);
                }
            }
        }

        private void emitMember(XmlaResponse response, Connection connection, Member member, Cube cube) {
            // Access control
            if (!canAccess(connection, member)) {
                return;
            }

            Row row = new Row();
            row.set(CatalogName.name, cube.getSchema().getName());
            row.set(SchemaName.name, cube.getSchema().getName());
            row.set(CubeName.name, cube.getName());
            row.set(MeasureName.name, member.getName());
            row.set(MeasureUniqueName.name, member.getUniqueName());
            row.set(MeasureCaption.name, member.getCaption());
            emit(row, response);
        }
    }

    static class MdschemaMembersRowset extends Rowset {
        MdschemaMembersRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, false, null);
        private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, true, true, null);
        private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
        private static final Column DimensionUniqueName = new Column("DIMENSION_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column HierarchyUniqueName = new Column("HIERARCHY_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column LevelUniqueName = new Column("LEVEL_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column LevelNumber = new Column("LEVEL_NUMBER", Type.UnsignedInteger, null, true, false, null);
        private static final Column MemberName = new Column("MEMBER_NAME", Type.String, null, true, false, null);
        private static final Column MemberOrdinal = new Column("MEMBER_ORDINAL", Type.Integer, null, true, false, null);
        private static final Column MemberUniqueName = new Column("MEMBER_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column MemberType = new Column("MEMBER_TYPE", Type.Integer, null, true, false, null);
        private static final Column MemberCaption = new Column("MEMBER_CAPTION", Type.String, null, true, false, null);
        private static final Column ChildrenCardinality = new Column("CHILDREN_CARDINALITY", Type.Integer, null, true, false, null);
        private static final Column ParentLevel = new Column("PARENT_LEVEL", Type.Integer, null, false, false, null);
        private static final Column ParentUniqueName = new Column("PARENT_UNIQUE_NAME", Type.String, null, true, true, null);
        private static final Column TreeOp = new Column("TREE_OP", Type.Enumeration, Enumeration.TreeOp.enumeration, true, true, null);
        /* Mondrian specified member properties. */
        private static final Column Depth = new Column("DEPTH", Type.Integer, null, false, false, null);

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_MEMBERS", MDSCHEMA_MEMBERS, null, new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionUniqueName,
                    HierarchyUniqueName,
                    LevelUniqueName,
                    LevelNumber,
                    MemberName,
                    MemberOrdinal,
                    MemberUniqueName,
                    MemberType,
                    MemberCaption,
                    ChildrenCardinality,
                    ParentLevel,
                    ParentUniqueName,
                    TreeOp,
                    Depth,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaMembersRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            final Connection connection = handler.getConnection(request);
            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];
                if (!passesRestriction(CubeName, cube.getName())) {
                    continue;
                }
                if (isRestricted(MemberUniqueName)) {
                    final String memberUniqueName = (String)
                            restrictions.get(MemberUniqueName.name);
                    final String[] nameParts = Util.explode(memberUniqueName);
                    Member member = cube.getSchemaReader(null).
                            getMemberByUniqueName(nameParts, false);
                    if (member != null) {
                        String treeOp0 = (String) restrictions.get(TreeOp.name);
                        int treeOp = Enumeration.TreeOp.Self.ordinal;
                        if (treeOp0 != null) {
                            try {
                                treeOp = Integer.parseInt(treeOp0);
                            } catch (NumberFormatException e) {
                                // stay with default value
                            }
                        }
                        unparseMember(connection, cube, member, response, treeOp);
                    }
                    continue;
                }
                final Dimension[] dimensions = cube.getDimensions();
                for (int j = 0; j < dimensions.length; j++) {
                    Dimension dimension = dimensions[j];
                    if (!passesRestriction(DimensionUniqueName,
                            dimension.getUniqueName())) {
                        continue;
                    }
                    final Hierarchy[] hierarchies = dimension.getHierarchies();
                    for (int k = 0; k < hierarchies.length; k++) {
                        Hierarchy hierarchy = hierarchies[k];
                        if (!passesRestriction(HierarchyUniqueName,
                                hierarchy.getUniqueName())) {
                            continue;
                        }
                        final Member[] rootMembers =
                                connection.getSchemaReader().
                                getHierarchyRootMembers(hierarchy);
                        for (int m = 0; m < rootMembers.length; m++) {
                            Member member = rootMembers[m];
                            // Note that we ignore treeOp, because
                            // MemberUniqueName was not restricted, and
                            // therefore we have nothing to work relative to.
                            // We supply our own treeOp expression here, for
                            // our own devious purposes.
                            unparseMember(connection, cube, member, response,
                                    Enumeration.TreeOp.Self.ordinal |
                                    Enumeration.TreeOp.Descendants.ordinal);
                        }
                    }
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
         * Outputs a member and, depending upon the <code>treeOp</code>
         * parameter, other relatives of the member. This method recursively
         * invokes itself to walk up, down, or across the hierarchy.
         */
        private void unparseMember(final Connection connection, Cube cube,
                Member member, XmlaResponse response,
                int treeOp) {
            // Visit node itself.
            if (mask(treeOp, Enumeration.TreeOp.Self.ordinal)) {
                emitMember(member, connection, cube, response);
            }
            // Visit node's siblings (not including itself).
            if (mask(treeOp, Enumeration.TreeOp.Siblings.ordinal)) {
                final Member parent =
                        connection.getSchemaReader().getMemberParent(member);
                final Member[] siblings;
                if (parent == null) {
                    siblings = connection.getSchemaReader().
                            getHierarchyRootMembers(member.getHierarchy());
                } else {
                    siblings = connection.getSchemaReader().
                            getMemberChildren(parent);
                }
                for (int i = 0; i < siblings.length; i++) {
                    Member sibling = siblings[i];
                    if (sibling == member) {
                        continue;
                    }
                    unparseMember(connection, cube, sibling, response,
                            Enumeration.TreeOp.Self.ordinal);
                }
            }
            // Visit node's descendants or its immediate children, but not both.
            if (mask(treeOp, Enumeration.TreeOp.Descendants.ordinal)) {
                final Member[] children =
                        connection.getSchemaReader().getMemberChildren(member);
                for (int i = 0; i < children.length; i++) {
                    Member child = children[i];
                    unparseMember(connection, cube, child, response,
                            Enumeration.TreeOp.Self.ordinal |
                            Enumeration.TreeOp.Descendants.ordinal);
                }
            } else if (mask(treeOp, Enumeration.TreeOp.Children.ordinal)) {
                final Member[] children =
                        connection.getSchemaReader().getMemberChildren(member);
                for (int i = 0; i < children.length; i++) {
                    Member child = children[i];
                    unparseMember(connection, cube, child, response,
                            Enumeration.TreeOp.Self.ordinal);
                }
            }
            // Visit node's ancestors or its immediate parent, but not both.
            if (mask(treeOp, Enumeration.TreeOp.Ancestors.ordinal)) {
                final Member parent =
                        connection.getSchemaReader().getMemberParent(member);
                if (parent != null) {
                    unparseMember(connection, cube, parent, response,
                            Enumeration.TreeOp.Self.ordinal |
                            Enumeration.TreeOp.Ancestors.ordinal);
                }
            } else if (mask(treeOp, Enumeration.TreeOp.Parent.ordinal)) {
                final Member parent =
                        connection.getSchemaReader().getMemberParent(member);
                if (parent != null) {
                    unparseMember(connection, cube, parent, response,
                            Enumeration.TreeOp.Self.ordinal);
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

        private void emitMember(Member member, final Connection connection,
                Cube cube, XmlaResponse response) {
            // Access control
            if (!canAccess(connection, member)) {
                return;
            }

            final Level level = member.getLevel();
            final Hierarchy hierarchy = level.getHierarchy();
            final Dimension dimension = hierarchy.getDimension();
            Row row = new Row();
            row.set(CatalogName.name, cube.getSchema().getName());
            row.set(SchemaName.name, cube.getSchema().getName());
            row.set(CubeName.name, cube.getName());
            row.set(DimensionUniqueName.name, dimension.getUniqueName());
            row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
            row.set(LevelUniqueName.name, level.getUniqueName());
            row.set(LevelNumber.name, level.getDepth());
            row.set(MemberName.name, member.getName());
            row.set(MemberOrdinal.name, member.getOrdinal());
            row.set(MemberUniqueName.name, member.getUniqueName());
            row.set(MemberType.name, member.getMemberType());
            row.set(MemberCaption.name, member.getCaption());
            row.set(ChildrenCardinality.name, member.getPropertyValue(Property.CHILDREN_CARDINALITY.name));
            row.set(ParentLevel.name, member.getParentMember() == null ? 0 : member.getParentMember().getDepth());
            row.set(ParentUniqueName.name, member.getParentUniqueName());
            row.set(Depth.name, member.getDepth());
            emit(row, response);
        }
    }

    static class MdschemaSetsRowset extends Rowset {
        MdschemaSetsRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, false, null);
        private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, true, true, null);
        private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
        private static final Column SetName = new Column("SET_NAME", Type.String, null, true, false, null);
        private static final Column Scope = new Column("SCOPE", Type.Integer, null, true, false, null);
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_SETS", MDSCHEMA_SETS, null, new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    SetName,
                    Scope,
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaSetsRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            throw new UnsupportedOperationException();
        }
    }

    // REF http://msdn.microsoft.com/library/en-us/oledb/htm/olapproperties_rowset.asp
    static class MdschemaPropertiesRowset extends Rowset {
        MdschemaPropertiesRowset(XmlaRequest request, XmlaHandler handler) {
            super(definition, request, handler);
        }

        private static final int MDPROP_MEMBER = 0x01;
        private static final int MDPROP_CELL = 0x02;

        private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, false, null);
        private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, true, true, null);
        private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
        private static final Column DimensionUniqueName = new Column("DIMENSION_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column HierarchyUniqueName = new Column("HIERARCHY_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column LevelUniqueName = new Column("LEVEL_UNIQUE_NAME", Type.String, null, true, false, null);
        private static final Column MemberUniqueName = new Column("MEMBER_UNIQUE_NAME", Type.String, null, true, true, null);
        private static final Column PropertyName = new Column("PROPERTY_NAME", Type.String, null, true, false, null);
        private static final Column PropertyType = new Column("PROPERTY_TYPE", Type.Integer, null, true, false, null);
        private static final Column PropertyContentType = new Column("PROPERTY_CONTENT_TYPE", Type.Integer, null, true, false, null);
        private static final Column PropertyCaption = new Column("PROPERTY_CAPTION", Type.String, null, true, false, null);
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MDSCHEMA_PROPERTIES", MDSCHEMA_PROPERTIES, null, new Column[] {
                    CatalogName,
                    SchemaName,
                    CubeName,
                    DimensionUniqueName,
                    HierarchyUniqueName,
                    LevelUniqueName,
                    MemberUniqueName,
                    PropertyName,
                    PropertyType,
                    PropertyContentType,
                    PropertyCaption
                }) {
            public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
                return new MdschemaPropertiesRowset(request, handler);
            }
        };

        public void unparse(XmlaResponse response) {
            final Connection connection = handler.getConnection(request);
            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];
                final Dimension[] dimensions = cube.getDimensions();
                for (int j = 0; j < dimensions.length; j++) {
                    final Dimension dimension = dimensions[j];
                    final Hierarchy[] hierarchies = dimension.getHierarchies();
                    for (int k = 0; k < hierarchies.length; k++) {
                        Hierarchy hierarchy = hierarchies[k];
                        Level[] levels = hierarchy.getLevels();
                        for (int l = 0; l < levels.length; l++) {
                            Level level = levels[l];
                            Property[] properties = level.getProperties();
                            for (int m = 0; m < properties.length; m++) {
                                Property property = properties[m];
                                Row row = new Row();
                                row.set(CatalogName.name, cube.getSchema().getName());
                                row.set(SchemaName.name, cube.getSchema().getName());
                                row.set(CubeName.name, cube.getName());
                                row.set(DimensionUniqueName.name, dimension.getUniqueName());
                                row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
                                row.set(LevelUniqueName.name, level.getUniqueName());
                                // row.set(MemberUniqueName.name, null);
                                row.set(PropertyName.name, property.getName());
                                row.set(PropertyType.name, MDPROP_MEMBER); // Only member properties now
                                row.set(PropertyContentType.name, 0);
                                row.set(PropertyCaption.name, property.getCaption());
                                emit(row, response);
                            }
                        }
                    }
                }
            }
        }
    }

    private static boolean canAccess(Connection conn, OlapElement elem) {
        Role role = conn.getSchemaReader().getRole();
        return role.canAccess(elem);
    }
}

// End RowsetDefinition.java
