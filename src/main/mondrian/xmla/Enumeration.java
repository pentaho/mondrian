/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde <jhyde@users.sf.net>
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
class Enumeration extends EnumeratedValues {
    public final String name;
    public final String description;
    public final RowsetDefinition.Type type;

    public Enumeration(String name, String description,
            RowsetDefinition.Type type, EnumeratedValues.Value[] values) {
        super(values);
        this.name = name;
        this.description = description;
        this.type = type;
    }

    public static final class Methods extends EnumeratedValues.BasicValue {
        public static final Methods discover = new Methods("Discover", 1);
        public static final Methods execute = new Methods("Execute", 2);
        public static final Methods discoverAndExecute = new Methods("Discover/Execute", 3);

        private Methods(String name, int ordinal) {
            super(name, ordinal, null);
        }
        public static final Enumeration enumeration = new Enumeration(
                "Methods",
                "Set of methods for which a property is applicable",
                RowsetDefinition.Type.Enumeration,
                new Methods[] {discover, execute, discoverAndExecute}
        );
    }

    public static final class Access extends EnumeratedValues.BasicValue {
        public static final Access read = new Access("Read", 1);
        public static final Access write = new Access("Write", 2);
        public static final Access readWrite = new Access("Read/Write", 3);

        private Access(String name, int ordinal) {
            super(name, ordinal, null);
        }
        public static final Enumeration enumeration = new Enumeration(
                "Access",
                "The read/write behavior of a property",
                RowsetDefinition.Type.Enumeration,
                new Access[] {read, write, readWrite}
        );
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
            return (Format) enumeration.getValue(name, true);
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
            return (AxisFormat) enumeration.getValue(name, true);
        }
    }

/*
// RME
    public static class Content extends EnumeratedValues.BasicValue {
        public static final EnumeratedValues enumeration = new EnumeratedValues();

        public Content(String name, int ordinal, String description) {
            super(name, ordinal, description);
        }
    }
*/
    public static final class Content extends EnumeratedValues.BasicValue {
        public static final int NONE_ORDINAL        = 0;
        public static final int SCHEMA_ORDINAL      = 1;
        public static final int DATA_ORDINAL        = 2;
        public static final int SCHEMA_DATA_ORDINAL = 3;

        public static final Content None = new Content("None", NONE_ORDINAL, "none");
        public static final Content Schema = new Content("Schema", SCHEMA_ORDINAL, "schema");
        public static final Content Data = new Content("Data", DATA_ORDINAL, "data");
        public static final Content SchemaData = new Content("SchemaData", SCHEMA_DATA_ORDINAL, "schemadata");
        public Content(String name, int ordinal, String description) {
            super(name, ordinal, description);
        }
        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new Content[] {None, Schema, Data, SchemaData}
        );

        public static Content getValue(String name) {
            return (Content) enumeration.getValue(name, true);
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

    static class AuthenticationMode extends EnumeratedValues.BasicValue {
        private AuthenticationMode(String name, int ordinal, String description) {
            super(name, ordinal, description);
        }
        public static final AuthenticationMode Unauthenticated = new AuthenticationMode("Unauthenticated", 0, "no user ID or password needs to be sent.");
        public static final AuthenticationMode Authenticated = new AuthenticationMode("Authenticated", 1, "User ID and Password must be included in the information required for the connection.");
        public static final AuthenticationMode Integrated = new AuthenticationMode("Integrated", 2, "the data source uses the underlying security to determine authorization, such as Integrated Security provided by Microsoft Internet Information Services (IIS).");
        public static final Enumeration enumeration = new Enumeration(
                "AuthenticationMode",
                "Specification of what type of security mode the data source uses.",
                RowsetDefinition.Type.EnumString,
                new AuthenticationMode[] {Unauthenticated, Authenticated, Integrated}
        );
    }

    static class ProviderType extends EnumeratedValues.BasicValue {
        private ProviderType(String name, int ordinal, String description) {
            super(name, ordinal, description);
        }
        public static final ProviderType TDP = new ProviderType("TDP", 0, "tabular data provider.");
        public static final ProviderType MDP = new ProviderType("MDP", 1, "multidimensional data provider.");
        public static final ProviderType DMP = new ProviderType("DMP", 2, "data mining provider. A DMP provider implements the OLE DB for Data Mining specification.");
        public static final Enumeration enumeration = new Enumeration(
                "ProviderType",
                "The types of data supported by the provider.",
                RowsetDefinition.Type.Array,
                new ProviderType[] {TDP, MDP, DMP}
        );
    }

    public static class Literal extends EnumeratedValues.BasicValue {
        public final String literalName;
        public final String literalValue;
        public final String literalInvalidChars;
        public final String literalInvalidStartingChars;
        public final int literalMaxLength;

        // Enum DBLITERALENUM and DBLITERALENUM20, OLEDB.H.
        public static final int DBLITERAL_INVALID   = 0,
        DBLITERAL_BINARY_LITERAL    = 1,
        DBLITERAL_CATALOG_NAME  = 2,
        DBLITERAL_CATALOG_SEPARATOR = 3,
        DBLITERAL_CHAR_LITERAL  = 4,
        DBLITERAL_COLUMN_ALIAS  = 5,
        DBLITERAL_COLUMN_NAME   = 6,
        DBLITERAL_CORRELATION_NAME  = 7,
        DBLITERAL_CURSOR_NAME   = 8,
        DBLITERAL_ESCAPE_PERCENT    = 9,
        DBLITERAL_ESCAPE_UNDERSCORE = 10,
        DBLITERAL_INDEX_NAME    = 11,
        DBLITERAL_LIKE_PERCENT  = 12,
        DBLITERAL_LIKE_UNDERSCORE   = 13,
        DBLITERAL_PROCEDURE_NAME    = 14,
        DBLITERAL_QUOTE = 15,
        DBLITERAL_QUOTE_PREFIX = DBLITERAL_QUOTE,
        DBLITERAL_SCHEMA_NAME   = 16,
        DBLITERAL_TABLE_NAME    = 17,
        DBLITERAL_TEXT_COMMAND  = 18,
        DBLITERAL_USER_NAME = 19,
        DBLITERAL_VIEW_NAME = 20,
        DBLITERAL_CUBE_NAME = 21,
        DBLITERAL_DIMENSION_NAME    = 22,
        DBLITERAL_HIERARCHY_NAME    = 23,
        DBLITERAL_LEVEL_NAME    = 24,
        DBLITERAL_MEMBER_NAME   = 25,
        DBLITERAL_PROPERTY_NAME = 26,
        DBLITERAL_SCHEMA_SEPARATOR  = 27,
        DBLITERAL_QUOTE_SUFFIX  = 28;

        Literal(String literalName, int ordinal, String literalValue, int literalMaxLength, String literalInvalidChars, String literalInvalidStartingChars, String description) {
            super(literalName, ordinal, description);
            this.literalName = literalName;
            this.literalValue = literalValue;
            this.literalInvalidChars = literalInvalidChars;
            this.literalInvalidStartingChars = literalInvalidStartingChars;
            this.literalMaxLength = literalMaxLength;
        }

        static final EnumeratedValues enumeration = new EnumeratedValues(
                new Literal[] {
                    new Literal("DBLITERAL_CATALOG_NAME", DBLITERAL_CATALOG_NAME, null, -1, ".", "0123456789", "A catalog name in a text command."),
                    new Literal("DBLITERAL_CATALOG_SEPARATOR", DBLITERAL_CATALOG_SEPARATOR, ".", 0, null, null, null),
                    new Literal("DBLITERAL_COLUMN_ALIAS", DBLITERAL_COLUMN_ALIAS, null, -1, "'\"[]", "0123456789", null),
                    new Literal("DBLITERAL_COLUMN_NAME", DBLITERAL_COLUMN_NAME, null, -1, ".", "0123456789", null),
                    new Literal("DBLITERAL_CORRELATION_NAME", DBLITERAL_CORRELATION_NAME, null, -1, "'\"[]", "0123456789", null),
                    new Literal("DBLITERAL_CUBE_NAME", DBLITERAL_CUBE_NAME, null, -1, ".", "0123456789", null),
                    new Literal("DBLITERAL_DIMENSION_NAME", DBLITERAL_DIMENSION_NAME, null, -1, ".", "0123456789", null),
                    new Literal("DBLITERAL_HIERARCHY_NAME", DBLITERAL_HIERARCHY_NAME, null, -1, ".", "0123456789", null),
                    new Literal("DBLITERAL_LEVEL_NAME", DBLITERAL_LEVEL_NAME, null, -1, ".", "0123456789", null),
                    new Literal("DBLITERAL_MEMBER_NAME", DBLITERAL_MEMBER_NAME, null, -1, ".", "0123456789", null),
                    new Literal("DBLITERAL_PROCEDURE_NAME", DBLITERAL_PROCEDURE_NAME, null, -1, ".", "0123456789", null),
                    new Literal("DBLITERAL_PROPERTY_NAME", DBLITERAL_PROPERTY_NAME, null, -1, ".", "0123456789", null),
                    new Literal("DBLITERAL_QUOTE_PREFIX", DBLITERAL_QUOTE_PREFIX, "[", -1, null, null, "The character used in a text command as the opening quote for quoting identifiers that contain special characters."),
                    new Literal("DBLITERAL_QUOTE_SUFFIX", DBLITERAL_QUOTE_SUFFIX, "]", -1, null, null, "The character used in a text command as the closing quote for quoting identifiers that contain special characters. 1.x providers that use the same character as the prefix and suffix may not return this literal value and can set the lt member of the DBLITERAL structure to DBLITERAL_INVALID if requested."),
                    new Literal("DBLITERAL_TABLE_NAME", DBLITERAL_TABLE_NAME, null, -1, ".", "0123456789", null),
                    new Literal("DBLITERAL_TEXT_COMMAND", DBLITERAL_TEXT_COMMAND, null, -1, null, null, "A text command, such as an SQL statement."),
                    new Literal("DBLITERAL_USER_NAME", DBLITERAL_USER_NAME, null, 0, null, null, null),
                });
    }

    public static class TreeOp extends EnumeratedValues.BasicValue {
        TreeOp(String name, int ordinal, String description) {
            super(name, ordinal, description);
        }

        public static final TreeOp Children = new TreeOp("MDTREEOP_CHILDREN", 1, "Returns only the immediate children");
        public static final TreeOp Siblings = new TreeOp("MDTREEOP_SIBLINGS", 2, "Returns members on the same level");
        public static final TreeOp Parent = new TreeOp("MDTREEOP_PARENT", 4, "Returns only the immediate parent");
        public static final TreeOp Self = new TreeOp("MDTREEOP_SELF", 8, "Returns the immediate member in the list of returned rows");
        public static final TreeOp Descendants = new TreeOp("MDTREEOP_DESCENDANTS", 16, "Returns all descendants");
        public static final TreeOp Ancestors = new TreeOp("MDTREEOP_ANCESTORS", 32, "Returns all ancestors");
        static final Enumeration enumeration = new Enumeration(
                "TREE_OP",
                "Bitmap which controls which relatives of a member are returned",
                RowsetDefinition.Type.Integer,
                new TreeOp[] {
                    Children,
                    Siblings,
                    Parent,
                    Self,
                    Descendants,
                    Ancestors,
                }
        );
    }
}

// End Enumeration.java
