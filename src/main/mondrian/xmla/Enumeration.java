/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2003-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, May 2, 2003
*/
package mondrian.xmla;

import java.util.List;
import java.util.ArrayList;

/**
 * Contains inner classes which define enumerations used in XML for Analysis.
 *
 * @author jhyde
 * @since May 2, 2003
 * @version $Id$
 */
class Enumeration {
    public final String name;
    public final String description;
    public final RowsetDefinition.Type type;
    private final Class<? extends Enum> clazz;

    public Enumeration(
        String name,
        String description,
        RowsetDefinition.Type type,
        Class<? extends Enum> clazz)
    {
        this.name = name;
        this.description = description;
        this.type = type;
        this.clazz = clazz;
    }

    public String getName() {
        return name;
    }

    public String[] getNames() {
        List<String> names = new ArrayList<String>();
        for (Enum anEnum : clazz.getEnumConstants()) {
            names.add(anEnum.name());
        }
        return names.toArray(new String[names.size()]);
    }

    public Enum<?> getValue(String valueName, boolean b) {
        return Enum.valueOf(clazz, valueName);
    }

    public enum Methods {
        discover,
        execute,
        discoverAndExecute;

        public static final Enumeration enumeration = new Enumeration(
            "Methods",
            "Set of methods for which a property is applicable",
            RowsetDefinition.Type.Enumeration,
            Methods.class);
    }

    public enum Access implements EnumWithOrdinal {
        Read(1),
        Write(2),
        ReadWrite(3);
        private final int userOrdinal;

        Access(int userOrdinal) {
            this.userOrdinal = userOrdinal;
        }

        public static final Enumeration enumeration =
            new Enumeration(
                "Access",
                "The read/write behavior of a property",
                RowsetDefinition.Type.Enumeration,
                Access.class);

        public int userOrdinal() {
            return userOrdinal;
        }
    }

    public enum Format implements EnumWithDesc {
        Tabular("a flat or hierarchical rowset. Similar to the XML RAW format in SQL. The Format property should be set to Tabular for OLE DB for Data Mining commands."),
        Multidimensional("Indicates that the result set will use the MDDataSet format (Execute method only)."),
        Native("The client does not request a specific format, so the provider may return the format  appropriate to the query. (The actual result type is identified by namespace of the result.)");
        private final String description;

        Format(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    public enum AxisFormat implements EnumWithDesc {
        TupleFormat("The MDDataSet axis is made up of one or more CrossProduct elements."),
        ClusterFormat("Analysis Services uses the TupleFormat format for this setting."),
        CustomFormat("The MDDataSet axis contains one or more Tuple elements.");
        private final String description;

        AxisFormat(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    public enum Content {
        None,
        Schema,
        Data,
        SchemaData;
    }

    enum MdxSupport {
        Core
    }

    enum StateSupport {
        None,
        Sessions
    }

    enum AuthenticationMode implements EnumWithDesc {
        Unauthenticated("no user ID or password needs to be sent."),
        Authenticated("User ID and Password must be included in the information required for the connection."),
        Integrated("the data source uses the underlying security to determine authorization, such as Integrated Security provided by Microsoft Internet Information Services (IIS).");
        private final String description;

        AuthenticationMode(String description) {
            this.description = description;
        }

        public static final Enumeration enumeration = new Enumeration(
                "AuthenticationMode",
                "Specification of what type of security mode the data source uses.",
                RowsetDefinition.Type.EnumString,
                AuthenticationMode.class);

        public String getDescription() {
            return description;
        }
    }

    enum ProviderType implements EnumWithDesc {
        TDP("tabular data provider."),
        MDP("multidimensional data provider."),
        DMP("data mining provider. A DMP provider implements the OLE DB for Data Mining specification.");
        private final String description;

        private ProviderType(String description) {
            this.description = description;
        }

        public static final Enumeration enumeration = new Enumeration(
                "ProviderType",
                "The types of data supported by the provider.",
                RowsetDefinition.Type.Array,
                ProviderType.class);

        public String getDescription() {
            return description;
        }
    }

    enum Literal implements EnumWithDesc {
        DBLITERAL_CATALOG_NAME(2, null, 24, ".", "0123456789", "A catalog name in a text command."),
        DBLITERAL_CATALOG_SEPARATOR(3, ".", 0, null, null, null),
        DBLITERAL_COLUMN_ALIAS(5, null, -1, "'\"[]", "0123456789", null),
        DBLITERAL_COLUMN_NAME(6, null, -1, ".", "0123456789", null),
        DBLITERAL_CORRELATION_NAME(7, null, -1, "'\"[]", "0123456789", null),
        DBLITERAL_CUBE_NAME(21, null, -1, ".", "0123456789", null),
        DBLITERAL_DIMENSION_NAME(22, null, -1, ".", "0123456789", null),
        DBLITERAL_HIERARCHY_NAME(23, null, -1, ".", "0123456789", null),
        DBLITERAL_LEVEL_NAME(24, null, -1, ".", "0123456789", null),
        DBLITERAL_MEMBER_NAME(25, null, -1, ".", "0123456789", null),
        DBLITERAL_PROCEDURE_NAME(14, null, -1, ".", "0123456789", null),
        DBLITERAL_PROPERTY_NAME(26, null, -1, ".", "0123456789", null),
        DBLITERAL_QUOTE(15, "[", -1, null, null, "The character used in a text command as the opening quote for quoting identifiers that contain special characters."),
        DBLITERAL_QUOTE_SUFFIX(28, "]", -1, null, null, "The character used in a text command as the closing quote for quoting identifiers that contain special characters. 1.x providers that use the same character as the prefix and suffix may not return this literal value and can set the lt member of the DBLITERAL structure to DBLITERAL_INVALID if requested."),
        DBLITERAL_TABLE_NAME(17, null, -1, ".", "0123456789", null),
        DBLITERAL_TEXT_COMMAND(18, null, -1, null, null, "A text command, such as an SQL statement."),
        DBLITERAL_USER_NAME(19, null, 0, null, null, null);
        private final String literalValue;
        private final int literalMaxLength;
        private final String literalInvalidChars;
        private final String literalInvalidStartingChars;
        private final String description;

        /*
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
*/

        Literal(int ordinal, String literalValue, int literalMaxLength, String literalInvalidChars, String literalInvalidStartingChars, String description) {
            this.literalValue = literalValue;
            this.literalMaxLength = literalMaxLength;
            this.literalInvalidChars = literalInvalidChars;
            this.literalInvalidStartingChars = literalInvalidStartingChars;
            this.description = description;
        }

        public String getLiteralName() {
            return name();
        }

        public String getLiteralValue() {
            return literalValue;
        }

        public String getLiteralInvalidChars() {
            return literalInvalidChars;
        }

        public String getLiteralInvalidStartingChars() {
            return literalInvalidStartingChars;
        }

        public int getLiteralMaxLength() {
            return literalMaxLength;
        }

        public String getDescription() {
            return description;
        }
    }

    enum TreeOp implements EnumWithDesc, EnumWithOrdinal, EnumWithName {
        Children("MDTREEOP_CHILDREN", 1, "Returns only the immediate children"),
        Siblings("MDTREEOP_SIBLINGS", 2, "Returns members on the same level"),
        Parent("MDTREEOP_PARENT", 4, "Returns only the immediate parent"),
        Self("MDTREEOP_SELF", 8, "Returns the immediate member in the list of returned rows"),
        Descendants("MDTREEOP_DESCENDANTS", 16, "Returns all descendants"),
        Ancestors("MDTREEOP_ANCESTORS", 32, "Returns all ancestors");
        private final String userName;
        private final int userOrdinal;
        private final String description;

        TreeOp(String userName, int userOrdinal, String description) {
            this.userName = userName;
            this.userOrdinal = userOrdinal;
            this.description = description;
        }

        static final Enumeration enumeration =
            new Enumeration(
                "TREE_OP",
                "Bitmap which controls which relatives of a member are returned",
                RowsetDefinition.Type.Integer,
                TreeOp.class);

        public int userOrdinal() {
            return userOrdinal;
        }

        public String userName() {
            return userName;
        }

        public String getDescription() {
            return description;
        }
    }

    enum VisualMode {
        Default("DBPROPVAL_VISUAL_MODE_DEFAULT", 0, "Provider-dependent. In Microsoft SQL Server 2000 Analysis Services, this is equivalent to DBPROPVAL_VISUAL_MODE_ORIGINAL."),
        Visual("DBPROPVAL_VISUAL_MODE_VISUAL", 1, "Visual totals are enabled."),
        Original("DBPROPVAL_VISUAL_MODE_ORIGINAL", 2, "Visual totals are not enabled.");

        VisualMode(String name2, int ordinal2, String description) {
        }

        static final Enumeration enumeration =
            new Enumeration(
                "VisualMode",
                "This property determines the default behavior for visual totals.",
                RowsetDefinition.Type.Integer,
                VisualMode.class);
    }

    interface EnumWithDesc {
        String getDescription();
    }

    interface EnumWithOrdinal {
        int userOrdinal();
    }

    interface EnumWithName {
        String userName();
    }
}

// End Enumeration.java
