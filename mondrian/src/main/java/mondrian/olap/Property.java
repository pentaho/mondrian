/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2018 Hitachi Vantara and others
// All Rights Reserved.
//
// jhyde, 12 September, 2002
*/
package mondrian.olap;

import mondrian.rolap.SqlStatement;
import mondrian.spi.PropertyFormatter;

import java.util.*;

/**
 * <code>Property</code> is the definition of a member property.
 *
 * <p>The following properties are mandatory for members:<ul>
 * <li>{@link #CATALOG_NAME}</li>
 * <li>{@link #SCHEMA_NAME}</li>
 * <li>{@link #CUBE_NAME}</li>
 * <li>{@link #DIMENSION_UNIQUE_NAME}</li>
 * <li>{@link #HIERARCHY_UNIQUE_NAME}</li>
 * <li>{@link #LEVEL_UNIQUE_NAME}</li>
 * <li>{@link #LEVEL_NUMBER}</li>
 * <li>{@link #MEMBER_UNIQUE_NAME}</li>
 * <li>{@link #MEMBER_NAME}</li>
 * <li>{@link #MEMBER_TYPE}</li>
 * <li>{@link #MEMBER_GUID}</li>
 * <li>{@link #MEMBER_CAPTION}</li>
 * <li>{@link #MEMBER_ORDINAL}</li>
 * <li>{@link #CHILDREN_CARDINALITY}</li>
 * <li>{@link #PARENT_LEVEL}</li>
 * <li>{@link #PARENT_UNIQUE_NAME}</li>
 * <li>{@link #PARENT_COUNT}</li>
 * <li>{@link #DESCRIPTION}</li>
 * </ul>
 *
 * The following propertiess are mandatory for cells:<ul>
 * <li>{@link #BACK_COLOR}</li>
 * <li>{@link #CELL_EVALUATION_LIST}</li>
 * <li>{@link #CELL_ORDINAL}</li>
 * <li>{@link #FORE_COLOR}</li>
 * <li>{@link #FONT_NAME}</li>
 * <li>{@link #FONT_SIZE}</li>
 * <li>{@link #FONT_FLAGS}</li>
 * <li>{@link #FORMAT_STRING}</li>
 * <li>{@link #FORMATTED_VALUE}</li>
 * <li>{@link #NON_EMPTY_BEHAVIOR}</li>
 * <li>{@link #SOLVE_ORDER}</li>
 * <li>{@link #VALUE}</li>
 * </ul>
 *
 * @author jhyde
 */
public class Property extends EnumeratedValues.BasicValue {

    public enum Datatype {
        TYPE_STRING(null),
        TYPE_NUMERIC(null),
        TYPE_INTEGER(SqlStatement.Type.INT),
        TYPE_LONG(SqlStatement.Type.LONG),
        TYPE_BOOLEAN(null),
        TYPE_DATE(null),
        TYPE_TIME(null),
        TYPE_TIMESTAMP(null),
        TYPE_OTHER(null);

        private SqlStatement.Type type;

        Datatype(SqlStatement.Type type) {
            this.type = type;
    }

        public SqlStatement.Type getInternalType() {
            return type;
        }

        public boolean isNumeric() {
            return this == TYPE_NUMERIC
              || this == TYPE_INTEGER
              || this == TYPE_LONG;
        }
    }

    /**
     * For properties which have synonyms, maps from the synonym to the
     * property.
     */
    private static final Map<String, Property> synonyms =
        new HashMap<String, Property>();

    /**
     * Map of upper-case names to property definitions, for case-insensitive
     * match. Also contains synonyms.
     */
    public static final Map<String, Property> mapUpperNameToProperties =
        new HashMap<String, Property>();

    public static final int FORMAT_EXP_PARSED_ORDINAL = 0;
    /**
     * Definition of the internal property which
     * holds the parsed format string (an object of type {@link Exp}).
     */
    public static final Property FORMAT_EXP_PARSED =
        new Property(
            "$format_exp", Datatype.TYPE_OTHER,
            FORMAT_EXP_PARSED_ORDINAL, true, false,
            false, null);

    public static final int AGGREGATION_TYPE_ORDINAL = 1;
    /**
     * Definition of the internal property which
     * holds the aggregation type. This is automatically set for stored
     * measures, based upon their SQL aggregation.
     */
    public static final Property AGGREGATION_TYPE =
        new Property(
            "$aggregation_type", Datatype.TYPE_OTHER, AGGREGATION_TYPE_ORDINAL,
            true, false, false, null);

    public static final int NAME_ORDINAL = 2;
    /**
     * Definition of the internal property which
     * holds a member's name.
     */
    public static final Property NAME =
        new Property(
            "$name", Datatype.TYPE_STRING, NAME_ORDINAL, true, false, false,
            null);

    public static final int CAPTION_ORDINAL = 3;
    /**
     * Definition of the internal property which
     * holds a member's caption.
     */
    public static final Property CAPTION =
        new Property(
            "$caption", Datatype.TYPE_STRING, CAPTION_ORDINAL, true, false,
            false, null);

    public static final int CONTRIBUTING_CHILDREN_ORDINAL = 4;

    /**
     * Definition of the internal property which
     * holds, for a member of a  parent-child hierarchy, a
     * {@link java.util.List} containing the member's data
     * member and all of its children (including non-visible children).
     *
     * @deprecated Property is not used and will be removed in mondrian-4.0;
     * use {@link mondrian.olap.SchemaReader#getParentChildContributingChildren}
     */
    public static final Property CONTRIBUTING_CHILDREN =
        new Property(
            "$contributingChildren", Datatype.TYPE_OTHER,
            CONTRIBUTING_CHILDREN_ORDINAL, true, false, false, null);

    public static final int FORMULA_ORDINAL = 5;
    /**
     * Definition of the internal property which
     * returns a calculated member's {@link Formula} object.
     */
    public static final Property FORMULA =
        new Property(
            "$formula", Datatype.TYPE_OTHER, FORMULA_ORDINAL, true, false,
            false, null);

    public static final int MEMBER_SCOPE_ORDINAL = 6;
    /**
     * Definition of the internal property which
     * describes whether a calculated member belongs to a query or a cube.
     */
    public static final Property MEMBER_SCOPE =
        new Property(
            "$member_scope", Datatype.TYPE_OTHER, MEMBER_SCOPE_ORDINAL, true,
            true, false, null);

    public static final int CATALOG_NAME_ORDINAL = 10;
    /**
     * Definition of the property which
     * holds the name of the current catalog.
     */
    public static final Property CATALOG_NAME =
        new Property(
            "CATALOG_NAME", Datatype.TYPE_STRING, CATALOG_NAME_ORDINAL, false,
            true, false,
            "Optional. The name of the catalog to which this member belongs. "
            + "NULL if the provider does not support catalogs.");

    public static final int SCHEMA_NAME_ORDINAL = 11;
    /**
     * Definition of the property which
     * holds the name of the current schema.
     */
    public static final Property SCHEMA_NAME =
        new Property(
            "SCHEMA_NAME", Datatype.TYPE_STRING, SCHEMA_NAME_ORDINAL, false,
            true, false,
            "Optional. The name of the schema to which this member belongs. "
            + "NULL if the provider does not support schemas.");

    public static final int CUBE_NAME_ORDINAL = 12;
    /**
     * Definition of the property which
     * holds the name of the current cube.
     */
    public static final Property CUBE_NAME =
        new Property(
            "CUBE_NAME", Datatype.TYPE_STRING, CUBE_NAME_ORDINAL, false, true,
            false, "Required. Name of the cube to which this member belongs.");

    public static final int DIMENSION_UNIQUE_NAME_ORDINAL = 13;
    /**
     * Definition of the property which
     * holds the unique name of the current dimension.
     */
    public static final Property DIMENSION_UNIQUE_NAME =
        new Property(
            "DIMENSION_UNIQUE_NAME", Datatype.TYPE_STRING,
            DIMENSION_UNIQUE_NAME_ORDINAL, false, true, false,
            "Required. Unique name of the dimension to which this member "
            + "belongs. For providers that generate unique names by "
            + "qualification, each component of this name is delimited.");

    public static final int HIERARCHY_UNIQUE_NAME_ORDINAL = 14;
    /**
     * Definition of the property which
     * holds the unique name of the current hierarchy.
     */
    public static final Property HIERARCHY_UNIQUE_NAME =
        new Property(
            "HIERARCHY_UNIQUE_NAME", Datatype.TYPE_STRING,
            HIERARCHY_UNIQUE_NAME_ORDINAL, false, true, false,
            "Required. Unique name of the hierarchy. If the member belongs "
            + "to more than one hierarchy, there is one row for each hierarchy "
            + "to which it belongs. For providers that generate unique names "
            + "by qualification, each component of this name is delimited.");

    public static final int LEVEL_UNIQUE_NAME_ORDINAL = 15;
    /**
     * Definition of the property which
     * holds the unique name of the current level.
     */
    public static final Property LEVEL_UNIQUE_NAME =
        new Property(
            "LEVEL_UNIQUE_NAME", Datatype.TYPE_STRING,
            LEVEL_UNIQUE_NAME_ORDINAL, false, true, false,
            "Required. Unique name of the level to which the member belongs. "
            + "For providers that generate unique names by qualification, "
            + "each component of this name is delimited.");

    public static final int LEVEL_NUMBER_ORDINAL = 16;
    /**
     * Definition of the property which
     * holds the ordinal of the current level.
     */
    public static final Property LEVEL_NUMBER =
        new Property(
            "LEVEL_NUMBER", Datatype.TYPE_STRING, LEVEL_NUMBER_ORDINAL, false,
            true, false,
            "Required. The distance of the member from the root of the "
            + "hierarchy. The root level is zero.");

    public static final int MEMBER_ORDINAL_ORDINAL = 17;
    /**
     * Definition of the property which
     * holds the ordinal of the current member.
     */
    public static final Property MEMBER_ORDINAL =
        new Property(
            "MEMBER_ORDINAL", Datatype.TYPE_NUMERIC, MEMBER_ORDINAL_ORDINAL,
            false, true, false,
            "Required. Ordinal number of the member. Sort rank of the member "
            + "when members of this dimension are sorted in their natural sort "
            + "order. If providers do not have the concept of natural "
            + "ordering, this should be the rank when sorted by MEMBER_NAME.");

    public static final int MEMBER_NAME_ORDINAL = 18;
    /**
     * Definition of the property which
     * holds the name of the current member.
     */
    public static final Property MEMBER_NAME =
        new Property(
            "MEMBER_NAME", Datatype.TYPE_STRING, MEMBER_NAME_ORDINAL, false,
            true, false, "Required. Name of the member.");

    public static final int MEMBER_UNIQUE_NAME_ORDINAL = 19;
    /**
     * Definition of the property which
     * holds the unique name of the current member.
     */
    public static final Property MEMBER_UNIQUE_NAME =
        new Property(
            "MEMBER_UNIQUE_NAME", Datatype.TYPE_STRING,
            MEMBER_UNIQUE_NAME_ORDINAL, false, true, false,
            "Required. Unique name of the member. For providers that "
            + "generate unique names by qualification, each component of "
            + "this name is delimited.");

    public static final int MEMBER_TYPE_ORDINAL = 20;
    /**
     * Definition of the property which
     * holds the type of the member.
     */
    public static final Property MEMBER_TYPE =
        new Property(
            "MEMBER_TYPE", Datatype.TYPE_NUMERIC, MEMBER_TYPE_ORDINAL, false,
            true, false,
            "Required. Type of the member. Can be one of the following values: "
            + "MDMEMBER_TYPE_REGULAR, MDMEMBER_TYPE_ALL, "
            + "MDMEMBER_TYPE_FORMULA, MDMEMBER_TYPE_MEASURE, "
            + "MDMEMBER_TYPE_UNKNOWN. MDMEMBER_TYPE_FORMULA takes precedence "
            + "over MDMEMBER_TYPE_MEASURE. Therefore, if there is a formula "
            + "(calculated) member on the Measures dimension, it is listed as "
            + "MDMEMBER_TYPE_FORMULA.");

    public static final int MEMBER_GUID_ORDINAL = 21;
    /**
     * Definition of the property which
     * holds the GUID of the member
     */
    public static final Property MEMBER_GUID =
        new Property(
            "MEMBER_GUID", Datatype.TYPE_STRING, MEMBER_GUID_ORDINAL, false,
            true, false, "Optional. Member GUID. NULL if no GUID exists.");

    public static final int MEMBER_CAPTION_ORDINAL = 22;
    /**
     * Definition of the property which
     * holds the label or caption associated with the member, or the
     * member's name if no caption is defined.
     *
     * <p>"CAPTION" is a synonym for this property.
     */
    public static final Property MEMBER_CAPTION =
        new Property(
            "MEMBER_CAPTION", Datatype.TYPE_STRING, MEMBER_CAPTION_ORDINAL,
            false, true, false,
            "Required. A label or caption associated with the member. Used "
            + "primarily for display purposes. If a caption does not exist, "
            + "MEMBER_NAME is returned.");

    public static final int CHILDREN_CARDINALITY_ORDINAL = 23;
    /**
     * Definition of the property which holds the
     * number of children this member has.
     */
    public static final Property CHILDREN_CARDINALITY = new Property(
        "CHILDREN_CARDINALITY", Datatype.TYPE_NUMERIC,
        CHILDREN_CARDINALITY_ORDINAL, false, true, false,
        "Required. Number of children that the member has. This can be an "
        + "estimate, so consumers should not rely on this to be the exact "
        + "count. Providers should return the best estimate possible.");

    public static final int PARENT_LEVEL_ORDINAL = 24;
    /**
     * Definition of the property which holds the
     * distance from the root of the hierarchy of this member's parent.
     */
    public static final Property PARENT_LEVEL =
        new Property(
            "PARENT_LEVEL", Datatype.TYPE_NUMERIC, PARENT_LEVEL_ORDINAL, false,
            true, false,
            "Required. The distance of the member's parent from the root level "
            + "of the hierarchy. The root level is zero.");

    public static final int PARENT_UNIQUE_NAME_ORDINAL = 25;
    /**
     * Definition of the property which holds the
     * Name of the current catalog.
     */
    public static final Property PARENT_UNIQUE_NAME =
        new Property(
            "PARENT_UNIQUE_NAME", Datatype.TYPE_STRING,
            PARENT_UNIQUE_NAME_ORDINAL, false, true, false,
            "Required. Unique name of the member's parent. NULL is returned "
            + "for any members at the root level. For providers that generate "
            + "unique names by qualification, each component of this name is "
            + "delimited.");

    public static final int PARENT_COUNT_ORDINAL = 26;
    /**
     * Definition of the property which holds the
     * number of parents that this member has. Generally 1, or 0 for root
     * members.
     */
    public static final Property PARENT_COUNT =
        new Property(
            "PARENT_COUNT", Datatype.TYPE_NUMERIC, PARENT_COUNT_ORDINAL, false,
            true, false, "Required. Number of parents that this member has.");

    public static final int DESCRIPTION_ORDINAL = 27;
    /**
     * Definition of the property which holds the
     * description of this member.
     */
    public static final Property DESCRIPTION =
        new Property(
            "DESCRIPTION", Datatype.TYPE_STRING, DESCRIPTION_ORDINAL, false,
            true, false,
            "Optional. A human-readable description of the member.");

    public static final int VISIBLE_ORDINAL = 28;
    /**
     * Definition of the internal property which holds the
     * name of the system property which determines whether to show a member
     * (especially a measure or calculated member) in a user interface such as
     * JPivot.
     */
    public static final Property VISIBLE =
        new Property(
            "$visible", Datatype.TYPE_BOOLEAN, VISIBLE_ORDINAL, true, false,
            false, null);

    public static final int CELL_FORMATTER_ORDINAL = 29;
    /**
     * Definition of the property which holds the
     * name of the class which formats cell values of this member.
     *
     * <p>The class must implement the {@link mondrian.spi.CellFormatter}
     * interface.
     *
     * <p>Despite its name, this is a member property.
     */
    public static final Property CELL_FORMATTER =
        new Property(
            "CELL_FORMATTER", Datatype.TYPE_STRING, CELL_FORMATTER_ORDINAL,
            false, true, false,
            "Name of the class which formats cell values of this member.");

    public static final int CELL_FORMATTER_SCRIPT_LANGUAGE_ORDINAL = 51;
    /**
     * Definition of the property which holds the
     * name of the scripting language in which a scripted cell formatter is
     * implemented, e.g. 'JavaScript'.
     *
     * <p>Despite its name, this is a member property.
     */
    public static final Property CELL_FORMATTER_SCRIPT_LANGUAGE =
        new Property(
            "CELL_FORMATTER_SCRIPT_LANGUAGE", Datatype.TYPE_STRING,
            CELL_FORMATTER_SCRIPT_LANGUAGE_ORDINAL,
            false, true, false,
            "Name of the scripting language in which a scripted cell formatter"
            + "is implemented, e.g. 'JavaScript'.");

    public static final int CELL_FORMATTER_SCRIPT_ORDINAL = 52;
    /**
     * Definition of the property which holds the
     * script with which to format cell values of this member.
     *
     * <p>Despite its name, this is a member property.
     */
    public static final Property CELL_FORMATTER_SCRIPT =
        new Property(
            "CELL_FORMATTER_SCRIPT", Datatype.TYPE_STRING,
            CELL_FORMATTER_SCRIPT_ORDINAL,
            false, true, false,
            "Name of the class which formats cell values of this member.");

    // Cell properties


    public static final int BACK_COLOR_ORDINAL = 30;
    public static final Property BACK_COLOR =
        new Property(
            "BACK_COLOR", Datatype.TYPE_STRING, BACK_COLOR_ORDINAL, false,
            false, true,
            "The background color for displaying the VALUE or FORMATTED_VALUE "
            + "property. For more information, see FORE_COLOR and BACK_COLOR "
            + "Contents.");

    public static final int CELL_EVALUATION_LIST_ORDINAL = 31;
    public static final Property CELL_EVALUATION_LIST =
        new Property(
            "CELL_EVALUATION_LIST", Datatype.TYPE_STRING,
            CELL_EVALUATION_LIST_ORDINAL, false, false, true,
            "The semicolon-delimited list of evaluated formulas applicable to "
            + "the cell, in order from lowest to highest solve order. For more "
            + "information about solve order, see Understanding Pass Order and "
            + "Solve Order");

    public static final int CELL_ORDINAL_ORDINAL = 32;
    public static final Property CELL_ORDINAL =
        new Property(
            "CELL_ORDINAL", Datatype.TYPE_NUMERIC, CELL_ORDINAL_ORDINAL, false,
            false, true, "The ordinal number of the cell in the dataset.");

    public static final int FORE_COLOR_ORDINAL = 33;
    public static final Property FORE_COLOR =
        new Property(
            "FORE_COLOR", Datatype.TYPE_STRING, FORE_COLOR_ORDINAL, false,
            false, true,
            "The foreground color for displaying the VALUE or FORMATTED_VALUE "
            + "property. For more information, see FORE_COLOR and BACK_COLOR "
            + "Contents.");

    public static final int FONT_NAME_ORDINAL = 34;
    public static final Property FONT_NAME =
        new Property(
            "FONT_NAME", Datatype.TYPE_STRING, FONT_NAME_ORDINAL, false, false,
            true,
            "The font to be used to display the VALUE or FORMATTED_VALUE "
            + "property.");

    public static final int FONT_SIZE_ORDINAL = 35;
    public static final Property FONT_SIZE =
        new Property(
            "FONT_SIZE", Datatype.TYPE_STRING, FONT_SIZE_ORDINAL, false, false,
            true,
            "Font size to be used to display the VALUE or FORMATTED_VALUE "
            + "property.");

    public static final int FONT_FLAGS_ORDINAL = 36;
    public static final Property FONT_FLAGS =
        new Property(
            "FONT_FLAGS", Datatype.TYPE_NUMERIC, FONT_FLAGS_ORDINAL, false,
            false, true,
            "The bitmask detailing effects on the font. The value is the "
            + "result of a bitwise OR operation of one or more of the "
            + "following constants: MDFF_BOLD  = 1, MDFF_ITALIC = 2, "
            + "MDFF_UNDERLINE = 4, MDFF_STRIKEOUT = 8. For example, the value "
            + "5 represents the combination of bold (MDFF_BOLD) and underline "
            + "(MDFF_UNDERLINE) font effects.");

    public static final int FORMATTED_VALUE_ORDINAL = 37;
    /**
     * Definition of the property which
     * holds the formatted value of a cell.
     */
    public static final Property FORMATTED_VALUE =
        new Property(
            "FORMATTED_VALUE", Datatype.TYPE_STRING, FORMATTED_VALUE_ORDINAL,
            false, false, true,
            "The character string that represents a formatted display of the "
            + "VALUE property.");

    public static final int FORMAT_STRING_ORDINAL = 38;
    /**
     * Definition of the property which
     * holds the format string used to format cell values.
     */
    public static final Property FORMAT_STRING =
        new Property(
            "FORMAT_STRING", Datatype.TYPE_STRING, FORMAT_STRING_ORDINAL, false,
            false, true,
            "The format string used to create the FORMATTED_VALUE property "
            + "value. For more information, see FORMAT_STRING Contents.");

    public static final int NON_EMPTY_BEHAVIOR_ORDINAL = 39;
    public static final Property NON_EMPTY_BEHAVIOR =
        new Property(
            "NON_EMPTY_BEHAVIOR", Datatype.TYPE_STRING,
            NON_EMPTY_BEHAVIOR_ORDINAL, false, false, true,
            "The measure used to determine the behavior of calculated members "
            + "when resolving empty cells.");

    public static final int SOLVE_ORDER_ORDINAL = 40;
    /**
     * Definition of the property which
     * determines the solve order of a calculated member with respect to other
     * calculated members.
     */
    public static final Property SOLVE_ORDER =
        new Property(
            "SOLVE_ORDER", Datatype.TYPE_NUMERIC, SOLVE_ORDER_ORDINAL, false,
            false, true, "The solve order of the cell.");

    public static final int VALUE_ORDINAL = 41;
    /**
     * Definition of the property which
     * holds the value of a cell. Is usually numeric (since most measures are
     * numeric) but is occasionally another type.
     *
     * <p>It is also applicable to members.
     */
    public static final Property VALUE =
        new Property(
            "VALUE", Datatype.TYPE_NUMERIC, VALUE_ORDINAL, false, true, true,
            "The unformatted value of the cell.");

    public static final int DATATYPE_ORDINAL = 42;
    /**
     * Definition of the property which
     * holds the datatype of a cell. Valid values are "String",
     * "Numeric", "Integer". The property's value derives from the
     * "datatype" attribute of the "Measure" element; if the datatype attribute
     * is not specified, the datatype is "Numeric" by default, except measures
     * whose aggregator is "Count", whose datatype is "Integer".
     */
    public static final Property DATATYPE =
        new Property(
            "DATATYPE", Datatype.TYPE_STRING, DATATYPE_ORDINAL, false, false,
            true, "The datatype of the cell.");

    public static final int DEPTH_ORDINAL = 43;
    /**
     * Definition of the property which
     * holds the level depth of a member.
     *
     * <p>Caution: Level depth of members in parent-child hierarchy isn't from
     * their levels. It's calculated from the underlying data dynamically.
     */
    public static final Property DEPTH =
        new Property(
            "DEPTH", Datatype.TYPE_NUMERIC, DEPTH_ORDINAL, true, true, false,
            "The level depth of a member");

    public static final int DISPLAY_INFO_ORDINAL = 44;

    /**
     * Definition of the property which
     * holds the DISPLAY_INFO required by XML/A.
     * Caution: This property's value is calculated based on a specified MDX
     * query, so it's value is dynamic at runtime.
     */
    public static final Property DISPLAY_INFO =
        new Property(
            "DISPLAY_INFO", Datatype.TYPE_NUMERIC, DISPLAY_INFO_ORDINAL, false,
            true, false, "Display instruction of a member for XML/A");

     public static final int MEMBER_KEY_ORDINAL = 45;
    /**
     * Definition of the property which
     * holds the member key of the current member.
     */
    public static final Property MEMBER_KEY =
        new Property(
            "MEMBER_KEY", Datatype.TYPE_STRING, MEMBER_KEY_ORDINAL, false, true,
            false, "Member key.");

     public static final int KEY_ORDINAL = 46;
    /**
     * Definition of the property which
     * holds the key of the current member.
     */
    public static final Property KEY =
        new Property(
            "KEY", Datatype.TYPE_STRING, KEY_ORDINAL, false, true, false,
            "Key.");

    public static final int SCENARIO_ORDINAL = 48;
    /**
     * Definition of the internal property which
     * holds the scenario object underlying a member of the scenario hierarchy.
     */
    public static final Property SCENARIO =
        new Property(
            "$scenario", Datatype.TYPE_OTHER,
            SCENARIO_ORDINAL, true, true, false, null);

    public static final int DISPLAY_FOLDER_ORDINAL = 49;
    /**
     * Definition of the property which
     * holds the DISPLAY_FOLDER. For measures, a client tool may use this
     * folder to display measures in groups. This property has no meaning for
     * other members.
     */
    public static final Property DISPLAY_FOLDER =
        new Property(
            "DISPLAY_FOLDER", Datatype.TYPE_STRING, DISPLAY_FOLDER_ORDINAL,
            false, true, false, "Folder in which to display a measure");

    public static final int LANGUAGE_ORDINAL = 50;

    /**
     * Definition of the property which
     * holds the translation expressed as an LCID.
     * Only valid for property translations.
     */
    public static final Property LANGUAGE =
        new Property(
            "LANGUAGE", Datatype.TYPE_NUMERIC, LANGUAGE_ORDINAL,
            false, false, true,
            "The translation expressed as an LCID. Only valid for property translations.");

    public static final int FORMAT_EXP_ORDINAL = 53;

    /**
     * Definition of the property which
     * holds the format string.
     */
    public static final Property FORMAT_EXP =
        new Property(
            "FORMAT_EXP", Datatype.TYPE_STRING, FORMAT_EXP_ORDINAL, true, true,
            false, null);

    public static final int ACTION_TYPE_ORDINAL = 54;

    /**
     * Definition of the property which
     * holds the format string.
     */
    public static final Property ACTION_TYPE =
        new Property(
            "ACTION_TYPE", Datatype.TYPE_NUMERIC, ACTION_TYPE_ORDINAL, false,
            false, true, null);

    public static final int DRILLTHROUGH_COUNT_ORDINAL = 55;

    /**
     * Definition of the property that
     * holds the number of fact rows that contributed to this cell.
     * If the cell is not drillable, returns -1.
     *
     * <p>Note that this property may be expensive to compute for some
     * cubes.</p>
     */
    public static final Property DRILLTHROUGH_COUNT =
        new Property(
            "DRILLTHROUGH_COUNT", Datatype.TYPE_NUMERIC,
            DRILLTHROUGH_COUNT_ORDINAL, false,
            false, true,
            "Number of fact rows that contributed to this cell. If the cell is "
            + "not drillable, value is -1.");

    /**
     * The various property names which define a format string.
     */
    static final Set<String> FORMAT_PROPERTIES =
        new HashSet<String>(
            Arrays.asList(
                "format", "format_string", "FORMAT", FORMAT_STRING.name));

    // ~ Data members ---------------------------------------------------------

    /**
     * The datatype of the property.
     */
    private final Datatype type;

    /**
     * Whether the property is internal.
     */
    private final boolean internal;
    private final boolean member;
    private final boolean cell;

    private static int nextOrdinal = 100;

    // ~ Methods --------------------------------------------------------------

    /**
     * Creates a property definition. If ordinal is negative, generates a
     * unique positive ordinal.
     */
    protected Property(
        String name,
        Datatype type,
        int ordinal,
        boolean internal,
        boolean member,
        boolean cell,
        String description)
    {
        super(name, ordinal < 0 ? nextOrdinal++ : ordinal, description);
        this.type = type;
        this.internal = internal;
        this.member = member;
        this.cell = cell;
    }

    /**
     * Returns the datatype of the property.
     */
    public Datatype getType() {
        return type;
    }

    public PropertyFormatter getFormatter() {
        return null;
    }

    /**
     * Returns the caption of this property.
     */
    public String getCaption() {
        return name;
    }

    /**
     * Returns whether this property is for system use only.
     */
    public boolean isInternal() {
        return internal;
    }

    /**
     * Returns whether this property is a standard member property.
     */
    public boolean isMemberProperty() {
        return member;
    }

    /**
     * Returns whether this property is a standard cell property.
     */
    public boolean isCellProperty() {
        return cell && isStandard();
    }

    /**
     * Returns whether this property is standard.
     */
    public boolean isStandard() {
        return ordinal < MAX_ORDINAL;
    }

    public static final EnumeratedValues<Property> enumeration =
        new EnumeratedValues<Property>(
            new Property[] {
                FORMAT_EXP_PARSED,
                AGGREGATION_TYPE,
                NAME,
                CAPTION,
                CONTRIBUTING_CHILDREN,
                FORMULA,
                CATALOG_NAME,
                SCHEMA_NAME,
                CUBE_NAME,
                DIMENSION_UNIQUE_NAME,
                HIERARCHY_UNIQUE_NAME,
                LEVEL_UNIQUE_NAME,
                LEVEL_NUMBER,
                MEMBER_UNIQUE_NAME,
                MEMBER_NAME,
                MEMBER_TYPE,
                MEMBER_GUID,
                MEMBER_CAPTION,
                MEMBER_ORDINAL,
                CHILDREN_CARDINALITY,
                PARENT_LEVEL,
                PARENT_UNIQUE_NAME,
                PARENT_COUNT,
                DESCRIPTION,
                VISIBLE,
                CELL_FORMATTER,
                CELL_FORMATTER_SCRIPT,
                CELL_FORMATTER_SCRIPT_LANGUAGE,
                BACK_COLOR,
                CELL_EVALUATION_LIST,
                CELL_ORDINAL,
                FORE_COLOR,
                FONT_NAME,
                FONT_SIZE,
                FONT_FLAGS,
                FORMAT_STRING,
                FORMATTED_VALUE,
                NON_EMPTY_BEHAVIOR,
                SOLVE_ORDER,
                VALUE,
                DATATYPE,
                MEMBER_KEY,
                KEY,
                SCENARIO,
                DISPLAY_FOLDER,
                FORMAT_EXP,
                ACTION_TYPE,
                DRILLTHROUGH_COUNT,
            });

    private static final int MAX_ORDINAL = 56;

    static {
        // Populate synonyms.
        synonyms.put("CAPTION", MEMBER_CAPTION);
        synonyms.put("FORMAT", FORMAT_STRING);

        // Populate map of upper-case property names.
        for (String propertyName : enumeration.getNames()) {
            final Property property = enumeration.getValue(propertyName, true);
            mapUpperNameToProperties.put(
                propertyName.toUpperCase(), property);
            assert property.getOrdinal() < MAX_ORDINAL;
        }

        // Add synonyms.
        for (Map.Entry<String, Property> entry : synonyms.entrySet()) {
            mapUpperNameToProperties.put(
                entry.getKey().toUpperCase(), entry.getValue());
        }
    }

    /**
     * Looks up a Property with a given ordinal.
     * Returns null if not found.
     */
    public static Property lookup(int ordinal) {
        return enumeration.getValue(ordinal);
    }

    /**
     * Looks up a Property with a given name.
     *
     * @param name Name of property
     * @param matchCase Whether to perform case-sensitive match
     * @return Property with given name, or null if not found.
     */
    public static Property lookup(String name, boolean matchCase) {
        if (matchCase) {
            Property property = enumeration.getValue(name, false);
            if (property != null) {
                return property;
            }
            return synonyms.get(name);
        } else {
            // No need to check synonyms separately - the map contains them.
            return mapUpperNameToProperties.get(name.toUpperCase());
        }
    }
}

// End Property.java
