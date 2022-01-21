/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.gui.validate;

import mondrian.gui.MondrianGuiDef;
import mondrian.gui.SchemaExplorer;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.lang.reflect.Field;
import java.util.TreeSet;

/**
 * Validates a <code>MondrianGuiDef</code>. Class contains <code>invalid</code>
 * method formerly from <code>mondrian.gui.SchemaTreeCellRenderer</code>.
 *
 * @author mlowery
 */
public class ValidationUtils {

    private static final Logger LOGGER =
        LogManager.getLogger(ValidationUtils.class);

    static String[] DEF_LEVEL = {
        "column", "nameColumn", "parentColumn", "ordinalColumn", "captionColumn"
    };

    /**
     * Validate a schema model and returns the first error message if it is
     * invalid.
     *
     * @param messages Message provider
     * @param jdbcValidator Validator
     * @param treeModel Tree model
     * @param tpath Path
     * @param value Value
     * @param cube Cube
     * @param parentDimension Parent dimension
     * @param parentHierarchy Parent hierarchy
     * @param parentLevel Parent level
     * @param isSchemaRequired Whether schema is required
     * @return Error message if element is invalid, null if it is valid
     */
    public static String invalid(
        Messages messages,
        JdbcValidator jdbcValidator,
        TreeModel treeModel,
        TreeModelPath tpath,
        Object value,
        MondrianGuiDef.Cube cube,
        MondrianGuiDef.Dimension parentDimension,
        MondrianGuiDef.Hierarchy parentHierarchy,
        MondrianGuiDef.Level parentLevel,
        boolean isSchemaRequired)
    {
        String nameMustBeSet = messages.getString(
            "schemaTreeCellRenderer.nameMustBeSet.alert", "Name must be set");

        if (!tpath.isEmpty()) {
            int pathcount = tpath.getPathCount();
            for (int i = 0;
                i < pathcount
                && (cube == null
                    || parentDimension == null
                    || parentHierarchy == null
                    || parentLevel == null);
                i++)
            {
                final Object p = tpath.getPathComponent(i);
                if (p instanceof MondrianGuiDef.Cube
                    && cube == null)
                {
                    cube = (MondrianGuiDef.Cube) p;
                }
                if (p instanceof MondrianGuiDef.Dimension
                    && parentDimension == null)
                {
                    parentDimension = (MondrianGuiDef.Dimension) p;
                }
                if (p instanceof MondrianGuiDef.Hierarchy
                    && parentHierarchy == null)
                {
                    parentHierarchy = (MondrianGuiDef.Hierarchy) p;
                }
                if (p instanceof MondrianGuiDef.Level
                    && parentLevel == null)
                {
                    parentLevel = (MondrianGuiDef.Level) p;
                }
            }
        }

        //Step 1: check validity of this value object
        if (value instanceof MondrianGuiDef.Schema) {
            if (isEmpty(((MondrianGuiDef.Schema) value).name)) {
                return nameMustBeSet;
            }
        } else if (value instanceof MondrianGuiDef.VirtualCube) {
            MondrianGuiDef.VirtualCube virtCube =
                (MondrianGuiDef.VirtualCube)value;
            if (isEmpty(virtCube.name)) {
                return nameMustBeSet;
            }
            if (isEmpty(virtCube.dimensions)) {
                return messages.getString(
                    "schemaTreeCellRenderer.cubeMustHaveDimensions.alert",
                    "Cube must contain dimensions");
            }
            if (isEmpty(virtCube.measures)) {
                return messages.getString(
                    "schemaTreeCellRenderer.cubeMustHaveMeasures.alert",
                    "Cube must contain measures");
            }
        } else if (value instanceof MondrianGuiDef.VirtualCubeDimension) {
            if (isEmpty(((MondrianGuiDef.VirtualCubeDimension) value).name)) {
                return nameMustBeSet;
            }
        } else if (value instanceof MondrianGuiDef.VirtualCubeMeasure) {
            if (isEmpty(((MondrianGuiDef.VirtualCubeMeasure) value).name)) {
                return nameMustBeSet;
            }
        } else if (value instanceof MondrianGuiDef.Cube) {
            MondrianGuiDef.Cube cubeVal = (MondrianGuiDef.Cube) value;
            if (isEmpty(cubeVal.name)) {
                return nameMustBeSet;
            }
            if (cubeVal.fact == null
                || ((cubeVal.fact instanceof MondrianGuiDef.Table)
                    && isEmpty(((MondrianGuiDef.Table) cubeVal.fact).name))
                || ((cubeVal.fact instanceof MondrianGuiDef.View)
                    && isEmpty(((MondrianGuiDef.View) cubeVal.fact).alias)))
            {
                return messages.getString(
                    "schemaTreeCellRenderer.factNameMustBeSet.alert",
                    "Fact name must be set");
            }
            if (isEmpty(cubeVal.dimensions)) {
                return messages.getString(
                    "schemaTreeCellRenderer.cubeMustHaveDimensions.alert",
                    "Cube must contain dimensions");
            }
            if (isEmpty(cubeVal.measures)) {
                return messages.getString(
                    "schemaTreeCellRenderer.cubeMustHaveMeasures.alert",
                    "Cube must contain measures");
            }
            // database validity check, if database connection is successful
            if (jdbcValidator.isInitialized()) {
                if (((MondrianGuiDef.Cube) value).fact
                    instanceof MondrianGuiDef.Table)
                {
                    final MondrianGuiDef.Table table =
                        (MondrianGuiDef.Table) cubeVal.fact;
                    String schemaName = table.schema;
                    String factTable = table.name;
                    if (!jdbcValidator.isTableExists(schemaName, factTable)) {
                        return messages.getFormattedString(
                            "schemaTreeCellRenderer.factTableDoesNotExist.alert",
                            "Fact table {0} does not exist in database {1}",
                            factTable,
                            ((schemaName == null || schemaName.equals(""))
                                ? "."
                                : "schema " + schemaName));
                    }
                }
            }
        } else {
            if (value instanceof MondrianGuiDef.CubeDimension) {
                if (isEmpty(((MondrianGuiDef.CubeDimension) value).name)) {
                    return nameMustBeSet;
                }
                if (value instanceof MondrianGuiDef.DimensionUsage) {
                    if (isEmpty(
                            ((MondrianGuiDef.DimensionUsage) value).source))
                    {
                        return messages.getString(
                            "schemaTreeCellRenderer.sourceMustBeSet.alert",
                            "Source must be set");
                    }
                    // Check source is name of one of dimensions of schema
                    // (shared dimensions)
                    MondrianGuiDef.Schema s =
                        (MondrianGuiDef.Schema) treeModel.getRoot();
                    MondrianGuiDef.Dimension ds[] = s.dimensions;
                    String sourcename =
                        ((MondrianGuiDef.DimensionUsage) value).source;
                    boolean notfound = true;
                    for (int j = 0; j < ds.length; j++) {
                        if (ds[j].name.equalsIgnoreCase(sourcename)) {
                            notfound = false;
                            break;
                        }
                    }
                    if (notfound) {
                        return messages.getFormattedString(
                            "schemaTreeCellRenderer.sourceInSharedDimensionDoesNotExist.alert",
                            "Source {0} does not exist as Shared Dimension of Schema",
                            sourcename);
                    }
                }
                if (value instanceof MondrianGuiDef.Dimension && cube != null) {
                    if (!isEmpty(
                            ((MondrianGuiDef.Dimension) value).foreignKey))
                    {
                        // database validity check, if database connection is
                        // successful
                        if (jdbcValidator.isInitialized()) {
                            // TODO: Need to add validation for Views
                            if (cube.fact instanceof MondrianGuiDef.Table) {
                                final MondrianGuiDef.Table factTable =
                                    (MondrianGuiDef.Table) cube.fact;
                                String foreignKey =
                                    ((MondrianGuiDef.Dimension) value)
                                    .foreignKey;
                                if (!jdbcValidator.isColExists(
                                        factTable.schema,
                                        factTable.name,
                                        foreignKey))
                                {
                                    return messages.getFormattedString(
                                        "schemaTreeCellRenderer.foreignKeyDoesNotExist.alert",
                                        "foreignKey {0} does not exist in fact table",
                                        foreignKey);
                                }
                            }
                        }
                    }
                }
            } else if (value instanceof MondrianGuiDef.Level) {
                // Check 'column' exists in 'table' if table is specified
                // otherwise :: case of join.

                // It should exist in relation table if it is specified
                // otherwise :: case of table.

                // It should exist in fact table :: case of degenerate dimension
                // where dimension columns exist in fact table and there is no
                // separate table.
                MondrianGuiDef.Level level = (MondrianGuiDef.Level) value;
                if (!isEmpty(level.levelType)) {
                    // Empty leveltype is treated as default value of "Regular""
                    // which is ok with standard/time dimension.
                    if (parentDimension != null) {
                        if ((isEmpty(parentDimension.type)
                             || parentDimension.type.equals(
                                 "StandardDimension"))
                            && !isEmpty(level.levelType)
                            && (!level.levelType.equals(
                                MondrianGuiDef.Level._levelType_values[0])))
                        {
                            // If dimension type is 'standard' then leveltype
                            // should be 'regular'
                            return messages.getFormattedString(
                                "schemaTreeCellRenderer.levelUsedOnlyInTimeDimension.alert",
                                "levelType {0} can only be used with a TimeDimension",
                                level.levelType);
                        } else if (!isEmpty(parentDimension.type)
                                   && (parentDimension.type.equals(
                                       "TimeDimension"))
                                   && !isEmpty(level.levelType)
                                   && (level.levelType.equals(
                                       MondrianGuiDef.Level
                                       ._levelType_values[0])))
                        {
                            // If dimension type is 'time' then leveltype value
                            // could be 'timeyears', 'timedays' etc'
                            return messages.getFormattedString(
                                "schemaTreeCellRenderer.levelUsedOnlyInStandardDimension.alert",
                                "levelType {0} can only be used with a StandardDimension",
                                level.levelType);
                        }
                    }
                }
                // verify level's name is set
                if (isEmpty(level.name)) {
                    return messages.getString(
                        "schemaTreeCellRenderer.nameMustBeSet.alert",
                        "Level name must be set"
                    );
                }

                // check level's column is in fact table
                String column = level.column;
                if (isEmpty(column)) {
                    if (level.properties == null
                        || level.properties.length == 0)
                    {
                        return messages.getString(
                            "schemaTreeCellRenderer.columnMustBeSet.alert",
                            "Column must be set");
                    }
                } else {
                    // Enforces validation for all column types against invalid
                    // value.
                    String theMessage = null;
                    try {
                        for (int i = 0; i < DEF_LEVEL.length; i++) {
                            Field theField =
                                level.getClass().getDeclaredField(DEF_LEVEL[i]);
                            column = (String) theField.get(level);
                            theMessage = validateColumn(
                                column,
                                DEF_LEVEL[i],
                                messages,
                                level,
                                jdbcValidator,
                                cube,
                                parentHierarchy);
                            if (theMessage != null) {
                                break;
                            }
                        }
                    } catch (Exception ex) {
                        LOGGER.error("ValidationUtils", ex);
                    }
                    return theMessage;
                }
            } else if (value instanceof MondrianGuiDef.Property) {
                // Check 'column' exists in 'table' if [level table] is
                // specified otherwise :: case of join.

                // It should exist in [hierarchy relation table] if it is
                // specified otherwise :: case of table.

                // It should exist in [fact table] :: case of degenerate
                // dimension where dimension columns exist in fact table and
                // there is no separate table.
                MondrianGuiDef.Property p = (MondrianGuiDef.Property) value;
                // check property's column is in table
                String column = p.column;
                if (isEmpty(column)) {
                    return messages.getString(
                        "schemaTreeCellRenderer.columnMustBeSet.alert",
                        "Column must be set");
                }
                // Database validity check, if database connection is successful
                if (jdbcValidator.isInitialized()) {
                    String table = null;
                    if (parentLevel != null) {
                        // specified table for level's column'
                        table = parentLevel.table;
                    }
                    if (isEmpty(table)) {
                        if (parentHierarchy != null) {
                            if (parentHierarchy.relation == null
                                && cube != null)
                            {
                                // Case of degenerate dimension within cube,
                                // hierarchy table not specified
                                final MondrianGuiDef.Table factTable =
                                    (MondrianGuiDef.Table) cube.fact;
                                if (!jdbcValidator.isColExists(
                                        factTable.schema,
                                        factTable.name,
                                        column))
                                {
                                    return messages.getFormattedString(
                                        "schemaTreeCellRenderer.degenDimensionColumnDoesNotExist.alert",
                                        "Degenerate dimension validation check - Column {0} does not exist in fact table",
                                        column);
                                }
                            } else if (parentHierarchy.relation
                                       instanceof MondrianGuiDef.Table)
                            {
                                final MondrianGuiDef.Table parentTable =
                                    (MondrianGuiDef.Table)
                                    parentHierarchy.relation;
                                if (!jdbcValidator.isColExists(
                                        parentTable.schema,
                                        parentTable.name,
                                        column))
                                {
                                    return messages.getFormattedString(
                                        "schemaTreeCellRenderer.columnInDimensionDoesNotExist.alert",
                                        "Column {0} does not exist in Dimension table",
                                        parentTable.name);
                                }
                            }
                        }
                    } else {
                        if (!jdbcValidator.isColExists(null, table, column)) {
                            return messages.getFormattedString(
                                "schemaTreeCellRenderer.columnInDimensionDoesNotExist.alert",
                                "Column {0} does not exist in Level table {1}",
                                column,
                                table);
                        }
                    }
                }
            } else if (value instanceof MondrianGuiDef.Measure) {
                final MondrianGuiDef.Measure measure =
                    (MondrianGuiDef.Measure) value;
                if (isEmpty(measure.name)) {
                    return nameMustBeSet;
                }
                if (isEmpty(measure.aggregator)) {
                    return messages.getString(
                        "schemaTreeCellRenderer.aggregatorMustBeSet.alert",
                        "Aggregator must be set");
                }
                if (measure.measureExp != null) {
                    // Measure expressions are OK
                } else if (isEmpty(measure.column)) {
                    return messages.getString(
                        "schemaTreeCellRenderer.columnMustBeSet.alert",
                        "Column must be set");
                } else if (cube != null && cube.fact != null) {
                    // Database validity check, if database connection is
                    // successful
                    if (cube.fact instanceof MondrianGuiDef.Table) {
                        final MondrianGuiDef.Table factTable =
                            (MondrianGuiDef.Table) cube.fact;
                        if (jdbcValidator.isInitialized()) {
                            String column = measure.column;
                            if (jdbcValidator.isColExists(
                                    factTable.schema,
                                    factTable.name,
                                    column))
                            {
                                // Check for aggregator type only if column
                                // exists in table.

                                // Check if aggregator selected is valid on
                                // the data type of the column selected.
                                int colType =
                                    jdbcValidator.getColumnDataType(
                                        factTable.schema,
                                        factTable.name,
                                        measure.column);
                                // Coltype of 2, 4,5, 7, 8, -5 is numeric types
                                // whereas 1, 12 are char varchar string
                                // and 91 is date type.
                                // Types are enumerated in java.sql.Types.
                                int agIndex = -1;
                                if ("sum".equals(
                                        measure.aggregator)
                                    || "avg".equals(
                                        measure.aggregator))
                                {
                                    // aggregator = sum or avg, column should
                                    // be numeric
                                    agIndex = 0;
                                }
                                if (!(agIndex == -1
                                    || (colType >= 2 && colType <= 8)
                                    || colType == -5 || colType == -6))
                                {
                                    return messages.getFormattedString(
                                        "schemaTreeCellRenderer.aggregatorNotValidForColumn.alert",
                                        "Aggregator {0} is not valid for the data type of the column {1}",
                                        measure.aggregator,
                                        measure.column);
                                }
                            }
                        }
                    }
                }
            } else if (value instanceof MondrianGuiDef.Hierarchy) {
                final MondrianGuiDef.Hierarchy hierarchy =
                    (MondrianGuiDef.Hierarchy) value;
                if (hierarchy.relation instanceof MondrianGuiDef.Join) {
                    if (isEmpty(hierarchy.primaryKeyTable)) {
                        if (isEmpty(hierarchy.primaryKey)) {
                            return messages.getString(
                                "schemaTreeCellRenderer.primaryKeyTableAndPrimaryKeyMustBeSet.alert",
                                "PrimaryKeyTable and PrimaryKey must be set for Join");
                        } else {
                            return messages.getString(
                                "schemaTreeCellRenderer.primaryKeyTableMustBeSet.alert",
                                "PrimaryKeyTable must be set for Join");
                        }
                    }
                    if (isEmpty(hierarchy.primaryKey)) {
                        return messages.getString(
                            "schemaTreeCellRenderer.primaryKeyMustBeSet.alert",
                            "PrimaryKey must be set for Join");
                    }
                }

                MondrianGuiDef.Level[] levels = hierarchy.levels;
                if (levels == null || levels.length == 0) {
                    return messages.getString(
                        "schemaTreeCellRenderer.atLeastOneLevelForHierarchy.alert",
                        "At least one Level must be set for Hierarchy");
                }

                // Validates that value in primaryKey exists in Table.
                String schema = null;
                String pkTable = null;
                if (hierarchy.relation instanceof MondrianGuiDef.Join) {
                    String[] schemaAndTable =
                        SchemaExplorer.getTableNameForAlias(
                            hierarchy.relation,
                            hierarchy.primaryKeyTable);
                    schema = schemaAndTable[0];
                    pkTable = schemaAndTable[1];
                } else if (hierarchy.relation instanceof MondrianGuiDef.Table) {
                    final MondrianGuiDef.Table table =
                        (MondrianGuiDef.Table) hierarchy.relation;
                    pkTable = table.name;
                    schema = table.schema;
                }

                if (pkTable != null
                    && !jdbcValidator.isColExists(
                        schema, pkTable, hierarchy.primaryKey))
                {
                    return messages.getFormattedString(
                        "schemaTreeCellRenderer.columnInTableDoesNotExist.alert",
                        "Column {0} defined in field {1} does not exist in table {2}",
                            isEmpty(hierarchy.primaryKey.trim())
                                ? "' '"
                                : hierarchy.primaryKey, "primaryKey",
                            pkTable);
                }

                // Validates against primaryKeyTable name on field when using
                // Table.
                if (hierarchy.relation instanceof MondrianGuiDef.Table) {
                    if (!isEmpty(hierarchy.primaryKeyTable)) {
                        return messages.getString(
                            "schemaTreeCellRenderer.fieldMustBeEmpty",
                            "Table field must be empty");
                    }
                }

                // Validates that the value at primaryKeyTable corresponds to
                // tables in joins.
                String primaryKeyTable = hierarchy.primaryKeyTable;
                if (!isEmpty(primaryKeyTable)
                    && (hierarchy.relation instanceof MondrianGuiDef.Join))
                {
                    TreeSet<String> joinTables = new TreeSet<String>();
                    SchemaExplorer.getTableNamesForJoin(
                        hierarchy.relation, joinTables);
                    if (!joinTables.contains(primaryKeyTable)) {
                        return messages.getString(
                            "schemaTreeCellRenderer.wrongTableValue",
                            "Table value does not correspond to any join");
                    }
                }

                if (!isEmpty(primaryKeyTable)
                    && (hierarchy.relation instanceof MondrianGuiDef.Table))
                {
                    MondrianGuiDef.Table theTable =
                        (MondrianGuiDef.Table) hierarchy.relation;
                    String compareTo =
                        (theTable.alias != null
                         && theTable.alias.trim().length() > 0)
                            ? theTable.alias
                            : theTable.name;
                    if (!primaryKeyTable.equals(compareTo)) {
                        return messages.getString(
                            "schemaTreeCellRenderer.tableDoesNotMatch",
                            "Table value does not correspond to Hierarchy Relation");
                    }
                }

            } else if (value instanceof MondrianGuiDef.NamedSet) {
                final MondrianGuiDef.NamedSet namedSet =
                    (MondrianGuiDef.NamedSet) value;
                if (isEmpty(namedSet.name)) {
                    return nameMustBeSet;
                }
                if (isEmpty(namedSet.formula)
                    && namedSet.formulaElement == null)
                {
                    return messages.getString(
                        "schemaTreeCellRenderer.formulaMustBeSet.alert",
                        "Formula must be set");
                }
            } else if (value instanceof MondrianGuiDef.Formula) {
                final MondrianGuiDef.Formula formula =
                    (MondrianGuiDef.Formula) value;
                if (isEmpty(formula.cdata)) {
                    return messages.getString(
                        "schemaTreeCellRenderer.formulaMustBeSet.alert",
                        "Formula must be set");
                }
            } else if (value instanceof MondrianGuiDef.UserDefinedFunction) {
                final MondrianGuiDef.UserDefinedFunction udf =
                    (MondrianGuiDef.UserDefinedFunction) value;
                if (isEmpty(udf.name)) {
                    return nameMustBeSet;
                }
                if (isEmpty(udf.className)
                    && udf.script == null)
                {
                    return messages.getString(
                        "Either a Class Name or a Script are required",
                        "Class name must be set");
                }
            } else if (value instanceof MondrianGuiDef.MemberFormatter) {
                final MondrianGuiDef.MemberFormatter f =
                    (MondrianGuiDef.MemberFormatter) value;
                if (isEmpty(f.className)
                    && f.script == null)
                {
                    return messages.getString(
                        "schemaTreeCellRenderer.classNameOrScriptRequired.alert",
                        "Either a Class Name or a Script are required");
                }
            } else if (value instanceof MondrianGuiDef.CellFormatter) {
                final MondrianGuiDef.CellFormatter f =
                    (MondrianGuiDef.CellFormatter) value;
                if (isEmpty(f.className)
                    && f.script == null)
                {
                    return messages.getString(
                        "schemaTreeCellRenderer.classNameOrScriptRequired.alert",
                        "Either a Class Name or a Script are required");
                }
            } else if (value instanceof MondrianGuiDef.PropertyFormatter) {
                final MondrianGuiDef.PropertyFormatter f =
                    (MondrianGuiDef.PropertyFormatter) value;
                if (isEmpty(f.className)
                    && f.script == null)
                {
                    return messages.getString(
                        "schemaTreeCellRenderer.classNameOrScriptRequired.alert",
                        "Either a Class Name or a Script are required");
                }
            } else if (value instanceof MondrianGuiDef.CalculatedMember) {
                final MondrianGuiDef.CalculatedMember calculatedMember =
                    (MondrianGuiDef.CalculatedMember) value;
                if (isEmpty(calculatedMember.name)) {
                    return nameMustBeSet;
                }
                if (isEmpty(calculatedMember.dimension)) {
                    return messages.getString(
                        "schemaTreeCellRenderer.dimensionMustBeSet.alert",
                        "Dimension must be set");
                }
                if (isEmpty(calculatedMember.formula)
                    && calculatedMember.formulaElement == null)
                {
                    return messages.getString(
                        "schemaTreeCellRenderer.formulaMustBeSet.alert",
                        "Formula must be set");
                }
            } else if (value instanceof MondrianGuiDef.Join) {
                final MondrianGuiDef.Join join = (MondrianGuiDef.Join) value;
                if (isEmpty(join.leftKey)) {
                    return messages.getString(
                        "schemaTreeCellRenderer.leftKeyMustBeSet.alert",
                        "Left key must be set");
                }
                if (isEmpty(join.rightKey)) {
                    return messages.getString(
                        "schemaTreeCellRenderer.rightKeyMustBeSet.alert",
                        "Right key must be set");
                }
            } else if (value instanceof MondrianGuiDef.Table) {
                final MondrianGuiDef.Table table = (MondrianGuiDef.Table) value;
                String tableName = table.name;
                if (!jdbcValidator.isTableExists(null, tableName)) {
                    return messages.getFormattedString(
                        "schemaTreeCellRenderer.tableDoesNotExist.alert",
                        "Table {0} does not exist in database",
                        tableName);
                }

                String theSchema = table.schema;
                if (!isEmpty(theSchema)
                    && !jdbcValidator.isSchemaExists(theSchema))
                {
                    return messages.getFormattedString(
                        "schemaTreeCellRenderer.schemaDoesNotExist.alert",
                        "Schema {0} does not exist",
                        theSchema);
                }
                if (isEmpty(theSchema) && isSchemaRequired) {
                    return messages.getString(
                        "schemaTreeCellRenderer.schemaMustBeSet.alert",
                        "Schema must be set");
                }
            }
        }

        // Step 2: check validity of all child objects for this value object.
        int childCnt = treeModel.getChildCount(value);
        for (int i = 0; i < childCnt; i++) {
            Object child = treeModel.getChild(value, i);
            String childErrMsg;
            if (child instanceof MondrianGuiDef.Cube) {
                // check current cube child and its children
                childErrMsg = invalid(
                    messages,
                    jdbcValidator,
                    treeModel,
                    tpath,
                    child,
                    (MondrianGuiDef.Cube) child,
                    parentDimension,
                    parentHierarchy,
                    parentLevel,
                    isSchemaRequired);
            } else if (child instanceof MondrianGuiDef.Dimension) {
                // check the current hierarchy and its children
                childErrMsg = invalid(
                    messages,
                    jdbcValidator,
                    treeModel,
                    tpath,
                    child,
                    cube,
                    (MondrianGuiDef.Dimension) child,
                    parentHierarchy,
                    parentLevel,
                    isSchemaRequired);
            } else if (child instanceof MondrianGuiDef.Hierarchy) {
                // special check for cube dimension where foreign key is blank :
                // allowed/not allowed
                if (value instanceof MondrianGuiDef.Dimension
                    && cube != null
                    && ((MondrianGuiDef.Hierarchy) child).relation != null)
                {
                    if (isEmpty(
                            ((MondrianGuiDef.Dimension) value).foreignKey))
                    {
                        // check foreignkey is not blank;
                        // if relation is null, foreignkey must be specified
                        return messages.getString(
                            "schemaTreeCellRenderer.foreignKeyMustBeSet.alert",
                            "Foreign key must be set");
                    }
                }
                // check the current hierarchy and its children
                childErrMsg = invalid(
                    messages,
                    jdbcValidator,
                    treeModel,
                    tpath,
                    child,
                    cube,
                    parentDimension,
                    (MondrianGuiDef.Hierarchy) child,
                    parentLevel,
                    isSchemaRequired);
            } else if (child instanceof MondrianGuiDef.Level) {
                // check the current hierarchy and its children
                childErrMsg = invalid(
                    messages,
                    jdbcValidator,
                    treeModel,
                    tpath,
                    child,
                    cube,
                    parentDimension,
                    parentHierarchy,
                    (MondrianGuiDef.Level) child,
                    isSchemaRequired);
            } else {
                // check this child and all its children objects with incoming
                // cube and hierarchy
                childErrMsg = invalid(
                    messages,
                    jdbcValidator,
                    treeModel,
                    tpath,
                    child,
                    cube,
                    parentDimension,
                    parentHierarchy,
                    parentLevel,
                    isSchemaRequired);
            }

            // If all children are valid then do a special check.
            // Special check for cubes to see if their child dimensions have
            // foreign key set and set the childErrMsg with error msg
            /* === Begin : disabled
            if (childErrMsg == null) {  // all children are valid
                if (child instanceof MondrianGuiDef.Cube) {
                    MondrianGuiDef.Cube c = (MondrianGuiDef.Cube) child;
                    MondrianGuiDef.CubeDimension [] ds = c.dimensions;
                    for (int j=0; j<ds.length; j++) {
                        MondrianGuiDef.CubeDimension d =
                            (MondrianGuiDef.CubeDimension) ds[j];
                        if (d instanceof MondrianGuiDef.DimensionUsage) {
                            continue;   // check the next dimension.
                        }

                        // check foreignkey is not blank
                        if(isEmpty(d.foreignKey)) {
                            childErrMsg = "ForeignKey" + emptyMsg;
                            break;
                        }

                        // database validity check, if database connection is
                        // successful
                        if (jdbcMetaData.getErrMsg() == null) {
                            String foreignKey = d.foreignKey;
                            if (! jdbcMetaData.isColExists(
                                ((MondrianGuiDef.Table) c.fact).schema,
                                 ((MondrianGuiDef.Table) c.fact).name,
                                  foreignKey))
                            {
                                childErrMsg =
                                 "ForeignKey '" + foreignKey +
                                  "' does not exist in fact table.";
                                break;
                            }
                           // check foreignKey is a fact table column
                            if (! allcols.contains(foreignKey)) {
                               childErrMsg =
                                "ForeignKey '" + foreignKey
                                + "' does not exist in fact table.";
                                break;
                            }
             * /
                        }
                    }
                }
            }
             * === End : disabled
             */
            // Now set the final errormsg
            if (childErrMsg != null) {
                String childClassName = child.getClass().getName();
                String simpleName[] = childClassName.split("[$.]", 0);
                String childName;
                try {
                    Field f = child.getClass().getField("name");
                    childName = (String) f.get(child);
                    if (childName == null) {
                        childName = "";
                    }
                    childErrMsg = messages.getFormattedString(
                        "schemaTreeCellRenderer.childErrorMessageWithName.alert",
                        "{0} {1} is invalid",
                        simpleName[simpleName.length - 1],
                        childName);
                } catch (Exception ex) {
                    childErrMsg = messages.getFormattedString(
                        "schemaTreeCellRenderer.childErrorExceptionMessage.alert",
                        "{0} is invalid",
                        simpleName[simpleName.length - 1]);
                }
                return childErrMsg;
            }
        }

        return null;
    }

    /**
     * Returns whether an object is null or the empty string.
     *
     * @param v Object
     * @return Whether object is null or the empty string
     */
    public static boolean isEmpty(String v) {
        return (v == null) || v.equals("");
    }

    /**
     * Returns whether an array is null or empty
     *
     * @param arr array
     * @return whether the array is null or empty
     */
    public static boolean isEmpty(Object[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * Validates a column, and returns an error message if it is invalid.
     *
     * @param column Column
     * @param fieldName Field name
     * @param messages Message provider
     * @param level  Level
     * @param jdbcValidator JDBC validator
     * @param cube Cube
     * @param parentHierarchy Hierarchy
     * @return Error message if invalid, null if valid
     */
    private static String validateColumn(
        String column,
        String fieldName,
        Messages messages,
        MondrianGuiDef.Level level,
        JdbcValidator jdbcValidator,
        MondrianGuiDef.Cube cube,
        MondrianGuiDef.Hierarchy parentHierarchy)
    {
        if (!isEmpty(column)) {
            // database validity check, if database connection is successful
            if (jdbcValidator.isInitialized()) {
                // specified table for level's column
                String table = level.table;
                // If table has been changed in join then sets the table value
                // to null to cause "tableMustBeSet" validation fail.
                if (!isEmpty(table)
                    && parentHierarchy != null
                    && parentHierarchy.relation instanceof MondrianGuiDef.Join)
                {
                    TreeSet<String> joinTables = new TreeSet<String>();
                    SchemaExplorer.getTableNamesForJoin(
                        parentHierarchy.relation, joinTables);
                    if (!joinTables.contains(table)) {
                        return messages.getString(
                            "schemaTreeCellRenderer.wrongTableValue",
                            "Table value does not correspond to any join");
                    }
                }

                if (!isEmpty(table)
                    && parentHierarchy != null
                    && parentHierarchy.relation instanceof MondrianGuiDef.Table)
                {
                    final MondrianGuiDef.Table parentTable =
                        (MondrianGuiDef.Table) parentHierarchy.relation;
                    MondrianGuiDef.Table theTable = parentTable;
                    String compareTo =
                        (theTable.alias != null
                         && theTable.alias.trim().length() > 0)
                        ? theTable.alias
                        : theTable.name;
                    if (!table.equals(compareTo)) {
                        return messages.getString(
                            "schemaTreeCellRenderer.tableDoesNotMatch",
                            "Table value does not correspond to Hierarchy Relation");
                    }
                }

                if (!isEmpty(table)
                    && parentHierarchy != null
                    && parentHierarchy.relation instanceof MondrianGuiDef.View)
                {
                    return messages.getString(
                        "schemaTreeCellRenderer.noTableForView",
                        "Table for column cannot be set in View");
                }

                if (isEmpty(table)) {
                    if (parentHierarchy != null) {
                        if (parentHierarchy.relation == null
                            && cube != null)
                        {
                            // case of degenerate dimension within cube,
                            // hierarchy table not specified
                            if (!jdbcValidator.isColExists(
                                    ((MondrianGuiDef.Table) cube.fact).schema,
                                    ((MondrianGuiDef.Table) cube.fact).name,
                                    column))
                            {
                                return messages.getFormattedString(
                                    "schemaTreeCellRenderer.degenDimensionColumnDoesNotExist.alert",
                                    "Degenerate dimension validation check - Column {0} does not exist in fact table",
                                    column);
                            }
                        } else if (parentHierarchy.relation
                                   instanceof MondrianGuiDef.Table)
                        {
                            final MondrianGuiDef.Table parentTable =
                                (MondrianGuiDef.Table) parentHierarchy.relation;
                            if (!jdbcValidator.isColExists(
                                    parentTable.schema,
                                    parentTable.name,
                                    column))
                            {
                                return messages.getFormattedString(
                                    "schemaTreeCellRenderer.columnInTableDoesNotExist.alert",
                                    "Column {0} defined in field {1} does not exist in table {2}",
                                    isEmpty(column.trim())
                                        ? "' '"
                                        : column,
                                    fieldName,
                                    parentTable.name);
                            }
                        } else if (parentHierarchy.relation
                            instanceof MondrianGuiDef.Join)
                        {
                            // relation is join, table should be specified
                            return messages.getString(
                                "schemaTreeCellRenderer.tableMustBeSet.alert",
                                "Table must be set");
                        }
                    }
                } else {
                    String schema = null;
                    // if using Joins then gets the table name for isColExists
                    // validation.
                    if (parentHierarchy != null
                        && parentHierarchy.relation
                        instanceof MondrianGuiDef.Join)
                    {
                        String[] schemaAndTable =
                            SchemaExplorer.getTableNameForAlias(
                                parentHierarchy.relation,
                                table);
                        schema = schemaAndTable[0];
                        table = schemaAndTable[1];
                    }
                    if (!jdbcValidator.isColExists(schema, table, column)) {
                        return messages.getFormattedString(
                            "schemaTreeCellRenderer.columnInTableDoesNotExist.alert",
                            "Column {0} defined in field {1} does not exist in table {2}",
                            isEmpty(column.trim())
                                ? "' '"
                                : column,
                            fieldName,
                            table);
                    }
                }
            }
        }
        return null;
    }

}

// End ValidationUtils.java
