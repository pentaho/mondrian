package mondrian.gui.validate;

import java.lang.reflect.Field;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import mondrian.gui.MondrianGuiDef;
import mondrian.gui.SchemaExplorer;

/**
 * Validates a <code>MondrianGuiDef</code>. Class contains <code>invalid</code>
 * method formerly from <code>mondrian.gui.SchemaTreeCellRenderer</code>.
 *
 * @author mlowery
 */
public class ValidationUtils {

    private static final Logger LOGGER = Logger.getLogger(ValidationUtils.class);

    static String[] DEF_LEVEL = {"column", "nameColumn", "parentColumn", "ordinalColumn", "captionColumn" };

    public static String invalid(Messages messages, JDBCValidator jdbcValidator, TreeModel treeModel,
            TreeModelPath tpath, Object value, Object icube, Object iparentDimension, Object iparentHierarchy,
            Object iparentLevel, boolean isSchemaRequired) {
        //String errMsg = null;
        String nameMustBeSet = messages.getString("schemaTreeCellRenderer.nameMustBeSet.alert", "Name must be set");

        MondrianGuiDef.Cube cube = (MondrianGuiDef.Cube) icube; //null;
        MondrianGuiDef.Dimension parentDimension = (MondrianGuiDef.Dimension) iparentDimension; // null // used only by level to check for leveltype value
        MondrianGuiDef.Hierarchy parentHierarchy = (MondrianGuiDef.Hierarchy) iparentHierarchy; //null; // used only by level validation
        MondrianGuiDef.Level parentLevel = (MondrianGuiDef.Level) iparentLevel; // null // used only by property validation

        if (!tpath.isEmpty()) {
            int pathcount = tpath.getPathCount();
            for (int i = 0; i < pathcount
                    && (cube == null || parentDimension == null || parentHierarchy == null || parentLevel == null); i++) {
                if (tpath.getPathComponent(i) instanceof MondrianGuiDef.Cube && cube == null) {
                    cube = (MondrianGuiDef.Cube) tpath.getPathComponent(i);
                }
                if (tpath.getPathComponent(i) instanceof MondrianGuiDef.Dimension && parentDimension == null) {
                    parentDimension = (MondrianGuiDef.Dimension) tpath.getPathComponent(i);
                }
                if (tpath.getPathComponent(i) instanceof MondrianGuiDef.Hierarchy && parentHierarchy == null) {
                    parentHierarchy = (MondrianGuiDef.Hierarchy) tpath.getPathComponent(i);
                }
                if (tpath.getPathComponent(i) instanceof MondrianGuiDef.Level && parentLevel == null) {
                    parentLevel = (MondrianGuiDef.Level) tpath.getPathComponent(i);
                }
            }
        }

        //Step 1: check validity of this value object
        if (value instanceof MondrianGuiDef.Schema) {
            if (isEmpty(((MondrianGuiDef.Schema) value).name)) {
                return nameMustBeSet;
            }
        } else if (value instanceof MondrianGuiDef.VirtualCube) {
            if (isEmpty(((MondrianGuiDef.VirtualCube) value).name)) {
                return nameMustBeSet;
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
            if (((MondrianGuiDef.Cube) value).fact == null
                    ||
                    ((cubeVal.fact instanceof MondrianGuiDef.Table) &&
                    isEmpty(((MondrianGuiDef.Table) cubeVal.fact).name)) //check name is not blank
                    ||
                    ((cubeVal.fact instanceof MondrianGuiDef.View) &&
                    isEmpty(((MondrianGuiDef.View) cubeVal.fact).alias))) //check alias is not blank
            {
                return messages.getString("schemaTreeCellRenderer.factNameMustBeSet.alert", "Fact name must be set");
            }

            // database validity check, if database connection is successful
            if (jdbcValidator.isInitialized()) {
                // Vector allTables = jdbcMetaData.getAllTables(((MondrianGuiDef.Table) cubeVal.fact).schema);
                if (((MondrianGuiDef.Cube) value).fact instanceof MondrianGuiDef.Table) {
                    String schemaName = ((MondrianGuiDef.Table) cubeVal.fact).schema;
                    String factTable = ((MondrianGuiDef.Table) cubeVal.fact).name;
                    if (!jdbcValidator.isTableExists(schemaName, factTable)) {
                        return messages.getFormattedString("schemaTreeCellRenderer.factTableDoesNotExist.alert",
                                "Fact table {0} does not exist in database {1}", new String[] { factTable,
                                        ((schemaName == null || schemaName.equals("")) ? "." : "schema " + schemaName) });
                    }
                }
            }
        } else if (value instanceof MondrianGuiDef.CubeDimension) {
            if (isEmpty(((MondrianGuiDef.CubeDimension) value).name)) { //check name is not blank
                return nameMustBeSet;
            }
            if (value instanceof MondrianGuiDef.DimensionUsage) {
                if (isEmpty(((MondrianGuiDef.DimensionUsage) value).source)) { //check source is not blank
                    return messages.getString("schemaTreeCellRenderer.sourceMustBeSet.alert", "Source must be set");
                }
                // check source is name of one of dimensions of schema (shared dimensions)
                MondrianGuiDef.Schema s = (MondrianGuiDef.Schema) treeModel.getRoot();
                MondrianGuiDef.Dimension ds[] = s.dimensions;
                String sourcename = ((MondrianGuiDef.DimensionUsage) value).source;
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
                            "Source {0} does not exist as Shared Dimension of Schema", new String[] { sourcename });
                }
            }
            if (value instanceof MondrianGuiDef.Dimension && cube != null) {
                /* //foreignkey can be blank if  hierarchy relation is null
                 * // this check moved to child hierarchies relation check below
                 */
                if (!isEmpty(((MondrianGuiDef.Dimension) value).foreignKey)) {
                    // database validity check, if database connection is successful
                    if (jdbcValidator.isInitialized()) {
                        // TODO: Need to add validation for Views
                        if (cube.fact instanceof MondrianGuiDef.Table) {
                            String foreignKey = ((MondrianGuiDef.Dimension) value).foreignKey;
                            if (!jdbcValidator.isColExists(((MondrianGuiDef.Table) cube.fact).schema,
                                    ((MondrianGuiDef.Table) cube.fact).name, foreignKey)) {
                                return messages.getFormattedString("schemaTreeCellRenderer.foreignKeyDoesNotExist.alert",
                                        "foreignKey {0} does not exist in fact table", new String[] { foreignKey });
                            }
                        }
                    }
                }
            }
        } else if (value instanceof MondrianGuiDef.Level) {
            /*
            // check 'column' exists in 'table' if table is specified otherwise :: case of join
            // it should exist in relation table if it is specified otherwise   :: case of table
            // it should exist in fact table  :: case of degenerate dimension where dimension columns exist in fact table
            // and there is no separate table
             */
            MondrianGuiDef.Level l = (MondrianGuiDef.Level) value;
            if (!isEmpty(l.levelType)) {
                // empty leveltype is treated as default value of "Regular"" which is ok with standard/time dimension
                if (parentDimension != null) {
                    if ((isEmpty(parentDimension.type) || parentDimension.type.equals("StandardDimension"))
                            && !isEmpty(l.levelType)
                            && (!l.levelType.equals(MondrianGuiDef.Level._levelType_values[0]))) {
                        // if dimension type is 'standard' then leveltype should be 'regular'
                        return messages.getFormattedString("schemaTreeCellRenderer.levelUsedOnlyInTimeDimension.alert",
                                "levelType {0} can only be used with a TimeDimension", new String[] { l.levelType });
                    } else if (!isEmpty(parentDimension.type) && (parentDimension.type.equals("TimeDimension"))
                            && !isEmpty(l.levelType) && (l.levelType.equals(MondrianGuiDef.Level._levelType_values[0]))) {
                        // if dimension type is 'time' then leveltype value could be 'timeyears', 'timedays' etc'
                        return messages
                                .getFormattedString("schemaTreeCellRenderer.levelUsedOnlyInStandardDimension.alert",
                                        "levelType {0} can only be used with a StandardDimension",
                                        new String[] { l.levelType });
                    }
                }
            }
            String column = l.column; // check level's column is in fact table'
            if (isEmpty(column)) {
                if (l.properties == null || l.properties.length == 0) {
                    return messages.getString("schemaTreeCellRenderer.columnMustBeSet.alert", "Column must be set");
                }
            } else {
                //EC: Enforces validation for all column types against invalid value.
                String theMessage = null;
                try {
                     for (int i = 0; i < DEF_LEVEL.length; i ++) {
                        Field theField = l.getClass().getDeclaredField(DEF_LEVEL[i]);
                        column = (String) theField.get(l);
                        theMessage = validateColumn(column, DEF_LEVEL[i], messages, l, jdbcValidator, cube, parentHierarchy);
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
            /*
            // check 'column' exists in 'table' if [level table] is specified otherwise :: case of join
            // it should exist in [hierarchy relation table] if it is specified otherwise   :: case of table
            // it should exist in [fact table]  :: case of degenerate dimension where dimension columns exist in fact table
            // and there is no separate table
             */
            MondrianGuiDef.Property p = (MondrianGuiDef.Property) value;
            String column = p.column; // check property's column is in table'
            if (isEmpty(column)) {
                return messages.getString("schemaTreeCellRenderer.columnMustBeSet.alert", "Column must be set");
            }
            // database validity check, if database connection is successful
            if (jdbcValidator.isInitialized()) {
                String table = null;
                if (parentLevel != null) {
                    table = parentLevel.table; // specified table for level's column'
                }
                if (isEmpty(table)) {
                    if (parentHierarchy != null) {
                        if (parentHierarchy.relation == null && cube != null) { // case of degenerate dimension within cube, hierarchy table not specified
                            if (!jdbcValidator.isColExists(((MondrianGuiDef.Table) cube.fact).schema,
                                    ((MondrianGuiDef.Table) cube.fact).name, column)) {
                                return messages
                                        .getFormattedString(
                                                "schemaTreeCellRenderer.degenDimensionColumnDoesNotExist.alert",
                                                "Degenerate dimension validation check - Column {0} does not exist in fact table",
                                                new String[] { column });
                            }
                        } else if (parentHierarchy.relation instanceof MondrianGuiDef.Table) {
                            if (!jdbcValidator.isColExists(((MondrianGuiDef.Table) parentHierarchy.relation).schema,
                                    ((MondrianGuiDef.Table) parentHierarchy.relation).name, column)) {
                                return messages.getFormattedString(
                                        "schemaTreeCellRenderer.columnInDimensionDoesNotExist.alert",
                                        "Column {0} does not exist in Dimension table",
                                        new String[] { ((MondrianGuiDef.Table) parentHierarchy.relation).name });
                            }
                        }
                    }
                } else {
                    if (!jdbcValidator.isColExists(null, table, column)) {
                        return messages.getFormattedString(
                                "schemaTreeCellRenderer.columnInDimensionDoesNotExist.alert",
                                "Column {0} does not exist in Level table {1}", new String[] { column, table });
                    }
                }
            }
        } else if (value instanceof MondrianGuiDef.Measure) {
            if (isEmpty(((MondrianGuiDef.Measure) value).name)) {
                return nameMustBeSet;
            }
            if (isEmpty(((MondrianGuiDef.Measure) value).aggregator)) {
                return messages.getString("schemaTreeCellRenderer.aggregatorMustBeSet.alert", "Aggregator must be set");
            }
            if (((MondrianGuiDef.Measure) value).measureExp != null) {
                // Measure expressions are OK
            } else if (isEmpty(((MondrianGuiDef.Measure) value).column)) {
                return messages.getString("schemaTreeCellRenderer.columnMustBeSet.alert", "Column must be set");
            } else if (cube != null && cube.fact != null) {
                // database validity check, if database connection is successful
                if (jdbcValidator.isInitialized()) {
                    //Vector allcols  = jdbcMetaData.getAllColumns(((MondrianGuiDef.Table) cube.fact).schema, ((MondrianGuiDef.Table) cube.fact).name);

                    String column = ((MondrianGuiDef.Measure) value).column;
                    if (jdbcValidator.isColExists(((MondrianGuiDef.Table) cube.fact).schema,
                            ((MondrianGuiDef.Table) cube.fact).name, column)) {
                        /* disabled check that the column value should exist in table because column could also be an expression
                        if (! jdbcMetaData.isColExists(((MondrianGuiDef.Table) cube.fact).schema, ((MondrianGuiDef.Table) cube.fact).name, column)) {
                        return "Column '"+column+"' does not exist in fact table.";
                        }
                         */
                        /*
                        if (! allcols.contains(column))        // check foreignKey is a fact table column
                        {   return "Column '"+column+"' does not exist in fact table.";}
                         */
                        // check for aggregator type only if column exists in table
                        // check if aggregator selected is valid on the data type of the column selected.
                        int colType = jdbcValidator.getColumnDataType(((MondrianGuiDef.Table) cube.fact).schema,
                                ((MondrianGuiDef.Table) cube.fact).name, ((MondrianGuiDef.Measure) value).column);
                        // colType of 2, 4,5, 7,8 is numeric types whereas 1, 12 are char varchar string and 91 is date type
                        int agIndex = -1;
                        if ("sum".equals(((MondrianGuiDef.Measure) value).aggregator)
                                || "avg".equals(((MondrianGuiDef.Measure) value).aggregator)) {
                            agIndex = 0; // aggregator = sum or avg, column should be numeric
                        }
                        if (!(agIndex == -1 || (colType >= 2 && colType <= 8))) {
                            return messages.getFormattedString(
                                    "schemaTreeCellRenderer.aggregatorNotValidForColumn.alert",
                                    "Aggregator {0} is not valid for the data type of the column {1}", new String[] {
                                            ((MondrianGuiDef.Measure) value).aggregator,
                                            ((MondrianGuiDef.Measure) value).column });
                        }
                    }
                }
            }
        } else if (value instanceof MondrianGuiDef.Hierarchy) {
            if (((MondrianGuiDef.Hierarchy) value).relation instanceof MondrianGuiDef.Join) {
                if (isEmpty(((MondrianGuiDef.Hierarchy) value).primaryKeyTable)) {
                    if (isEmpty(((MondrianGuiDef.Hierarchy) value).primaryKey)) {
                        return messages.getString("schemaTreeCellRenderer.primaryKeyTableAndPrimaryKeyMustBeSet.alert",
                                "PrimaryKeyTable and PrimaryKey must be set for Join");
                    } else {
                        return messages.getString("schemaTreeCellRenderer.primaryKeyTableMustBeSet.alert",
                                "PrimaryKeyTable must be set for Join");
                    }
                }
                if (isEmpty(((MondrianGuiDef.Hierarchy) value).primaryKey)) {
                    return messages.getString("schemaTreeCellRenderer.primaryKeyMustBeSet.alert",
                            "PrimaryKey must be set for Join");
                }
            }

            MondrianGuiDef.Hierarchy hierarchy = ((MondrianGuiDef.Hierarchy) value);
            MondrianGuiDef.Level[] levels = hierarchy.levels;
            if (levels == null || levels.length == 0) {
                return messages.getFormattedString(
                        "schemaExplorer.hierarchyElementLevels.title",
                        "Hierarchy {0} must have levels", new String[] { hierarchy.name});
            }

            // Validates that value in primaryKey exists in Table.
            String pkTable = null;
            if (hierarchy.relation instanceof MondrianGuiDef.Join) {
                pkTable = SchemaExplorer.getTableNameForAlias(hierarchy.relation, hierarchy.primaryKeyTable);
            } else if (hierarchy.relation instanceof MondrianGuiDef.Table) {
                pkTable = ((MondrianGuiDef.Table) hierarchy.relation).name;
            }
            if (!jdbcValidator.isColExists(null, pkTable, hierarchy.primaryKey)) {
                return messages.getFormattedString(
                        "schemaTreeCellRenderer.columnInTableDoesNotExist.alert",
                        "Column {0} defined in field {1} does not exist in table {2}",
                        new String[] { isEmpty(hierarchy.primaryKey.trim()) ? "' '" : hierarchy.primaryKey, "primaryKey", pkTable });
            }

            // Validates against primaryKeyTable name on field when using Table.
            if (hierarchy.relation instanceof MondrianGuiDef.Table) {
                if (!isEmpty(hierarchy.primaryKeyTable)) {
                    return messages.getString("schemaTreeCellRenderer.fieldMustBeEmpty","Table field must be empty");
                }
            }

            // Validates that the value at primaryKeyTable corresponds to tables in joins.
            String primaryKeyTable = hierarchy.primaryKeyTable;
            if (!isEmpty(primaryKeyTable) && (hierarchy.relation instanceof MondrianGuiDef.Join)) {
                TreeSet<String> joinTables = new TreeSet<String>();
                SchemaExplorer.getTableNamesForJoin(hierarchy.relation, joinTables);
                if (!joinTables.contains(primaryKeyTable)) {
                    return messages.getString("schemaTreeCellRenderer.wrongTableValue",  "Table value does not correspond to any join");
                }
            }

            if (!isEmpty(primaryKeyTable) &&
                (hierarchy.relation instanceof MondrianGuiDef.Table)) {
                MondrianGuiDef.Table theTable =
                    (MondrianGuiDef.Table)hierarchy.relation;
                String compareTo = (theTable.alias != null &&
                        theTable.alias.trim().length() > 0) ?
                        theTable.alias : theTable.name;
                if (!primaryKeyTable.equals(compareTo)) {
                    return messages.getString(
                        "schemaTreeCellRenderer.tableDoesNotMatch",
                        "Table value does not correspond to Hierarchy Relation");
                }
            }

        } else if (value instanceof MondrianGuiDef.NamedSet) {
            if (isEmpty(((MondrianGuiDef.NamedSet) value).name)) {
                return nameMustBeSet;
            }
            if (isEmpty(((MondrianGuiDef.NamedSet) value).formula)) {
                return messages.getString("schemaTreeCellRenderer.formulaMustBeSet.alert", "Formula must be set");
            }
        } else if (value instanceof MondrianGuiDef.UserDefinedFunction) {
            if (isEmpty(((MondrianGuiDef.UserDefinedFunction) value).name)) {
                return nameMustBeSet;
            }
            if (isEmpty(((MondrianGuiDef.UserDefinedFunction) value).className)) {
                return messages.getString("schemaTreeCellRenderer.classNameMustBeSet.alert", "Class name must be set");
            }
        } else if (value instanceof MondrianGuiDef.CalculatedMember) {
            if (isEmpty(((MondrianGuiDef.CalculatedMember) value).name)) {
                return nameMustBeSet;
            }
            if (isEmpty(((MondrianGuiDef.CalculatedMember) value).dimension)) {
                return messages.getString("schemaTreeCellRenderer.dimensionMustBeSet.alert", "Dimension must be set");
            }
        } else if (value instanceof MondrianGuiDef.Join) {
            if (isEmpty(((MondrianGuiDef.Join) value).leftKey)) {
                return messages.getString("schemaTreeCellRenderer.leftKeyMustBeSet.alert", "Left key must be set");
            }
            if (isEmpty(((MondrianGuiDef.Join) value).rightKey)) {
                return messages.getString("schemaTreeCellRenderer.rightKeyMustBeSet.alert", "Right key must be set");
            }
        } else if (value instanceof MondrianGuiDef.Table) {
            String tableName = ((MondrianGuiDef.Table)value).name;
            if (!jdbcValidator.isTableExists(null, tableName)) {
                return messages.getFormattedString("schemaTreeCellRenderer.tableDoesNotExist.alert",
                        "Table {0} does not exist in database", new String[] { tableName });
            }

            String theSchema = ((MondrianGuiDef.Table)value).schema;
            if (!isEmpty(theSchema) && !jdbcValidator.isSchemaExists(theSchema)) {
                return messages.getFormattedString(
                        "schemaTreeCellRenderer.schemaDoesNotExist.alert", "Schema {0} does not exist",
                        new String[] { theSchema });
            }
            if (isEmpty(theSchema) && isSchemaRequired) {
                return messages.getString("schemaTreeCellRenderer.schemaMustBeSet.alert", "Schema must be set");
            }
        }

        // Step 2: check validity of all child objects for this value object.
        int childCnt = treeModel.getChildCount(value);
        for (int i = 0; i < childCnt; i++) {
            Object child = treeModel.getChild(value, i);
            String childErrMsg;
            if (child instanceof MondrianGuiDef.Cube) {
                childErrMsg = invalid(messages, jdbcValidator, treeModel, tpath, child, child, parentDimension,
                        parentHierarchy, parentLevel, isSchemaRequired); //check current cube child and its children
            } else if (child instanceof MondrianGuiDef.Dimension) {
                childErrMsg = invalid(messages, jdbcValidator, treeModel, tpath, child, cube, child, parentHierarchy,
                        parentLevel, isSchemaRequired); //check the current hierarchy and its children
            } else if (child instanceof MondrianGuiDef.Hierarchy) {
                // special check for cube dimension where foreign key is blank : allowed /not allowed
                if (value instanceof MondrianGuiDef.Dimension && cube != null
                        && ((MondrianGuiDef.Hierarchy) child).relation != null) {
                    if (isEmpty(((MondrianGuiDef.Dimension) value).foreignKey)) { //check foreignkey is not blank
                        // if relation is null, foreignkey must be specified
                        return messages.getString("schemaTreeCellRenderer.foreignKeyMustBeSet.alert",
                                "Foreign key must be set");
                    }
                }
                childErrMsg = invalid(messages, jdbcValidator, treeModel, tpath, child, cube, parentDimension, child,
                        parentLevel, isSchemaRequired); //check the current hierarchy and its children
            } else if (child instanceof MondrianGuiDef.Level) {
                childErrMsg = invalid(messages, jdbcValidator, treeModel, tpath, child, cube, parentDimension,
                        parentHierarchy, child, isSchemaRequired); //check the current hierarchy and its children
            } else {
                childErrMsg = invalid(messages, jdbcValidator, treeModel, tpath, child, cube, parentDimension,
                        parentHierarchy, parentLevel, isSchemaRequired); //check this child and all its children objects with incoming cube and hierarchy
            }

            /* If all children are valid then do a special check.
             * Special check for cubes to see if their child dimensions have foreign key set and set the childErrMsg with error msg
             */
            /* === Begin : disabled
            if (childErrMsg == null) {  // all children are valid
                if (child instanceof MondrianGuiDef.Cube) {
                    MondrianGuiDef.Cube c = (MondrianGuiDef.Cube) child;
                    MondrianGuiDef.CubeDimension [] ds = c.dimensions;
                    for (int j=0; j<ds.length; j++) {
                        MondrianGuiDef.CubeDimension d = (MondrianGuiDef.CubeDimension) ds[j];
                        if (d instanceof MondrianGuiDef.DimensionUsage) {
                            continue;   // check the next dimension.
                        }

                        if(isEmpty(d.foreignKey))    //check foreignkey is not blank
                        { childErrMsg = "ForeignKey" + emptyMsg;
                          break;
                        }

                        // database validity check, if database connection is successful
                        if (jdbcMetaData.getErrMsg() == null) {
                            //Vector allcols  = jdbcMetaData.getAllColumns(((MondrianGuiDef.Table) c.fact).schema, ((MondrianGuiDef.Table) c.fact).name);
                            String foreignKey = d.foreignKey;
                            if (! jdbcMetaData.isColExists(((MondrianGuiDef.Table) c.fact).schema, ((MondrianGuiDef.Table) c.fact).name, foreignKey)) {
                                childErrMsg = "ForeignKey '"+foreignKey+"' does not exist in fact table.";
                                break;
                            }
                            /*
                            if (! allcols.contains(foreignKey))        // check foreignKey is a fact table column
                            {   childErrMsg = "ForeignKey '"+foreignKey+"' does not exist in fact table.";
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
                    childErrMsg = messages.getFormattedString("schemaTreeCellRenderer.childErrorMessageWithName.alert",
                            "{0} {1} is invalid", new String[] { simpleName[simpleName.length - 1], childName });
                } catch (Exception ex) {
                    childErrMsg = messages.getFormattedString(
                            "schemaTreeCellRenderer.childErrorExceptionMessage.alert", "{0} is invalid",
                            new String[] { simpleName[simpleName.length - 1] });
                }
                return childErrMsg;
            }
        }

        return null;
    }

    private static boolean isEmpty(Object v) {
        if ((v == null) || v.equals("")) {
            return true;
        } else {
            return false;
        }
    }

    private static String validateColumn(
        String column,
        String fieldName,
        Messages messages,
        MondrianGuiDef.Level l,
        JDBCValidator jdbcValidator,
        MondrianGuiDef.Cube cube,
        MondrianGuiDef.Hierarchy parentHierarchy)
    {
        /* // level column may be blank, if it has properties defined with cols.
        if (isEmpty(column)) {
            return "Column" + emptyMsg;
        }
         */
        if (!isEmpty(column)) {
            // database validity check, if database connection is successful
            if (jdbcValidator.isInitialized()) {
                String table = l.table; // specified table for level's column'
                //EC: If table has been changed in join then sets the table value to null to cause "tableMustBeSet" validation fail.
                if (!isEmpty(table) && (parentHierarchy != null && parentHierarchy.relation instanceof MondrianGuiDef.Join)) {
                    TreeSet<String> joinTables = new TreeSet<String>();
                    SchemaExplorer.getTableNamesForJoin(parentHierarchy.relation, joinTables);
                    if (!joinTables.contains(table)) {
                        return messages.getString("schemaTreeCellRenderer.wrongTableValue",  "Table value does not correspond to any join");
                    }
                }

                if (!isEmpty(table) && (parentHierarchy != null &&
                    parentHierarchy.relation instanceof MondrianGuiDef.Table)) {
                    MondrianGuiDef.Table theTable =
                        (MondrianGuiDef.Table)parentHierarchy.relation;
                    String compareTo = (theTable.alias != null &&
                            theTable.alias.trim().length() > 0) ?
                            theTable.alias : theTable.name;
                    if (!table.equals(compareTo)) {
                        return messages.getString(
                            "schemaTreeCellRenderer.tableDoesNotMatch",
                            "Table value does not correspond to Hierarchy Relation");
                    }
                }

                if (isEmpty(table)) {
                    if (parentHierarchy != null) {
                        if (parentHierarchy.relation == null && cube != null) { // case of degenerate dimension within cube, hierarchy table not specified
                            if (!jdbcValidator.isColExists(((MondrianGuiDef.Table) cube.fact).schema,
                                    ((MondrianGuiDef.Table) cube.fact).name, column)) {
                                return messages
                                        .getFormattedString(
                                                "schemaTreeCellRenderer.degenDimensionColumnDoesNotExist.alert",
                                                "Degenerate dimension validation check - Column {0} does not exist in fact table",
                                                new String[] { column });
                            }
                        } else if (parentHierarchy.relation instanceof MondrianGuiDef.Table) {
                            if (!jdbcValidator.isColExists(
                                    ((MondrianGuiDef.Table) parentHierarchy.relation).schema,
                                    ((MondrianGuiDef.Table) parentHierarchy.relation).name, column)) {
                                return messages.getFormattedString(
                                        "schemaTreeCellRenderer.columnInTableDoesNotExist.alert",
                                        "Column {0} defined in field {1} does not exist in table {2}", new String[] { isEmpty(column.trim()) ? "' '" : column, fieldName, ((MondrianGuiDef.Table) parentHierarchy.relation).name });
                            }
                        } else if (parentHierarchy.relation instanceof MondrianGuiDef.Join) { // relation is join, table should be specified
                            return messages.getString("schemaTreeCellRenderer.tableMustBeSet.alert",
                                    "Table must be set");
                        }
                    }
                } else {
                    //EC: if using Joins then gets the table name for isColExists validation.
                    if (parentHierarchy != null && parentHierarchy.relation instanceof MondrianGuiDef.Join) {
                       table = SchemaExplorer.getTableNameForAlias(parentHierarchy.relation, table);
                    }
                    if (!jdbcValidator.isColExists(null, table, column)) {
                        return messages.getFormattedString(
                                "schemaTreeCellRenderer.columnInTableDoesNotExist.alert",
                                "Column {0} defined in field {1} does not exist in table {2}", new String[] { isEmpty(column.trim()) ? "' '" : column, fieldName, table });
                    }
                }
            }
        }
        return null;
    }

}

// End ValidationUtils.java
