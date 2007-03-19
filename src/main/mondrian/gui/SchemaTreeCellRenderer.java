/*
/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2007 Julian Hyde and others
// Copyright (C) 2006-2007 CINCOM SYSTEMS, INC.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.gui;

import javax.swing.tree.TreePath;

import javax.swing.*;
import java.awt.*;
import java.lang.reflect.Field;
import java.util.ResourceBundle;

import org.eigenbase.xom.ElementDef;

/**
 * Render an entry for the tree.
 *
 * @author  sean
 * @version $Id$
 */
public class SchemaTreeCellRenderer extends javax.swing.tree.DefaultTreeCellRenderer {

    private final ClassLoader myClassLoader;
    public boolean invalidFlag;
    private JDBCMetaData jdbcMetaData;

    /** Creates a new instance of SchemaTreeCellRenderer */

    public SchemaTreeCellRenderer(JDBCMetaData jdbcMetaData  ){
        this();
        this.jdbcMetaData = jdbcMetaData;
    }
    public SchemaTreeCellRenderer() {
        super();
        myClassLoader = this.getClass().getClassLoader();

    }

    public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf, int row, boolean hasFocus) {
        super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);
        ResourceBundle resources = ResourceBundle.getBundle("mondrian.gui.resources.gui");

        invalidFlag = isInvalid(tree, value, row);

        this.setPreferredSize(null); // This allows the layout mgr to calculate the pref size of renderer.
        if (value instanceof MondrianGuiDef.Cube) {
            setText(invalidFlag, ((MondrianGuiDef.Cube) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("cube"))));
        } else if (value instanceof MondrianGuiDef.Column) {
            setText(invalidFlag, ((MondrianGuiDef.Column) value).name);
        } else if (value instanceof MondrianGuiDef.Dimension) {
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("dimension"))));
            setText(invalidFlag, ((MondrianGuiDef.CubeDimension) value).name);
            /* Do not remove this line.
             * This sets the preferred width of tree cell displaying dimension name.
             * This resolves the ambiguous problem of last char or last word truncated from dimension name in the tree cell.
             * This problem was there with only Dimension objects, while all other objects display their names
             * without any truncation of characters. Therefore, we have to force the setting of preferred width to desired width
             * so that characters do not truncate from dimension name.
             * Along with this the preferred size of other objects should be set to null, so that the layout mgr can calculate the
             * preferred width in case of other objects.
             */
            this.setPreferredSize(new java.awt.Dimension(this.getPreferredSize().width+1, 25)); //Do not remove this
        } else if (value instanceof MondrianGuiDef.DimensionUsage)      {
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("dimensionUsage"))));
            setText(invalidFlag, ((MondrianGuiDef.CubeDimension) value).name);
        } else if (value instanceof MondrianGuiDef.KeyExpression) {
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("key"))));
            setText("Key Expression");
        } else if (value instanceof MondrianGuiDef.NameExpression) {
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("name"))));
            setText("Name Expression");
        } else if (value instanceof MondrianGuiDef.OrdinalExpression) {
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("ordinal"))));
            setText("Ordinal Expression");
        } else if (value instanceof MondrianGuiDef.ParentExpression) {
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("parent"))));
            setText("Parent Expression");
        } else if (value instanceof MondrianGuiDef.Expression) {
            setText("Expression");
        } else if (value instanceof MondrianGuiDef.ExpressionView) {
            setText("ExpressionView");
        } else if (value instanceof MondrianGuiDef.Hierarchy) {
            setText(invalidFlag, "Hierarchy");
            //setText(((MondrianGuiDef.Hierarchy) value).name);    // hierarchies do not have names
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("hierarchy"))));
            this.setPreferredSize(new java.awt.Dimension(this.getPreferredSize().width+1, 25)); //Do not remove this

//        } else if (value instanceof MondrianGuiDef.Relation) {
        } else if ((value instanceof MondrianGuiDef.Relation) ||
                (value instanceof MondrianGuiDef.Table)) {
            TreePath tpath = tree.getPathForRow(row);
            String prefix = "";
            if (tpath != null) {
                TreePath parentpath = tpath.getParentPath();
                if (parentpath != null) {
                    Object parent = parentpath.getLastPathComponent();
                    if (parent instanceof MondrianGuiDef.Join) {
                        int indexOfChild = tree.getModel().getIndexOfChild(parent, value);
                        switch (indexOfChild) {
                            case 0: prefix="Left : "; break;
                            case 1: prefix="Right : "; break;
                        }
                    }
                }
            }
            if (value instanceof MondrianGuiDef.Join) {
                setText(prefix+"Join");
                super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("join"))));
            } else if (value instanceof MondrianGuiDef.Table) {
                setText(prefix+"Table: "+ ((MondrianGuiDef.Table) value).name);
                super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("table"))));
            }
            this.getPreferredSize();
            this.setPreferredSize(new Dimension(this.getPreferredSize().width+35, 24)); //Do not remove this
            //this.setSize(new Dimension(this.getPreferredSize().width, 24)); //Do not remove this
            //this.setPreferredSize(new Dimension(170, 24)); //Do not remove this
            //setText("Relation");

        } else if (value instanceof MondrianGuiDef.Level) {
            setText(invalidFlag, ((MondrianGuiDef.Level) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("level"))));
            /* Do not remove this line.
             * This sets the preferred width of tree cell displaying Level name.
             * This resolves the ambiguous problem of last char or last word truncated from Level name in the tree cell.
             * This problem was there with Level objects, while all other objects display their names
             * without any truncation of characters. Therefore, we have to force the setting of preferred width to desired width
             * so that characters do not truncate from dimension name.
             * Along with this the preferred size of other objects should be set to null, so that the layout mgr can calculate the
             * preferred width in case of other objects.
             */
            this.setPreferredSize(new java.awt.Dimension(this.getPreferredSize().width+1, 25)); //Do not remove this
        } else if (value instanceof MondrianGuiDef.Measure) {

            setText(invalidFlag, ((MondrianGuiDef.Measure) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("measure"))));
        } else if (value instanceof MondrianGuiDef.MemberReaderParameter) {
            setText(invalidFlag, ((MondrianGuiDef.MemberReaderParameter) value).name);
        } else if (value instanceof MondrianGuiDef.Property) {
            setText(invalidFlag, ((MondrianGuiDef.Property) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("property"))));
        } else if (value instanceof MondrianGuiDef.Schema) {
            setText(invalidFlag, "Schema");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("schema"))));
        } else if (value instanceof MondrianGuiDef.NamedSet) {
            setText(invalidFlag, ((MondrianGuiDef.NamedSet) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("namedSet"))));
        } else if (value instanceof MondrianGuiDef.CalculatedMember) {
            setText(invalidFlag, ((MondrianGuiDef.CalculatedMember) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("calculatedMember"))));
        } else if (value instanceof MondrianGuiDef.CalculatedMemberProperty) {
            setText(invalidFlag, ((MondrianGuiDef.CalculatedMemberProperty) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("nopic"))));
        } else if (value instanceof MondrianGuiDef.UserDefinedFunction) {
            setText(invalidFlag, ((MondrianGuiDef.UserDefinedFunction) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("userDefinedFunction"))));
        } else if (value instanceof MondrianGuiDef.Role) {
            setText(invalidFlag, ((MondrianGuiDef.Role) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("role"))));
        } else if (value instanceof MondrianGuiDef.SchemaGrant) {
            setText(invalidFlag, "Schema Grant");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("schemaGrant"))));
        } else if (value instanceof MondrianGuiDef.CubeGrant) {
            setText(invalidFlag, "Cube Grant");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("cubeGrant"))));
        } else if (value instanceof MondrianGuiDef.DimensionGrant) {
            setText(invalidFlag, "Dimension Grant");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("dimensionGrant"))));
        } else if (value instanceof MondrianGuiDef.HierarchyGrant) {
            setText(invalidFlag, "Hierarchy Grant");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("hierarchyGrant"))));
        } else if (value instanceof MondrianGuiDef.MemberGrant) {
            setText(invalidFlag, "Member Grant");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("memberGrant"))));
        } else if (value instanceof MondrianGuiDef.SQL) {
            setText("SQL");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("sql"))));
        } else if (value instanceof MondrianGuiDef.View) {
            setText("View");
        } else if (value instanceof MondrianGuiDef.VirtualCube) {
            setText(invalidFlag, ((MondrianGuiDef.VirtualCube) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("virtualCube"))));
        } else if (value instanceof MondrianGuiDef.VirtualCubeDimension) {
            setText(invalidFlag, ((MondrianGuiDef.VirtualCubeDimension) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("virtualCubeDimension"))));
        } else if (value instanceof MondrianGuiDef.VirtualCubeMeasure) {
            setText(invalidFlag, ((MondrianGuiDef.VirtualCubeMeasure) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("virtualCubeMeasure"))));
        } else if (value instanceof MondrianGuiDef.AggName) {
            setText(invalidFlag, "Aggregate Name");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("aggTable"))));
        } else if (value instanceof MondrianGuiDef.AggForeignKey) {
            setText(invalidFlag, "Aggregate Foreign Key");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("aggForeignKey"))));
        } else if (value instanceof MondrianGuiDef.AggIgnoreColumn) {
            setText(invalidFlag, "Aggregate Ignore Column");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("aggIgnoreColumn"))));
        } else if (value instanceof MondrianGuiDef.AggLevel) {
            setText(invalidFlag, "Aggregate Level");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("aggLevel"))));
        } else if (value instanceof MondrianGuiDef.AggMeasure) {
            setText(invalidFlag, "Aggregate Measure");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("aggMeasure"))));
        } else if (value instanceof MondrianGuiDef.AggPattern) {
            setText(invalidFlag, "Aggregate Pattern");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("aggPattern"))));
        } else if (value instanceof MondrianGuiDef.AggExclude) {
            setText(invalidFlag, "Aggregate Exclude");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("aggExclude"))));
        } else if (value instanceof MondrianGuiDef.Closure) {
            setText(invalidFlag, "Closure");
            super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("closure"))));
        } else if (value instanceof ElementDef) {
            setText(((ElementDef) value).getName());
        } else {
            super.setText("");
        }

        return this;

    }

    // called from external methods
    public String invalid(JTree tree, TreePath tpath, Object value) {
        return this.invalid(tree, tpath, value, null, null, null, null);
    }

    public String invalid(JTree tree, TreePath tpath, Object value, Object icube, Object iparentDimension, Object iparentHierarchy, Object iparentLevel) {
        String errMsg = null;
        String nameLiteral = "Name";
        //String valueClass = value.getClass().getSimpleName();
        String emptyMsg = " must be set";

        MondrianGuiDef.Cube cube = (MondrianGuiDef.Cube) icube ; //null;
        MondrianGuiDef.Dimension parentDimension = (MondrianGuiDef.Dimension) iparentDimension; // null // used only by level to check for leveltype value
        MondrianGuiDef.Hierarchy parentHierarchy = (MondrianGuiDef.Hierarchy) iparentHierarchy ; //null; // used only by level validation
        MondrianGuiDef.Level parentLevel =  (MondrianGuiDef.Level) iparentLevel ; // null // used only by property validation

        if (tpath != null ) {
            int pathcount = tpath.getPathCount();
            for(int i=0; i<pathcount && (cube==null || parentDimension==null || parentHierarchy==null || parentLevel==null) ;i++) {
                //System.out.println("path element "+i+" ="+tree.getSelectionPath().getPathComponent(i).getClass().toString());
                if (tpath.getPathComponent(i) instanceof MondrianGuiDef.Cube && cube==null) {
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
                //System.out.println("Cube fact table name ="+((MondrianGuiDef.Table) ((MondrianGuiDef.Cube) e.getPath().getPathComponent(i)).fact).name);
            }
        }

        //Step 1: check validity of this value object
        if (value instanceof MondrianGuiDef.Schema) {
            if ( isEmpty(((MondrianGuiDef.Schema) value).name) ) {
                return nameLiteral + emptyMsg;
            }
        } else if(value instanceof MondrianGuiDef.VirtualCube) {
            if ( isEmpty(((MondrianGuiDef.VirtualCube) value).name) ) {
                return nameLiteral + emptyMsg;
            }
        } else if(value instanceof MondrianGuiDef.VirtualCubeDimension) {
            if ( isEmpty(((MondrianGuiDef.VirtualCubeDimension) value).name) ) {
                return nameLiteral + emptyMsg;
            }
        } else if(value instanceof MondrianGuiDef.VirtualCubeMeasure) {
            if ( isEmpty(((MondrianGuiDef.VirtualCubeMeasure) value).name) ) {
                return nameLiteral + emptyMsg;
            }
        } else if (value instanceof MondrianGuiDef.Cube) {
            if ( isEmpty(((MondrianGuiDef.Cube) value).name) ) {
                return nameLiteral + emptyMsg;}
            if ( ((MondrianGuiDef.Cube) value).fact == null || isEmpty(((MondrianGuiDef.Table) ((MondrianGuiDef.Cube) value).fact).name)   )    //check name is not blank
            {    return "Fact name" + emptyMsg;}

            // database validity check, if database connection is successful
            if (jdbcMetaData.getErrMsg() == null) {

                //Vector allTables            = jdbcMetaData.getAllTables(((MondrianGuiDef.Table) ((MondrianGuiDef.Cube) value).fact).schema);
                String schemaName = ((MondrianGuiDef.Table) ((MondrianGuiDef.Cube) value).fact).schema;
                String factTable = ((MondrianGuiDef.Table) ((MondrianGuiDef.Cube) value).fact).name;
                if (! jdbcMetaData.isTableExists(schemaName, factTable)) {
                    return "Fact table '"+factTable+"' does not exist in database "+((schemaName==null || schemaName.equals(""))?".":"schema "+schemaName);
                }
            }
        } else if (value instanceof MondrianGuiDef.CubeDimension) {
            if (isEmpty(((MondrianGuiDef.CubeDimension) value).name))    //check name is not blank
            {   return nameLiteral + emptyMsg;}
            if (value instanceof MondrianGuiDef.DimensionUsage) {
                if(isEmpty(((MondrianGuiDef.DimensionUsage) value).source))    //check source is not blank
                {   return "Source" + emptyMsg;}
                // check source is name of one of dimensions of schema (shared dimensions)
                MondrianGuiDef.Schema s = (MondrianGuiDef.Schema) tree.getModel().getRoot();
                MondrianGuiDef.Dimension ds[] = s.dimensions;
                String sourcename = ((MondrianGuiDef.DimensionUsage) value).source;
                boolean notfound = true;
                for(int j=0; j<ds.length; j++) {
                    if (ds[j].name.equalsIgnoreCase(sourcename)) {
                        notfound = false;
                        break;
                    }
                }
                if (notfound) {
                    return "Source '"+sourcename+"' does not exist as Shared Dimension of Schema";
                }
            }
            if (value instanceof MondrianGuiDef.Dimension && cube != null) {
                /* //foreignkey can be blank if  hierarchy relation is null
                 * // this check moved to child hierarchies relation check below
                if(isEmpty(((MondrianGuiDef.Dimension) value).foreignKey))    //check foreignkey is not blank
                { return "ForeignKey" + emptyMsg;}
                 */
                if(! isEmpty(((MondrianGuiDef.Dimension) value).foreignKey)) {
                    // database validity check, if database connection is successful
                    if (jdbcMetaData.getErrMsg() == null) {

                        //Vector allcols  = jdbcMetaData.getAllColumns(((MondrianGuiDef.Table) cube.fact).schema, ((MondrianGuiDef.Table) cube.fact).name);
                        String foreignKey = ((MondrianGuiDef.Dimension) value).foreignKey;
                        if (! jdbcMetaData.isColExists(((MondrianGuiDef.Table) cube.fact).schema, ((MondrianGuiDef.Table) cube.fact).name, foreignKey)) {
                            return "foreignKey '"+foreignKey+"' does not exist in fact table.";
                        }
                    /*
                    if (! allcols.contains(foreignKey))        // check foreignKey is a fact table column
                    {   return "ForeignKey '"+foreignKey+"' does not exist in fact table.";}
                     */
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
            if (! isEmpty(l.levelType)) {
                // empty leveltype is treated as default value of "Regular"" which is ok with standard/time dimension
                if (parentDimension != null) {
                    if ((isEmpty(parentDimension.type) || parentDimension.type.equals("StandardDimension")) &&
                            (! l.levelType.equals(MondrianGuiDef.Level._levelType_values[0]))) {
                     // if dimension type is 'standard' then leveltype should be 'regular'
                        return "levelType '"+l.levelType+"' can only be used with a TimeDimension.";
                    }
                    if ((parentDimension.type.equals("TimeDimension")) &&
                     // if dimension type is 'time' then leveltype value could be 'timeyears', 'timedays' etc'
                            (l.levelType.equals(MondrianGuiDef.Level._levelType_values[0]))) {
                        return "levelType '"+l.levelType+"' can only be used with a StandardDimension.";
                    }
                }
            }
            String column = l.column; // check level's column is in fact table'
            /* // level column may be blank, if it has properties defined with cols.
            if (isEmpty(column)) {
                return "Column" + emptyMsg;
            }
             */
            if (isEmpty(column)) {
                if (l.properties == null || l.properties.length == 0) {
                    return "column" + emptyMsg;
                }
            } else {
                // database validity check, if database connection is successful
                if (jdbcMetaData.getErrMsg() == null) {
                    String table = l.table;   // specified table for level's column'
                    if (isEmpty(table)) {
                        if (parentHierarchy != null  ) {
                            if (parentHierarchy.relation == null  && cube != null) { // case of degenerate dimension within cube, hierarchy table not specified
                                if (! jdbcMetaData.isColExists(((MondrianGuiDef.Table) cube.fact).schema, ((MondrianGuiDef.Table) cube.fact).name, column)) {
                                    return "Degenerate dimension validation check - Column '"+column+"' does not exist in fact table.";
                                }
                            } else if (parentHierarchy.relation instanceof MondrianGuiDef.Table){
                                if (! jdbcMetaData.isColExists(((MondrianGuiDef.Table) parentHierarchy.relation).schema, ((MondrianGuiDef.Table) parentHierarchy.relation).name, column)) {
                                    return "column '"+column+"' does not exist in Dimension table '"+((MondrianGuiDef.Table) parentHierarchy.relation).name+"'.";
                                }
                            } else  if (parentHierarchy.relation instanceof MondrianGuiDef.Join){    // relation is join, table should be specified
                                return "table" + emptyMsg;
                            }
                        }
                    } else {
                        if (! jdbcMetaData.isColExists(null, table, column)) {
                            return "column '"+column+"' does not exist in table '"+table+"'.";
                        }
                    }
                }
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
                return "column" + emptyMsg;
            }
            // database validity check, if database connection is successful
            if (jdbcMetaData.getErrMsg() == null) {
                String table = null;
                if (parentLevel != null) {
                    table = parentLevel.table;   // specified table for level's column'
                }
                if (isEmpty(table)) {
                    if (parentHierarchy != null  ) {
                        if (parentHierarchy.relation == null  && cube != null) { // case of degenerate dimension within cube, hierarchy table not specified
                            if (! jdbcMetaData.isColExists(((MondrianGuiDef.Table) cube.fact).schema, ((MondrianGuiDef.Table) cube.fact).name, column)) {
                                return "Degenerate dimension validation check - Column '"+column+"' does not exist in fact table.";
                            }
                        } else if (parentHierarchy.relation instanceof MondrianGuiDef.Table){
                            if (! jdbcMetaData.isColExists(((MondrianGuiDef.Table) parentHierarchy.relation).schema, ((MondrianGuiDef.Table) parentHierarchy.relation).name, column)) {
                                return "column '"+column+"' does not exist in Dimension table '"+((MondrianGuiDef.Table) parentHierarchy.relation).name+"'.";
                            }
                        }
                    }
                } else {
                    if (! jdbcMetaData.isColExists(null, table, column)) {
                        return "column '"+column+"' does not exist in Level table '"+table+"'.";
                    }
                }
            }
        } else if (value instanceof MondrianGuiDef.Measure) {
            if ( isEmpty(((MondrianGuiDef.Measure) value).name) ) {
                return  nameLiteral + emptyMsg;}
            if (isEmpty(((MondrianGuiDef.Measure) value).aggregator) ) {
                return "aggregator" + emptyMsg;}
            if (isEmpty(((MondrianGuiDef.Measure) value).column)   ) {
                return "column" + emptyMsg;}
            if (cube != null && cube.fact != null) {

                // database validity check, if database connection is successful
                if (jdbcMetaData.getErrMsg() == null) {

                    //Vector allcols  = jdbcMetaData.getAllColumns(((MondrianGuiDef.Table) cube.fact).schema, ((MondrianGuiDef.Table) cube.fact).name);

                    String column = ((MondrianGuiDef.Measure) value).column;
                    if (jdbcMetaData.isColExists(((MondrianGuiDef.Table) cube.fact).schema, ((MondrianGuiDef.Table) cube.fact).name, column)) {
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
                        int colType = jdbcMetaData.getColumnDataType(((MondrianGuiDef.Table) cube.fact).schema, ((MondrianGuiDef.Table) cube.fact).name, ((MondrianGuiDef.Measure) value).column);
                        // colType of 2, 4,5, 7,8 is numeric types whereas 1, 12 are char varchar string and 91 is date type
                        int agIndex = -1;
                        if ("sum".equals(((MondrianGuiDef.Measure) value).aggregator) || "avg".equals(((MondrianGuiDef.Measure) value).aggregator)) {
                            agIndex=0;  // aggregator = sum or avg, column should be numeric
                        }
                        if (! (agIndex == -1 || (colType >=2 && colType <=8))) {
                            return "aggregator '"+((MondrianGuiDef.Measure) value).aggregator+"' is not valid on the data type of the column '"+((MondrianGuiDef.Measure) value).column+"'";
                        }
                    }
                }
            }
        } else if (value instanceof MondrianGuiDef.Hierarchy) {
            if (((MondrianGuiDef.Hierarchy)value).relation instanceof MondrianGuiDef.Join) {
                String returnMsg = "";
                if ( isEmpty(((MondrianGuiDef.Hierarchy) value).primaryKeyTable)) {
                    returnMsg = "primaryKeyTable ";
                }
                if ( isEmpty(((MondrianGuiDef.Hierarchy) value).primaryKey)) {
                    if (returnMsg.length() > 0) {
                        returnMsg = returnMsg + "and";
                    }
                    returnMsg = returnMsg + " primaryKey ";
                }
                if (returnMsg.length() > 0) {
                    return (returnMsg + emptyMsg + " for Join");
                }
            }
        } else if (value instanceof MondrianGuiDef.NamedSet) {
            if ( isEmpty(((MondrianGuiDef.NamedSet) value).name) ) {
                return nameLiteral + emptyMsg;}
            if ( isEmpty(((MondrianGuiDef.NamedSet) value).formula) ) {
                return "formula " + emptyMsg;}
        } else if (value instanceof MondrianGuiDef.UserDefinedFunction) {
            if ( isEmpty(((MondrianGuiDef.UserDefinedFunction) value).name) ) {
                return nameLiteral + emptyMsg;}
            if(  isEmpty(((MondrianGuiDef.UserDefinedFunction) value).className)) {
                return "className" + emptyMsg;}
        } else if (value instanceof MondrianGuiDef.CalculatedMember) {
            if ( isEmpty(((MondrianGuiDef.CalculatedMember) value).name) ) {
                return nameLiteral + emptyMsg;}
            if (isEmpty(((MondrianGuiDef.CalculatedMember) value).dimension) ) {
                return "dimension" + emptyMsg;}
        } else if (value instanceof MondrianGuiDef.Join) {
            if ( isEmpty(((MondrianGuiDef.Join) value).leftKey) ) {
                return "leftKey" + emptyMsg;}
            if (isEmpty(((MondrianGuiDef.Join) value).rightKey) ) {
                return "rightKey" + emptyMsg;}
        }

        // Step 2: check validity of all child objects for this value object.
        SchemaTreeModel model = (SchemaTreeModel) tree.getModel();
        int childCnt = model.getChildCount(value);
        for (int i=0; i<childCnt; i++) {
            Object child = model.getChild(value, i);
            String childErrMsg;
            if (child instanceof MondrianGuiDef.Cube) {
                childErrMsg = invalid(tree, tpath, child, child, parentDimension, parentHierarchy, parentLevel);   //check current cube child and its children
            } else if (child instanceof MondrianGuiDef.Dimension) {
                childErrMsg = invalid(tree, tpath, child, cube, child, parentHierarchy, parentLevel);   //check the current hierarchy and its children
            } else if (child instanceof MondrianGuiDef.Hierarchy) {
                // special check for cube dimension where foreign key is blank : allowed /not allowed
                if (value instanceof MondrianGuiDef.Dimension && cube != null && ((MondrianGuiDef.Hierarchy)child).relation != null) {
                    if(isEmpty(((MondrianGuiDef.Dimension) value).foreignKey))    //check foreignkey is not blank
                    {    return "foreignKey" + emptyMsg;    // if relation is null, foreignkey must be specified
                    }
                }
                childErrMsg = invalid(tree, tpath, child, cube, parentDimension, child, parentLevel);   //check the current hierarchy and its children
            } else if (child instanceof MondrianGuiDef.Level) {
                childErrMsg = invalid(tree, tpath, child, cube, parentDimension, parentHierarchy, child);   //check the current hierarchy and its children
            } else {
                childErrMsg = invalid(tree, tpath, child, cube, parentDimension, parentHierarchy, parentLevel);   //check this child and all its children objects with incoming cube and hierarchy
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
                String simpleName[] = childClassName.split("[$.]",0);
                String childName;
                try {
                    Field f = child.getClass().getField("name");
                    childName = (String) f.get(child) ;
                    if (childName == null) {
                        childName="";
                    } else {
                        childName = " '" + childName + "'";
                    }
                    childErrMsg = simpleName[simpleName.length-1] + " " +
                        childName +" is invalid.";
                } catch(Exception ex) {
                    childErrMsg = simpleName[simpleName.length-1] + " is invalid.";
                }
                return childErrMsg;
            }
        }

        return errMsg;
    }

    private boolean isEmpty(Object v) {
        if ((v == null) || v.equals("") ) {
            return true;
        } else {
            return false;
        }
    }
    private boolean isInvalid(JTree tree, Object value, int row) {
        //return (invalid(tree.getSelectionPath(), value) ==null)?false:true;
        /* (TreePath) tree.getPathForRow(row) returns null for new objects added to tree in the first run of rendering.
         * Check for null before calling methods on Treepath returned.
         */
        return (invalid(tree, tree.getPathForRow(row), value) ==null)?false:true;
        //return (invalid(null, value) ==null)?false:true;
    }

    public void setText(boolean invalidFlag, String myText) {
        if (invalidFlag) {
            myText = "<html><FONT COLOR=RED><b>x</b></FONT><FONT COLOR="+ getForeground().hashCode()+">"+myText+"</FONT></html>";
        }
        setText(myText);
    }
}

// End SchemaTreeCellRenderer.java
