/*
/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2008 Julian Hyde and others
// Copyright (C) 2006-2007 CINCOM SYSTEMS, INC.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.gui;

import java.awt.Component;
import java.awt.Dimension;

import javax.swing.ImageIcon;
import javax.swing.JTree;
import javax.swing.tree.TreePath;

import mondrian.gui.validate.ValidationUtils;
import mondrian.gui.validate.impl.WorkbenchJDBCValidator;
import mondrian.gui.validate.impl.WorkbenchMessages;
import mondrian.gui.validate.impl.WorkbenchTreeModel;
import mondrian.gui.validate.impl.WorkbenchTreeModelPath;

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
    private Workbench workbench;

    /** Creates a new instance of SchemaTreeCellRenderer */

    public SchemaTreeCellRenderer(Workbench wb, JDBCMetaData jdbcMetaData) {
        this();
        this.workbench = wb;
        this.jdbcMetaData = jdbcMetaData;
    }
    public SchemaTreeCellRenderer() {
        super();
        myClassLoader = this.getClass().getClassLoader();

    }

    public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf, int row, boolean hasFocus) {
        super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);

        invalidFlag = isInvalid(tree, value, row);

        this.setPreferredSize(null); // This allows the layout mgr to calculate the pref size of renderer.
        if (value instanceof MondrianGuiDef.Cube) {
            setText(invalidFlag, ((MondrianGuiDef.Cube) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("cube"))));
        } else if (value instanceof MondrianGuiDef.Column) {
            setText(invalidFlag, ((MondrianGuiDef.Column) value).name);
        } else if (value instanceof MondrianGuiDef.Dimension) {
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("dimension"))));
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
            this.setPreferredSize(new java.awt.Dimension(this.getPreferredSize().width + 1, 25)); //Do not remove this
        } else if (value instanceof MondrianGuiDef.DimensionUsage)      {
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("dimensionUsage"))));
            setText(invalidFlag, ((MondrianGuiDef.CubeDimension) value).name);
        } else if (value instanceof MondrianGuiDef.KeyExpression) {
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("key"))));
            setText(workbench.getResourceConverter().getString("common.keyExpression.title","Key Expression"));
        } else if (value instanceof MondrianGuiDef.NameExpression) {
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("name"))));
            setText(workbench.getResourceConverter().getString("common.nameExpression.title","Name Expression"));
        } else if (value instanceof MondrianGuiDef.OrdinalExpression) {
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("ordinal"))));
            setText(workbench.getResourceConverter().getString("common.ordinalExpression.title","Ordinal Expression"));
        } else if (value instanceof MondrianGuiDef.ParentExpression) {
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("parent"))));
            setText(workbench.getResourceConverter().getString("common.parentExpression.title","Parent Expression"));
        } else if (value instanceof MondrianGuiDef.Expression) {
            setText(workbench.getResourceConverter().getString("common.expression.title","Expression"));
        } else if (value instanceof MondrianGuiDef.ExpressionView) {
            setText(workbench.getResourceConverter().getString("common.expressionView.title","Expression View"));
        } else if (value instanceof MondrianGuiDef.Hierarchy) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.hierarchy.title","Hierarchy"));
            //setText(((MondrianGuiDef.Hierarchy) value).name);    // hierarchies do not have names
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("hierarchy"))));
            this.setPreferredSize(new java.awt.Dimension(this.getPreferredSize().width + 1, 25)); //Do not remove this
        } else if ((value instanceof MondrianGuiDef.Table)) {
            setText(invalidFlag, ((MondrianGuiDef.Table) value).name);
        } else if ((value instanceof MondrianGuiDef.RelationOrJoin) ||
            // REVIEW: '||' is superfluous - a Table is always a RelationOrJoin
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
                        case 0:
                            prefix = workbench.getResourceConverter().getString("common.leftPrefix.title","Left") + " ";
                            break;
                        case 1:
                            prefix = workbench.getResourceConverter().getString("common.rightPrefix.title","Right") + " ";
                            break;
                        }
                    }
                }
            }
            if (value instanceof MondrianGuiDef.Join) {
                setText(workbench.getResourceConverter().getFormattedString("schemaTreeCellRenderer.join.title",
                        "{0} : Join", new String[] {prefix}));
                //setText(prefix + " " + workbench.getResourceConverter().getString("common.join.title","Join"));
                super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("join"))));
            } else if (value instanceof MondrianGuiDef.Table) {
                //setText(prefix+"Table: "+ ((MondrianGuiDef.Table) value).name);
                //EC: Sets the table name to alias if present.
                MondrianGuiDef.Table theTable = (MondrianGuiDef.Table) value;
                String theName = (theTable.alias != null && theTable.alias.trim().length() > 0) ? theTable.alias : theTable.name;
                setText(workbench.getResourceConverter().getFormattedString("schemaTreeCellRenderer.table.title",
                        "{0}Table: {1}", new String[] {(prefix.length() == 0 ? "" : prefix + " : "),theName}));
                super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("table"))));
            }
            // REVIEW: Need to deal with InlineTable and View here
            this.getPreferredSize();
            this.setPreferredSize(new Dimension(this.getPreferredSize().width + 35, 24)); //Do not remove this
            //this.setSize(new Dimension(this.getPreferredSize().width, 24)); //Do not remove this
            //this.setPreferredSize(new Dimension(170, 24)); //Do not remove this
            //setText("Relation");

        } else if (value instanceof MondrianGuiDef.Level) {
            setText(invalidFlag, ((MondrianGuiDef.Level) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("level"))));
            /* Do not remove this line.
             * This sets the preferred width of tree cell displaying Level name.
             * This resolves the ambiguous problem of last char or last word truncated from Level name in the tree cell.
             * This problem was there with Level objects, while all other objects display their names
             * without any truncation of characters. Therefore, we have to force the setting of preferred width to desired width
             * so that characters do not truncate from dimension name.
             * Along with this the preferred size of other objects should be set to null, so that the layout mgr can calculate the
             * preferred width in case of other objects.
             */
            this.setPreferredSize(new java.awt.Dimension(this.getPreferredSize().width + 1, 25)); //Do not remove this
        } else if (value instanceof MondrianGuiDef.Measure) {

            setText(invalidFlag, ((MondrianGuiDef.Measure) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("measure"))));
        } else if (value instanceof MondrianGuiDef.MemberReaderParameter) {
            setText(invalidFlag, ((MondrianGuiDef.MemberReaderParameter) value).name);
        } else if (value instanceof MondrianGuiDef.Property) {
            setText(invalidFlag, ((MondrianGuiDef.Property) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("property"))));
        } else if (value instanceof MondrianGuiDef.Schema) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.schema.title","Schema"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("schema"))));
        } else if (value instanceof MondrianGuiDef.NamedSet) {
            setText(invalidFlag, ((MondrianGuiDef.NamedSet) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("namedSet"))));
        } else if (value instanceof MondrianGuiDef.CalculatedMember) {
            setText(invalidFlag, ((MondrianGuiDef.CalculatedMember) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("calculatedMember"))));
        } else if (value instanceof MondrianGuiDef.CalculatedMemberProperty) {
            setText(invalidFlag, ((MondrianGuiDef.CalculatedMemberProperty) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("nopic"))));
        } else if (value instanceof MondrianGuiDef.UserDefinedFunction) {
            setText(invalidFlag, ((MondrianGuiDef.UserDefinedFunction) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("userDefinedFunction"))));
        } else if (value instanceof MondrianGuiDef.Role) {
            setText(invalidFlag, ((MondrianGuiDef.Role) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("role"))));
        } else if (value instanceof MondrianGuiDef.SchemaGrant) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.schemaGrant.title","Schema Grant"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("schemaGrant"))));
        } else if (value instanceof MondrianGuiDef.CubeGrant) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.cubeGrant.title","Cube Grant"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("cubeGrant"))));
        } else if (value instanceof MondrianGuiDef.DimensionGrant) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.dimensionGrant.title","Dimension Grant"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("dimensionGrant"))));
        } else if (value instanceof MondrianGuiDef.HierarchyGrant) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.hierarchyGrant.title","Hierarchy Grant"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("hierarchyGrant"))));
        } else if (value instanceof MondrianGuiDef.MemberGrant) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.memberGrant.title","Member Grant"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("memberGrant"))));
        } else if (value instanceof MondrianGuiDef.SQL) {
            setText(invalidFlag, ((MondrianGuiDef.SQL) value).dialect);
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("sql"))));
        } else if (value instanceof MondrianGuiDef.View) {
            setText(workbench.getResourceConverter().getString("common.view.title","View"));
        } else if (value instanceof MondrianGuiDef.VirtualCube) {
            setText(invalidFlag, ((MondrianGuiDef.VirtualCube) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("virtualCube"))));
        } else if (value instanceof MondrianGuiDef.VirtualCubeDimension) {
            setText(invalidFlag, ((MondrianGuiDef.VirtualCubeDimension) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("virtualCubeDimension"))));
        } else if (value instanceof MondrianGuiDef.VirtualCubeMeasure) {
            setText(invalidFlag, ((MondrianGuiDef.VirtualCubeMeasure) value).name);
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("virtualCubeMeasure"))));
        } else if (value instanceof MondrianGuiDef.AggName) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.aggName.title","Aggregate Name"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("aggTable"))));
        } else if (value instanceof MondrianGuiDef.AggForeignKey) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.aggForeignKey.title","Aggregate Foreign Key"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("aggForeignKey"))));
        } else if (value instanceof MondrianGuiDef.AggIgnoreColumn) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.aggIgnoreColumn.title","Aggregate Ignore Column"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("aggIgnoreColumn"))));
        } else if (value instanceof MondrianGuiDef.AggLevel) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.aggLevel.title","Aggregate Level"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("aggLevel"))));
        } else if (value instanceof MondrianGuiDef.AggMeasure) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.aggMeasure.title","Aggregate Measure"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("aggMeasure"))));
        } else if (value instanceof MondrianGuiDef.AggPattern) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.aggPattern.title","Aggregate Pattern"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("aggPattern"))));
        } else if (value instanceof MondrianGuiDef.AggExclude) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.aggExclude.title","Aggregate Exclude"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("aggExclude"))));
        } else if (value instanceof MondrianGuiDef.Closure) {
            setText(invalidFlag, workbench.getResourceConverter().getString("common.closure.title","Closure"));
            super.setIcon(new ImageIcon(myClassLoader.getResource(workbench.getResourceConverter().getGUIReference("closure"))));
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
        return ValidationUtils.invalid(new WorkbenchMessages(workbench.getResourceConverter()), new WorkbenchJDBCValidator(jdbcMetaData),
                new WorkbenchTreeModel((SchemaTreeModel) tree.getModel()), new WorkbenchTreeModelPath(tpath), value, icube, iparentDimension,
                iparentHierarchy, iparentLevel, jdbcMetaData.getRequireSchema());
    }
    private boolean isInvalid(JTree tree, Object value, int row) {
        //return (invalid(tree.getSelectionPath(), value) ==null)?false:true;
        /* (TreePath) tree.getPathForRow(row) returns null for new objects added to tree in the first run of rendering.
         * Check for null before calling methods on Treepath returned.
         */
        return (invalid(tree, tree.getPathForRow(row), value) == null) ? false : true;
        //return (invalid(null, value) ==null)?false:true;
    }

    public void setText(boolean invalidFlag, String myText) {
        if (invalidFlag) {
            myText = "<html><FONT COLOR=RED><b>x</b></FONT><FONT COLOR=" + getForeground().hashCode() + ">" + myText + "</FONT></html>";
        }
        setText(myText);
    }

    public void setMetaData(JDBCMetaData aMetaData) {
        //EC: Called from the SchemaExplorer.resetMetadata(). A call to the updateUI() should be
        //made on the owning SchemaFrame to reflect the use of the JDBCMetaData being set.
        this.jdbcMetaData = aMetaData;
    }
}

// End SchemaTreeCellRenderer.java
