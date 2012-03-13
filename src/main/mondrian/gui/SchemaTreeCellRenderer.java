/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2010 Pentaho and others
// Copyright (C) 2006-2007 CINCOM SYSTEMS, INC.
// All Rights Reserved.
*/
package mondrian.gui;

import mondrian.gui.validate.ValidationUtils;
import mondrian.gui.validate.impl.*;

import org.eigenbase.xom.ElementDef;

import java.awt.*;
import javax.swing.*;
import javax.swing.tree.TreePath;

/**
 * Render an entry for the tree.
 *
 * @author sean
 */
public class SchemaTreeCellRenderer
    extends javax.swing.tree.DefaultTreeCellRenderer
{
    private final ClassLoader myClassLoader;
    public boolean invalidFlag;
    private JdbcMetaData jdbcMetaData;
    private final Workbench workbench;

    /**
     * Creates a SchemaTreeCellRenderer with Workbench and metadata.
     */
    public SchemaTreeCellRenderer(
        Workbench workbench,
        JdbcMetaData jdbcMetaData)
    {
        super();
        this.myClassLoader = this.getClass().getClassLoader();
        this.workbench = workbench;
        this.jdbcMetaData = jdbcMetaData;
    }

    /**
     * Creates a SchemaTreeCellRenderer.
     */
    public SchemaTreeCellRenderer() {
        this(null, null);
    }

    public Component getTreeCellRendererComponent(
        JTree tree,
        Object value,
        boolean sel,
        boolean expanded,
        boolean leaf,
        int row,
        boolean hasFocus)
    {
        super.getTreeCellRendererComponent(
            tree, value, sel, expanded, leaf, row, hasFocus);

        invalidFlag = isInvalid(tree, value, row);

        // Allow the layout mgr to calculate the pref size of renderer.
        this.setPreferredSize(null);
        if (value instanceof MondrianGuiDef.Cube) {
            setText(invalidFlag, ((MondrianGuiDef.Cube) value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench
                            .getResourceConverter().getGUIReference("cube"))));
        } else if (value instanceof MondrianGuiDef.Column) {
            setText(invalidFlag, ((MondrianGuiDef.Column) value).name);
        } else if (value instanceof MondrianGuiDef.Dimension) {
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "dimension"))));
            setText(invalidFlag, ((MondrianGuiDef.CubeDimension) value).name);
            // Do not remove this line.  This sets the preferred width of tree
            // cell displaying dimension name.  This resolves the ambiguous
            // problem of last char or last word truncated from dimension name
            // in the tree cell.  This problem was there with only Dimension
            // objects, while all other objects display their names without any
            // truncation of characters. Therefore, we have to force the setting
            // of preferred width to desired width so that characters do not
            // truncate from dimension name.  Along with this the preferred size
            // of other objects should be set to null, so that the layout mgr
            // can calculate the preferred width in case of other objects.
            this.setPreferredSize(
                new java.awt.Dimension(
                    this.getPreferredSize().width + 1,
                    25));
        } else if (value instanceof MondrianGuiDef.DimensionUsage) {
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "dimensionUsage"))));
            setText(invalidFlag, ((MondrianGuiDef.CubeDimension) value).name);
        } else if (value instanceof MondrianGuiDef.KeyExpression) {
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench
                            .getResourceConverter().getGUIReference("key"))));
            setText(
                workbench.getResourceConverter().getString(
                    "common.keyExpression.title",
                    "Key Expression"));
        } else if (value instanceof MondrianGuiDef.NameExpression) {
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench
                            .getResourceConverter().getGUIReference("name"))));
            setText(
                workbench.getResourceConverter().getString(
                    "common.nameExpression.title",
                    "Name Expression"));
        } else if (value instanceof MondrianGuiDef.OrdinalExpression) {
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "ordinal"))));
            setText(
                workbench.getResourceConverter().getString(
                    "common.ordinalExpression.title",
                    "Ordinal Expression"));
        } else if (value instanceof MondrianGuiDef.CaptionExpression) {
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench
                            .getResourceConverter().getGUIReference("name"))));
            setText(
                workbench.getResourceConverter().getString(
                    "common.captionExpression.title",
                    "Caption Expression"));
        } else if (value instanceof MondrianGuiDef.ParentExpression) {
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "parent"))));
            setText(
                workbench.getResourceConverter().getString(
                    "common.parentExpression.title",
                    "Parent Expression"));
        } else if (value instanceof MondrianGuiDef.Expression) {
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "expression"))));
            setText(
                workbench.getResourceConverter().getString(
                    "common.expression.title",
                    "Expression"));
        } else if (value instanceof MondrianGuiDef.ExpressionView) {
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "expression"))));
            setText(
                workbench.getResourceConverter().getString(
                    "common.expressionView.title",
                    "Expression View"));
        } else if (value instanceof MondrianGuiDef.Hierarchy) {
            String name = ((MondrianGuiDef.Hierarchy) value).name;

            if (name == null || name.trim().length() == 0) {
                setText(
                    invalidFlag,
                    workbench.getResourceConverter().getString(
                        "common.hierarchy.default.name",
                        "default"));
            } else {
                setText(invalidFlag, name);
            }
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "hierarchy"))));
            this.setPreferredSize(
                new java.awt.Dimension(
                    this.getPreferredSize().width + 1,
                    25));
        } else if (value instanceof MondrianGuiDef.RelationOrJoin) {
            TreePath tpath = tree.getPathForRow(row);
            String prefix = "";
            if (tpath != null) {
                TreePath parentpath = tpath.getParentPath();
                if (parentpath != null) {
                    Object parent = parentpath.getLastPathComponent();
                    if (parent instanceof MondrianGuiDef.Join) {
                        int indexOfChild = tree.getModel().getIndexOfChild(
                            parent, value);
                        switch (indexOfChild) {
                        case 0:
                            prefix = workbench.getResourceConverter().getString(
                                "common.left.title",
                                "Left")
                                     + " ";
                            break;
                        case 1:
                            prefix = workbench.getResourceConverter().getString(
                                "common.right.title",
                                "Right")
                                     + " ";
                            break;
                        }
                    }
                }
            }
            if (value instanceof MondrianGuiDef.Join) {
                setText(
                    workbench.getResourceConverter().getFormattedString(
                        "schemaTreeCellRenderer.join.title",
                        "{0}Join",
                        prefix));
                super.setIcon(
                    new ImageIcon(
                        myClassLoader.getResource(
                            workbench.getResourceConverter().getGUIReference(
                                "join"))));
            } else if (value instanceof MondrianGuiDef.Table) {
                // Set the table name to alias if present.
                MondrianGuiDef.Table theTable = (MondrianGuiDef.Table) value;
                String theName =
                    (theTable.alias != null
                     && theTable.alias.trim().length() > 0)
                    ? theTable.alias
                    : theTable.name;
                setText(
                    workbench.getResourceConverter().getFormattedString(
                        "schemaTreeCellRenderer.table.title",
                        "{0}Table: {1}",
                        prefix.length() == 0
                            ? ""
                            : prefix + " : ",
                        theName));
                super.setIcon(
                    new ImageIcon(
                        myClassLoader.getResource(
                            workbench.getResourceConverter().getGUIReference(
                                "table"))));
            } else if (value instanceof MondrianGuiDef.View) {
                setText(
                    workbench.getResourceConverter().getFormattedString(
                        "schemaTreeCellRenderer.view.title",
                        "View"));
            }
            // REVIEW: Need to deal with InlineTable here
            this.getPreferredSize();
            // Do not remove this
            this.setPreferredSize(
                new Dimension(
                    this.getPreferredSize().width + 35,
                    24));
        } else if (value instanceof MondrianGuiDef.Level) {
            setText(invalidFlag, ((MondrianGuiDef.Level) value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench
                            .getResourceConverter().getGUIReference("level"))));
            // See earlier comments about setPreferredSize.
            this.setPreferredSize(
                new java.awt.Dimension(
                    this.getPreferredSize().width + 1,
                    25)); //Do not remove this
        } else if (value instanceof MondrianGuiDef.Measure) {
            setText(invalidFlag, ((MondrianGuiDef.Measure) value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "measure"))));
        } else if (value instanceof MondrianGuiDef.Formula) {
            setText(invalidFlag, ((MondrianGuiDef.Formula) value).getName());
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "formula"))));
        } else if (value instanceof MondrianGuiDef.MemberReaderParameter) {
            setText(
                invalidFlag,
                ((MondrianGuiDef.MemberReaderParameter) value).name);
        } else if (value instanceof MondrianGuiDef.Property) {
            setText(invalidFlag, ((MondrianGuiDef.Property) value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "property"))));
        } else if (value instanceof MondrianGuiDef.Schema) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.schema.title",
                    "Schema"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "schema"))));
        } else if (value instanceof MondrianGuiDef.NamedSet) {
            setText(invalidFlag, ((MondrianGuiDef.NamedSet) value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "namedSet"))));
        } else if (value instanceof MondrianGuiDef.CalculatedMember) {
            setText(
                invalidFlag, ((MondrianGuiDef.CalculatedMember) value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "calculatedMember"))));
        } else if (value instanceof MondrianGuiDef.CalculatedMemberProperty) {
            setText(
                invalidFlag,
                ((MondrianGuiDef.CalculatedMemberProperty) value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench
                            .getResourceConverter().getGUIReference("nopic"))));
        } else if (value instanceof MondrianGuiDef.UserDefinedFunction) {
            setText(
                invalidFlag, ((MondrianGuiDef.UserDefinedFunction) value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "userDefinedFunction"))));
        } else if (value instanceof MondrianGuiDef.MemberFormatter) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.memberFormatter.title",
                    "Member Formatter"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "format"))));
        } else if (value instanceof MondrianGuiDef.CellFormatter) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.cellFormatter.title",
                    "Cell Formatter"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "format"))));
        } else if (value instanceof MondrianGuiDef.PropertyFormatter) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.propertyFormatter.title",
                    "Property Formatter"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "format"))));
        } else if (value instanceof MondrianGuiDef.Script) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.script.title",
                    "Script"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "script"))));
        } else if (value instanceof MondrianGuiDef.Role) {
            setText(invalidFlag, ((MondrianGuiDef.Role) value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench
                            .getResourceConverter().getGUIReference("role"))));
        } else if (value instanceof MondrianGuiDef.Parameter) {
            setText(invalidFlag, ((MondrianGuiDef.Parameter) value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "parameter"))));
        } else if (value instanceof MondrianGuiDef.SchemaGrant) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.schemaGrant.title",
                    "Schema Grant"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "schemaGrant"))));
        } else if (value instanceof MondrianGuiDef.CubeGrant) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.cubeGrant.title",
                    "Cube Grant"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "cubeGrant"))));
        } else if (value instanceof MondrianGuiDef.DimensionGrant) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.dimensionGrant.title",
                    "Dimension Grant"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "dimensionGrant"))));
        } else if (value instanceof MondrianGuiDef.HierarchyGrant) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.hierarchyGrant.title",
                    "Hierarchy Grant"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "hierarchyGrant"))));
        } else if (value instanceof MondrianGuiDef.MemberGrant) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.memberGrant.title",
                    "Member Grant"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "memberGrant"))));
        } else if (value instanceof MondrianGuiDef.Annotations) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.annotations.title",
                    "Annotations"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "annotations"))));
        } else if (value instanceof MondrianGuiDef.Annotation) {
            setText(
                invalidFlag, ((MondrianGuiDef.Annotation)value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "annotation"))));
        } else if (value instanceof MondrianGuiDef.SQL) {
            setText(invalidFlag, ((MondrianGuiDef.SQL) value).dialect);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench
                            .getResourceConverter().getGUIReference("sql"))));
        } else if (value instanceof MondrianGuiDef.View) {
            setText(
                workbench.getResourceConverter().getString(
                    "common.view.title",
                    "View"));
        } else if (value instanceof MondrianGuiDef.VirtualCube) {
            setText(invalidFlag, ((MondrianGuiDef.VirtualCube) value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "virtualCube"))));
        } else if (value instanceof MondrianGuiDef.VirtualCubeDimension) {
            setText(
                invalidFlag,
                ((MondrianGuiDef.VirtualCubeDimension) value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "virtualCubeDimension"))));
        } else if (value instanceof MondrianGuiDef.VirtualCubeMeasure) {
            setText(
                invalidFlag, ((MondrianGuiDef.VirtualCubeMeasure) value).name);
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "virtualCubeMeasure"))));
        } else if (value instanceof MondrianGuiDef.AggName) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.aggName.title",
                    "Aggregate Name"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "aggTable"))));
        } else if (value instanceof MondrianGuiDef.AggForeignKey) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.aggForeignKey.title",
                    "Aggregate Foreign Key"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "aggForeignKey"))));
        } else if (value instanceof MondrianGuiDef.AggIgnoreColumn) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.aggIgnoreColumn.title",
                    "Aggregate Ignore Column"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "aggIgnoreColumn"))));
        } else if (value instanceof MondrianGuiDef.AggLevel) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.aggLevel.title", "Aggregate Level"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "aggLevel"))));
        } else if (value instanceof MondrianGuiDef.AggMeasure) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.aggMeasure.title",
                    "Aggregate Measure"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "aggMeasure"))));
        } else if (value instanceof MondrianGuiDef.AggPattern) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.aggPattern.title",
                    "Aggregate Pattern"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "aggPattern"))));
        } else if (value instanceof MondrianGuiDef.AggExclude) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.aggExclude.title",
                    "Aggregate Exclude"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "aggExclude"))));
        } else if (value instanceof MondrianGuiDef.Closure) {
            setText(
                invalidFlag, workbench.getResourceConverter().getString(
                    "common.closure.title",
                    "Closure"));
            super.setIcon(
                new ImageIcon(
                    myClassLoader.getResource(
                        workbench.getResourceConverter().getGUIReference(
                            "closure"))));
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

    public String invalid(
        JTree tree,
        TreePath tpath,
        Object value,
        MondrianGuiDef.Cube cube,
        MondrianGuiDef.Dimension parentDimension,
        MondrianGuiDef.Hierarchy parentHierarchy,
        MondrianGuiDef.Level parentLevel)
    {
        return ValidationUtils.invalid(
            new WorkbenchMessages(workbench.getResourceConverter()),
            new WorkbenchJdbcValidator(jdbcMetaData),
            new WorkbenchTreeModel((SchemaTreeModel) tree.getModel()),
            new WorkbenchTreeModelPath(tpath),
            value,
            cube,
            parentDimension,
            parentHierarchy,
            parentLevel,
            jdbcMetaData.getRequireSchema());
    }

    private boolean isInvalid(JTree tree, Object value, int row) {
        // getPathForRow(row) returns null for new objects added to tree in the
        // first run of rendering. Check for null before calling methods on
        // Treepath returned.
        return invalid(tree, tree.getPathForRow(row), value) != null;
    }

    public void setText(boolean invalidFlag, String myText) {
        if (invalidFlag) {
            myText = "<html><FONT COLOR=RED><b>x</b></FONT><FONT COLOR="
                     + getForeground().hashCode()
                     + ">"
                     + myText
                     + "</FONT></html>";
        }
        setText(myText);
    }

    /**
     * Called from {@link SchemaExplorer#resetMetaData(JdbcMetaData)}. A call to
     * {@link #updateUI} should be made on the owning SchemaFrame to reflect the
     * use of the JdbcMetaData being set.
     *
     * @param jdbcMetaData Meta data
     */
    public void setMetaData(JdbcMetaData jdbcMetaData) {
        this.jdbcMetaData = jdbcMetaData;
    }
}

// End SchemaTreeCellRenderer.java
