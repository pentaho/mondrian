/*
 * SchemaTreeCellRenderer.java
 *
 * Created on September 27, 2002, 2:40 PM
 */

package mondrian.gui;

import mondrian.olap.*;

import javax.swing.*;
import java.awt.*;

/**
 *
 * @author  sean
 */
public class SchemaTreeCellRenderer extends javax.swing.tree.DefaultTreeCellRenderer {
    
    /** Creates a new instance of SchemaTreeCellRenderer */
    public SchemaTreeCellRenderer() {
        super();
    }
    
        public Component getTreeCellRendererComponent(
                        JTree tree,
                        Object value,
                        boolean sel,
                        boolean expanded,
                        boolean leaf,
                        int row,
                        boolean hasFocus) {

        super.getTreeCellRendererComponent(
                        tree, value, sel,
                        expanded, leaf, row,
                        hasFocus);
        if (value instanceof MondrianDef.Cube) {
            setText(((MondrianDef.Cube)value).name);
            super.setIcon(new ImageIcon("images/cube24.gif"));
        } else if (value instanceof MondrianDef.Column) {
            setText(((MondrianDef.Column)value).name);
            //super.setIcon(new ImageIcon("images/column24.gif"));
        } else if (value instanceof MondrianDef.Dimension) {
            setText(((MondrianDef.Dimension)value).name);
            super.setIcon(new ImageIcon("images/dimension24.gif"));
        } else if (value instanceof MondrianDef.DimensionUsage) {
            super.setIcon(new ImageIcon("images/dimensionUsage24.gif"));
            setText(((MondrianDef.DimensionUsage)value).name);
        } else if (value instanceof MondrianDef.Expression) {
            //setText(((MondrianDef.Expression)value).getText());
            setText("Expression");
            //super.setIcon(new ImageIcon("images/expression24.gif"));
        } else if (value instanceof MondrianDef.ExpressionView) {
            //setText(((MondrianDef.ExpressionView)value).name);
            setText("ExpressionView");
            //super.setIcon(new ImageIcon("images/expressionView24.gif"));
        } else if (value instanceof MondrianDef.Hierarchy) {
            //setText(((MondrianDef.Hierarchy)value).name);
            setText("Hierarchy");
            super.setIcon(new ImageIcon("images/hierarchy24.gif"));
        } else if (value instanceof MondrianDef.Join) {
            setText("Join");
            //setText(((MondrianDef.Join)value).name);
            //super.setIcon(new ImageIcon("images/join24.gif"));
        } else if (value instanceof MondrianDef.Level) {
            setText(((MondrianDef.Level)value).name);
            super.setIcon(new ImageIcon("images/level24.gif"));
        } else if (value instanceof MondrianDef.Measure) {
            setText(((MondrianDef.Measure)value).name);
            super.setIcon(new ImageIcon("images/measure24.gif"));
        } else if (value instanceof MondrianDef.Parameter) {
            setText(((MondrianDef.Parameter)value).name);
        } else if (value instanceof MondrianDef.Property) {
            setText(((MondrianDef.Property)value).name);
        } else if (value instanceof MondrianDef.Relation) {
            setText("Relation");
            //setText(((MondrianDef.Relation)value).name);
        } else if (value instanceof MondrianDef.Schema) {
            setText("Schema");
            super.setIcon(new ImageIcon("images/schema24.gif"));
        } else if (value instanceof MondrianDef.SQL) {
            setText("SQL");
            //setText(((MondrianDef.SQL)value).name);
        } else if (value instanceof MondrianDef.Table) {
            setText(((MondrianDef.Table)value).name);
        } else if (value instanceof MondrianDef.View) {
            setText("View");
            //setText(((MondrianDef.View)value).name);
        } else if (value instanceof MondrianDef.VirtualCube) {
            setText(((MondrianDef.VirtualCube)value).name);
            super.setIcon(new ImageIcon("images/virtualCube24.gif"));
        } else if (value instanceof MondrianDef.VirtualCubeDimension) {
            setText(((MondrianDef.VirtualCubeDimension)value).name);
            super.setIcon(new ImageIcon("images/virtualCubeDimension24.gif"));
        } else if (value instanceof MondrianDef.VirtualCubeMeasure) {
            setText(((MondrianDef.VirtualCubeMeasure)value).name);
            super.setIcon(new ImageIcon("images/virtualCubeMeasure24.gif"));
        } else if (value instanceof mondrian.xom.ElementDef) {
            setText(((mondrian.xom.ElementDef)value).getName());
        } else {
            super.setText("");
        }
        
        return this;

        }    
}
