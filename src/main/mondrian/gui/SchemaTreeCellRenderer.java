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
        } else if (value instanceof MondrianDef.Dimension) {
            setText(((MondrianDef.Dimension)value).name);
            super.setIcon(new ImageIcon("images/dimension24.gif"));
        } else if (value instanceof MondrianDef.Measure) {
            setText(((MondrianDef.Measure)value).name);
            super.setIcon(new ImageIcon("images/measure24.gif"));
        } else if (value instanceof MondrianDef.Level) {
            setText(((MondrianDef.Level)value).name);
        } else if (value instanceof MondrianDef.CubeDimension) {
            setText(((MondrianDef.CubeDimension)value).name);
            super.setIcon(new ImageIcon("images/dimensionUsage24.gif"));
        } else if (value instanceof MondrianDef.DimensionUsage) {
            super.setIcon(new ImageIcon("images/dimensionUsage24.gif"));
            setText(((MondrianDef.DimensionUsage)value).name);
        } else if (value instanceof MondrianDef.Parameter) {
            setText(((MondrianDef.Parameter)value).name);
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
