/*
 * SchemaTreeCellRenderer.java
 *
 * Created on September 27, 2002, 2:40 PM
 */

package mondrian.gui;

import mondrian.olap.MondrianDef;

import javax.swing.*;
import java.awt.*;
import java.util.ResourceBundle;

/**
 * Render an entry for the tree
 * @author  sean
 */
public class SchemaTreeCellRenderer extends javax.swing.tree.DefaultTreeCellRenderer
{

	private final ClassLoader myClassLoader;
	/** Creates a new instance of SchemaTreeCellRenderer */
	public SchemaTreeCellRenderer()
	{
		super();
		myClassLoader = this.getClass().getClassLoader();
	}

	public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf, int row, boolean hasFocus)
	{
		super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);

		ResourceBundle resources = ResourceBundle.getBundle("mondrian.gui.resources.gui");

		if (value instanceof MondrianDef.Cube)
		{
			setText(((MondrianDef.Cube) value).name);
			super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("cube"))));
		}
		else if (value instanceof MondrianDef.Column)
		{
			setText(((MondrianDef.Column) value).name);
		}
		else if (value instanceof MondrianDef.Dimension)
		{
			setText(((MondrianDef.Dimension) value).name);
			super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("dimension"))));
		}
		else if (value instanceof MondrianDef.DimensionUsage)
		{
			super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("dimensionUsage"))));
			setText(((MondrianDef.DimensionUsage) value).name);
		}
		else if (value instanceof MondrianDef.Expression)
		{
			setText("Expression");
		}
		else if (value instanceof MondrianDef.ExpressionView)
		{
			setText("ExpressionView");
		}
		else if (value instanceof MondrianDef.Hierarchy)
		{
			setText("Hierarchy");
			super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("hierarchy"))));
		}
		else if (value instanceof MondrianDef.Join)
		{
			setText("Join");
		}
		else if (value instanceof MondrianDef.Level)
		{
			setText(((MondrianDef.Level) value).name);
			super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("level"))));
		}
		else if (value instanceof MondrianDef.Measure)
		{
			setText(((MondrianDef.Measure) value).name);
			super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("measure"))));
		}
		else if (value instanceof MondrianDef.Parameter)
		{
			setText(((MondrianDef.Parameter) value).name);
		}
		else if (value instanceof MondrianDef.Property)
		{
			setText(((MondrianDef.Property) value).name);
			super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("property"))));
		}
		else if (value instanceof MondrianDef.Relation)
		{
			setText("Relation");
		}
		else if (value instanceof MondrianDef.Schema)
		{
			setText("Schema");
			super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("schema"))));
		}
		else if (value instanceof MondrianDef.SQL)
		{
			setText("SQL");
		}
		else if (value instanceof MondrianDef.Table)
		{
			setText(((MondrianDef.Table) value).name);
		}
		else if (value instanceof MondrianDef.View)
		{
			setText("View");
		}
		else if (value instanceof MondrianDef.VirtualCube)
		{
			setText(((MondrianDef.VirtualCube) value).name);
			super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("virtualCube"))));
		}
		else if (value instanceof MondrianDef.VirtualCubeDimension)
		{
			setText(((MondrianDef.VirtualCubeDimension) value).name);
			super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("virtualCubeDimension"))));
		}
		else if (value instanceof MondrianDef.VirtualCubeMeasure)
		{
			setText(((MondrianDef.VirtualCubeMeasure) value).name);
			super.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("virtualCubeMeasure"))));
		}
		else if (value instanceof mondrian.xom.ElementDef)
		{
			setText(((mondrian.xom.ElementDef) value).getName());
		}
		else
		{
			super.setText("");
		}

		return this;

	}
}
