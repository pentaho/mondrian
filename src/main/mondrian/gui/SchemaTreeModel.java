/*
 * $File$
 *
 * $DateTime$ - $Change$ - $Revision$ - $Author$
 */

package mondrian.gui;

import mondrian.olap.MondrianDef;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import java.util.Vector;

/**
 *
 * @author  sean
 */
public class SchemaTreeModel extends DefaultTreeModel {
    /**
     * @param arg0
     */
    public SchemaTreeModel() {
        super(null);
        // TODO Auto-generated constructor stub
    }

    MondrianDef.Schema schema;
    private Vector treeModelListeners = new Vector();

    /** Creates a new instance of SchemaTreeModel */
    public SchemaTreeModel(MondrianDef.Schema s) {
        super(new DefaultMutableTreeNode(s.name));
        this.schema = s;
    }


    /** Returns the child of <code>parent</code> at index <code>index</code>
     * in the parent's
     * child array.  <code>parent</code> must be a node previously obtained
     * from this data source. This should not return <code>null</code>
     * if <code>index</code>
     * is a valid index for <code>parent</code> (that is <code>index >= 0 &&
     * index < getChildCount(parent</code>)).
     *
     * @param   parent  a node in the tree, obtained from this data source
     * @return  the child of <code>parent</code> at index <code>index</code>
     *
     */
    public Object getChild(Object parent, int index) {
        if (parent instanceof MondrianDef.Column) {
            MondrianDef.Column c = (MondrianDef.Column)parent;
        } else if (parent instanceof MondrianDef.Cube) {
            MondrianDef.Cube c = (MondrianDef.Cube)parent;
            //return children in this order: dimensions, measures
            if (c.dimensions.length > index) {
                return c.dimensions[index];
            } else if (c.measures.length + c.dimensions.length > index) {
                return c.measures[index - c.dimensions.length];
            }
        } else if (parent instanceof MondrianDef.Dimension) {
            MondrianDef.Dimension d = (MondrianDef.Dimension)parent;
            if (d.hierarchies.length > index) {
                return d.hierarchies[index];
            }
        } else if (parent instanceof MondrianDef.DimensionUsage) {
            MondrianDef.DimensionUsage c = (MondrianDef.DimensionUsage)parent;
        } else if (parent instanceof MondrianDef.ExpressionView) {
            MondrianDef.ExpressionView ev = (MondrianDef.ExpressionView)parent;
            if (ev.expressions.length > index) {
                return ev.expressions[index];
            }
        } else if (parent instanceof MondrianDef.Hierarchy) {
            MondrianDef.Hierarchy h = (MondrianDef.Hierarchy)parent;
            //return children in this order: levels, memberReaderParameters
            if (h.levels.length > index) {
                return h.levels[index];
            } else if (h.memberReaderParameters.length + h.levels.length > index) {
                return h.memberReaderParameters[index];
            }
        } else if (parent instanceof MondrianDef.Join) {
            MondrianDef.Join j = (MondrianDef.Join)parent;
        } else if (parent instanceof MondrianDef.Level) {
            MondrianDef.Level l = (MondrianDef.Level)parent;
            if (l.properties.length > index) {
                return l.properties[index];
            }
        } else if (parent instanceof MondrianDef.Measure) {
            MondrianDef.Measure m = (MondrianDef.Measure)parent;
        } else if (parent instanceof MondrianDef.Parameter) {
            MondrianDef.Parameter p = (MondrianDef.Parameter)parent;
        } else if (parent instanceof MondrianDef.Property) {
            MondrianDef.Property p = (MondrianDef.Property)parent;
        } else if (parent instanceof MondrianDef.Schema) {
            MondrianDef.Schema s = (MondrianDef.Schema)parent;
            //return children in this order: cubes, virtual cubes, dimensions
            if (s.cubes.length > index) {
                return s.cubes[index];
            } else if (s.virtualCubes.length + s.cubes.length > index) {
                return s.virtualCubes[index - s.cubes.length];
            } else if (s.dimensions.length + s.virtualCubes.length + s.cubes.length > index) {
                return s.dimensions[index - s.cubes.length - s.virtualCubes.length];
            }
        } else if (parent instanceof MondrianDef.SQL) {
            MondrianDef.SQL s = (MondrianDef.SQL)parent;
        } else if (parent instanceof MondrianDef.Table) {
            MondrianDef.Table t = (MondrianDef.Table)parent;
        } else if (parent instanceof MondrianDef.View) {
            MondrianDef.View v = (MondrianDef.View)parent;
            if (v.selects.length > index) {
                return v.selects[index];
            }
        } else if (parent instanceof MondrianDef.VirtualCube) {
            MondrianDef.VirtualCube c = (MondrianDef.VirtualCube)parent;
            //return children in this order: dimensions, measures
            if (c.dimensions.length > index) {
                return c.dimensions[index];
            } else if (c.measures.length + c.dimensions.length > index) {
                return c.measures[index - c.dimensions.length];
            }
        } else if (parent instanceof MondrianDef.VirtualCubeDimension) {
            MondrianDef.VirtualCubeDimension vcd = (MondrianDef.VirtualCubeDimension)parent;
        } else if (parent instanceof MondrianDef.VirtualCubeMeasure) {
            MondrianDef.VirtualCubeMeasure vcd = (MondrianDef.VirtualCubeMeasure)parent;
        }
        return null;
    }

    /** Returns the number of children of <code>parent</code>.
     * Returns 0 if the node
     * is a leaf or if it has no children.  <code>parent</code> must be a node
     * previously obtained from this data source.
     *
     * @param   parent  a node in the tree, obtained from this data source
     * @return  the number of children of the node <code>parent</code>
     *
     */
    public int getChildCount(Object parent) {
        int childCount = 0;
        if (parent instanceof MondrianDef.Cube) {
            MondrianDef.Cube c = (MondrianDef.Cube)parent;
            if( c.measures != null ) childCount += c.measures.length;
            if( c.dimensions != null ) childCount += c.dimensions.length;
        } else if (parent instanceof MondrianDef.Dimension) {
            MondrianDef.Dimension d = (MondrianDef.Dimension)parent;
            if( d.hierarchies != null ) childCount += d.hierarchies.length;
        } else if (parent instanceof MondrianDef.ExpressionView) {
            MondrianDef.ExpressionView ev = (MondrianDef.ExpressionView)parent;
            if( ev.expressions != null ) childCount = ev.expressions.length;
        } else if (parent instanceof MondrianDef.Hierarchy) {
            MondrianDef.Hierarchy h = (MondrianDef.Hierarchy)parent;
            if( h.memberReaderParameters != null ) childCount += h.memberReaderParameters.length;
            if( h.levels != null ) childCount += h.levels.length;
        } else if (parent instanceof MondrianDef.Level) {
            MondrianDef.Level l = (MondrianDef.Level)parent;
            if( l.properties != null ) childCount = l.properties.length;
        } else if (parent instanceof MondrianDef.Schema) {
            MondrianDef.Schema s = (MondrianDef.Schema)parent;
            if( s.dimensions != null ) childCount += s.dimensions.length;
            if( s.virtualCubes != null ) childCount += s.virtualCubes.length;
            if( s.cubes != null ) childCount += s.cubes.length;
        } else if (parent instanceof MondrianDef.View) {
            MondrianDef.View v = (MondrianDef.View)parent;
            if( v.selects != null ) childCount += v.selects.length;
        } else if (parent instanceof MondrianDef.VirtualCube) {
            MondrianDef.VirtualCube c = (MondrianDef.VirtualCube)parent;
            if( c.measures != null ) childCount += c.measures.length;
            if( c.dimensions != null ) childCount += c.dimensions.length;
        }
        return childCount;
    }

    /** Returns the index of child in parent.  If <code>parent</code>
     * is <code>null</code> or <code>child</code> is <code>null</code>,
     * returns -1.
     *
     * @param parent a note in the tree, obtained from this data source
     * @param child the node we are interested in
     * @return the index of the child in the parent, or -1 if either
     *    <code>child</code> or <code>parent</code> are <code>null</code>
     *
     */
    public int getIndexOfChild(Object parent, Object child) {
        if (parent instanceof MondrianDef.Column) {
            return -1;
        } else if (parent instanceof MondrianDef.Cube) {
            MondrianDef.Cube c = (MondrianDef.Cube)parent;
            if (child instanceof MondrianDef.CubeDimension) {
                for (int i=0; i<c.dimensions.length; i++) {
                    if (c.dimensions[i].equals(child))
                        return i;
                }
                return -1;
            } else if (child instanceof MondrianDef.Measure) {
                for (int i=0; i<c.measures.length; i++) {
                    if (c.measures[i].equals(child))
                        return i + c.dimensions.length;
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianDef.Dimension) {
            MondrianDef.Dimension d = (MondrianDef.Dimension)parent;
            if (child instanceof MondrianDef.Hierarchy) {
                for (int i=0; i<d.hierarchies.length; i++) {
                    if (d.hierarchies[i].equals(child))
                        return i;
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianDef.ExpressionView) {
            MondrianDef.ExpressionView ev = (MondrianDef.ExpressionView)parent;
            if (child instanceof MondrianDef.SQL) {
                for (int i=0; i<ev.expressions.length; i++) {
                    if (ev.expressions[i].equals(child))
                        return i;
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianDef.Hierarchy) {
            MondrianDef.Hierarchy h = (MondrianDef.Hierarchy)parent;
            if (child instanceof MondrianDef.Level) {
                for (int i=0; i<h.levels.length; i++) {
                    if (h.levels[i].equals(child))
                        return i;
                }
                return -1;
            } else if (child instanceof MondrianDef.Parameter) {
                for (int i=0; i<h.memberReaderParameters.length; i++) {
                    if (h.memberReaderParameters[i].equals(child))
                        return i + h.levels.length;
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianDef.Level) {
            MondrianDef.Level l = (MondrianDef.Level)parent;
            if (child instanceof MondrianDef.Property) {
                for (int i=0; i<l.properties.length; i++) {
                    if (l.properties[i].equals(child))
                        return i;
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianDef.Measure) {
            MondrianDef.Measure m = (MondrianDef.Measure)parent;
            return -1;
        } else if (parent instanceof MondrianDef.Schema) {
            MondrianDef.Schema s = (MondrianDef.Schema)parent;
            //return children in this order: cubes, virtual cubes, dimensions
            if (child instanceof MondrianDef.Cube) {
                for (int i=0; i<s.cubes.length; i++) {
                    if (s.cubes[i].equals(child))
                        return i;
                }
                return -1;
            } else if (child instanceof MondrianDef.VirtualCube) {
                for (int i=0; i<s.virtualCubes.length; i++) {
                    if (s.virtualCubes[i].equals(child))
                        return i + s.cubes.length;
                }
                return -1;
            } else if (child instanceof MondrianDef.Dimension) {
                for (int i=0; i<s.dimensions.length; i++) {
                    if (s.dimensions[i].equals(child))
                        return i + s.cubes.length + s.dimensions.length;
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianDef.View) {
            MondrianDef.View v = (MondrianDef.View)parent;
            if (child instanceof MondrianDef.SQL) {
                for (int i=0; i<v.selects.length; i++) {
                    if (v.selects[i].equals(child)) {
                        return i;
                    }
                }
                return -1;
            }
            return -1;
        } else if (parent instanceof MondrianDef.VirtualCube) {
            MondrianDef.VirtualCube c = (MondrianDef.VirtualCube)parent;
            if (child instanceof MondrianDef.VirtualCubeDimension) {
                for (int i=0; i<c.dimensions.length; i++) {
                    if (c.dimensions[i].equals(child))
                        return i;
                }
                return -1;
            } else if (child instanceof MondrianDef.VirtualCubeMeasure) {
                for (int i=0; i<c.measures.length; i++) {
                    if (c.measures[i].equals(child))
                        return i + c.dimensions.length;
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianDef.VirtualCubeDimension) {
            MondrianDef.VirtualCubeDimension d = (MondrianDef.VirtualCubeDimension)parent;
            return -1;
        } else if (parent instanceof MondrianDef.VirtualCubeMeasure) {
            MondrianDef.VirtualCubeMeasure d = (MondrianDef.VirtualCubeMeasure)parent;
            return -1;
        } else {
            return -1;
        }
    }

    /** Returns the root of the tree.  Returns <code>null</code>
     * only if the tree has no nodes.
     *
     * @return  the root of the tree
     *
     */
    public Object getRoot() {
        return schema;
    }

    /** Returns <code>true</code> if <code>node</code> is a leaf.
     * It is possible for this method to return <code>false</code>
     * even if <code>node</code> has no children.
     * A directory in a filesystem, for example,
     * may contain no files; the node representing
     * the directory is not a leaf, but it also has no children.
     *
     * @param   node  a node in the tree, obtained from this data source
     * @return  true if <code>node</code> is a leaf
     *
     */
    public boolean isLeaf(Object node) {
        return getChildCount(node) == 0;
    }

}