/*
 * SchemaTreeModel.java
 *
 * Created on September 27, 2002, 1:33 PM
 */

package mondrian.gui;

import javax.swing.tree.*;
import javax.swing.event.*;

import java.util.*;

import mondrian.olap.*;

/**
 *
 * @author  sean
 */
public class SchemaTreeModel implements TreeModel {
    MondrianDef.Schema schema;
    private Vector treeModelListeners = new Vector();

    /** Creates a new instance of SchemaTreeModel */
    public SchemaTreeModel(MondrianDef.Schema s) {
        this.schema = s;
    }
    
    /** Adds a listener for the <code>TreeModelEvent</code>
     * posted after the tree changes.
     *
     * @param   l       the listener to add
     * @see     #removeTreeModelListener
     *
     */
    public void addTreeModelListener(TreeModelListener l) {
        treeModelListeners.addElement(l);
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
        if (parent instanceof MondrianDef.Schema) {
            MondrianDef.Schema s = (MondrianDef.Schema)parent;
            //return children in this order: cubes, virtual cubes, dimensions
            if (s.cubes.length > index) {
                return s.cubes[index];
            } else if (s.virtualCubes.length + s.cubes.length > index) {
                return s.virtualCubes[index - s.cubes.length];
            } else if (s.dimensions.length + s.virtualCubes.length + s.cubes.length > index) {
                return s.dimensions[index - s.cubes.length - s.virtualCubes.length];
            } else {
                return null;
            }
        } else if (parent instanceof MondrianDef.Cube) {
            MondrianDef.Cube c = (MondrianDef.Cube)parent;
            //return children in this order: dimensions, measures
            if (c.dimensions.length > index) {
                return c.dimensions[index];
            } else if (c.measures.length + c.dimensions.length > index) {
                return c.measures[index - c.dimensions.length];
            } else {
                return null;
            }
        } else if (parent instanceof MondrianDef.VirtualCube) {
            MondrianDef.VirtualCube c = (MondrianDef.VirtualCube)parent;
            //return children in this order: dimensions, measures
            if (c.dimensions.length > index) {
                return c.dimensions[index];
            } else if (c.measures.length + c.dimensions.length > index) {
                return c.measures[index - c.dimensions.length];
            } else {
                return null;
            }
        } else if (parent instanceof MondrianDef.Measure) {
            MondrianDef.Measure m = (MondrianDef.Measure)parent;
            return null;
        } else if (parent instanceof MondrianDef.Dimension) {
            MondrianDef.Dimension d = (MondrianDef.Dimension)parent;
            if (d.hierarchies.length > index) {
                return d.hierarchies[index];
            } else {
                return null;
            }
        } else if (parent instanceof MondrianDef.Hierarchy) {
            MondrianDef.Hierarchy h = (MondrianDef.Hierarchy)parent;
            //return children in this order: levels, memberReaderParameters
            if (h.levels.length > index) {
                return h.levels[index];
            } else if (h.memberReaderParameters.length + h.levels.length > index) {
                return h.memberReaderParameters[index];
            } else {
                return null;
            }
        } else {
            return null;
        }
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
        if (parent instanceof MondrianDef.Schema) {
            MondrianDef.Schema s = (MondrianDef.Schema)parent;
            return s.dimensions.length + s.virtualCubes.length + s.cubes.length;
        } else if (parent instanceof MondrianDef.Cube) {
            MondrianDef.Cube c = (MondrianDef.Cube)parent;
            return c.measures.length + c.dimensions.length;
        } else if (parent instanceof MondrianDef.VirtualCube) {
            MondrianDef.VirtualCube c = (MondrianDef.VirtualCube)parent;
            return c.measures.length + c.dimensions.length;
        } else if (parent instanceof MondrianDef.Measure) {
            MondrianDef.Measure m = (MondrianDef.Measure)parent;
            return 0;
        } else if (parent instanceof MondrianDef.Dimension) {
            MondrianDef.Dimension d = (MondrianDef.Dimension)parent;
            return d.hierarchies.length;
        } else if (parent instanceof MondrianDef.Hierarchy) {
            MondrianDef.Hierarchy h = (MondrianDef.Hierarchy)parent;
            return h.memberReaderParameters.length + h.levels.length;
        } else {
            return 0;
        }
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
        if (parent instanceof MondrianDef.Schema) {
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
        } else if (parent instanceof MondrianDef.Measure) {
            MondrianDef.Measure m = (MondrianDef.Measure)parent;
            return -1;
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
        } else if (parent instanceof MondrianDef.VirtualCubeDimension) {
            MondrianDef.VirtualCubeDimension d = (MondrianDef.VirtualCubeDimension)parent;
            return -1;
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
    
    /** Removes a listener previously added with
     * <code>addTreeModelListener</code>.
     *
     * @see     #addTreeModelListener
     * @param   l       the listener to remove
     *
     */
    public void removeTreeModelListener(TreeModelListener l) {
        treeModelListeners.removeElement(l);
    }
    
    /** Messaged when the user has altered the value for the item identified
     * by <code>path</code> to <code>newValue</code>.
     * If <code>newValue</code> signifies a truly new value
     * the model should post a <code>treeNodesChanged</code> event.
     *
     * @param path path to the node that the user has altered
     * @param newValue the new value from the TreeCellEditor
     *
     */
    public void valueForPathChanged(TreePath path, Object newValue) {
    }
    
}
