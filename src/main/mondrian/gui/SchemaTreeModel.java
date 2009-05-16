/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2002-2009 Julian Hyde and others
// Copyright (C) 2006-2007 CINCOM SYSTEMS, INC.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.gui;

import javax.swing.tree.TreePath;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import java.util.Vector;

/**
 *
 * @author  sean
 * @version $Id$
 */
public class SchemaTreeModel extends DefaultTreeModel {
    public SchemaTreeModel() {
        super(null);
        // TODO Auto-generated constructor stub
    }

    MondrianGuiDef.Schema schema;
    private Vector treeModelListeners = new Vector();

    /** Creates a new instance of SchemaTreeModel */
    public SchemaTreeModel(MondrianGuiDef.Schema s) {
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
        if (parent instanceof MondrianGuiDef.Column) {
            MondrianGuiDef.Column c = (MondrianGuiDef.Column)parent;
        } else if (parent instanceof MondrianGuiDef.Cube) {
            MondrianGuiDef.Cube c = (MondrianGuiDef.Cube)parent;
            //return children in this order: fact table, dimensions, measures, calculatedMembers, namedSets
            if (1 > index) {
                return c.fact;
            } else if (1 + c.dimensions.length > index) {
                return c.dimensions[index - 1];
            } else if (1 + c.measures.length + c.dimensions.length > index) {
                return c.measures[index - c.dimensions.length - 1];
            } else if (1 + c.calculatedMembers.length + c.measures.length + c.dimensions.length > index) {
                return c.calculatedMembers[index - c.dimensions.length - c.measures.length - 1];
            } else if (1 + c.namedSets.length + c.calculatedMembers.length + c.measures.length + c.dimensions.length > index) {
                return c.namedSets[index - c.dimensions.length - c.measures.length - c.calculatedMembers.length - 1];
            }
        } else if (parent instanceof MondrianGuiDef.Dimension) {
            MondrianGuiDef.Dimension d = (MondrianGuiDef.Dimension)parent;
            if (d.hierarchies.length > index) {
                return d.hierarchies[index];
            }
        } else if (parent instanceof MondrianGuiDef.DimensionUsage) {
            MondrianGuiDef.DimensionUsage c = (MondrianGuiDef.DimensionUsage)parent;

        } else if (parent instanceof MondrianGuiDef.ExpressionView) {
            MondrianGuiDef.ExpressionView ev = (MondrianGuiDef.ExpressionView)parent;
            if (ev.expressions.length > index) {
                return ev.expressions[index];
            }
        } else if (parent instanceof MondrianGuiDef.Hierarchy) {
            MondrianGuiDef.Hierarchy h = (MondrianGuiDef.Hierarchy)parent;
            //return children in this order: levels, memberReaderParameters, relation
            if (h.levels.length > index) {
                return h.levels[index];
            } else if (h.memberReaderParameters.length + h.levels.length > index) {
                return h.memberReaderParameters[index - h.levels.length];
            } else if (1 + h.memberReaderParameters.length + h.levels.length > index) {
                return h.relation;
            }
        } else if (parent instanceof MondrianGuiDef.Join) {
            MondrianGuiDef.Join j = (MondrianGuiDef.Join)parent;
            if (index == 0) {
                return j.left;
            } else if (index == 1) {
                return j.right;
            }
        } else if (parent instanceof MondrianGuiDef.Level) {
            MondrianGuiDef.Level l = (MondrianGuiDef.Level)parent;
            if (l.properties != null && l.properties.length > index) {
                return l.properties[index];
            }
            int otherIndex = 0;
            if (l.properties != null) {
                otherIndex = index - l.properties.length;
            }
            if (otherIndex >= 0) {
                int counter = 0;
                if (l.keyExp != null) {
                    if (counter == otherIndex) {
                        return l.keyExp;
                    }
                    counter += 1;
                }
                if (l.nameExp != null) {
                    if (counter == otherIndex) {
                        return l.nameExp;
                    }
                    counter += 1;
                }
                if (l.ordinalExp != null) {
                    if (counter == otherIndex) {
                        return l.ordinalExp;
                    }
                    counter += 1;
                }
                if (l.parentExp != null) {
                    if (counter == otherIndex) {
                        return l.parentExp;
                    }
                }
                if (l.closure != null) {
                    return l.closure;
                }
            }
        } else if (parent instanceof MondrianGuiDef.CalculatedMember) {
            MondrianGuiDef.CalculatedMember c = (MondrianGuiDef.CalculatedMember)parent;
            int fc = 0;
            if (c.formulaElement != null) {
                fc = 1;
            }
            if (1 > index && fc == 1) {
                return c.formulaElement;
            } else if (fc + c.memberProperties.length > index) {
                return c.memberProperties[index - fc];
            }
        } else if (parent instanceof MondrianGuiDef.Measure) {
            MondrianGuiDef.Measure m = (MondrianGuiDef.Measure)parent;
            if (m.measureExp != null) {
                return m.measureExp;
            }
        } else if (parent instanceof MondrianGuiDef.NamedSet) {
            MondrianGuiDef.NamedSet m = (MondrianGuiDef.NamedSet)parent;
            if (m.formulaElement != null) {
                return m.formulaElement;
            }
        /*
        } else if (parent instanceof MondrianGuiDef.MemberReaderParameter) {
            MondrianGuiDef.MemberReaderParameter p = (MondrianGuiDef.MemberReaderParameter)parent;
        } else if (parent instanceof MondrianGuiDef.Property) {
            MondrianGuiDef.Property p = (MondrianGuiDef.Property)parent;
        }
        */
        } else if (parent instanceof MondrianGuiDef.Schema) {
            MondrianGuiDef.Schema s = (MondrianGuiDef.Schema)parent;
            //return children in this order: cubes,  dimensions, namedSets, userDefinedFunctions, virtual cubes, roles
            if (s.cubes.length > index) {
                return s.cubes[index];
            } else if (s.dimensions.length + s.cubes.length > index) {
                return s.dimensions[index - s.cubes.length];
            } else if (s.namedSets.length + s.dimensions.length + s.cubes.length > index) {
                return s.namedSets[index - s.cubes.length - s.dimensions.length];
            } else if (s.userDefinedFunctions.length + s.namedSets.length + s.dimensions.length + s.cubes.length > index) {
                return s.userDefinedFunctions[index - s.cubes.length - s.dimensions.length - s.namedSets.length];
            } else if (s.virtualCubes.length + s.userDefinedFunctions.length + s.namedSets.length + s.dimensions.length + s.cubes.length > index) {
                return s.virtualCubes[index - s.cubes.length - s.dimensions.length - s.namedSets.length - s.userDefinedFunctions.length];
            } else if (s.roles.length + s.virtualCubes.length + s.userDefinedFunctions.length + s.namedSets.length + s.dimensions.length + s.cubes.length > index) {
                return s.roles[index - s.cubes.length - s.dimensions.length - s.namedSets.length - s.userDefinedFunctions.length - s.virtualCubes.length];
            } else if (s.parameters.length + s.roles.length + s.virtualCubes.length + s.userDefinedFunctions.length + s.namedSets.length + s.dimensions.length + s.cubes.length > index) {
                return s.parameters[index - s.cubes.length - s.dimensions.length - s.namedSets.length - s.userDefinedFunctions.length - s.virtualCubes.length - s.roles.length];
            }
        } else if (parent instanceof MondrianGuiDef.SQL) {
            MondrianGuiDef.SQL s = (MondrianGuiDef.SQL)parent;
        } else if (parent instanceof MondrianGuiDef.Table) {
            MondrianGuiDef.Table t = (MondrianGuiDef.Table)parent;
            if (t.aggTables.length > index) {
                return t.aggTables[index];
            } else if (t.aggExcludes.length + t.aggTables.length > index) {
                return t.aggExcludes[index - t.aggTables.length];
            }
        } else if (parent instanceof MondrianGuiDef.AggTable) {
            MondrianGuiDef.AggTable t = (MondrianGuiDef.AggTable)parent;
            int fc = 0;
            if (t.factcount != null) {
                fc = 1;
            }
            if (1 > index && fc == 1) {
                return t.factcount;
            } else if (fc + t.ignoreColumns.length > index) {
                return t.ignoreColumns[index - fc];
            } else if (fc + t.foreignKeys.length + t.ignoreColumns.length > index) {
                return t.foreignKeys[index - t.ignoreColumns.length - fc];
            } else if (fc + t.measures.length + t.foreignKeys.length + t.ignoreColumns.length > index) {
                return t.measures[index - t.ignoreColumns.length - t.foreignKeys.length - fc];
            } else if (fc + t.levels.length + t.measures.length + t.foreignKeys.length + t.ignoreColumns.length > index) {
                return t.levels[index - t.ignoreColumns.length - t.foreignKeys.length - t.measures.length - fc];
            } else if (t instanceof MondrianGuiDef.AggPattern) {
                if (((MondrianGuiDef.AggPattern) t).excludes.length  + t.levels.length + t.measures.length + t.foreignKeys.length + t.ignoreColumns.length + fc > index) {
                    return ((MondrianGuiDef.AggPattern) t).excludes[index - t.ignoreColumns.length - t.foreignKeys.length - t.measures.length - t.levels.length - fc];
                }
            }
        } else if (parent instanceof MondrianGuiDef.View) {
            MondrianGuiDef.View v = (MondrianGuiDef.View)parent;
            if (v.selects.length > index) {
                return v.selects[index];
            }
        } else if (parent instanceof MondrianGuiDef.VirtualCube) {
            MondrianGuiDef.VirtualCube c = (MondrianGuiDef.VirtualCube)parent;
            //return children in this order: dimensions, measures
            if (c.dimensions.length > index) {
                return c.dimensions[index];
            } else if (c.measures.length + c.dimensions.length > index) {
                return c.measures[index - c.dimensions.length];
            } else if (c.calculatedMembers.length + c.measures.length + c.dimensions.length > index) {
                return c.calculatedMembers[index - c.dimensions.length - c.measures.length];
            }
        } else if (parent instanceof MondrianGuiDef.VirtualCubeDimension) {
            MondrianGuiDef.VirtualCubeDimension vcd = (MondrianGuiDef.VirtualCubeDimension)parent;
        } else if (parent instanceof MondrianGuiDef.VirtualCubeMeasure) {
            MondrianGuiDef.VirtualCubeMeasure vcd = (MondrianGuiDef.VirtualCubeMeasure)parent;
        } else if (parent instanceof MondrianGuiDef.Role) {
            MondrianGuiDef.Role c = (MondrianGuiDef.Role)parent;
            //return children in this order: schemagrant
            if (c.schemaGrants.length > index) {
                return c.schemaGrants[index];
            }
        } else if (parent instanceof MondrianGuiDef.SchemaGrant) {
            MondrianGuiDef.SchemaGrant c = (MondrianGuiDef.SchemaGrant)parent;
            //return children in this order: cubegrant
            if (c.cubeGrants.length > index) {
                return c.cubeGrants[index];
            }
        } else if (parent instanceof MondrianGuiDef.CubeGrant) {
            MondrianGuiDef.CubeGrant c = (MondrianGuiDef.CubeGrant)parent;
            //return children in this order: dimensiongrant, hierarchygrant
            if (c.dimensionGrants.length > index) {
                return c.dimensionGrants[index];
            } else if (c.hierarchyGrants.length + c.dimensionGrants.length > index) {
                return c.hierarchyGrants[index - c.dimensionGrants.length];
            }
        } else if (parent instanceof MondrianGuiDef.HierarchyGrant) {
            MondrianGuiDef.HierarchyGrant c = (MondrianGuiDef.HierarchyGrant)parent;
            //return children in this order: membergrant
            if (c.memberGrants.length > index) {
                return c.memberGrants[index];
            }
        } else if (parent instanceof MondrianGuiDef.Closure) {
            MondrianGuiDef.Closure c = (MondrianGuiDef.Closure)parent;
            if (c.table != null) {
                return c.table;
            }
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
        if (parent instanceof MondrianGuiDef.Cube) {
            MondrianGuiDef.Cube c = (MondrianGuiDef.Cube)parent;
            if (c.fact != null) {
                childCount += 1;
            }
            if (c.dimensions != null) {
                childCount += c.dimensions.length;
            }
            if (c.measures != null) {
                childCount += c.measures.length;
            }
            if (c.calculatedMembers != null) {
                childCount += c.calculatedMembers.length;
            }
            if (c.namedSets != null) {
                childCount += c.namedSets.length;
            }
        } else if (parent instanceof MondrianGuiDef.Dimension) {
            MondrianGuiDef.Dimension d = (MondrianGuiDef.Dimension)parent;
            if (d.hierarchies != null) {
                childCount += d.hierarchies.length;
            }
        } else if (parent instanceof MondrianGuiDef.ExpressionView) {
            MondrianGuiDef.ExpressionView ev = (MondrianGuiDef.ExpressionView)parent;
            if (ev.expressions != null) {
                childCount = ev.expressions.length;
            }
        } else if (parent instanceof MondrianGuiDef.Hierarchy) {
            MondrianGuiDef.Hierarchy h = (MondrianGuiDef.Hierarchy)parent;
            if (h.memberReaderParameters != null) {
                childCount += h.memberReaderParameters.length;
            }
            if (h.levels != null) {
                childCount += h.levels.length;
            }
            if (h.relation != null) {
                childCount += 1;
            }
        } else if (parent instanceof MondrianGuiDef.Join) {
            //MondrianGuiDef.Join j = (MondrianGuiDef.Join)parent;
            childCount += 2;
        } else if (parent instanceof MondrianGuiDef.Table) {
            MondrianGuiDef.Table h = (MondrianGuiDef.Table)parent;
            if (h.aggTables != null) {
                childCount += h.aggTables.length;
            }
            if (h.aggExcludes != null) {
                childCount += h.aggExcludes.length;
            }
        } else if (parent instanceof MondrianGuiDef.AggTable) {
            MondrianGuiDef.AggTable h = (MondrianGuiDef.AggTable)parent;
            if (h.factcount != null) {
                childCount += 1;
            }
            if (h.ignoreColumns != null) {
                childCount += h.ignoreColumns.length;
            }
            if (h.foreignKeys != null) {
                childCount += h.foreignKeys.length;
            }
            if (h.measures != null) {
                childCount += h.measures.length;
            }
            if (h.levels != null) {
                childCount += h.levels.length;
            }
            if (parent instanceof MondrianGuiDef.AggPattern) {
                if (((MondrianGuiDef.AggPattern)h).excludes != null) {
                    childCount += ((MondrianGuiDef.AggPattern)h).excludes.length;
                }
            }
        } else if (parent instanceof MondrianGuiDef.Level) {
            MondrianGuiDef.Level l = (MondrianGuiDef.Level)parent;
            if (l.properties != null) {
                childCount = l.properties.length;
            }
            if (l.keyExp != null) {
                childCount += 1;
            }
            if (l.nameExp != null) {
                childCount += 1;
            }
            if (l.ordinalExp != null) {
                childCount += 1;
            }
            if (l.parentExp != null) {
                childCount += 1;
            }
            if (l.closure != null) {
                childCount += 1;
            }
        } else if (parent instanceof MondrianGuiDef.CalculatedMember) {
            MondrianGuiDef.CalculatedMember l = (MondrianGuiDef.CalculatedMember)parent;
            if (l.memberProperties != null) {
                childCount = l.memberProperties.length;
            }
        } else if (parent instanceof MondrianGuiDef.Schema) {
            //return children in this order: cubes,  dimensions, namedSets, userDefinedFunctions, virtual cubes, roles
            MondrianGuiDef.Schema s = (MondrianGuiDef.Schema)parent;
            if (s.cubes != null) {
                childCount += s.cubes.length;
            }
            if (s.dimensions != null) {
                childCount += s.dimensions.length;
            }
            if (s.namedSets != null) {
                childCount += s.namedSets.length;
            }
            if (s.userDefinedFunctions != null) {
                childCount += s.userDefinedFunctions.length;
            }
            if (s.virtualCubes != null) {
                childCount += s.virtualCubes.length;
            }
            if (s.roles != null) {
                childCount += s.roles.length;
            }
        } else if (parent instanceof MondrianGuiDef.View) {
            MondrianGuiDef.View v = (MondrianGuiDef.View)parent;
            if (v.selects != null) {
                childCount += v.selects.length;
            }
        } else if (parent instanceof MondrianGuiDef.VirtualCube) {
            MondrianGuiDef.VirtualCube c = (MondrianGuiDef.VirtualCube)parent;
            if (c.dimensions != null) {
                childCount += c.dimensions.length;
            }
            if (c.measures != null) {
                childCount += c.measures.length;
            }
            if (c.calculatedMembers != null) {
                childCount += c.calculatedMembers.length;
            }
        } else if (parent instanceof MondrianGuiDef.Role) {
            MondrianGuiDef.Role c = (MondrianGuiDef.Role)parent;
            if (c.schemaGrants != null) {
                childCount += c.schemaGrants.length;
            }
        } else if (parent instanceof MondrianGuiDef.SchemaGrant) {
            MondrianGuiDef.SchemaGrant c = (MondrianGuiDef.SchemaGrant)parent;
            if (c.cubeGrants != null) {
                childCount += c.cubeGrants.length;
            }
        } else if (parent instanceof MondrianGuiDef.CubeGrant) {
            MondrianGuiDef.CubeGrant c = (MondrianGuiDef.CubeGrant)parent;
            if (c.dimensionGrants != null) {
                childCount += c.dimensionGrants.length;
            }
            if (c.hierarchyGrants != null) {
                childCount += c.hierarchyGrants.length;
            }
        } else if (parent instanceof MondrianGuiDef.HierarchyGrant) {
            MondrianGuiDef.HierarchyGrant c = (MondrianGuiDef.HierarchyGrant)parent;
            if (c.memberGrants != null) {
                childCount += c.memberGrants.length;
            }
        } else if (parent instanceof MondrianGuiDef.Closure) {
            MondrianGuiDef.Closure c = (MondrianGuiDef.Closure)parent;
            if (c.table != null) {
                childCount += 1;
            }
        } else if (parent instanceof MondrianGuiDef.Measure) {
            MondrianGuiDef.Measure m = (MondrianGuiDef.Measure)parent;
            if (m.measureExp != null) {
                childCount += 1;
            }
        } else if (parent instanceof MondrianGuiDef.NamedSet) {
            MondrianGuiDef.NamedSet m = (MondrianGuiDef.NamedSet)parent;
            if (m.formulaElement != null) {
                childCount += 1;
            }
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
        if (parent instanceof MondrianGuiDef.Column) {
            return -1;
        } else if (parent instanceof MondrianGuiDef.Cube) {
            MondrianGuiDef.Cube c = (MondrianGuiDef.Cube)parent;
            if (child instanceof MondrianGuiDef.RelationOrJoin) {
                if (c.fact.equals(child)) {
                    return 0;
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.CubeDimension) {
                for (int i = 0; i < c.dimensions.length; i++) {
                    if (c.dimensions[i].equals(child) && (c.dimensions[i] == child)) {
                        // check equality of parent class attributes
                        MondrianGuiDef.CubeDimension match = (MondrianGuiDef.CubeDimension) c.dimensions[i];
                        MondrianGuiDef.CubeDimension d = (MondrianGuiDef.CubeDimension) child;
                        if (((match.name == null && d.name == null) || (match.name != null && match.name.equals(d.name))) &&
                                ((match.caption == null && d.caption == null) || (match.caption != null && match.caption.equals(d.name))) &&
                                ((match.foreignKey == null && d.foreignKey == null) || (match.foreignKey != null && match.name.equals(d.foreignKey)))) {
                            return i  + 1;
                        }
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.Measure) {
                for (int i = 0; i < c.measures.length; i++) {
                    if (c.measures[i].equals(child)) {
                        return i + c.dimensions.length  + 1;}
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.CalculatedMember) {
                for (int i = 0; i < c.calculatedMembers.length; i++) {
                    if (c.calculatedMembers[i].equals(child)) {
                        return i + c.measures.length + c.dimensions.length  + 1;}
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.NamedSet) {
                for (int i = 0; i < c.namedSets.length; i++) {
                    if (c.namedSets[i].equals(child)) {
                        return i + c.calculatedMembers.length + c.measures.length + c.dimensions.length  + 1;}
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.Dimension) {
            MondrianGuiDef.Dimension d = (MondrianGuiDef.Dimension)parent;
            if (child instanceof MondrianGuiDef.Hierarchy) {
                for (int i = 0; i < d.hierarchies.length; i++) {
                    if (d.hierarchies[i].equals(child) && (d.hierarchies[i] == child)) {
                        return i;
                    }
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.ExpressionView) {
            MondrianGuiDef.ExpressionView ev = (MondrianGuiDef.ExpressionView)parent;
            if (child instanceof MondrianGuiDef.SQL) {
                for (int i = 0; i < ev.expressions.length; i++) {
                    // if (ev.expressions[i].equals(child)) {
                    if (ev.expressions[i] == child) {  // object reference is equal
                        return i;
                    }
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.Hierarchy) {
            MondrianGuiDef.Hierarchy h = (MondrianGuiDef.Hierarchy)parent;
            if (child instanceof MondrianGuiDef.Level) {
                for (int i = 0; i < h.levels.length; i++) {
                    if (h.levels[i].equals(child)) {
                        return i;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.MemberReaderParameter) {
                for (int i = 0; i < h.memberReaderParameters.length; i++) {
                    if (h.memberReaderParameters[i].equals(child)) {
                        return i + h.levels.length;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.RelationOrJoin) {
                if (h.relation.equals(child) && (h.relation == child)) {
                    return h.levels.length + h.memberReaderParameters.length; }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.Level) {
            MondrianGuiDef.Level l = (MondrianGuiDef.Level)parent;
            int counter = 0;
            if (l.properties != null) {
                counter = l.properties.length;
            }
            if (child instanceof MondrianGuiDef.Property) {
                for (int i = 0; i < l.properties.length; i++) {
                    if (l.properties[i].equals(child)) {
                        return i;
                    }
                }
                return -1;
            }
            if (child instanceof MondrianGuiDef.KeyExpression) {
                if (child.equals(l.keyExp)) {
                    return counter;
                }
                return -1;
            }
            if (l.keyExp != null) {
                counter += 1;
            }
            if (child instanceof MondrianGuiDef.NameExpression) {
                if (child.equals(l.nameExp)) {
                    return counter;
                }
                return -1;
            }
            if (l.nameExp != null) {
                counter += 1;
            }
            if (child instanceof MondrianGuiDef.OrdinalExpression) {
                if (child.equals(l.ordinalExp)) {
                    return counter;
                }
                return -1;
            }
            if (l.ordinalExp != null) {
                counter += 1;
            }
            if (child instanceof MondrianGuiDef.ParentExpression) {
                if (child.equals(l.parentExp)) {
                    return counter;
                }
                return -1;
            }
            if (l.parentExp != null) {
                counter += 1;
            }
            if (child instanceof MondrianGuiDef.Closure) {
                if (child.equals(l.closure)) {
                    return counter;
                }
                return -1;
            }
            return -1;
        } else if (parent instanceof MondrianGuiDef.Join) {
            MondrianGuiDef.Join j = (MondrianGuiDef.Join)parent;
            if (child instanceof MondrianGuiDef.RelationOrJoin) {
                if (j.left.equals(child)) {
                    return 0;
                } else if (j.right.equals(child)) {
                    return 1;
                } else {
                    return -1;
                }
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.Table) {
            MondrianGuiDef.Table l = (MondrianGuiDef.Table)parent;
            if (child instanceof MondrianGuiDef.AggTable) {
                for (int i = 0; i < l.aggTables.length; i++) {
                    if (l.aggTables[i].equals(child)) {
                        return i;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.AggExclude) {
                for (int i = 0; i < l.aggExcludes.length; i++) {
                    if (l.aggExcludes[i].equals(child)) {
                        return i + l.aggTables.length;
                    }
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.AggTable) {
            MondrianGuiDef.AggTable l = (MondrianGuiDef.AggTable)parent;
            if (child instanceof MondrianGuiDef.AggFactCount) {
                if (l.factcount.equals(child)) {
                    return 0;
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.AggIgnoreColumn) {
                for (int i = 0; i < l.ignoreColumns.length; i++) {
                    if (l.ignoreColumns[i].equals(child)) {
                        return i;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.AggForeignKey) {
                for (int i = 0; i < l.foreignKeys.length; i++) {
                    if (l.foreignKeys[i].equals(child)) {
                        return i + l.ignoreColumns.length;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.AggMeasure) {
                for (int i = 0; i < l.measures.length; i++) {
                    if (l.measures[i].equals(child)) {
                        return i + l.ignoreColumns.length + l.foreignKeys.length;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.AggLevel) {
                for (int i = 0; i < l.levels.length; i++) {
                    if (l.levels[i].equals(child)) {
                        return i + l.ignoreColumns.length + l.foreignKeys.length + l.measures.length;
                    }
                }
                return -1;
            } else if (parent instanceof MondrianGuiDef.AggPattern &&
                    child instanceof MondrianGuiDef.AggExclude) {
                for (int i = 0; i < ((MondrianGuiDef.AggPattern)l).excludes.length; i++) {
                    if (((MondrianGuiDef.AggPattern)l).excludes[i].equals(child)) {
                        return i + l.ignoreColumns.length + l.foreignKeys.length + l.measures.length + l.levels.length;
                    }
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.CalculatedMember) {
            MondrianGuiDef.CalculatedMember l = (MondrianGuiDef.CalculatedMember)parent;
            int fc = 0;
            if (l.formulaElement != null) {
                fc = 1;
            }
            if (child instanceof MondrianGuiDef.Formula) {
                if (child.equals(l.formulaElement)) {
                    return 0;
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.CalculatedMemberProperty) {
                for (int i = 0; i < l.memberProperties.length; i++) {
                    if (l.memberProperties[i].equals(child)) {
                        return i + fc;}
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.NamedSet) {
            MondrianGuiDef.NamedSet m = (MondrianGuiDef.NamedSet)parent;
            if (child instanceof MondrianGuiDef.Formula) {
                if (child.equals(m.formulaElement)) {
                    return 0;
                }
                return -1;
            }
            return -1;
        } else if (parent instanceof MondrianGuiDef.Measure) {
            MondrianGuiDef.Measure m = (MondrianGuiDef.Measure)parent;
            if (child instanceof MondrianGuiDef.MeasureExpression) {
                if (child.equals(m.measureExp)) {
                    return 0;
                }
                return -1;
            }
            return -1;
        } else if (parent instanceof MondrianGuiDef.Schema) {
            //return children in this order: cubes,  dimensions, namedSets, userDefinedFunctions, virtual cubes, roles
            MondrianGuiDef.Schema s = (MondrianGuiDef.Schema)parent;
            //return children in this order: cubes, virtual cubes, dimensions
            if (child instanceof MondrianGuiDef.Cube) {
                for (int i = 0; i < s.cubes.length; i++) {
                    if (s.cubes[i].equals(child)) {
                        return i;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.Dimension) {
                for (int i = 0; i < s.dimensions.length; i++) {
                    if (s.dimensions[i].equals(child)) {
                        return i + s.cubes.length;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.NamedSet) {
                for (int i = 0; i < s.namedSets.length; i++) {
                    if (s.namedSets[i].equals(child)) {
                        return i + s.cubes.length + s.dimensions.length;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.UserDefinedFunction) {
                for (int i = 0; i < s.userDefinedFunctions.length; i++) {
                    if (s.userDefinedFunctions[i].equals(child)) {
                        return i + s.cubes.length + s.dimensions.length + s.namedSets.length;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.VirtualCube) {
                for (int i = 0; i < s.virtualCubes.length; i++) {
                    if (s.virtualCubes[i].equals(child)) {
                        return i + s.cubes.length + s.dimensions.length + s.namedSets.length + s.userDefinedFunctions.length;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.Role) {
                for (int i = 0; i < s.roles.length; i++) {
                    if (s.roles[i].equals(child)) {
                        return i + s.cubes.length + s.dimensions.length + s.namedSets.length + s.userDefinedFunctions.length + s.virtualCubes.length;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.Parameter) {
                for (int i = 0; i < s.parameters.length; i++) {
                    if (s.parameters[i].equals(child)) {
                        return i + s.cubes.length + s.dimensions.length + s.namedSets.length + s.userDefinedFunctions.length + s.virtualCubes.length + s.roles.length;}
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.View) {
            MondrianGuiDef.View v = (MondrianGuiDef.View)parent;
            if (child instanceof MondrianGuiDef.SQL) {
                for (int i = 0; i < v.selects.length; i++) {
                    if (v.selects[i].equals(child)) {
                        return i;
                    }
                }
                return -1;
            }
            return -1;
        } else if (parent instanceof MondrianGuiDef.VirtualCube) {
            MondrianGuiDef.VirtualCube c = (MondrianGuiDef.VirtualCube)parent;
            if (child instanceof MondrianGuiDef.VirtualCubeDimension) {
                for (int i = 0; i < c.dimensions.length; i++) {
                    if (c.dimensions[i].equals(child)) {
                        return i;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.VirtualCubeMeasure) {
                for (int i = 0; i < c.measures.length; i++) {
                    if (c.measures[i].equals(child)) {
                        return i + c.dimensions.length;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.CalculatedMember) {
                for (int i = 0; i < c.calculatedMembers.length; i++) {
                    if (c.calculatedMembers[i].equals(child)) {
                        return i + c.dimensions.length + c.measures.length;
                    }
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.VirtualCubeDimension) {
            MondrianGuiDef.VirtualCubeDimension d = (MondrianGuiDef.VirtualCubeDimension)parent;
            return -1;
        } else if (parent instanceof MondrianGuiDef.VirtualCubeMeasure) {
            MondrianGuiDef.VirtualCubeMeasure d = (MondrianGuiDef.VirtualCubeMeasure)parent;
            return -1;
        } else if (parent instanceof MondrianGuiDef.Role) {
            MondrianGuiDef.Role c = (MondrianGuiDef.Role)parent;
            if (child instanceof MondrianGuiDef.SchemaGrant) {
                for (int i = 0; i < c.schemaGrants.length; i++) {
                    if (c.schemaGrants[i].equals(child)) {
                        return i;
                    }
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.SchemaGrant) {
            MondrianGuiDef.SchemaGrant c = (MondrianGuiDef.SchemaGrant)parent;
            if (child instanceof MondrianGuiDef.CubeGrant) {
                for (int i = 0; i < c.cubeGrants.length; i++) {
                    if (c.cubeGrants[i].equals(child)) {
                        return i;
                    }
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.CubeGrant) {
            MondrianGuiDef.CubeGrant c = (MondrianGuiDef.CubeGrant)parent;
            if (child instanceof MondrianGuiDef.DimensionGrant) {
                for (int i = 0; i < c.dimensionGrants.length; i++) {
                    if (c.dimensionGrants[i].equals(child)) {
                        return i;
                    }
                }
                return -1;
            } else if (child instanceof MondrianGuiDef.HierarchyGrant) {
                for (int i = 0; i < c.hierarchyGrants.length; i++) {
                    if (c.hierarchyGrants[i].equals(child)) {
                        return i + c.dimensionGrants.length ;
                    }
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.HierarchyGrant) {
            MondrianGuiDef.HierarchyGrant c = (MondrianGuiDef.HierarchyGrant)parent;
            if (child instanceof MondrianGuiDef.MemberGrant) {
                for (int i = 0; i < c.memberGrants.length; i++) {
                    if (c.memberGrants[i].equals(child)) {
                        return i;
                    }
                }
                return -1;
            } else {
                return -1;
            }
        } else if (parent instanceof MondrianGuiDef.Closure) {
            MondrianGuiDef.Closure c = (MondrianGuiDef.Closure)parent;
            if (child instanceof MondrianGuiDef.Table) {
                return 0;
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

    public void valueForPathChanged(TreePath path, Object newValue) {
        //super.valueForPathChanged(path, newValue);
    }

}

// End SchemaTreeModel.java
