/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// wgorman, 19 October 2007
*/
package mondrian.rolap;

import mondrian.olap.LevelType;
import mondrian.olap.MemberFormatter;
import mondrian.olap.MondrianDef;
import mondrian.olap.Level;

/**
 * RolapCubeLevel wraps a RolapLevel for a specific Cube.
 * 
 * @author Will Gorman (wgorman@pentaho.org)
 * @version $Id$
 */
public class RolapCubeLevel extends RolapLevel {
    
    RolapLevel rolapLevel;
    RolapCubeHierarchy rolapHierarchy;
    MondrianDef.Expression currentKeyExp = null;
    MondrianDef.Expression currentNameExp = null;
    MondrianDef.Expression currentCaptionExp = null;
    MondrianDef.Expression currentOrdinalExp = null;
    MondrianDef.Expression currentParentExp = null;
    
    public RolapCubeLevel(RolapLevel level, RolapCubeHierarchy hierarchy) {
        super(hierarchy, level.getDepth(), level.getName(), level.getKeyExp(), 
                level.getNameExp(), level.getCaptionExp(), 
                level.getOrdinalExp(), level.getParentExp(), 
                level.getNullParentValue(), null, level.getProperties(), 
                level.getFlags(), level.getDatatype(), 
                level.getHideMemberCondition(),
                level.getLevelType(), "" + level.getApproxRowCount());

        this.rolapLevel = level;
        this.rolapHierarchy = hierarchy;
        MondrianDef.Relation hierarchyRel = hierarchy.getRelation();
        currentKeyExp = convertExpression(level.getKeyExp(), hierarchyRel);
        currentNameExp = convertExpression(level.getNameExp(), hierarchyRel);
        currentCaptionExp = 
            convertExpression(level.getCaptionExp(), hierarchyRel);
        currentOrdinalExp = 
            convertExpression(level.getOrdinalExp(), hierarchyRel);
        currentParentExp = 
            convertExpression(level.getParentExp(), hierarchyRel);
        
        if (isAll()) {
            this.levelReader = new AllLevelReaderImpl();
        } else if (getLevelType() == LevelType.Null) {
            this.levelReader = new NullLevelReader();
        } else if (rolapLevel.xmlClosure != null) {
            RolapCubeDimension rolapCubeDimension =
                rolapHierarchy.getDimension();
            RolapCube cube = rolapCubeDimension.getCube();
            MondrianDef.CubeDimension xmlDimension = 
                rolapCubeDimension.xmlDimension;
            RolapDimension dimension = 
                hierarchy.createClosedPeerDimension(
                            this, rolapLevel.xmlClosure, cube, xmlDimension);
            dimension.init(cube, xmlDimension);
            cube.registerDimension(dimension);
            RolapLevel closedPeer = 
                (RolapLevel) dimension.getHierarchies()[0].getLevels()[1];
            this.levelReader = new ParentChildLevelReaderImpl(closedPeer);
            
        } else {
            this.levelReader = new RegularLevelReader();
        }
        
    }
    
    /**
     * Converts an expression to new aliases if necessary.
     * 
     * @param exp the expression to convert
     * @param rel the parent relation
     * @return returns the converted expression
     */
    private MondrianDef.Expression convertExpression(
            MondrianDef.Expression exp, MondrianDef.Relation rel) {
        if (this.rolapHierarchy.isUsingCubeFact()) {
            // no conversion necessary
            return exp;
        } else if (exp == null || rel == null) {
            return null;
        } else if (exp instanceof MondrianDef.Column) {
            MondrianDef.Column col = (MondrianDef.Column)exp;
            if (rel instanceof MondrianDef.Table) {
                return 
                    new MondrianDef.Column(rel.getAlias(), col.getColumnName());
            } else if (rel instanceof MondrianDef.Join 
                        || rel instanceof MondrianDef.View) {
                // need to determine correct name of alias for this level. 
                // this may be defined in level
                // col.table
                String alias = rolapHierarchy.lookupAlias(col.getTableAlias());
                return new MondrianDef.Column(alias, col.getColumnName());
            }
        } else if (exp instanceof MondrianDef.ExpressionView) {
            // this is a limitation, in the future, we may need
            // to replace the table name in the sql provided 
            // with the new aliased name
            return exp;
        }
        throw new RuntimeException("conversion of Class "+ exp.getClass() + 
                                    " unsupported at this time");
    }

    /**
     * Returns the (non virtual) cube this level belongs to.
     *
     * @return cube
     */
    public RolapCube getCube() {
        return rolapHierarchy.getDimension().getCube();
    }
    
    // override with stricter return type
    public final RolapCubeHierarchy getHierarchy() {
        return (RolapCubeHierarchy) super.getHierarchy();
    }

    // override with stricter return type
    public final RolapCubeLevel getChildLevel() {
        return (RolapCubeLevel) super.getChildLevel();
    }

    public RolapLevel getRolapLevel() {
        return rolapLevel;
    }
    
    public MondrianDef.Expression getKeyExp() {
        return currentKeyExp;
    }
    
    MondrianDef.Expression getOrdinalExp() {
        return currentOrdinalExp;
    }
    
    public MondrianDef.Expression getCaptionExp() {
        return currentCaptionExp;
    }

    MondrianDef.Expression getParentExp() {
        return currentParentExp;
    }

    public MondrianDef.Expression getNameExp() {
        return currentNameExp;
    }
    
    public boolean equals(RolapCubeLevel level) {
        // verify the levels are part of the same hierarchy
        return super.equals(level) 
                && getCube().equals(level.getCube());
    }
    
    boolean hasClosedPeer() {
        return rolapLevel.hasClosedPeer();
    }

    public MemberFormatter getMemberFormatter() {
        return rolapLevel.getMemberFormatter();
    }
}

// End RolapCubeLevel.java
