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

import mondrian.olap.*;
import mondrian.rolap.sql.SqlQuery;

/**
 * RolapCubeLevel wraps a RolapLevel for a specific Cube.
 * 
 * @author Will Gorman (wgorman@pentaho.org)
 * @version $Id$
 */
public class RolapCubeLevel extends RolapLevel {
    
    private final RolapLevel rolapLevel;
    private RolapStar.Column rolapStarColumn = null;
    
    public RolapCubeLevel(RolapLevel level, RolapCubeHierarchy hierarchy) {
        super(hierarchy, level.getDepth(), level.getName(), level.getKeyExp(), 
                level.getNameExp(), level.getCaptionExp(), 
                level.getOrdinalExp(), level.getParentExp(), 
                level.getNullParentValue(), null, level.getProperties(), 
                level.getFlags(), level.getDatatype(), 
                level.getHideMemberCondition(),
                level.getLevelType(), "" + level.getApproxRowCount());

        this.rolapLevel = level;
        MondrianDef.RelationOrJoin hierarchyRel = hierarchy.getRelation();
        keyExp = convertExpression(level.getKeyExp(), hierarchyRel);
        nameExp = convertExpression(level.getNameExp(), hierarchyRel);
        captionExp = convertExpression(level.getCaptionExp(), hierarchyRel);
        ordinalExp = convertExpression(level.getOrdinalExp(), hierarchyRel);
        parentExp = convertExpression(level.getParentExp(), hierarchyRel);
    }
    
    void init(MondrianDef.CubeDimension xmlDimension) {
        if (isAll()) {
            this.levelReader = new AllLevelReaderImpl();
        } else if (getLevelType() == LevelType.Null) {
            this.levelReader = new NullLevelReader();
        } else if (rolapLevel.xmlClosure != null) {
            ParentChildLevelReaderImpl lvlReader = 
                (ParentChildLevelReaderImpl)rolapLevel.getLevelReader();
            // wrap the already created RolapDimension with a 
            // RolapCubeDimension, and register it with the cube
            RolapDimension dimension = 
                (RolapDimension)lvlReader.closedPeer
                .getHierarchy().getDimension();

            RolapCubeDimension cubeDimension = 
                new RolapCubeDimension(
                        getCube(), dimension, xmlDimension, 
                        getDimension().getName() + "$Closure", 
                        getHierarchy().getDimension().getOrdinal());

            /*
            RME HACK
              WG: Note that the reason for registering this usage is so that
              when registerDimension is called, the hierarchy is registered 
              successfully to the star.  This type of hack will go away once 
              HierarchyUsage is phased out 
            */
            getCube().createUsage(
                    (RolapCubeHierarchy)cubeDimension.getHierarchies()[0], 
                    xmlDimension);
            
            cubeDimension.init(xmlDimension);
            getCube().registerDimension(cubeDimension);
            RolapLevel closedPeer = 
                (RolapLevel) cubeDimension.getHierarchies()[0].getLevels()[1];
            
            this.levelReader = new ParentChildLevelReaderImpl(closedPeer);
        } else {
            this.levelReader = new RegularLevelReader();
        }
    }
    
    /**
     * The RolapCubeLevel does not need to realias the expression,
     * that work has already been done during initialization.
     * 
     * @param sqlQuery
     * @param expr
     * @return expression string
     */
    public String getExpressionWithAlias(
            SqlQuery sqlQuery,
            MondrianDef.Expression expr)
        {
            return expr.getExpression(sqlQuery);
        }
    
    /**
     * Converts an expression to new aliases if necessary.
     * 
     * @param exp the expression to convert
     * @param rel the parent relation
     * @return returns the converted expression
     */
    private MondrianDef.Expression convertExpression(
        MondrianDef.Expression exp,
        MondrianDef.RelationOrJoin rel)
    {
        if (getHierarchy().isUsingCubeFact()) {
            // no conversion necessary
            return exp;
        } else if (exp == null || rel == null) {
            return null;
        } else if (exp instanceof MondrianDef.Column) {
            MondrianDef.Column col = (MondrianDef.Column)exp;
            if (rel instanceof MondrianDef.Table) {
                return new MondrianDef.Column(
                    ((MondrianDef.Table) rel).getAlias(),
                    col.getColumnName());
            } else if (rel instanceof MondrianDef.Join 
                        || rel instanceof MondrianDef.Relation) {
                // need to determine correct name of alias for this level. 
                // this may be defined in level
                // col.table
                String alias = getHierarchy().lookupAlias(col.getTableAlias());
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
    
    public void setRolapStarColumn(RolapStar.Column column) {
        rolapStarColumn = column;
    }
    
    /**
     * rolapStarColumn is the eventual replacement of levelToColumnMap within
     * the RolapStar.  Currently only the addLevelConstraint code utilizes this
     * new data structure.
     * 
     * @return the RolapStar.Column related to this RolapCubeLevel
     */
    public RolapStar.Column getRolapStarColumn() {
        return rolapStarColumn;
    }

    /**
     * Returns the (non virtual) cube this level belongs to.
     *
     * @return cube
     */
    public RolapCube getCube() {
        return getHierarchy().getDimension().getCube();
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
