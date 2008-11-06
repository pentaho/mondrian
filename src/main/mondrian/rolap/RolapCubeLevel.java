/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// wgorman, 19 October 2007
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.agg.MemberColumnPredicate;
import mondrian.rolap.agg.MemberTuplePredicate;
import mondrian.rolap.agg.RangeColumnPredicate;
import mondrian.rolap.agg.ValueColumnPredicate;

/**
 * RolapCubeLevel wraps a RolapLevel for a specific Cube.
 *
 * @author Will Gorman (wgorman@pentaho.org)
 * @version $Id$
 */
public class RolapCubeLevel extends RolapLevel {

    private final RolapLevel rolapLevel;
    private RolapStar.Column starKeyColumn = null;

    protected LevelReader levelReader;

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
            RolapDimension dimension =
                (RolapDimension)rolapLevel.getClosedPeer()
                                    .getHierarchy().getDimension();

            RolapCubeDimension cubeDimension =
                new RolapCubeDimension(
                        getCube(), dimension, xmlDimension,
                        getDimension().getName() + "$Closure",
                        getHierarchy().getDimension().getOrdinal(),
                        getHierarchy().getDimension().isHighCardinality());

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
            RolapCubeLevel closedPeer =
                (RolapCubeLevel) cubeDimension.getHierarchies()[0].getLevels()[1];

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
        throw new RuntimeException("conversion of Class " + exp.getClass() +
                                    " unsupported at this time");
    }

    public void setStarKeyColumn(RolapStar.Column column) {
        starKeyColumn = column;
    }

    /**
     * This is the RolapStar.Column that is related to this RolapCubeLevel
     *
     * @return the RolapStar.Column related to this RolapCubeLevel
     */
    public RolapStar.Column getStarKeyColumn() {
        return starKeyColumn;
    }

    LevelReader getLevelReader() {
        return levelReader;
    }

    /**
     * this method returns the RolapStar.Column if non-virtual,
     * if virtual, find the base cube level and return it's
     * column
     *
     * @param baseCube the base cube for the specificed virtual level
     * @return the RolapStar.Column related to this RolapCubeLevel
     */
    public RolapStar.Column getBaseStarKeyColumn(RolapCube baseCube) {
        RolapStar.Column column = null;
        if (getCube().isVirtual() && baseCube != null) {
            RolapCubeLevel lvl = baseCube.findBaseCubeLevel(this);
            if (lvl != null) {
                column = lvl.getStarKeyColumn();
            }
        } else {
            column = getStarKeyColumn();
        }
        return column;
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

    // override with stricter return type
    public RolapCubeLevel getParentLevel() {
        return (RolapCubeLevel) super.getParentLevel();
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



    /**
     * Encapsulation of the difference between levels in terms of how
     * constraints are generated. There are implementations for 'all' levels,
     * the 'null' level, parent-child levels and regular levels.
     */
    interface LevelReader {

        /**
         * Adds constraints to a cell request for a member of this level.
         *
         * @param member Member to be constrained
         * @param baseCube base cube if virtual level
         * @param request Request to be constrained
         *
         * @return true if request is unsatisfiable (e.g. if the member is the
         * null member)
         */
        boolean constrainRequest(
            RolapCubeMember member,
            RolapCube baseCube,
            CellRequest request);

        /**
         * Adds constraints to a cache region for a member of this level.
         *
         * @param predicate Predicate
         * @param baseCube base cube if virtual level
         * @param cacheRegion Cache region to be constrained
         */
        void constrainRegion(
            StarColumnPredicate predicate,
            RolapCube baseCube,
            RolapCacheRegion cacheRegion);
    }

    /**
     * Level reader for a regular level.
     */
    class RegularLevelReader implements LevelReader {
        public boolean constrainRequest(
            RolapCubeMember member,
            RolapCube baseCube,
            CellRequest request)
        {
            assert member.getLevel() == RolapCubeLevel.this;
            if (member.getKey() == null) {
                if (member == member.getHierarchy().getNullMember()) {
                    // cannot form a request if one of the members is null
                    return true;
                } else {
                    throw Util.newInternal("why is key null?");
                }
            }

            RolapStar.Column column = getBaseStarKeyColumn(baseCube);

            if (column == null) {
                // This hierarchy is not one which qualifies the starMeasure
                // (this happens in virtual cubes). The starMeasure only has
                // a value for the 'all' member of the hierarchy (or for the
                // default member if the hierarchy has no 'all' member)
                return member != hierarchy.getDefaultMember() ||
                    hierarchy.hasAll();
            }

            final StarColumnPredicate predicate;
            if (member.isCalculated()) {
                predicate = null;
            } else {
                predicate = false ? new MemberColumnPredicate(column, member) :
                    new ValueColumnPredicate(column, member.getKey());
            }

            // use the member as constraint; this will give us some
            //  optimization potential
            request.addConstrainedColumn(column, predicate);

            if (request.extendedContext &&
                getNameExp() != null)
            {
                final RolapStar.Column nameColumn = column.getNameColumn();

                assert nameColumn != null;
                request.addConstrainedColumn(nameColumn, null);
            }

            if (member.isCalculated()) {
                return false;
            }

            // If member is unique without reference to its parent,
            // no further constraint is required.
            if (isUnique()) {
                return false;
            }

            // Constrain the parent member, if any.
            RolapCubeMember parent = member.getParentMember();
            while (true) {
                if (parent == null) {
                    return false;
                }
                RolapCubeLevel level = parent.getLevel();
                final LevelReader levelReader = level.levelReader;
                if (levelReader == this) {
                    // We are looking at a parent in a parent-child hierarchy,
                    // for example, we have moved from Fred to Fred's boss,
                    // Wilma. We don't want to include Wilma's key in the
                    // request.
                    parent = parent.getParentMember();
                    continue;
                }
                return levelReader.constrainRequest(
                    parent, baseCube, request);
            }
        }

        public void constrainRegion(
            StarColumnPredicate predicate,
            RolapCube baseCube,
            RolapCacheRegion cacheRegion)
        {
            RolapStar.Column column = getBaseStarKeyColumn(baseCube);

            if (column == null) {
                // This hierarchy is not one which qualifies the starMeasure
                // (this happens in virtual cubes). The starMeasure only has
                // a value for the 'all' member of the hierarchy (or for the
                // default member if the hierarchy has no 'all' member)
                return;
            }

            if (predicate instanceof MemberColumnPredicate) {
                MemberColumnPredicate memberColumnPredicate =
                    (MemberColumnPredicate) predicate;
                RolapMember member = memberColumnPredicate.getMember();
                assert member.getLevel() == RolapCubeLevel.this;
                assert !member.isCalculated();
                assert memberColumnPredicate.getMember().getKey() != null;
                assert !member.isNull();

                // use the member as constraint, this will give us some
                //  optimization potential
                cacheRegion.addPredicate(column, predicate);
                return;
            } else if (predicate instanceof RangeColumnPredicate) {
                RangeColumnPredicate rangeColumnPredicate =
                    (RangeColumnPredicate) predicate;
                final ValueColumnPredicate lowerBound =
                    rangeColumnPredicate.getLowerBound();
                RolapMember lowerMember;
                if (lowerBound == null) {
                    lowerMember = null;
                } else if (lowerBound instanceof MemberColumnPredicate) {
                    MemberColumnPredicate memberColumnPredicate =
                        (MemberColumnPredicate) lowerBound;
                    lowerMember = memberColumnPredicate.getMember();
                } else {
                    throw new UnsupportedOperationException();
                }
                final ValueColumnPredicate upperBound =
                    rangeColumnPredicate.getUpperBound();
                RolapMember upperMember;
                if (upperBound == null) {
                    upperMember = null;
                } else if (upperBound instanceof MemberColumnPredicate) {
                    MemberColumnPredicate memberColumnPredicate =
                        (MemberColumnPredicate) upperBound;
                    upperMember = memberColumnPredicate.getMember();
                } else {
                    throw new UnsupportedOperationException();
                }
                MemberTuplePredicate predicate2 =
                    new MemberTuplePredicate(
                        baseCube,
                        lowerMember,
                        !rangeColumnPredicate.getLowerInclusive(),
                        upperMember,
                        !rangeColumnPredicate.getUpperInclusive());
                // use the member as constraint, this will give us some
                //  optimization potential
                cacheRegion.addPredicate(predicate2);
                return;
            }

            // Unknown type of constraint.
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Level reader for a parent-child level which has a closed peer level.
     */
    class ParentChildLevelReaderImpl extends RegularLevelReader {
        /**
         * For a parent-child hierarchy with a closure provided by the schema,
         * the equivalent level in the closed hierarchy; otherwise null.
         */
        protected final RolapCubeLevel closedPeer;

        ParentChildLevelReaderImpl(RolapCubeLevel closedPeer) {
            this.closedPeer = closedPeer;
        }

        public boolean constrainRequest(
            RolapCubeMember member,
            RolapCube baseCube,
            CellRequest request)
        {
            // Replace a parent/child level by its closed equivalent, when
            // available; this is always valid, and improves performance by
            // enabling the database to compute aggregates.
            if (member.getDataMember() == null) {
                // Member has no data member because it IS the data
                // member of a parent-child hierarchy member. Leave
                // it be. We don't want to aggregate.
                return super.constrainRequest(member, baseCube, request);
            } else if (request.drillThrough) {
                member = (RolapCubeMember) member.getDataMember();
                return super.constrainRequest(member, baseCube, request);
            } else {
                RolapCubeLevel level = closedPeer;
                final RolapMember wrappedAllMember =
                    (RolapMember)rolapLevel.getClosedPeer().getHierarchy()
                            .getDefaultMember();


                final RolapCubeMember allMember = (RolapCubeMember)
                        level.getHierarchy().getDefaultMember();
                assert allMember.isAll();

                // isn't creating a member on the fly a bad idea?
                RolapMember wrappedMember =
                        new RolapMember(
                            wrappedAllMember,
                            rolapLevel.getClosedPeer(),
                            member.getKey());
                member =
                    new RolapCubeMember(
                        allMember,
                        wrappedMember,
                        closedPeer,
                        RolapCubeLevel.this.getCube());

                return level.getLevelReader().constrainRequest(
                    member, baseCube, request);
            }
        }

        public void constrainRegion(
            StarColumnPredicate predicate,
            RolapCube baseCube,
            RolapCacheRegion cacheRegion)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Level reader for the level which contains the 'all' member.
     */
    static class AllLevelReaderImpl implements LevelReader {
        public boolean constrainRequest(
            RolapCubeMember member,
            RolapCube baseCube,
            CellRequest request)
        {
            // We don't need to apply any constraints.
            return false;
        }

        public void constrainRegion(
            StarColumnPredicate predicate,
            RolapCube baseCube,
            RolapCacheRegion cacheRegion)
        {
            // We don't need to apply any constraints.
        }
    }

    /**
     * Level reader for the level which contains the null member.
     */
    static class NullLevelReader implements LevelReader {
        public boolean constrainRequest(
            RolapCubeMember member,
            RolapCube baseCube,
            CellRequest request)
        {
            return true;
        }

        public void constrainRegion(
            StarColumnPredicate predicate,
            RolapCube baseCube,
            RolapCacheRegion cacheRegion)
        {
        }
    }

}

// End RolapCubeLevel.java
