/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// wgorman, 19 October 2007
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.agg.*;

import java.util.Collections;
import java.util.List;

/**
 * RolapCubeLevel wraps a RolapLevel for a specific Cube.
 *
 * @author Will Gorman
 * @version $Id$
 */
public class RolapCubeLevel extends RolapLevel {

    private final RolapLevel rolapLevel;
    /**
     * For a parent-child hierarchy with a closure provided by the schema,
     * the equivalent level in the closed hierarchy; otherwise null.
     */
    private RolapCubeLevel closedPeerCubeLevel;
    protected LevelReader levelReader;
    private final RolapCubeHierarchy cubeHierarchy;
    final RolapCubeDimension cubeDimension;
    private final RolapCube cube;
    private final RolapCubeLevel parentCubeLevel;
    private RolapCubeLevel childCubeLevel;

    public RolapCubeLevel(RolapLevel level, RolapCubeHierarchy cubeHierarchy) {
        super(
            cubeHierarchy,
            level.getName(),
            level.getCaption(),
            level.getDescription(),
            level.getDepth(),
            level.attribute,
            level.getHideMemberCondition(),
            null,
            level.getAnnotationMap());

        this.rolapLevel = level;
        this.cubeHierarchy = cubeHierarchy;
        this.cubeDimension = cubeHierarchy.getDimension();
        cube = cubeDimension.getCube();
        parentCubeLevel = (RolapCubeLevel) super.getParentLevel();
        if (parentCubeLevel != null) {
            parentCubeLevel.childCubeLevel = this;
        }
        attribute = level.getAttribute();
    }

    @Override
    void initLevel(
        RolapSchemaLoader schemaLoader,
        boolean closure)
    {
        if (isAll()) {
            this.levelReader = new AllLevelReaderImpl();
        } else if (getLevelType() == org.olap4j.metadata.Level.Type.NULL) {
            this.levelReader = new NullLevelReader();
        } else if (closure) {
            RolapDimension dimension =
                rolapLevel.getClosedPeer().getHierarchy().getDimension();

            RolapCubeDimension cubeDimension =
                new RolapCubeDimension(
                    schemaLoader,
                    getCube(),
                    dimension,
                    getDimension().getName() + "$Closure",
                    null,
                    null,
                    null,
                    -1,
                    getCube().hierarchyList,
                    Collections.<String, Annotation>emptyMap());

            schemaLoader.initDimension(cubeDimension);
            closedPeerCubeLevel =
                cubeDimension
                    .getRolapCubeHierarchyList().get(0)
                    .getRolapCubeLevelList().get(1);

            this.levelReader = new ParentChildLevelReaderImpl(this);
        } else {
            this.levelReader = new RegularLevelReader(this);
        }
    }

    LevelReader getLevelReader() {
        return levelReader;
    }

    /**
     * Returns the RolapStar.Column if non-virtual;
     * if virtual, find the base cube level and return its
     * column.
     *
     * @param measureGroup Measure group
     * @return the RolapStar.Column related to this RolapCubeLevel
     */
    public RolapStar.Column getBaseStarKeyColumn(
        RolapMeasureGroup measureGroup)
    {
        assert measureGroup != null;
        throw new UnsupportedOperationException();
        /*
        // the base cube for the specificed virtual level
        Util.deprecated("remove this method?", false);
        // TODO: was a parameter, should not be needed, if we use the physColumn
        RolapCube baseCube = Util.deprecated(null, false);
        if (getCube().isVirtual() && baseCube != null) {
            RolapCubeLevel level = baseCube.findBaseCubeLevel(this);
            if (level != null) {
                final RolapSchema.PhysExpr expr = level.getStarKeyExpr();
                return star.getColumn(expr, true);
            } else {
                return null;
            }
        } else {
            final RolapSchema.PhysExpr expr = getKeyExp(); // getStarKeyExpr();
            // REVIEW: pass fail as parameter? Make other calls to getColumn
            //    use it?
            final boolean fail = false;
            return star.getColumn(expr, fail);
        }
        */
    }

    /**
     * Returns the (non virtual) cube this level belongs to.
     *
     * @return cube
     */
    public final RolapCube getCube() {
        return cube;
    }

    // override with stricter return type
    public final RolapCubeDimension getDimension() {
        return cubeDimension;
    }

    // override with stricter return type
    public final RolapCubeHierarchy getHierarchy() {
        return cubeHierarchy;
    }

    // override with stricter return type
    public final RolapCubeLevel getChildLevel() {
        return childCubeLevel;
    }

    // override with stricter return type
    public final RolapCubeLevel getParentLevel() {
        return parentCubeLevel;
    }

    public String getCaption() {
        return rolapLevel.getCaption();
    }

    public void setCaption(String caption) {
        // Cannot set the caption on the underlying level; other cube levels
        // might be using it.
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the underlying level.
     *
     * @return Underlying level
     */
    public RolapLevel getRolapLevel() {
        return rolapLevel;
    }

    public boolean equals(RolapCubeLevel level) {
        if (this == level) {
            return true;
        }
        // verify the levels are part of the same hierarchy
        return super.equals(level)
                && getCube().equals(level.getCube());
    }

    boolean hasClosedPeer() {
        return closedPeerCubeLevel != null;
    }

    public RolapCubeLevel getClosedPeer() {
        return closedPeerCubeLevel;
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
         * @param measureGroup Measure group, or null
         * @param request Request to be constrained
         * @return true if request is unsatisfiable (e.g. if the member is the
         * null member)
         */
        boolean constrainRequest(
            RolapCubeMember member,
            RolapMeasureGroup measureGroup,
            CellRequest request);

        /**
         * Adds constraints to a cache region for a member of this level.
         *
         * @param predicate Predicate
         * @param measureGroup Measure group
         * @param cacheRegion Cache region to be constrained
         */
        void constrainRegion(
            StarPredicate predicate,
            RolapMeasureGroup measureGroup,
            RolapCacheRegion cacheRegion);
    }

    /**
     * Level reader for a regular level.
     */
    static final class RegularLevelReader implements LevelReader {
        private RolapCubeLevel cubeLevel;

        RegularLevelReader(
            RolapCubeLevel cubeLevel)
        {
            this.cubeLevel = cubeLevel;
        }

        public boolean constrainRequest(
            RolapCubeMember member,
            RolapMeasureGroup measureGroup,
            CellRequest request)
        {
            assert member.getLevel() == cubeLevel;
            final List<Object> key = member.getKeyAsList();
            if (key.isEmpty()) {
                if (member == member.getHierarchy().getNullMember()) {
                    // cannot form a request if one of the members is null
                    return true;
                } else if (member.isCalculated()) {
                    return false;
                } else {
                    throw Util.newInternal("why is key empty?");
                }
            }

            int keyOrdinal = 0;
            for (RolapSchema.PhysColumn column : cubeLevel.attribute.keyList) {
                RolapStar.Column starColumn =
                    measureGroup.getRolapStarColumn(
                        cubeLevel.cubeDimension, column, false);
                if (starColumn == null) {
                    // This hierarchy is not one which qualifies the starMeasure
                    // (this happens in virtual cubes). The starMeasure only has
                    // a value for the 'all' member of the hierarchy (or for the
                    // default member if the hierarchy has no 'all' member)
                    return member != cubeLevel.hierarchy.getDefaultMember()
                           || cubeLevel.hierarchy.hasAll();
                }

                final StarColumnPredicate predicate;
                if (member.isCalculated() && !member.isParentChildLeaf()) {
                    predicate = null;
                } else {
                    predicate =
                        new ValueColumnPredicate(
                            column, key.get(keyOrdinal));
                }

                // use the member as constraint; this will give us some
                //  optimization potential
                request.addConstrainedColumn(starColumn, predicate);
                ++keyOrdinal;
            }

            if (request.extendedContext
                && cubeLevel.attribute.nameExp != null)
            {
                final RolapStar.Column nameColumn =
                    measureGroup.getRolapStarColumn(
                        cubeLevel.cubeDimension,
                        cubeLevel.attribute.nameExp,
                        true);
                request.addConstrainedColumn(nameColumn, null);
            }

            // Request is satisfiable.
            return false;
        }

        public void constrainRegion(
            StarPredicate predicate,
            RolapMeasureGroup measureGroup,
            RolapCacheRegion cacheRegion)
        {
            if (!measureGroup.existsLink(cubeLevel.cubeDimension)) {
                // This hierarchy is not one which qualifies the starMeasure
                // (this happens in virtual cubes). The starMeasure only has
                // a value for the 'all' member of the hierarchy (or for the
                // default member if the hierarchy has no 'all' member)
                return;
            }

            if (predicate instanceof MemberPredicate) {
                MemberColumnPredicate memberColumnPredicate =
                    (MemberColumnPredicate) predicate;
                RolapMember member = memberColumnPredicate.getMember();
                assert member.getLevel() == cubeLevel;
                assert !member.isCalculated();
                assert memberColumnPredicate.getMember().getKey() != null;
                assert !member.isNull();

                // use the member as constraint, this will give us some
                //  optimization potential
                for (RolapSchema.PhysColumn physColumn
                    : cubeLevel.attribute.keyList)
                {
                    cacheRegion.addPredicate(predicate);
                }
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
                    Predicates.range(
                        measureGroup.getStar().getSchema().physicalSchema,
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
    // final for performance
    static final class ParentChildLevelReaderImpl implements LevelReader {
        private final RegularLevelReader regularLevelReader;
        private final RolapCubeLevel closedPeerCubeLevel;
        private final RolapLevel closedPeerLevel;
        private final RolapMember wrappedAllMember;
        private final RolapCubeMember allMember;

        ParentChildLevelReaderImpl(RolapCubeLevel cubeLevel)
        {
            this.regularLevelReader = new RegularLevelReader(cubeLevel);

            // inline a bunch of fields for performance
            this.closedPeerCubeLevel = cubeLevel.closedPeerCubeLevel;
            this.closedPeerLevel = cubeLevel.rolapLevel.getClosedPeer();
            this.wrappedAllMember = (RolapMember)
                closedPeerLevel.getHierarchy().getDefaultMember();
            this.allMember =
                closedPeerCubeLevel.getHierarchy().getDefaultMember();
            assert allMember.isAll();
        }

        public boolean constrainRequest(
            RolapCubeMember member,
            RolapMeasureGroup measureGroup,
            CellRequest request)
        {
            // Replace a parent/child level by its closed equivalent, when
            // available; this is always valid, and improves performance by
            // enabling the database to compute aggregates.
            if (member.getDataMember() == null) {
                // Member has no data member because it IS the data
                // member of a parent-child hierarchy member. Leave
                // it be. We don't want to aggregate.
                return regularLevelReader.constrainRequest(
                    member, measureGroup, request);
            } else if (request.drillThrough) {
                return regularLevelReader.constrainRequest(
                    member.getDataMember(), measureGroup, request);
            } else {
                // isn't creating a member on the fly a bad idea?
                RolapMember wrappedMember =
                    new RolapMemberBase(
                        wrappedAllMember, closedPeerLevel, member.getKey());
                member =
                    new RolapCubeMember(
                        allMember,
                        wrappedMember, closedPeerCubeLevel);

                return closedPeerCubeLevel.getLevelReader().constrainRequest(
                    member, measureGroup, request);
            }
        }

        public void constrainRegion(
            StarPredicate predicate,
            RolapMeasureGroup measureGroup,
            RolapCacheRegion cacheRegion)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Level reader for the level which contains the 'all' member.
     */
    static final class AllLevelReaderImpl implements LevelReader {
        public boolean constrainRequest(
            RolapCubeMember member,
            RolapMeasureGroup measureGroup,
            CellRequest request)
        {
            // We don't need to apply any constraints.
            return false;
        }

        public void constrainRegion(
            StarPredicate predicate,
            RolapMeasureGroup measureGroup,
            RolapCacheRegion cacheRegion)
        {
            // We don't need to apply any constraints.
        }
    }

    /**
     * Level reader for the level which contains the null member.
     */
    static final class NullLevelReader implements LevelReader {
        public boolean constrainRequest(
            RolapCubeMember member,
            RolapMeasureGroup measureGroup,
            CellRequest request)
        {
            return true;
        }

        public void constrainRegion(
            StarPredicate predicate,
            RolapMeasureGroup measureGroup,
            RolapCacheRegion cacheRegion)
        {
        }
    }

}

// End RolapCubeLevel.java
