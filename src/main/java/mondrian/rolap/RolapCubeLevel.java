/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.agg.*;
import mondrian.spi.MemberFormatter;
import mondrian.util.Bug;

import java.util.*;

/**
 * RolapCubeLevel wraps a RolapLevel for a specific Cube.
 *
 * @author Will Gorman, 19 October 2007
 */
public class RolapCubeLevel extends RolapLevel {

    private final RolapLevel rolapLevel;
    /**
     * For a parent-child hierarchy with a closure provided by the schema,
     * the equivalent level in the closed hierarchy; otherwise null.
     */
    private final RolapCubeLevel closedPeerCubeLevel;
    protected LevelReader levelReader;
    final RolapCubeHierarchy cubeHierarchy;
    final RolapCubeDimension cubeDimension;
    final RolapCube cube;
    private final RolapCubeLevel parentCubeLevel;
    private RolapCubeLevel childCubeLevel;

    /**
     * Creates a RolapCubeLevel.
     *
     * @param level Shared level
     * @param cubeHierarchy Cube hierarchy
     * @param resourceMap Resource map, if there are resources to override
     *                    captions or descriptions of members in this level;
     *                    otherwise null
     * @param closure Description of closure table, or null
     */
    public RolapCubeLevel(
        RolapLevel level,
        RolapCubeHierarchy cubeHierarchy,
        Map<String, List<Larders.Resource>> resourceMap,
        RolapClosure closure)
    {
        super(
            cubeHierarchy,
            level.getName(),
            level.isVisible(),
            level.getDepth(),
            level.attribute,
            level.parentAttribute,
            level.getOrderByList(),
            level.nullParentValue,
            closure,
            level.getHideMemberCondition(),
            level.getLarder(),
            resourceMap);

        this.rolapLevel = level;
        this.cubeHierarchy = cubeHierarchy;
        this.cubeDimension = cubeHierarchy.getDimension();
        cube = cubeDimension.getCube();
        parentCubeLevel = (RolapCubeLevel) super.getParentLevel();
        if (parentCubeLevel != null) {
            parentCubeLevel.childCubeLevel = this;
        }
        attribute = level.getAttribute();
        closedPeerCubeLevel =
            closure == null
                ? null
                : (RolapCubeLevel) closure.closedPeerLevel;

        assert closure == null
            || ((RolapCubeLevel) closure.closedPeerLevel).getRolapLevel()
            == level.closure.closedPeerLevel;
    }

    @Override
    void initLevel(RolapSchemaLoader schemaLoader) {
        super.initLevel(schemaLoader);
        if (isAll()) {
            this.levelReader = new AllLevelReaderImpl();
        } else if (getLevelType() == org.olap4j.metadata.Level.Type.NULL) {
            this.levelReader = new NullLevelReader();
        } else if (hasClosedPeer()) {
            this.levelReader = new ParentChildLevelReaderImpl(this);
        } else {
            this.levelReader = new RegularLevelReader(this);
        }
    }

    LevelReader getLevelReader() {
        return levelReader;
    }

    public boolean isMeasure() {
        return cubeHierarchy.getOrdinalInCube() == 0;
    }

    /**
     * Determines whether the specified level is too ragged for native
     * evaluation, which is able to handle one special case of a ragged
     * hierarchy: when the level specified in the query is the leaf level of
     * the hierarchy and HideMemberCondition for the level is IfBlankName.
     * This is true even if higher levels of the hierarchy can be hidden
     * because even in that case the only column that needs to be read is the
     * column that holds the leaf. IfParentsName can't be handled even at the
     * leaf level because in the general case we aren't reading the column
     * that holds the parent. Also, IfBlankName can't be handled for non-leaf
     * levels because we would have to read the column for the next level
     * down for members with blank names.
     *
     * @return true if the specified level is too ragged for native
     *         evaluation.
     */
    protected boolean isTooRagged() {
        // Is this the special case of raggedness that native evaluation
        // is able to handle?
        if (getDepth() == getHierarchy().getLevelList().size() - 1) {
            switch (getHideMemberCondition()) {
            case Never:
            case IfBlankName:
                return false;
            default:
                return true;
            }
        }
        // Handle the general case in the traditional way.
        return getHierarchy().isRagged();
    }

    /**
     * Indicates that level is not ragged and not a parent/child level.
     */
    public boolean isSimple() {
        // most ragged hierarchies are not simple -- see isTooRagged.
        if (isTooRagged()) {
            return false;
        }
        if (isParentChild()) {
            return false;
        }
        // does not work for measures
        return !isMeasure();
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
    @Override
    public final RolapCubeDimension getDimension() {
        return cubeDimension;
    }

    // override with stricter return type
    @Override
    public final RolapCubeHierarchy getHierarchy() {
        return cubeHierarchy;
    }

    // override with stricter return type
    @Override
    public final RolapCubeLevel getChildLevel() {
        return childCubeLevel;
    }

    // override with stricter return type
    @Override
    public final RolapCubeLevel getParentLevel() {
        return parentCubeLevel;
    }

    @Override
    public String getCaption() {
        return rolapLevel.getCaption();
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

    /**
     * Returns true when the level is part of a parent/child hierarchy and has
     * an equivalent closed level.
     */
    boolean hasClosedPeer() {
        return closedPeerCubeLevel != null;
    }

    public RolapCubeLevel getClosedPeer() {
        return closedPeerCubeLevel;
    }

    @Override
    public MemberFormatter getMemberFormatter() {
        return rolapLevel.getMemberFormatter();
    }

    @Override
    public OlapElement lookupChild(
        SchemaReader schemaReader, Id.Segment name, MatchType matchType)
    {
        if (name instanceof Id.KeySegment) {
            Id.KeySegment keySegment = (Id.KeySegment) name;
            List<Comparable> keyValues = new ArrayList<Comparable>();
            for (Id.NameSegment nameSegment : keySegment.getKeyParts()) {
                final String keyValue = nameSegment.name;
                if (RolapUtil.mdxNullLiteral().equalsIgnoreCase(keyValue)) {
                    keyValues.add(RolapUtil.sqlNullValue);
                } else {
                    keyValues.add(keyValue);
                }
            }
            Collections.reverse(keyValues);
            final List<RolapSchema.PhysColumn> keyExps = attribute.getKeyList();
            if (keyExps.size() != keyValues.size()) {
                throw Util.newError(
                    "Wrong number of values in member key; "
                    + keySegment + " has " + keyValues.size()
                    + " values, whereas level's key has " + keyExps.size()
                    + " columns "
                    + new AbstractList<String>() {
                        public String get(int index) {
                            return keyExps.get(keyExps.size() - 1 - index)
                                .toSql();
                        }

                        public int size() {
                            return keyExps.size();
                        }
                    }
                    + ".");
            }
            return cubeHierarchy.getMemberReader().getMemberByKey(
                this, keyValues);
        }
        final List<RolapMember> levelMembers =
            Util.cast(schemaReader.getLevelMembers(this, true));
        if (levelMembers.size() > 0) {
            final RolapMember parent = levelMembers.get(0).getParentMember();
            return
                RolapUtil.findBestMemberMatch(
                    levelMembers,
                    parent,
                    this,
                    name,
                    matchType);
        }
        return null;
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
            RolapMember member,
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
            RolapMember member,
            RolapMeasureGroup measureGroup,
            CellRequest request)
        {
            assert member.getLevel() == cubeLevel;
            final List<Comparable> key = member.getKeyAsList();
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

            if (member.getDimension().hanger) {
                return false;
            }

            int keyOrdinal = 0;
            for (RolapSchema.PhysColumn column
                : cubeLevel.attribute.getKeyList())
            {
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
                            new PredicateColumn(
                                new RolapSchema.CubeRouter(
                                    measureGroup,
                                    cubeLevel.cubeDimension),
                                column),
                            key.get(keyOrdinal));
                }

                // use the member as constraint; this will give us some
                //  optimization potential
                request.addConstrainedColumn(starColumn, predicate);
                ++keyOrdinal;
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
                Util.deprecated("adding predicate multiple times?", false);
                for (RolapSchema.PhysColumn physColumn
                    : cubeLevel.attribute.getKeyList())
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
                        new RolapSchema.CubeRouter(
                            measureGroup,
                            cubeLevel.cubeDimension),
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
        private final RolapCubeLevel closedPeerLevel;
        private final RolapMember wrappedAllMember;

        ParentChildLevelReaderImpl(RolapCubeLevel cubeLevel)
        {
            this.regularLevelReader = new RegularLevelReader(cubeLevel);

            // inline a bunch of fields for performance
            this.closedPeerLevel = cubeLevel.closedPeerCubeLevel;
            this.wrappedAllMember = closedPeerLevel.getHierarchy().allMember;
        }

        public boolean constrainRequest(
            RolapMember member,
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
                    member.getDataMember(),
                    measureGroup,
                    request);
            } else {
                // isn't creating a member on the fly a bad idea?
                RolapMember wrappedMember =
                    new RolapMemberBase(
                        wrappedAllMember,
                        closedPeerLevel,
                        member.getKeyCompact(),
                        member.getMemberType(),
                        Util.makeFqName(
                            wrappedAllMember.getHierarchy(), member.getName()),
                        Larders.ofName(member.getName()));
                return closedPeerLevel.getLevelReader().constrainRequest(
                    wrappedMember, measureGroup, request);
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
            RolapMember member,
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
            RolapMember member,
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
