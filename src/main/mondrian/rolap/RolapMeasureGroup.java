/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2009-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.util.Pair;

import org.apache.commons.collections.iterators.FilterIterator;

import org.olap4j.impl.NamedListImpl;
import org.olap4j.metadata.NamedList;

import java.util.*;

/**
 * A group of measures that are in the same cube and share the same fact
 * table.
 *
 * <p>What used to be called a virtual cube -- a cube with several
 * underlying fact tables -- is now a cube with several measure groups.
 *
 * <p>For each of the dimensions of a cube, there may or may not be a link
 * between the measure group and the dimension. If a link exists, there
 * generally corresponds a join between the fact table containing the
 * measure group and the dimension table containing the dimension. (This is
 * the usual linking strategy; other strategies exist.)
 *
 * @author jhyde
 */
public class RolapMeasureGroup {
    private final RolapCube cube;
    private final String name;
    public final boolean ignoreUnrelatedDimensions;
    private final RolapStar star;
    final boolean aggregate;
    final NamedList<RolapStoredMeasure> measureList =
        new NamedListImpl<RolapStoredMeasure>();
    final List<RolapMeasureRef> measureRefList;
    private final Map<RolapDimension, RolapSchema.PhysPath> dimensionMap =
        new HashMap<RolapDimension, RolapSchema.PhysPath>();

    /**
     * As dimensionMap, but keys are dimensions, not cube-dimensions.
     *
     * @see Util#deprecated(Object, boolean) todo cleanup
     */
    public final Map<RolapDimension, RolapSchema.PhysPath> dimensionMap2 =
        new HashMap<RolapDimension, RolapSchema.PhysPath>();

    public final Map<RolapCubeDimension, RolapSchema.PhysPath> dimensionMap3 =
        new HashMap<RolapCubeDimension, RolapSchema.PhysPath>();

    /**
     * Measure which computes the number of rows in the fact table. Used for
     * value allocation during writeback.
     */
    RolapBaseCubeMeasure factCountMeasure;

    final Map<Pair<RolapCubeDimension, RolapSchema.PhysColumn>,
              RolapStar.Column>
        starColumnMap =
        new HashMap<Pair<RolapCubeDimension, RolapSchema.PhysColumn>,
                    RolapStar.Column>();

    /**
     * Columns in this measure group that hold columns from other dimensions.
     */
    final List<Pair<RolapStar.Column, RolapSchema.PhysColumn>> copyColumnList =
        new ArrayList<Pair<RolapStar.Column, RolapSchema.PhysColumn>>();

    /**
     * Creates a RolapMeasureGroup.
     *
     * @param cube Cube
     * @param name Name
     * @param ignoreUnrelatedDimensions If true, dimensions that are not related
     *     to measures in this measure group will be pushed to top level member
     * @param star Star
     * @param aggregate Whether this measure group represents an aggregate table
     *                  (or a hybrid fact/aggregate), as opposed to a fact table
     */
    public RolapMeasureGroup(
        RolapCube cube,
        String name,
        boolean ignoreUnrelatedDimensions,
        RolapStar star,
        boolean aggregate)
    {
        this.name = name;
        this.cube = cube;
        this.ignoreUnrelatedDimensions = ignoreUnrelatedDimensions;
        this.star = star;
        this.aggregate = aggregate;
        if (aggregate) {
            measureRefList = new ArrayList<RolapMeasureRef>();
        } else {
            // read-only empty list, to protect against accidents
            measureRefList = Collections.emptyList();
        }
        assert cube != null;
        assert name != null;
        assert star != null;
    }

    /**
     * Finds out non-joining dimensions for this measure group.
     *
     * <p>Useful for finding out non-joining dimensions for a stored measure
     * from a base cube.
     *
     * @param tuple List of members
     * @return Set of dimensions that do not exist (non joining) in this cube
     */
    public Iterable<RolapCubeDimension> nonJoiningDimensions(List<Member> tuple)
    {
        Set<RolapCubeDimension> otherDims = new HashSet<RolapCubeDimension>();
        for (Member member : tuple) {
            if (!member.isCalculated()) {
                otherDims.add((RolapCubeDimension) member.getDimension());
            }
        }
        return nonJoiningDimensions(otherDims);
    }

    /**
     * Finds out non-joining dimensions for this measure group.
     *
     * <p>The result preserves the order in {@code otherDims}.</p>
     *
     * @param otherDims Collection of dimensions to be tested for existence in
     *                  this measure group
     * @return Set of dimensions that do not exist (non joining) in this cube
     */
    public Iterable<RolapCubeDimension> nonJoiningDimensions(
        final Iterable<? extends RolapCubeDimension> otherDims)
    {
        return new Iterable<RolapCubeDimension>() {
            public Iterator<RolapCubeDimension> iterator() {
                //noinspection unchecked
                return (Iterator<RolapCubeDimension>) new FilterIterator(
                    otherDims.iterator(),
                    new Util.Predicate1<RolapCubeDimension>() {
                        public boolean test(RolapCubeDimension dimension) {
                            return !dimensionMap3.containsKey(dimension)
                                && !dimension.isMeasures();
                        }
                    });
            }
        };
    }

    /**
     * Returns whether a link exists from this measure group to the given
     * dimension.
     *
     * @param dimension Dimension
     * @return Whether there is a link
     */
    // REVIEW: maybe create a Link class, and return a link or null
    boolean existsLink(RolapCubeDimension dimension) {
        return dimensionMap3.containsKey(dimension);
    }

    /**
     * Defines a link from the measure group's fact table to a dimension.
     *
     * @param dimension Dimension
     * @param path Path, consisting of a sequence of attributes
     */
    void addLink(RolapDimension dimension, RolapSchema.PhysPath path) {
        dimensionMap.put(dimension, path);
        if (dimension instanceof RolapCubeDimension) {
            final RolapCubeDimension cubeDimension =
                (RolapCubeDimension) dimension;
            dimensionMap3.put(cubeDimension, path);
            dimension = cubeDimension.rolapDimension;
        }
        dimensionMap2.put(dimension, path);
    }

    /**
     * Returns the name of this measure group.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the star in which all measures in this group are stored.
     *
     * @return Star, never null
     */
    public RolapStar getStar() {
        return star;
    }

    /**
     * Returns the cube that this measure group belongs to.
     *
     * @return Cube, never null
     */
    public RolapCube getCube() {
        return cube;
    }

    /**
     * Returns the system measure that counts the number of fact table rows in
     * a given cell.
     *
     * <p>Never null, because if there is no count measure explicitly defined,
     * the system creates one.
     */
    public RolapMeasure getFactCountMeasure() {
        return factCountMeasure;
    }

    /**
     * Returns the system measure that counts the number of atomic cells in
     * a given cell.
     *
     * <p>A cell is atomic if all dimensions are at their lowest level.
     * If the fact table has a primary key, this measure is equivalent to the
     * {@link #getFactCountMeasure() fact count measure}.
     *
     * @return Measure that counts the number of atomic cells in a given cell
     */
    RolapMeasure getAtomicCellCountMeasure() {
        // TODO: separate measure
        return factCountMeasure;
    }

    public RolapSchema.PhysRelation getFactRelation() {
        return star.getFactTable().getRelation();
    }

    /**
     * Returns the path by which a given dimension is joined to this measure
     * group.
     *
     * <p>If the dimension is not connected, returns null. If the dimension
     * lies in the same fact table as the measure group (is degenerate) returns
     * a path of length 0.
     *
     * @param dimension Dimension
     * @return Path from this measure group's fact table to the dimension's key
     */
    public RolapSchema.PhysPath getPath(RolapDimension dimension) {
        return dimensionMap.get(dimension);
    }

    /**
     * Returns the {@link mondrian.rolap.RolapStar.Column} that connects a
     * given column to a fact table.
     *
     * <p>The RolapStar column contains a
     * {@link mondrian.rolap.RolapSchema.PhysPath} and is uniquely identified
     * within the {@link RolapStar}
     * by the combination of path and physical column.
     * It also has a unique ordinal within the RolapStar.
     *
     * <p>Note that a given physColumn may occur more than once in the map.
     * The occurrences will have different paths, and therefore will have come
     * from uses of the physColumn in different dimensions.
     *
     * @param cubeDimension Dimension
     * @param column Physical column
     * @return RolapStar column
     */
    public RolapStar.Column getRolapStarColumn(
        RolapCubeDimension cubeDimension,
        RolapSchema.PhysColumn column)
    {
        return starColumnMap.get(
            Pair.of(
                cubeDimension, column));
    }

    public RolapStar.Column getRolapStarColumn(
        RolapCubeDimension cubeDimension,
        RolapSchema.PhysColumn column,
        boolean fail)
    {
        final RolapStar.Column starColumn = getRolapStarColumn(
            cubeDimension, column);
        assert !(fail && starColumn == null);
        return starColumn;
    }

    /**
     * Finds the path from the fact table of this measure group to a given
     * column, by way of a joining dimension.
     *
     * <p>The dimension disambiguates if the same column is used twice, for
     * example, if the 'year' column is used via two time dimensions.</p>
     *
     * @param cubeDimension Joining dimension
     * @param column Physical column
     * @return Path from fact table to physical column
     */
    public RolapSchema.PhysPath getPath(
        RolapCubeDimension cubeDimension,
        RolapSchema.PhysColumn column)
    {
        final RolapSchema.PhysPathBuilder pathBuilder =
            new RolapSchema.PhysPathBuilder(
                getPath(cubeDimension));
        try {
            column.relation.getSchema().getGraph().findPath(
                pathBuilder, column.relation);
        } catch (RolapSchema.PhysSchemaException e) {
            throw new RuntimeException(
                "Could not find path",
                e);
        }
        return pathBuilder.done();
    }

    /**
     * Reference to a measure defined in another measure group.
     */
    static class RolapMeasureRef {
        final RolapBaseCubeMeasure measure;
        final RolapSchema.PhysColumn aggColumn;

        public RolapMeasureRef(
            RolapBaseCubeMeasure measure,
            RolapSchema.PhysColumn aggColumn)
        {
            this.measure = measure;
            this.aggColumn = aggColumn;
        }
    }
}

// End RolapMeasureGroup.java
