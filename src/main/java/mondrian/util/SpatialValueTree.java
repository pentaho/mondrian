/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.util;

import java.util.List;
import java.util.Set;

/**
 * A SpatialValueTree is a multidimensional index of values. The data is
 * organized in a fixed number of dimensions, each of which contain a fixed
 * number of nodes. The node of an axis is called a bound. Each node might
 * contain X number of values.
 *
 * <p>You can think of a SpatialValueTree as a multi dimensional list where
 * collections of values are stored in each possible tuple.
 *
 * <p>
 * When performing operations on the tree, such as adding values or retrieving
 * them, we use a {@link SpatialRegion}. Each region can cover more than one
 * bound per dimension.
 *
 * <p>When evaluating a region, if a dimension is omitted form a region,
 * this means that the region doesn't overlap the dimension at all. It is not
 * the same as covering all the values of the dimension axis.
 *
 * <p>
 * Example. A tree of years and states containing a X values per leaf node would
 * look something like this:
 *
 * <p>
 * year:1997<br />
 * &nbsp;&nbsp;&nbsp;&nbsp;state:NY<br />
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;value:0x000423<br />
 * &nbsp;&nbsp;&nbsp;&nbsp;state:FL<br />
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;value:0x000236<br />
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;value:0x000423<br />
 * year:1998<br />
 * &nbsp;&nbsp;&nbsp;&nbsp;state:NY<br />
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;value:[EMPTY]<br />
 * &nbsp;&nbsp;&nbsp;&nbsp;state:FL<br />
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;value:0x000501<br />
 *
 * <p>
 * A region key consists of a list of dimensions included in the region along
 * with an array of bounds for each of these dimensions. The boundaries of a
 * given region along a given dimension axis is specified by an array of
 * objects. All of these objects must be nodes of the specified dimension for a
 * region to be valid within a tree context. As an example, the following key:
 *
 * <p>
 * <code>[ {'year', [1997]}, {'state', ['FL']} ]</code>
 *
 * <p>
 * ... would return { [0x000236] [0x000423] }.
 *
 *<p>
 * The region keys can have more than one predicate value per axis. If we use
 * the same tree as above, and we query it using the following region key:
 *
 * <p>
 * <code>[ {'year', [1997]}, {'state', ['NY','FL']} ]
 *
 * <p>
 * ... would return { [0x000236] [0x000423] }.
 *
 * <p>
 * The region key also supports wildcard values. If you want to represent
 * all of the values of a given axis, you can put a single reference to
 * SpatialValueTree#AXIS_WILDCARD.
 *
 * <p>
 * <code>[ {'year', [AXIS_WILDCARD]}, {'state', ['NY','FL']} ]</code>
 *
 * <p>
 * ... would return { [0x000236] [0x000423] [0x000501] }.
 *
 * <p>
 * By convention, implementations are required to compare all generic types
 * using {@link Object#hashCode()} and {@link Object#equals(Object)}. Users of
 * this class should also use generic types which implement
 * {@link Object#hashCode()} and {@link Object#equals(Object)} to avoid
 * performance and consistency issues.
 *
 * @param <K>
 *            Type of the dimensions.
 * @param <E>
 *            Type of the dimension bounds.
 * @param <V>
 *            Type of the values to store.
 */
public interface SpatialValueTree
        <K extends Object, E extends Object, V extends Object>
{
    /**
     * Used as a token to represent all the values of an axis.
     * Overrides {@link Object#equals(Object)} and
     * {@link Object#hashCode()} so that only identity comparison
     * are used.
     */
    public static final Object AXIS_WILDCARD =
        new Object() {
            public int hashCode() {
                return 42;
            }
        };

    /**
     * Stores a string value at all points which intersect
     * with the passed region key.
     */
    void add(SpatialRegion<K, E> regionkey, V value);

    /**
     * Clears all the values found at the provided region
     * key.
     *
     * @param regionKey The region key of the values to clear.
     */
    void clear(SpatialRegion<K, E> regionKey);

    /**
     * Looks up all the values registered in nodes intersecting
     * with the provided region key.
     * @param regionKey The region key inside of which to search for
     * value nodes.
     * @return An unordered set of all the unique values intersecting
     * with the region.
     */
    Set<V> get(SpatialRegion<K, E> regionKey);

    /**
     * Looks up all the values registered in nodes intersecting
     * with the provided region key. If a value is present in all
     * of the nodes, a unique set of all the values found will be
     * returned. An empty set is returned if no complete match
     * could be found.
     * @param regionKey The region key inside of which to search for
     * value nodes.
     * @return An unordered set of all the unique values intersecting
     * with the region and covering it entirely, or an empty set
     * otherwise.
     */
    Set<V> match(SpatialRegion<K, E> regionKey);

    /**
     * Returns a list of all the dimensions present in this tree.
     * @return A list of dimension unique ids.
     */
    List<K> getDimensions();

    /**
     * Tells the number of dimensions in this tree.
     * @return The number of dimensions.
     */
    int getDimensionality();

    /**
     * Describes a spatial region within a {@link SpatialValueTree}.
     * @param <K> Type of the dimension key.
     * @param <E> Type of the values along the dimension's axis.
     */
    public interface SpatialRegion
            <K extends Object, E extends Object>
    {
        /**
         * Provides a list of the dimensions included in this
         * region.
         *
         * @return List of dimensions
         */
        List<K> getDimensions();
        /**
         * Provides an array of objects describing this region's
         * bounds within the specified dimension's axis.
         *
         * @param dimension Dimension
         * @return An array of the bounds touched by this region.
         */
        E[] getValues(K dimension);
    }
}
// End SpatialValueTree.java
