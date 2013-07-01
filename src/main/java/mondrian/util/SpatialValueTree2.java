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
import java.util.Map;

/**
 * Variation of Luc's SpatialValueTree.
 *
 * <p>Notes:
 *
 * <p>1. For clarity, I got rid of the of template args. We might add back
 * say '&lt;Q extends SpatialRegionRequest,
 * R extends SpatialRegion,
 * D extends SpatialDimension&gt;' if it saves a lot of casting in client code.
 *
 * <p>2. It might be useful to introduce an interface SpatialDimensionality
 * that represents a set of dimensions. It will certainly be useful for the
 * implementation; however, it is not clear that it is necessary in the API.
 *
 * <p>3. In mondrian a 'region value' might be a struct that contains
 *    a weak reference to a segment in cache, and a reference to a segment
 *    in an external cache. Either of the references can be null.
 *
 * <p>4. In a SpatialRegionRequest, it might be useful to coalesce a list
 *    of values into a range (e.g. {2009, 2010, 2011} would be [2009, 2011]).
 *
 * <p>Mondrian requests currently have raw values, but converting those raw
 *    values into ranges might allow the tree to be more efficient.
 *
 * <p>Ranges could be recognized if we know all allowable values. For example,
 *    if we know the set of states, we can say that {"CA", "CO", "CT"
 *    "DE"} is equivalent to ["CA", "DE"].
 *    A range could be recognized if the data type is discreet. For example,
 *    {2009, 2010, 2011} is equivalent to [2009, 2011] because year is an int.
 *
 * <p>5. For performance, and atomicity of operations,
 * some of the methods might
 * contain a callback (e.g. {@link #rollup(java.util.Map)} could take a
 * functor that is applied when a rollup is found. But we can validate the
 * design introducing functors just yet.
 *
 * @author jhyde
  */
public interface SpatialValueTree2
{
    /**
     * Returns a list of all the dimensions present in this tree.
     *
     * @return A list of dimensions.
     */
    List<SpatialDimension> getDimensions();

    /**
     * Stores a region in this tree.
     *
     * <p>REVIEW: What is the behavior if there is another region with an equal
     * {@link SpatialRegionRequest}?
     *
     * @param region Region
     */
    void add(SpatialRegion region);

    /**
     * Removes a region from the tree.
     *
     * @param region The region key of the values to clear.
     */
    void clear(SpatialRegion region);

    /**
     * Looks up all the values registered in nodes intersecting
     * with the provided region key.
     *
     * <p>REVIEW: Does it have to PRECISELY fulfill the request, or can it
     * be a superset? Does the return value's {@link SpatialRegion#getRequest()}
     * method return the {@code regionRequest} parameter? (That would be
     * expensive to implement.) Do we even need this method, or is
     *
     * @param regionRequest The region key inside of which to search for
     * value nodes.
     *
     * @return Region fulfilling the request, or null
     */
    SpatialRegion get(SpatialRegionRequest regionRequest);

    /**
     * Returns a region containing a given cell.
     *
     * <p>If there are multiple regions that contain the cell, returns just
     * one of them. If there are no regions, returns null.
     *
     * @param coordinates Coordinates of cell - a value for each constraining
     *    dimension
     * @return a region that contains the cell, or null if there is no such
     */
    SpatialRegion getRegionContaining(
        Map<SpatialDimension, Object> coordinates);

    /**
     * Returns a collection of regions that can be combined to compute a given
     * cell.
     *
     * <p>The regions are not necessarily disjoint. Nor are they necessarily
     * of the same dimensionality.
     *
     * <p>If multiple rollups are possible, gives the set of regions with the
     * smallest number of cells. (It is cheaper to roll up from regions that are
     * already highly aggregated.)
     *
     * <p>If no rollup is possible, returns null.
     *
     * @param dimensions Coordinates of cell
     * @return List of spatial regions to roll up; or null
     */
    List<SpatialRegion> rollup(
        Map<SpatialDimension, Object> dimensions);

    public interface SpatialDimension
    {
        /**
         * Ordinal of dimension. Dimension ordinals are unique and contiguous
         * within a tree.
         *
         * @return ordinal of dimension
         */
        int ordinal();

        /**
         * Declares that a particular dimension has a finite set of values. With
         * this information, an implementation may be able to perform rollups
         * that it would not otherwise.
         *
         * <p>For example, if the user asks for cell (year=2010, measure=sales)
         * and the tree has regions (year=2010, gender=M, measure=sales) and
         * (year=2010, gender=F, measure=sales) then the tree can compute the
         * cell only if it knows that the only values of gender are {M, F}.
         *
         * <p>Returns null if the set of values is unbounded, not known, or
         * too large to be any use in optimizing.
         *
         * <p>The values are distinct and sorted (per
         * {@link Comparable}, and all not null. If you wish to represent a
         * null value, use a dummy object.
         *
         * <p>The client must not modify the array.
         *
         * @return set of values that this dimension may have
         */
        Object[] getValues();
    }

    /**
     * A request for a region. The request has a number of dimensions, a
     * subset of the dimensions of the tree, and for each dimension it either
     * requests a list of values or requests all values.
     */
    public interface SpatialRegionRequest
    {
        /**
         * Provides a list of the dimensions included in this
         * region.
         *
         * @return Dimensions of this region
         */
        List<SpatialDimension> getDimensions();

        /**
         * Provides an array of objects describing this region's
         * bounds within the specified dimension's axis.
         *
         * <p>The values are unique and are sorted. The client must not modify
         * the array.
         *
         * <p>A null array means wildcard. The caller wanted all possible
         * values of this dimension.
         *
         * @param dimension Dimension
         * @return An array of the bounds touched by this region.
         */
        Object[] getValues(SpatialDimension dimension);

        /**
         * Returns whether a request might contain a particular cell.
         *
         * @param coordinates Value for each dimension of cell's coordinates.
         *
         * @return Whether cell is within the bounds of this request
         */
        boolean mightContainCell(Map<SpatialDimension, Object> coordinates);
    }

    public interface SpatialRegion
    {
        /**
         * Returns the specification of this region.
         *
         * @return Region request
         */
        SpatialRegionRequest getRequest();

        /**
         * Returns the value of a cell.
         *
         * <p>Assumes that this region body is valid for the request. (That is,
         * {@link SpatialRegionRequest#mightContainCell(java.util.Map)} would
         * return true.) For example, suppose that the region request was
         *
         * <blockquote>(gender=M, year=any, measure=sales)</blockquote>
         *
         * this region body is
         *
         * <blockquote>(gender=M, year={2009, 2010}, measure=sales)</blockquote>
         *
         * and the cell coordinates are
         *
         * <blockquote>(gender=F, year=2009, measure=sales)</blockquote>
         *
         * <p>Because the coordinate 'gender=F' falls outside the region
         * request, behavior is unspecified. The implementation might
         * return null, throw an error, or return a value for the cell
         * (gender=M, year=2009, measure=sales); any of these behaviors would
         * be valid.
         *
         * @param coordinates Value for each dimension
         *
         * @return cell value
         */
        Object getCellValue(Map<SpatialDimension, Object> coordinates);

        /**
         * Version of {@link #getCellValue(java.util.Map)} optimized for
         * {@code int} values.
         *
         * <p>If value is null, writes 'true' into wasNull[0] and returns 0.
         * Otherwise, does not modify wasNull.
         *
         * @param coordinates Value of each dimension
         * @param wasNull 1-element array to be informed if value was null
         * @return Value, or 0 if value is null
         */
        int getCellValueInt(
            Map<SpatialDimension, Object> coordinates,
            boolean[] wasNull);

        /**
         * Version of {@link #getCellValue(java.util.Map)} optimized for
         * {@code double} values.
         *
         * <p>If value is null, writes 'true' into wasNull[0] and returns 0.
         * Otherwise, does not modify wasNull.
         *
         * @param coordinates Value of each dimension
         * @param wasNull 1-element array to be informed if value was null
         * @return Value, or 0 if value is null
         */
        double getCellValueDouble(
            Map<SpatialDimension, Object> coordinates,
            boolean[] wasNull);
    }
}

// End SpatialValueTree2.java
