/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.rolap.cache;

import mondrian.rolap.BitKey;
import mondrian.rolap.agg.SegmentBuilder;
import mondrian.server.Execution;
import mondrian.spi.*;
import mondrian.util.ByteString;

import java.io.PrintWriter;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Data structure that identifies which segments contain cells.
 *
 * <p>Not thread-safe.</p>
 *
 * @author Julian Hyde
 */
public interface SegmentCacheIndex {
    /**
     * Identifies the segment headers that contain a given cell.
     *
     * @param schemaName Schema name
     * @param schemaChecksum Schema checksum
     * @param cubeName Cube name
     * @param measureName Measure name
     * @param rolapStarFactTableName Fact table table
     * @param constrainedColsBitKey Bit key
     * @param coordinates Coordinates
     * @param compoundPredicates Compound predicates
     * @return Empty list if not found; never null
     */
    List<SegmentHeader> locate(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        Map<String, Comparable> coordinates,
        List<String> compoundPredicates);

    /**
     * Returns a list of segments that can be rolled up to satisfy a given
     * cell request.
     *
     * @param schemaName Schema name
     * @param schemaChecksum Schema checksum
     * @param cubeName Cube name
     * @param measureName Measure name
     * @param rolapStarFactTableName Fact table table
     * @param constrainedColsBitKey Bit key
     * @param coordinates Coordinates
     * @param compoundPredicates Compound predicates
     *
     * @return List of candidates; each element is a list of headers that, when
     * combined using union, are sufficient to answer the given cell request
     */
    List<List<SegmentHeader>> findRollupCandidates(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        Map<String, Comparable> coordinates,
        List<String> compoundPredicates);

    /**
     * Finds a list of headers that intersect a given region.
     *
     * <p>This method is used to find out which headers need to be trimmed
     * or removed during a flush.</p>
     *
     * @param schemaName Schema name
     * @param schemaChecksum Schema checksum
     * @param cubeName Cube name
     * @param measureName Measure name
     * @param rolapStarFactTableName Fact table table
     * @param region Region
     * @return List of intersecting headers
     */
    public List<SegmentHeader> intersectRegion(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        SegmentColumn[] region);

    /**
     * Adds a header to the index.
     *
     * <p>If {@code loading} is true, there must follow a call to
     * {@link #loadSucceeded} or {@link #loadFailed}.</p>
     *
     * @param header Segment header
     * @param loading Whether segment is pending a load from SQL
     * @param converter Segment converter
     * @return True or false, if a new element was inserted into the index.
     */
    boolean add(
        SegmentHeader header,
        boolean loading,
        SegmentBuilder.SegmentConverter converter);

    /**
     * Changes the state of a header from loading to loaded.
     *
     * <p>The segment must have previously been added by calling {@link #add}
     * with a not-null value of the {@code bodyFuture} parameter;
     * neither {@code loadSucceeded} nor {@link #loadFailed} must have been
     * called.</p>
     *
     * <p>Informs anyone waiting on the future supplied with
     * {@link #add}.</p>
     *
     * @param header Segment header
     * @param body Segment body
     */
    void loadSucceeded(
        SegmentHeader header,
        SegmentBody body);

    /**
     * Notifies the segment index that a segment failed to load, and removes the
     * segment from the index.
     *
     * <p>The segment must have previously been added using {@link #add}
     * with a not-null value of the {@code bodyFuture} parameter;
     * neither {@link #loadSucceeded} nor {@code loadFailed} must have been
     * called.</p>
     *
     * <p>Informs anyone waiting on the future supplied with
     * {@link #add}.</p>
     *
     * @param header Header
     * @param throwable Error message
     */
    void loadFailed(
        SegmentHeader header,
        Throwable throwable);

    /**
     * Removes a header from the index.
     *
     * @param header Segment header
     */
    void remove(SegmentHeader header);

    /**
     * Prints the state of the cache to the given writer.
     *
     * @param pw Print writer
     */
    void printCacheState(PrintWriter pw);

    /**
     * Returns a future slot for a segment body, if a segment is currently
     * loading, otherwise null. This is the method to use to get segments
     * 'hot out of the oven'.
     *
     * <p>When this method is invoked, the execution instance of the
     * thread is automatically added to the list of clients for the
     * given segment. The calling code is responsible for calling
     * {@link #cancel(Execution)} when it is done with the segments,
     * or else this registration will prevent others from canceling
     * the running SQL statements associated to this segment.
     *
     * @param header Segment header
     * @return Slot, or null
     */
    Future<SegmentBody> getFuture(Execution exec, SegmentHeader header);

    /**
     * This method must remove all registrations as a client
     * for the given execution.
     *
     * This must terminate all SQL activity for any orphaned
     * segments.
     * @param exec The execution to unregister.
     */
    void cancel(Execution exec);

    /**
     * Tells whether or not a given segment is known to this index.
     */
    public boolean contains(SegmentHeader header);

    /**
     * Allows to link a {@link Statement} to a segment. This allows
     * the index to cleanup when {@link #cancel(Execution)} is
     * invoked and orphaned segments are left.
     * @param header The segment.
     * @param stmt The SQL statement.
     */
    public void linkSqlStatement(SegmentHeader header, Statement stmt);

    /**
     * Returns a converter that can convert the given header to internal
     * format.
     *
     * @param schemaName Schema name
     * @param schemaChecksum Schema checksum
     * @param cubeName Cube name
     * @param rolapStarFactTableName Fact table
     * @param measureName Measure name
     * @param compoundPredicates Compound predicates
     * @return Converter
     */
    SegmentBuilder.SegmentConverter getConverter(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String rolapStarFactTableName,
        String measureName,
        List<String> compoundPredicates);

    /**
     * Sets a converter that can convert headers in for a given measure to
     * internal format.
     *
     * @param schemaName Schema name
     * @param schemaChecksum Schema checksum
     * @param cubeName Cube name
     * @param rolapStarFactTableName Fact table
     * @param measureName Measure name
     * @param compoundPredicates Compound predicates
     * @param converter Converter to store
     */
    void setConverter(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String rolapStarFactTableName,
        String measureName,
        List<String> compoundPredicates,
        SegmentBuilder.SegmentConverter converter);
}

// End SegmentCacheIndex.java
