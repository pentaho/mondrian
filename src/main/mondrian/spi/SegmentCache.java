/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/
package mondrian.spi;

import java.util.List;
import java.util.concurrent.Future;

import mondrian.olap.MondrianProperties;
import mondrian.rolap.agg.SegmentBody;
import mondrian.rolap.agg.SegmentHeader;

/**
 * SPI definition of the segments cache.
 *
 * <p>Lookups are performed using {@link SegmentHeader}s and
 * {@link SegmentBody}s. Both are immutable and fully serializable.
 *
 * <p>There are two ways to declare a SegmentCache implementation in
 * Mondrian. The first one (and the one which will be used by default)
 * is to set the {@link MondrianProperties#SegmentCache} property. The
 * second one is to use the Java Services API. You will need to create
 * a jar file, accessible through the same class loader as Mondrian,
 * and add a file called <code>/META-INF/services/mondrian.spi.SegmentCache
 * </code> which contains the name of the segment cache implementation
 * to use. If more than one SegmentCache Java service is found, the first
 * one found is used. This is a non-deterministic choice as there are
 * no guarantees as to which will appear first. This later mean of discovery
 * is overridden by defining the {@link MondrianProperties#SegmentCache}
 * property.
 *
 * <p>Implementations are expected to be thread-safe.
 * It is the responsibility of the cache implementation
 * to maintain a consistent state.
 *
 * <p>Implementations must provide a default empty constructor.
 * Segment caches are instantiated as a singleton but can be
 * hot swapped by modifying {@link MondrianProperties#SegmentCache}.
 *
 * <p>Implementations will get a termination signal through
 * {@link SegmentCache#tearDown()} but Mondrian will relinquish
 * control of the termination thread and will not be listening
 * to thrown any exceptions encountered while the tearing down
 * operation is taking place..
 *
 * @see MondrianProperties#SegmentCache
 * @author LBoudreau
 * @version $Id$
 */
public interface SegmentCache {
    /**
     * Returns a future SegmentBody object once the
     * cache has returned any results, or null of no
     * segment corresponding to the header could be found.
     * @param header The header of the segment to find.
     * Consider this as a key.
     * @return A Future SegmentBody or a Future <code>null</code>
     * if no corresponding segment could be found in cache.
     */
    Future<SegmentBody> get(SegmentHeader header);

    /**
     * Checks if the cache contains a {@link SegmentBody} corresponding
     * to the supplied {@link SegmentHeader}.
     * @param header A header to lookup in the cache.
     * @return A Future true or a Future false
     * if no corresponding segment could be found in cache.
     */
    Future<Boolean> contains(SegmentHeader header);

    /**
     * Returns a list of all segments present in the cache.
     * @return A List of segment headers describing the
     * contents of the cache.
     */
    Future<List<SegmentHeader>> getSegmentHeaders();

    /**
     * Stores a segment data in the cache.
     * @return A Future object which returns true or false
     * depending on the success of the caching operation.
     * @param header The header of the segment.
     * @param body The segment body to cache.
     */
    Future<Boolean> put(SegmentHeader header, SegmentBody body);

    /**
     * Removes a segment from the cache.
     * @param header The header of the segment we want to remove.
     * @return True if the segment was found and removed,
     * false otherwise.
     */
    Future<Boolean> remove(SegmentHeader header);

    /**
     * Tear down and clean up the cache.
     */
    void tearDown();
}
// End SegmentCache.java
