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
package mondrian.rolap.agg;

import java.util.List;
import java.util.concurrent.Future;

import mondrian.olap.MondrianProperties;

/**
 * SPI definition of the segments cache. Implementations are
 * expected to be thread-safe. It is the responsibility of the
 * cache implementation to maintain a consistent state.
 *
 * <p>Implementations must provide a default empty constructor.
 * It will be called by each segment loader. The SegmentCache
 * objects are spawned often. We recommend using
 * a facade object which points to a singleton cache instance.
 *
 * <p>Lookups are performed using {@link SegmentHeader}s and
 * {@link SegmentBody}s. Both are immutable and fully serializable.
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
}
// End SegmentCache.java
