/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.MondrianProperties;
import mondrian.resource.MondrianResource;
import mondrian.spi.*;
import mondrian.util.ClassResolver;
import mondrian.util.ServiceDiscovery;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * Utility class to interact with the {@link SegmentCache}.
 *
 * @author LBoudreau
 * @see SegmentCache
 */
public final class SegmentCacheWorker {

    private static final Logger LOGGER =
        Logger.getLogger(SegmentCacheWorker.class);

    final SegmentCache cache;
    private final Thread cacheMgrThread;
    private final boolean supportsRichIndex;

    /**
     * Creates a worker.
     *
     * @param cache Cache managed by this worker
     * @param cacheMgrThread Thread that the cache manager actor is running on,
     *                       and which therefore should not be used for
     *                       potentially long-running calls this this cache.
     *                       Pass null if methods can be called from any thread.
     */
    public SegmentCacheWorker(SegmentCache cache, Thread cacheMgrThread) {
        this.cache = cache;
        this.cacheMgrThread = cacheMgrThread;

        // no need to call checkThread(): supportsRichIndex is a fast call
        this.supportsRichIndex = cache.supportsRichIndex();

        LOGGER.debug(
            "Segment cache initialized: "
            + cache.getClass().getName());
    }

    /**
     * Instantiates a cache. Returns null if there is no external cache defined.
     *
     * @return Cache
     */
    public static List<SegmentCache> initCache() {
        final List<SegmentCache> caches =
            new ArrayList<SegmentCache>();
        // First try to get the segmentcache impl class from
        // mondrian properties.
        final String cacheName =
            MondrianProperties.instance().SegmentCache.get();
        if (cacheName != null) {
            caches.add(instantiateCache(cacheName));
        }

        // There was no property set. Let's look for Java services.
        final List<Class<SegmentCache>> implementors =
            ServiceDiscovery.forClass(SegmentCache.class).getImplementor();
        if (implementors.size() > 0) {
            // The contract is to use the first implementation found.
            SegmentCache cache =
                instantiateCache(implementors.get(0).getName());
            if (cache != null) {
                caches.add(cache);
            }
        }

        // Check the SegmentCacheInjector
        // People might have sent instances into this thing.
        caches.addAll(SegmentCache.SegmentCacheInjector.getCaches());

        // Done.
        return caches;
    }

    /**
     * Instantiates a cache, given the name of the cache class.
     *
     * @param cacheName Name of class that implements the
     *     {@link mondrian.spi.SegmentCache} SPI
     *
     * @return Cache instance, or null on error
     */
    private static SegmentCache instantiateCache(String cacheName) {
        try {
            LOGGER.debug("Starting cache instance: " + cacheName);
            return ClassResolver.INSTANCE.instantiateSafe(cacheName);
        } catch (ClassCastException e) {
            throw MondrianResource.instance()
                .SegmentCacheIsNotImplementingInterface.ex();
        } catch (Exception e) {
            LOGGER.error(
                MondrianResource.instance()
                    .SegmentCacheFailedToInstanciate.baseMessage,
                e);
            throw MondrianResource.instance()
                .SegmentCacheFailedToInstanciate.ex(e);
        }
    }

    /**
     * Returns a segment body corresponding to a header.
     *
     * <p>If no cache is configured or there is an error while
     * querying the cache, null is returned none the less.
     *
     * @param header Header to search.
     * @return Either a segment body object or null if there
     * was no cache configured or no segment could be found
     * for the passed header.
     */
    public SegmentBody get(SegmentHeader header) {
        checkThread();
        try {
            return cache.get(header);
        } catch (Throwable t) {
            LOGGER.error(
                MondrianResource.instance()
                    .SegmentCacheFailedToLoadSegment
                    .baseMessage,
                t);
            throw MondrianResource.instance()
                .SegmentCacheFailedToLoadSegment.ex(t);
        }
    }

    /**
     * Places a segment in the cache. Returns true or false
     * if the operation succeeds.
     *
     * @param header A header to search for in the segment cache.
     * @param body The segment body to cache.
     */
    public void put(SegmentHeader header, SegmentBody body) {
        checkThread();
        try {
            final boolean result = cache.put(header, body);
            if (!result) {
                LOGGER.error(
                    MondrianResource.instance()
                        .SegmentCacheFailedToSaveSegment
                        .baseMessage);
                throw MondrianResource.instance()
                    .SegmentCacheFailedToSaveSegment.ex();
            }
        } catch (Throwable t) {
            LOGGER.error(
                MondrianResource.instance()
                    .SegmentCacheFailedToSaveSegment
                    .baseMessage,
                t);
            throw MondrianResource.instance()
                .SegmentCacheFailedToSaveSegment.ex(t);
        }
    }

    /**
     * Removes a segment from the cache.
     *
     * @param header A header to remove in the segment cache.
     * @return Whether a segment was removed
     */
    public boolean remove(SegmentHeader header) {
        checkThread();
        try {
            return cache.remove(header);
        } catch (Throwable t) {
            LOGGER.error(
                MondrianResource.instance()
                    .SegmentCacheFailedToDeleteSegment
                    .baseMessage,
                t);
            throw MondrianResource.instance()
                .SegmentCacheFailedToDeleteSegment.ex(t);
        }
    }

    /**
     * Returns a list of segments present in the cache.
     *
     * @return List of headers in the cache
     */
    public List<SegmentHeader> getSegmentHeaders() {
        checkThread();
        try {
            return cache.getSegmentHeaders();
        } catch (Throwable t) {
            LOGGER.error("Failed to get a list of segment headers.", t);
            throw MondrianResource.instance()
                .SegmentCacheFailedToScanSegments.ex(t);
        }
    }

    public boolean supportsRichIndex() {
        return supportsRichIndex;
    }

    public void shutdown() {
        checkThread();
        cache.tearDown();
    }

    private void checkThread() {
        assert cacheMgrThread != Thread.currentThread()
            : "this method is potentially slow; you should not call it from "
            + "the cache manager thread, " + cacheMgrThread;
    }
}

// End SegmentCacheWorker.java
