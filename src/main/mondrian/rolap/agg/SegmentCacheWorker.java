/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.olap.MondrianProperties;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.SegmentHeader.ConstrainedColumn;
import mondrian.spi.SegmentCache;
import mondrian.util.ServiceDiscovery;

import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

/**
 * Utility class to interact with the {@link SegmentCache}.
 *
 * @author LBoudreau
 * @see SegmentCache
 * @version $Id$
 */
public final class SegmentCacheWorker {

    private final static Logger LOGGER =
        Logger.getLogger(SegmentCacheWorker.class);

    private final SegmentCache cache;

    public SegmentCacheWorker() {
        cache = initCache();

        LOGGER.debug(
            "Segment cache initialized: "
            + cache.getClass().getName());
    }

    /**
     * Instantiates a cache. Never returns null: either returns a not-null value
     * or throws.
     *
     * @return Cache
     */
    private SegmentCache initCache() {
        // First try to get the segmentcache impl class from
        // mondrian properties.
        final String cacheName =
            MondrianProperties.instance().SegmentCache.get();
        if (cacheName != null) {
            return instantiateCache(cacheName);
        }

        // There was no property set. Let's look for Java services.
        final List<Class<SegmentCache>> implementors =
            ServiceDiscovery.forClass(SegmentCache.class).getImplementor();
        if (implementors.size() > 0) {
            // The contract is to use the first implementation found.
            SegmentCache cache =
                instantiateCache(implementors.get(0).getName());
            if (cache != null) {
                return cache;
            }
        }

        throw MondrianResource.instance()
            .SegmentCacheFailedToInstanciate.ex();
    }

    /**
     * Instantiates a cache, given the name of the cache class.
     *
     * @param cacheName Name of class that implements the
     *     {@link mondrian.spi.SegmentCache} SPI
     *
     * @return Cache instance, or null on error
     */
    private SegmentCache instantiateCache(String cacheName) {
        try {
            LOGGER.debug("Starting cache instance: " + cacheName);
            Class<?> clazz =
                Class.forName(cacheName);
            Object scObject = clazz.newInstance();
            if (!(scObject instanceof SegmentCache)) {
                throw MondrianResource.instance()
                    .SegmentCacheIsNotImplementingInterface.ex();
            }
            return (SegmentCache) scObject;
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
     * To adjust timeout values,
     * set {@link MondrianProperties#SegmentCacheReadTimeout}
     *
     * @param header Header to search.
     * @return Either a segment body object or null if there
     * was no cache configured or no segment could be found
     * for the passed header.
     */
    public SegmentBody get(SegmentHeader header) {
        try {
            return cache.get(header)
                .get(
                    MondrianProperties.instance()
                        .SegmentCacheReadTimeout.get(),
                    TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOGGER.error(
                MondrianResource.instance()
                    .SegmentCacheReadTimeout.baseMessage,
                e);
            throw MondrianResource.instance()
                .SegmentCacheReadTimeout.ex(e);
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
     * Returns whether there is a cached segment body available
     * for a given segment header.
     *
     * <p>If no cache is configured or there is an error while
     * querying the cache, false is returned none the less.
     * To adjust timeout values, set
     * {@link MondrianProperties#SegmentCacheLookupTimeout}.
     *
     * @param header A header to search for in the segment cache.
     * @return True or false, whether there is a segment body
     * available in a segment cache.
     */
    public boolean contains(SegmentHeader header) {
        try {
            return cache.contains(header)
                .get(
                    MondrianProperties.instance()
                        .SegmentCacheLookupTimeout.get(),
                    TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOGGER.error(
                MondrianResource.instance()
                    .SegmentCacheLookupTimeout.baseMessage,
                e);
            throw MondrianResource.instance()
                .SegmentCacheLookupTimeout.ex(e);
        } catch (Throwable t) {
            LOGGER.error(
                MondrianResource.instance()
                    .SegmentCacheFailedToLookupSegment.baseMessage,
                t);
            throw MondrianResource.instance()
                .SegmentCacheFailedToLookupSegment.ex(t);
        }
    }

    /**
     * Places a segment in the cache. Returns true or false
     * if the operation succeeds.
     *
     * <p>To adjust timeout values, set the
     * {@link MondrianProperties#SegmentCacheWriteTimeout} property.
     *
     * @param header A header to search for in the segment cache.
     * @param body The segment body to cache.
     */
    public void put(SegmentHeader header, SegmentBody body) {
        try {
            final boolean result =
                cache.put(header, body)
                    .get(
                        MondrianProperties.instance()
                            .SegmentCacheWriteTimeout.get(),
                        TimeUnit.MILLISECONDS);
            if (!result) {
                LOGGER.error(
                    MondrianResource.instance()
                        .SegmentCacheFailedToSaveSegment
                        .baseMessage);
                throw MondrianResource.instance()
                    .SegmentCacheFailedToSaveSegment.ex();
            }
        } catch (TimeoutException e) {
            LOGGER.error(
                MondrianResource.instance()
                    .SegmentCacheReadTimeout.baseMessage,
                e);
            throw MondrianResource.instance()
                .SegmentCacheReadTimeout.ex(e);
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
     * Removes a segment from the cache. Returns true or false
     * if the operation succeeds.
     *
     * <p>To adjust timeout values, set the
     * {@link MondrianProperties#SegmentCacheWriteTimeout} property.
     *
     * @param header A header to remove in the segment cache.
     */
    public void remove(SegmentHeader header) {
        try {
            final boolean result =
                cache.remove(header)
                    .get(
                        MondrianProperties.instance()
                            .SegmentCacheWriteTimeout.get(),
                        TimeUnit.MILLISECONDS);
            if (!result) {
                LOGGER.error(
                    MondrianResource.instance()
                        .SegmentCacheFailedToDeleteSegment
                        .baseMessage);
                throw MondrianResource.instance()
                    .SegmentCacheFailedToDeleteSegment.ex();
            }
        } catch (TimeoutException e) {
            LOGGER.error(
                MondrianResource.instance()
                    .SegmentCacheReadTimeout.baseMessage,
                e);
            throw MondrianResource.instance()
                .SegmentCacheReadTimeout.ex(e);
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
     * Flushes a segment from the cache. Returns true or false
     * if the operation succeeds.
     *
     * <p>To adjust timeout values, set the
     * {@link MondrianProperties#SegmentCacheWriteTimeout} property.
     *
     * @param region A region to flush from the segment cache.
     */
    public void flush(ConstrainedColumn[] region) {
        try {
            final boolean result =
                cache.flush(region)
                    .get(
                        MondrianProperties.instance()
                            .SegmentCacheWriteTimeout.get(),
                        TimeUnit.MILLISECONDS);
            if (!result) {
                LOGGER.error(
                    MondrianResource.instance()
                        .SegmentCacheFailedToDeleteSegment
                        .baseMessage);
                throw MondrianResource.instance()
                    .SegmentCacheFailedToDeleteSegment.ex();
            }
        } catch (TimeoutException e) {
            LOGGER.error(
                MondrianResource.instance()
                    .SegmentCacheReadTimeout.baseMessage,
                e);
            throw MondrianResource.instance()
                .SegmentCacheReadTimeout.ex(e);
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
     * <p>If no cache is configured or there is an error while
     * querying the cache, an empty list is returned none the less.
     * To adjust timeout values, set
     * {@link MondrianProperties#SegmentCacheScanTimeout}
     *
     * @return Either a list of header objects or an empty list if there
     * was no cache configured or no segment could be found
     */
    public List<SegmentHeader> getSegmentHeaders() {
        try {
            return cache.getSegmentHeaders()
                .get(
                    MondrianProperties.instance()
                        .SegmentCacheScanTimeout.get(),
                    TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOGGER.error(
                MondrianResource.instance()
                    .SegmentCacheScanTimeout.baseMessage,
                e);
            throw MondrianResource.instance()
                .SegmentCacheScanTimeout.ex(e);
        } catch (Throwable t) {
            LOGGER.error("Failed to get a list of segment headers.", t);
            throw MondrianResource.instance()
                .SegmentCacheFailedToScanSegments.ex(t);
        }
    }
}

// End SegmentCacheWorker.java
