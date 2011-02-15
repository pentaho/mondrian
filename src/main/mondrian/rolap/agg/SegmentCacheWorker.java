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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import mondrian.olap.MondrianProperties;
import mondrian.resource.MondrianResource;
import mondrian.spi.SegmentCache;
import mondrian.util.ServiceDiscovery;

import org.apache.log4j.Logger;
import org.eigenbase.util.property.Property;
import org.eigenbase.util.property.TriggerBase;

/**
 * Utility class to interact with the {@link SegmentCache}.
 * @author LBoudreau
 * @see SegmentCache
 * @version $Id$
 */
public final class SegmentCacheWorker {

    private final static Logger LOGGER =
        Logger.getLogger(SegmentCacheWorker.class);
    private static SegmentCache segmentCache = null;
    private final static ExecutorService executor =
        Executors.newCachedThreadPool();
    private final static ServiceDiscovery<SegmentCache> serviceDiscovery =
        ServiceDiscovery.forClass(SegmentCache.class);

    static {
        // First try to get the segmentcache impl class from
        // mondrian properties.
        final String cacheName =
            MondrianProperties.instance().SegmentCache.get();
        if (cacheName != null) {
            // There was a property defined. We use this one
            // by default.
            setCache(cacheName);
        } else {
            // There was no property set. Let's look for Java services.
            final List<Class<SegmentCache>> implementors =
                serviceDiscovery.getImplementor();
            if (implementors.size() > 0) {
                // The contract is to use the first implementation found.
                setCache(implementors.get(0).getName());
            }
        }
        // Rig up a trigger to the SegmentCache property to hot-swap the cache.
        MondrianProperties.instance().SegmentCache.addTrigger(
            new TriggerBase(true) {
                public void execute(Property property, String value) {
                    setCache(value);
                }
            }
       );
    }

    /**
     * Sets the current cache implementation, closing the current cache (if
     * any).
     *
     * <p>If {@code cacheName} is null, just closes the current cache.
     *
     * @param cacheName Name of class that implements the {@link SegmentCache}
     *     API
     */
    public static synchronized void setCache(String cacheName) {
        try {
            final SegmentCache closureSC = segmentCache;
            if (closureSC != null
                && cacheName != null
                && closureSC.getClass().getName().equals(cacheName))
            {
                // No need to reload the cache.
                // It's the same property value.
                return;
            }
            if (closureSC != null) {
                executor.submit(
                    new Runnable() {
                        public void run() {
                            LOGGER.debug("Tearing down segment cache.");
                            closureSC.tearDown();
                        }
                    });
            }
            if (cacheName == null) {
                return;
            }
            segmentCache = null;
            LOGGER.debug("Starting cache instance:" + cacheName);
            Class<?> clazz =
                Class.forName(cacheName);
            Object scObject = clazz.newInstance();
            if (scObject instanceof SegmentCache) {
                segmentCache = (SegmentCache) scObject;
                LOGGER.debug(
                    "Segment cache initialized:"
                    + segmentCache.getClass().getName());
            } else {
                LOGGER.error(
                    MondrianResource.instance()
                        .SegmentCacheIsNotImplementingInterface
                            .baseMessage);
            }
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
     * <p>If no cache is configured or there is an error while
     * querying the cache, null is returned none the less.
     * To adjust timeout values,
     * set {@link MondrianProperties#SegmentCacheReadTimeout}
     * @param header Header to search.
     * @return Either a segment body object or null if there
     * was no cache configured or no segment could be found
     * for the passed header.
     */
    public static SegmentBody get(SegmentHeader header) {
        final SegmentCache closureSC = segmentCache;
        if (closureSC != null) {
            try {
                return closureSC.get(header)
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
        return null;
    }

    /**
     * Returns whether there is a cached segment body available
     * for a given segment header.
     *
     * <p>If no cache is configured or there is an error while
     * querying the cache, false is returned none the less.
     * To adjust timeout values, set
     * {@link MondrianProperties#SegmentCacheLookupTimeout}
     * @param header A header to search for in the segment cache.
     * @return True or false, whether there is a segment body
     * available in a segment cache.
     */
    public static boolean contains(SegmentHeader header) {
        final SegmentCache closureSC = segmentCache;
        if (closureSC != null) {
            try {
                return closureSC.contains(header)
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
        return false;
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
    public static void put(SegmentHeader header, SegmentBody body) {
        final SegmentCache closureSC = segmentCache;
        if (closureSC != null) {
            try {
                final boolean result =
                    closureSC.put(header, body)
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
    public static List<SegmentHeader> getSegmentHeaders() {
        final SegmentCache closureSC = segmentCache;
        if (closureSC != null) {
            try {
                return closureSC.getSegmentHeaders()
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
        return Collections.emptyList();
    }
}

// End SegmentCacheWorker.java
