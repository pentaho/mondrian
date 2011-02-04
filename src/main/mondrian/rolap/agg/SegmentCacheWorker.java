/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011 Julian Hyde and others
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

import org.apache.log4j.Logger;
import org.eigenbase.util.property.Property;
import org.eigenbase.util.property.TriggerBase;

/**
 * Utility class to interact with the SegmentCache.
 * @author LBoudreau
 * @version $Id$
 */
public final class SegmentCacheWorker {

    private final static Logger LOGGER =
        Logger.getLogger(SegmentCacheWorker.class);
    private static SegmentCache segmentCache = null;
    private final static ExecutorService executor =
        Executors.newCachedThreadPool();

    static {
        final String cacheName =
            MondrianProperties.instance().SegmentCache.get();
        if (cacheName != null) {
            setCache(cacheName);
        }
        // Rig up a trigger to that property to hot-swap the cache.
        MondrianProperties.instance().SegmentCache.addTrigger(
            new TriggerBase(true) {
                public void execute(Property property, String value) {
                    setCache(value);
                }
            }
       );
    }

    private static final void setCache(String cacheName) {
        try {
            final SegmentCache scopedSC = segmentCache;
            if (scopedSC != null
                && scopedSC.getClass().equals(cacheName))
            {
                // No need to reload the cache.
                // It's the same property value.
                return;
            }
            if (scopedSC != null) {
                executor.submit(
                    new Runnable() {
                        public void run() {
                            LOGGER.debug("Tearing down segment cache.");
                            scopedSC.tearDown();
                        }
                    });
            }
            segmentCache = null;
            LOGGER.debug("Starting cache instance:" + cacheName);
            Class<?> clazz =
                Class.forName(cacheName);
            Object scObject = clazz.newInstance();
            if (scObject instanceof SegmentCache) {
                segmentCache = (SegmentCache) scObject;
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
            if (MondrianProperties.instance()
                .SegmentCacheFailOnError.get())
            {
                throw MondrianResource.instance()
                    .SegmentCacheFailedToInstanciate.ex(e);
            }
        }
    }

    /**
     * Returns a segment body corresponding to a header.
     * <p>If no cache is configured or there is an error while
     * querying the cache, null is returned none the less.
     * To throw an exception, enable
     * {@link MondrianProperties#SegmentCacheFailOnError} To adjust
     * timeout values, set {@link MondrianProperties#SegmentCacheReadTimeout}
     * @param header Header to search.
     * @return Either a segment body object or null if there
     * was no cache configured or no segment could be found
     * for the passed header.
     */
    public final static SegmentBody get(SegmentHeader header) {
        if (segmentCache != null) {
            try {
                return segmentCache.get(header)
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
                if (MondrianProperties.instance()
                    .SegmentCacheFailOnError.get())
                {
                    throw MondrianResource.instance()
                        .SegmentCacheFailedToLoadSegment.ex(t);
                }
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
     * To throw an exception, enable
     * {@link MondrianProperties#SegmentCacheFailOnError} To adjust
     * timeout values, set {@link MondrianProperties#SegmentCacheLookupTimeout}
     * @param header A header to search for in the segment cache.
     * @return True or false, whether there is a segment body
     * available in a segment cache.
     */
    public final static Boolean contains(SegmentHeader header) {
        if (segmentCache != null) {
            try {
                return segmentCache.contains(header)
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
                if (MondrianProperties.instance()
                    .SegmentCacheFailOnError.get())
                {
                    throw MondrianResource.instance()
                        .SegmentCacheFailedToLookupSegment.ex(t);
                }
            }
        }
        return false;
    }

    /**
     * Places a segment in the cache. Returns true or false
     * if the operation succeeds.
     *
     * <p>If no cache is configured or there is an error while
     * querying the cache, false is returned none the less.
     * To throw an exception, enable
     * {@link MondrianProperties#SegmentCacheFailOnError} To adjust
     * timeout values, set {@link MondrianProperties#SegmentCacheWriteTimeout}
     * @param header A header to search for in the segment cache.
     * @return True or false, whether there is a segment body
     * available in a segment cache.
     */
    public final static Boolean put(SegmentHeader header, SegmentBody body) {
        if (segmentCache != null) {
            try {
                return segmentCache.put(header, body)
                    .get(
                        MondrianProperties.instance()
                        .SegmentCacheWriteTimeout.get(),
                        TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOGGER.error(
                    MondrianResource.instance()
                        .SegmentCacheReadTimeout.baseMessage,
                    e);
                if (MondrianProperties.instance()
                    .SegmentCacheFailOnError.get())
                {
                    throw MondrianResource.instance()
                        .SegmentCacheReadTimeout.ex(e);
                }
            } catch (Throwable t) {
                LOGGER.error("Failed to save segment to cache.", t);
                if (MondrianProperties.instance()
                    .SegmentCacheFailOnError.get())
                {
                    throw MondrianResource.instance()
                        .SegmentCacheFailedToSaveSegment.ex(t);
                }
            }
        }
        return false;
    }

    /**
     * Returns a list of segments present in the cache.
     *
     * <p>If no cache is configured or there is an error while
     * querying the cache, an empty list is returned none the less.
     * To throw an exception, enable
     * {@link MondrianProperties#SegmentCacheFailOnError} To adjust
     * timeout values, set {@link MondrianProperties#SegmentCacheScanTimeout}
     *
     * @param header Header to search.
     * @return Either a segment body object or null if there
     * was no cache configured or no segment could be found
     * for the passed header.
     */
    public final static List<SegmentHeader> getSegmentHeaders() {
        if (segmentCache != null) {
            try {
                return segmentCache.getSegmentHeaders()
                    .get(
                        MondrianProperties.instance()
                        .SegmentCacheScanTimeout.get(),
                        TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOGGER.error(
                    MondrianResource.instance()
                        .SegmentCacheScanTimeout.baseMessage,
                    e);
                if (MondrianProperties.instance()
                    .SegmentCacheFailOnError.get())
                {
                    throw MondrianResource.instance()
                        .SegmentCacheScanTimeout.ex(e);
                }
            } catch (Throwable t) {
                LOGGER.error("Failed to get a list of segment headers.", t);
                if (MondrianProperties.instance()
                    .SegmentCacheFailOnError.get())
                {
                    throw MondrianResource.instance()
                        .SegmentCacheFailedToScanSegments.ex(t);
                }
            }
        }
        return Collections.emptyList();
    }
}

// End SegmentCacheWorker.java
