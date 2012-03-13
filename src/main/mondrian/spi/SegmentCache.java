/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.spi;

import mondrian.olap.MondrianProperties;

import java.util.List;
import java.util.concurrent.Future;

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
 * <p>Implementations are expected to be thread-safe. Mondrian is likely to
 * submit multiple requests at the same time, from different threads. It is the
 * responsibility of the cache implementation to maintain a consistent
 * state.</p>
 *
 * <p>Implementations must implement a time-out policy, if needed. Mondrian
 * knows that a call to the cache might take a while. (Mondrian uses worker
 * threads to call into the cache for precisely that reason.) Left to its
 * own devices, Mondrian will wait forever for a call to complete. The cache
 * implementation might know that a call to {@link #get} that has taken 100
 * milliseconds already is probably hung, so it should return null or throw
 * an exception. Then Mondrian can get on with its life, and get the segment
 * some other way.</p>
 *
 * <p>Implementations must provide a default empty constructor.
 * Mondrian creates one segment cache instance per Mondrian server.
 * There could be more than one Mondrian server running in the same JVM.
 *
 * @see MondrianProperties#SegmentCache
 * @author LBoudreau
 */
public interface SegmentCache {
    /**
     * Returns a SegmentBody once the
     * cache has returned any results, or null if no
     * segment corresponding to the header could be found.
     *
     * <p>Cache implementations are at liberty to 'forget' segments. Therefore
     * it is allowable for this method to return null at any time, even if
     * {@link #contains(SegmentHeader)} for this segment previously returned
     * true.</p>
     *
     * @param header The header of the segment to find.
     * Consider this as a key.
     *
     * @return A SegmentBody, or <code>null</code>
     * if no corresponding segment could be found in cache.
     */
    SegmentBody get(SegmentHeader header);

    /**
     * Checks if the cache contains a {@link SegmentBody} corresponding
     * to the supplied {@link SegmentHeader}.
     *
     * @param header A header to lookup in the cache.
     * @return Whether corresponding segment could be found in cache.
     */
    boolean contains(SegmentHeader header);

    /**
     * Returns a list of all segments present in the cache.
     *
     * @return A List of segment headers describing the
     * contents of the cache.
     */
    List<SegmentHeader> getSegmentHeaders();

    /**
     * Stores a segment data in the cache.
     *
     * @return Whether the cache write succeeded
     * @param header The header of the segment.
     * @param body The segment body to cache.
     */
    boolean put(SegmentHeader header, SegmentBody body);

    /**
     * Removes a segment from the cache.
     *
     * @param header The header of the segment we want to remove.
     *
     * @return True if the segment was found and removed,
     * false otherwise.
     */
    boolean remove(SegmentHeader header);

    /**
     * Tear down and clean up the cache.
     */
    void tearDown();

    /**
     * Adds a listener to this segment cache implementation.
     * The listener will get notified via
     * {@link SegmentCacheListener.SegmentCacheEvent} instances.
     *
     * @param listener The listener to attach to this cache.
     */
    void addListener(SegmentCacheListener listener);

    /**
     * Unregisters a listener from this segment cache implementation.
     *
     * @param listener The listener to remove.
     */
    void removeListener(SegmentCacheListener listener);

    /**
     * Tells Mondrian whether this segment cache uses the {@link SegmentHeader}
     * objects as an index, thus preserving them in a serialized state, or if
     * it uses its identification number only.
     *
     * <p>Not using a rich index prevents
     * Mondrian from doing partial cache invalidation.</p>
     *
     * <p>It is assumed that this method returns fairly quickly, and for a given
     * cache always returns the same value.</p>
     *
     * @return Whether this segment cache preserves headers in serialized state
     */
    boolean supportsRichIndex();

    /**
     * {@link SegmentCacheListener} objects are used to listen
     * to the state of the cache and be notified of changes to its
     * state or its entries. Mondrian will automatically register
     * a listener with the implementations it uses.
     *
     * Implementations of SegmentCache should only send events if the
     * cause of the event is not Mondrian itself. Only in cases where
     * the cache gets updated by other Mondrian nodes or by a third
     * party application is it required to use this interface.
     */
    interface SegmentCacheListener {
        /**
         * Handle an event
         * @param e Event to handle.
         */
        void handle(SegmentCacheEvent e);

        /**
         * Defines the event types that a listener can look for.
         */
        interface SegmentCacheEvent {
            /**
             * Defined the possible types of events used by
             * the {@link SegmentCacheListener} class.
             */
            enum EventType {
                /**
                 * An Entry was created in cache.
                 */
                ENTRY_CREATED,
                /**
                 * An entry was deleted from the cache.
                 */
                ENTRY_DELETED
            }

            /**
             * Returns the event type of the current SegmentCacheEvent
             * instance.
             */
            EventType getEventType();

            /**
             * Returns the segment header at the source of the event.
             */
            SegmentHeader getSource();

            /**
             * Tells whether or not this event was a local event or
             * an event triggered by an operation on a remote node.
             * If the implementation cannot differentiate or doesn't
             * support remote nodes, always return false.
             */
            boolean isLocal();
        }
    }
}
// End SegmentCache.java
