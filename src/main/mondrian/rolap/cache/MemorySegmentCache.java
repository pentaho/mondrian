/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.cache;

import mondrian.spi.*;

import java.lang.ref.SoftReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Implementation of {@link mondrian.spi.SegmentCache} that stores segments
 * in memory.
 *
 * <p>Segments are held via soft references, so the garbage collector can remove
 * them if it sees fit.</p>
 *
 * @author Julian Hyde
 */
public class MemorySegmentCache implements SegmentCache {
    // Use a thread-safe map because the SegmentCache
    // interface requires thread safety.
    private final Map<SegmentHeader, SoftReference<SegmentBody>> map =
        new ConcurrentHashMap<SegmentHeader, SoftReference<SegmentBody>>();
    private final List<SegmentCacheListener> listeners =
        new CopyOnWriteArrayList<SegmentCacheListener>();

    public SegmentBody get(SegmentHeader header) {
        final SoftReference<SegmentBody> ref = map.get(header);
        if (ref == null) {
            return null;
        }
        final SegmentBody body = ref.get();
        if (body == null) {
            map.remove(header);
        }
        return body;
    }

    public boolean contains(SegmentHeader header) {
        final SoftReference<SegmentBody> ref = map.get(header);
        if (ref == null) {
            return false;
        }
        final SegmentBody body = ref.get();
        if (body == null) {
            map.remove(header);
            return false;
        }
        return true;
    }

    public List<SegmentHeader> getSegmentHeaders() {
        return new ArrayList<SegmentHeader>(map.keySet());
    }

    public boolean put(final SegmentHeader header, SegmentBody body) {
        // REVIEW: What's the difference between returning false
        // and throwing an exception?
        assert header != null;
        assert body != null;
        map.put(header, new SoftReference<SegmentBody>(body));
        fireSegmentCacheEvent(
            new SegmentCache.SegmentCacheListener.SegmentCacheEvent() {
                public boolean isLocal() {
                    return true;
                }
                public SegmentHeader getSource() {
                    return header;
                }
                public EventType getEventType() {
                    return SegmentCacheListener.SegmentCacheEvent
                        .EventType.ENTRY_CREATED;
                }
            });
        return true; // success
    }

    public boolean remove(final SegmentHeader header) {
        final boolean result =
            map.remove(header) != null;
        if (result) {
            fireSegmentCacheEvent(
                new SegmentCache.SegmentCacheListener.SegmentCacheEvent() {
                    public boolean isLocal() {
                        return true;
                    }
                    public SegmentHeader getSource() {
                        return header;
                    }
                    public EventType getEventType() {
                        return
                            SegmentCacheListener.SegmentCacheEvent
                                .EventType.ENTRY_DELETED;
                    }
                });
        }
        return result;
    }

    public void tearDown() {
        map.clear();
        listeners.clear();
    }

    public void addListener(SegmentCacheListener listener) {
        listeners.add(listener);
    }

    public void removeListener(SegmentCacheListener listener) {
        listeners.remove(listener);
    }

    public boolean supportsRichIndex() {
        return true;
    }

    public void fireSegmentCacheEvent(
        SegmentCache.SegmentCacheListener.SegmentCacheEvent evt)
    {
        for (SegmentCacheListener listener : listeners) {
            listener.handle(evt);
        }
    }
}

// End MemorySegmentCache.java
