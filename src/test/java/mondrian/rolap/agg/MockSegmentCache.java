/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.spi.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Mock implementation of {@link SegmentCache} that is used for automated
 * testing.
 *
 * <P>It tries to marshall / unmarshall all {@link SegmentHeader} and
 * {@link SegmentBody} objects that are sent to it.
 *
 * @author LBoudreau
 */
public class MockSegmentCache implements SegmentCache {
    private static final Map<SegmentHeader, SegmentBody> cache =
        new ConcurrentHashMap<SegmentHeader, SegmentBody>();

    private final List<SegmentCacheListener> listeners =
        new CopyOnWriteArrayList<SegmentCacheListener>();

    private Random rnd;

    private static final int maxElements = 100;

    public boolean contains(SegmentHeader header) {
        return cache.containsKey(header);
    }

    public SegmentBody get(SegmentHeader header) {
        return cache.get(header);
    }

    public boolean put(
        final SegmentHeader header,
        final SegmentBody body)
    {
        // Try to serialize back and forth. if the tests fail because of this,
        // then the objects could not be serialized properly.
        // First try with the header
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(header);
            oos.close();
            // deserialize
            byte[] pickled = out.toByteArray();
            InputStream in = new ByteArrayInputStream(pickled);
            ObjectInputStream ois = new ObjectInputStream(in);
            SegmentHeader o = (SegmentHeader) ois.readObject();
            Util.discard(o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Now try it with the body.
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(body);
            oos.close();
            // deserialize
            byte[] pickled = out.toByteArray();
            InputStream in = new ByteArrayInputStream(pickled);
            ObjectInputStream ois = new ObjectInputStream(in);
            SegmentBody o = (SegmentBody) ois.readObject();
            Util.discard(o);
        } catch (NotSerializableException e) {
            throw new RuntimeException(
                "while serializing " + body,
                e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        cache.put(header, body);
        fireSegmentCacheEvent(
            new SegmentCache.SegmentCacheListener.SegmentCacheEvent()
            {
                public boolean isLocal() {
                    return true;
                }
                public SegmentHeader getSource() {
                    return header;
                }
                public EventType getEventType() {
                    return
                        SegmentCacheListener.SegmentCacheEvent
                            .EventType.ENTRY_CREATED;
                }
            });
        if (cache.size() > maxElements) {
            // Cache is full. pop one out at random.
            if (rnd == null) {
                rnd = new Random();
            }
            int index = rnd.nextInt(maxElements);
            for (Iterator<SegmentHeader> iterator = cache.keySet().iterator();
                 iterator.hasNext();)
            {
                Util.discard(iterator.next());
                if (index-- == 0) {
                    iterator.remove();
                    break;
                }
            }
            fireSegmentCacheEvent(
                new SegmentCache.SegmentCacheListener.SegmentCacheEvent()
                {
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
        return true;
    }

    public List<SegmentHeader> getSegmentHeaders() {
        return new ArrayList<SegmentHeader>(cache.keySet());
    }

    public boolean remove(final SegmentHeader header) {
        cache.remove(header);
        fireSegmentCacheEvent(
            new SegmentCache.SegmentCacheListener.SegmentCacheEvent()
            {
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
        return true;
    }

    public void tearDown() {
        listeners.clear();
        cache.clear();
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
        SegmentCache.SegmentCacheListener.SegmentCacheEvent event)
    {
        for (SegmentCacheListener listener : listeners) {
            listener.handle(event);
        }
    }
}

// End MockSegmentCache.java
