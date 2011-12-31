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

import mondrian.olap.Util;
import mondrian.rolap.agg.SegmentHeader.ConstrainedColumn;
import mondrian.spi.SegmentCache;

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
 * @version $Id$
 */
public class MockSegmentCache implements SegmentCache {
    private static final Map<SegmentHeader, SegmentBody> cache =
        new ConcurrentHashMap<SegmentHeader, SegmentBody>();

    private final static int maxElements = 100;

    /**
     * Executor for the tests. Thread-factory ensures that thread does not
     * prevent shutdown.
     */
    private static final ExecutorService executor =
        Util.getExecutorService(
            1,
            "mondrian.rolap.agg.MockSegmentCache$ExecutorThread");

    public Future<Boolean> contains(final SegmentHeader header) {
        return executor.submit(
            new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    return cache.containsKey(header);
                }
            });
    }

    public Future<SegmentBody> get(final SegmentHeader header) {
        return executor.submit(
            new Callable<SegmentBody>() {
                public SegmentBody call() throws Exception {
                    return cache.get(header);
                }
            });
    }

    public Future<Boolean> put(
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
        return executor.submit(
            new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    cache.put(header, body);
                    if (cache.size() > maxElements) {
                        // Cache is full. pop one out at random.
                        cache.remove(
                            Math.floor(maxElements * Math.random()));
                    }
                    return true;
                }
            });
    }

    public Future<List<SegmentHeader>> getSegmentHeaders() {
        return executor.submit(
            new Callable<List<SegmentHeader>>() {
                public List<SegmentHeader> call() throws Exception {
                    return new ArrayList<SegmentHeader>(cache.keySet());
                }
            });
    }

    public Future<Boolean> remove(final SegmentHeader header) {
        return executor.submit(
            new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    cache.remove(header);
                    return true;
                }
            });
    }

    public Future<Boolean> flush(final ConstrainedColumn[] region) {
        return executor.submit(
            new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    final Set<SegmentHeader> toEvict =
                        new HashSet<SegmentHeader>();
                    for (SegmentHeader sh : cache.keySet()) {
                        final List<ConstrainedColumn> cc2 =
                            Arrays.asList(region);
                        for (ConstrainedColumn cc : region) {
                            if (cc2.contains(cc.getColumnExpression())) {
                                // Must flush.
                                toEvict.add(sh);
                            }
                        }
                    }
                    for (SegmentHeader sh : toEvict) {
                        cache.remove(sh);
                    }
                    return true;
                }
            });
    }

    public void tearDown() {
        cache.clear();
    }
}

// End MockSegmentCache.java
