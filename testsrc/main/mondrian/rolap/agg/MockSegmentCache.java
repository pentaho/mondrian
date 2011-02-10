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

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import mondrian.olap.Util;
import mondrian.spi.SegmentCache;

/**
 * This mock segment cache is used for automated testing. It will try to
 * marshall / unmarshall all {@link SegmentHeader} and {@link SegmentBody}
 * objects that are sent to it.
 * @author LBoudreau
 * @version $Id$
 */
public class MockSegmentCache implements SegmentCache {
    private final static Map<SegmentHeader, SegmentBody> cache =
        new ConcurrentHashMap<SegmentHeader, SegmentBody>();
    final static ExecutorService executor =
        Executors.newSingleThreadExecutor();
    /*
     * package protected property to help testing.
     * if true, calls to the SegmentCache SPI will
     * be delayed by 30 seconds and return bogus returns.
     * It's mainly used to test the timeout function.
     */
    final static boolean sleep = false;
    public Future<Boolean> contains(final SegmentHeader header) {
        FutureTask<Boolean>  task =
            new FutureTask<Boolean>(new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    synchronized (cache) {
                        return cache.containsKey(header);
                    }
                }
            });
        executor.submit(task);
        return task;
    }
    public Future<SegmentBody> get(final SegmentHeader header) {
        FutureTask<SegmentBody>  task =
            new FutureTask<SegmentBody>(new Callable<SegmentBody>() {
                public SegmentBody call() throws Exception {
                    synchronized (cache) {
                        return cache.get(header);
                    }
                }
            });
        executor.submit(task);
        return task;
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
            //deserialize
            byte[] pickled = out.toByteArray();
            InputStream in = new ByteArrayInputStream(pickled);
            ObjectInputStream ois = new ObjectInputStream(in);
            Object o = ois.readObject();
            Util.discard((SegmentHeader) o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Now try it with the body.
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(body);
            oos.close();
            //deserialize
            byte[] pickled = out.toByteArray();
            InputStream in = new ByteArrayInputStream(pickled);
            ObjectInputStream ois = new ObjectInputStream(in);
            Object o = ois.readObject();
            Util.discard((SegmentBody) o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        FutureTask<Boolean>  task =
            new FutureTask<Boolean>(new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    synchronized (cache) {
                        cache.put(header, body);
                        return true;
                    }
                }
            });
        executor.submit(task);
        return task;
    }

    public Future<List<SegmentHeader>> getSegmentHeaders() {
        FutureTask<List<SegmentHeader>> task =
            new FutureTask<List<SegmentHeader>>(
                new Callable<List<SegmentHeader>>() {
                    public List<SegmentHeader> call() throws Exception {
                        synchronized (cache) {
                            return new ArrayList<SegmentHeader>(cache.keySet());
                        }
                    }
                });
        executor.submit(task);
        return task;
    }

    public void tearDown() {
        synchronized (cache) {
            cache.clear();
        }
    }
}
// End MockSegmentCache.java
