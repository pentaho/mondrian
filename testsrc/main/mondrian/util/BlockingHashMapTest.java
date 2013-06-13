/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import junit.framework.TestCase;

import java.util.Random;
import java.util.concurrent.*;

/**
 * Testcase for {@link BlockingHashMap}.
 *
 * @author mcampbell
 */
public class BlockingHashMapTest extends TestCase {

    final Random random = new Random(new Random().nextLong());

    public void testBlockingHashMap() throws InterruptedException {
        ExecutorService exec = Executors.newFixedThreadPool(20);
        BlockingHashMap<Integer, Integer> map =
            new BlockingHashMap<Integer, Integer>(20);

        for (int i = 0; i < 100; i++) {
            exec.submit(new Puter(i, i, map));
            exec.submit(new Getter(i, map));
        }
        exec.shutdown();
        boolean finished = exec.awaitTermination(
            2, TimeUnit.SECONDS);
        assertTrue(finished);
    }


    private class Puter implements Runnable {
        private final Integer key;
        private final Integer value;
        private final BlockingHashMap<Integer, Integer> map;

        public Puter(
            Integer key, Integer value,
            BlockingHashMap<Integer, Integer> response)
        {
            this.key = key;
            this.value = value;
            this.map = response;
        }

        public void run() {
            try {
                Thread.sleep(random.nextInt(50));
                map.put(key, value);
                // System.out.println("putting key: " + key);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private class Getter implements Runnable {
        private final BlockingHashMap<Integer, Integer> map;
        private final Integer key;

        public Getter(Integer key, BlockingHashMap<Integer, Integer> map) {
            this.key = key;
            this.map = map;
        }

        public void run() {
            try {
                Thread.sleep(random.nextInt(50));
                Integer val = map.get(key);
                // System.out.println("getting key: " + key);
                assertEquals(key, val);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

// End BlockingHashMapTest.java
