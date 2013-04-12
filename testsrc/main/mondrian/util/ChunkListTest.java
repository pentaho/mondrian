/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.Util;
import mondrian.olap.Util.Function0;
import mondrian.test.PerformanceTest;
import mondrian.test.PerformanceTest.Statistician;

import junit.framework.TestCase;

import java.util.*;

/**
 * Unit and performance test for {@link ChunkList}.
 */
public class ChunkListTest extends TestCase {

    /** Unit test for {@link mondrian.util.ChunkList}. */
    public void testChunkList() {
        final ChunkList<Integer> list = new ChunkList<Integer>();
        assertEquals(0, list.size());
        assertTrue(list.isEmpty());
        assertEquals("[]", list.toString());

        try {
            list.remove(0);
            fail("expected exception");
        } catch (IndexOutOfBoundsException e) {
            // ok
        }

        list.add(7);
        assertEquals(1, list.size());
        assertEquals(7, (int) list.get(0));
        assertFalse(list.isEmpty());
        assertEquals("[7]", list.toString());

        list.add(9);
        list.add(null);
        list.add(11);
        assertEquals(4, list.size());
        assertEquals(7, (int) list.get(0));
        assertEquals(9, (int) list.get(1));
        assertNull(list.get(2));
        assertEquals(11, (int) list.get(3));
        assertFalse(list.isEmpty());
        assertEquals("[7, 9, null, 11]", list.toString());

        assertTrue(list.contains(9));
        assertFalse(list.contains(8));

        list.addAll(Collections.nCopies(40, 1));
        assertEquals(44, list.size());
        assertEquals(1, (int) list.get(40));

        int n = 0;
        for (Integer integer : list) {
            ++n;
        }
        assertEquals(n, list.size());

        int i = list.indexOf(null);
        assertEquals(2, i);

        // can't sort if null is present
        list.set(2, 123);

        i = list.indexOf(null);
        assertEquals(-1, i);

        Collections.sort(list);

        list.remove((Integer) 7);
        Collections.sort(list);
        assertEquals(1, (int) list.get(3));

        // remove all instances of a value that exists
        boolean b = list.removeAll(Arrays.asList(9));
        assertTrue(b);

        // remove all instances of a non-existent value
        b = list.removeAll(Arrays.asList(99));
        assertFalse(b);

        // remove all instances of a value that occurs in the last chunk
        list.add(12345);
        b = list.removeAll(Arrays.asList(12345));
        assertTrue(b);

        // remove all instances of a value that occurs in the last chunk but
        // not as the last value
        list.add(12345);
        list.add(123);
        b = list.removeAll(Arrays.asList(12345));
        assertTrue(b);

        assertEquals(
            1000, new ChunkList<Integer>(Collections.nCopies(1000, 77)).size());

        // add to an empty list via iterator
        //noinspection MismatchedQueryAndUpdateOfCollection
        final ChunkList<String> list2 = new ChunkList<String>();
        list2.listIterator(0).add("x");
        assertEquals("[x]", list2.toString());

        // add at start
        list2.add(0, "y");
        assertEquals("[y, x]", list2.toString());
    }

    /** Unit test for {@link mondrian.util.ChunkList} that applies random
     * operations. */
    public void testRandom() {
        final int ITERATION_COUNT = 50000;
        checkRandom(new Random(1), new ChunkList<Integer>(), ITERATION_COUNT);
        final Random random = new Random(2);
        for (int j = 0; j < 10; j++) {
            checkRandom(random, new ChunkList<Integer>(), ITERATION_COUNT);
        }
        checkRandom(
            new Random(3), new ChunkList<Integer>(Collections.nCopies(1000, 5)),
            ITERATION_COUNT);
    }

    void checkRandom(
        Random random,
        ChunkList<Integer> list,
        int iterationCount)
    {
        int removeCount = 0;
        int addCount = 0;
        final int initialCount = list.size();
        for (int i = 0; i < iterationCount; i++) {
            assert list.isValid(false, true);
            switch (random.nextInt(8)) {
            case 0:
                // remove last
                if (!list.isEmpty()) {
                    list.remove(list.size() - 1);
                    ++removeCount;
                }
                break;
            case 1:
                // add to end
                list.add(random.nextInt(1000));
                ++addCount;
                break;
            case 2:
                int n = 0;
                final int size = list.size();
                for (Integer integer : list) {
                    assertTrue(n++ < size);
                }
                break;
            case 3:
                // remove all instances of a particular value
                int sizeBefore = list.size();
                boolean b = list.removeAll(
                    Collections.singletonList(random.nextInt(500)));
                if (b) {
                    assertTrue(list.size() < sizeBefore);
                } else {
                    assertTrue(list.size() == sizeBefore);
                }
                removeCount += (sizeBefore - list.size());
                break;
            case 4:
                // remove at random position
                if (!list.isEmpty()) {
                    list.remove(random.nextInt(list.size()));
                    ++removeCount;
                }
                break;
            case 5:
                // add at random position
                int count = random.nextInt(list.size() + 1);
                ListIterator<Integer> it = list.listIterator();
                for (int j = 0; j < count; j++) {
                    it.next();
                }
                it.add(list.size());
                ++addCount;
                break;
            default:
                // add at random position
                list.add(random.nextInt(list.size() + 1), list.size());
                ++addCount;
                break;
            }
            assertEquals(list.size(), initialCount + addCount - removeCount);
        }
        assert list.isValid(true, true);
    }

    public void testPerformance() {
        //noinspection unchecked
        final Iterable<Pair<Function0<List<Integer>>, String>> factories0 =
            Pair.iterate(
                Arrays.asList(
                    new Function0<List<Integer>>() {
                        public List<Integer> apply() {
                            return new ArrayList<Integer>();
                        }
                    },
                    new Function0<List<Integer>>() {
                        public List<Integer> apply() {
                            return new LinkedList<Integer>();
                        }
                    },
                    new Function0<List<Integer>>() {
                        public List<Integer> apply() {
                            return new ChunkList<Integer>();
                        }
                    }
                ),
                Arrays.asList("ArrayList", "LinkedList", "ChunkList-64"));
        final List<Pair<Function0<List<Integer>>, String>> factories1 =
            new ArrayList<Pair<Function0<List<Integer>>, String>>();
        for (Pair<Function0<List<Integer>>, String> pair : factories0) {
            factories1.add(pair);
        }
        List<Pair<Function0<List<Integer>>, String>> factories =
            factories1.subList(2, 3);
        Iterable<Pair<Integer, String>> sizes =
            Pair.iterate(
                Arrays.asList(100000, 1000000, 10000000),
                Arrays.asList("100k", "1m", "10m"));
        for (final Pair<Function0<List<Integer>>, String> pair : factories) {
            new PerformanceTest.Benchmarker(
                "add 10m values, " + pair.right,
                new Util.Function1<Statistician, Void>() {
                    public Void apply(Statistician statistician) {
                        final List<Integer> list = pair.left.apply();
                        long start = System.currentTimeMillis();
                        for (int i = 0; i < 10000000; i++) {
                            list.add(1);
                        }
                        statistician.record(start);
                        assertValid(list);
                        return null;
                    }
                },
                10).run();
        }
        for (final Pair<Function0<List<Integer>>, String> pair : factories) {
            new PerformanceTest.Benchmarker(
                "iterate over 10m values, " + pair.right,
                new Util.Function1<Statistician, Void>() {
                    public Void apply(Statistician statistician) {
                        final List<Integer> list = pair.left.apply();
                        list.addAll(Collections.nCopies(10000000, 1));
                        long start = System.currentTimeMillis();
                        int count = 0;
                        for (Integer integer : list) {
                            count += integer;
                        }
                        statistician.record(start);
                        assert count == 10000000;
                        assertValid(list);
                        return null;
                    }
                },
                10).run();
        }
        for (final Pair<Function0<List<Integer>>, String> pair : factories) {
            for (final Pair<Integer, String> size : sizes) {
                if (size.left > 1000000) {
                    continue;
                }
                new PerformanceTest.Benchmarker(
                    "delete 10% of " + size.right + " values, " + pair.right,
                    new Util.Function1<Statistician, Void>() {
                        public Void apply(Statistician statistician) {
                            final List<Integer> list = pair.left.apply();
                            list.addAll(Collections.nCopies(size.left, 1));
                            long start = System.currentTimeMillis();
                            int n = 0;
                            for (Iterator<Integer> it = list.iterator();
                                 it.hasNext();)
                            {
                                Integer next = it.next();
                                if (n++ % 10 == 0) {
                                    it.remove();
                                }
                            }
                            statistician.record(start);
                            assertValid(list);
                            return null;
                        }
                    },
                    10).run();
            }
        }
        for (final Pair<Function0<List<Integer>>, String> pair : factories) {
            for (final Pair<Integer, String> size : sizes) {
                if (size.left > 1000000) {
                    continue;
                }
                new PerformanceTest.Benchmarker(
                    "get from " + size.right + " values, " + (size.left / 1000)
                    + " times, " + pair.right,
                    new Util.Function1<Statistician, Void>() {
                        public Void apply(Statistician statistician) {
                            final List<Integer> list = pair.left.apply();
                            list.addAll(Collections.nCopies(size.left, 1));
                            final int probeCount = size.left / 1000;
                            final Random random = new Random(1);
                            long start = System.currentTimeMillis();
                            int n = 0;
                            for (int i = 0; i < probeCount; i++) {
                                n += list.get(random.nextInt(list.size()));
                            }
                            assert n == probeCount;
                            statistician.record(start);
                            assertValid(list);
                            return null;
                        }
                    },
                    10).run();
            }
        }
        for (final Pair<Function0<List<Integer>>, String> pair : factories) {
            for (final Pair<Integer, String> size : sizes) {
                if (size.left > 1000000) {
                    continue;
                }
                new PerformanceTest.Benchmarker(
                    "add " + size.right
                    + " values, delete 10%, insert 20%, get 1%, using "
                    + pair.right,
                    new Util.Function1<Statistician, Void>() {
                        public Void apply(Statistician statistician) {
                            final List<Integer> list = pair.left.apply();
                            final int probeCount = size.left / 100;
                            long start = System.currentTimeMillis();
                            list.addAll(Collections.nCopies(size.left, 1));
                            final Random random = new Random(1);
                            for (Iterator<Integer> it = list.iterator();
                                 it.hasNext();)
                            {
                                Integer next = it.next();
                                if (random.nextInt(10) == 0) {
                                    it.remove();
                                }
                            }
                            for (ListIterator<Integer> it =
                                     list.listIterator(); it.hasNext();)
                            {
                                Integer next = it.next();
                                if (random.nextInt(5) == 0) {
                                    it.add(2);
                                }
                            }
                            int n = 0;
                            for (int i = 0; i < probeCount; i++) {
                                n += list.get(random.nextInt(list.size()));
                            }
                            assert n > probeCount;
                            statistician.record(start);
                            assertValid(list);
                            return null;
                        }
                    },
                    10).run();
            }
        }
    }

    private void assertValid(List list) {
        if (list instanceof ChunkList) {
            ChunkList chunkList = (ChunkList) list;
            assert chunkList.isValid(true, true);
        }
    }
}

// End ChunkListTest.java
