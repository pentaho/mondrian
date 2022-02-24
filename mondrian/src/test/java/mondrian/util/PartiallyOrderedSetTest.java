/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2019 Hitachi Vantara..  All rights reserved.
*/

package mondrian.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import org.olap4j.test.TestContext;

/**
 * Unit test for {@link PartiallyOrderedSet}.
 */
public class PartiallyOrderedSetTest {
    private static final boolean debug = false;
    private final int SCALE = 250; // 100, 1000, 3000 are also reasonable values
    final long seed = new Random().nextLong();
    final Random random = new Random(seed);

    static final PartiallyOrderedSet.Ordering<String> stringSubsetOrdering =
        new PartiallyOrderedSet.Ordering<String>() {
            public boolean lessThan(String e1, String e2) {
                // e1 < e2 if every char in e1 is also in e2
                for (int i = 0; i < e1.length(); i++) {
                    if (e2.indexOf(e1.charAt(i)) < 0) {
                        return false;
                    }
                }
                return true;
            }
        };

    // Integers, ordered by division. Top is 1, its children are primes,
    // etc.
    static final PartiallyOrderedSet.Ordering<Integer> isDivisor =
        new PartiallyOrderedSet.Ordering<Integer>() {
            public boolean lessThan(Integer e1, Integer e2) {
                return e2 % e1 == 0;
            }
        };

    // Bottom is 1, parents are primes, etc.
    static final PartiallyOrderedSet.Ordering<Integer> isDivisorInverse =
        new PartiallyOrderedSet.Ordering<Integer>() {
            public boolean lessThan(Integer e1, Integer e2) {
                return e1 % e2 == 0;
            }
        };

    static final PartiallyOrderedSet.Ordering<Integer> isDivisorWithNulls =
      new PartiallyOrderedSet.Ordering<Integer>() {
          public boolean lessThan(Integer e1, Integer e2) {
              if (e1 == null || e2 == null) {
                  return true;
              }
              return e2 % e1 == 0;
          }
      };

    // Ordered by bit inclusion. E.g. the children of 14 (1110) are
    // 12 (1100), 10 (1010) and 6 (0110).
    static final PartiallyOrderedSet.Ordering<Integer> isBitSubset =
        new PartiallyOrderedSet.Ordering<Integer>() {
            public boolean lessThan(Integer e1, Integer e2) {
                return (e2 & e1) == e2;
            }
        };

    // Ordered by bit inclusion. E.g. the children of 14 (1110) are
    // 12 (1100), 10 (1010) and 6 (0110).
    static final PartiallyOrderedSet.Ordering<Integer> isBitSuperset =
        new PartiallyOrderedSet.Ordering<Integer>() {
            public boolean lessThan(Integer e1, Integer e2) {
                return (e2 & e1) == e1;
            }
        };
    
    @Test
    public void testPoset() {
        String empty = "''";
        String abcd = "'abcd'";
        PartiallyOrderedSet<String> poset =
            new PartiallyOrderedSet<String>(stringSubsetOrdering);
        assertEquals(0, poset.size());

        final StringBuilder buf = new StringBuilder();
        poset.out(buf);
        TestContext.assertEqualsVerbose(
            "PartiallyOrderedSet size: 0 elements: {\n"
            + "}",
            buf.toString());

        poset.add("a");
        printValidate(poset);
        poset.add("b");
        printValidate(poset);

        poset.clear();
        assertEquals(0, poset.size());
        poset.add(empty);
        printValidate(poset);
        poset.add(abcd);
        printValidate(poset);
        assertEquals(2, poset.size());
        assertEquals("['abcd']", poset.getNonChildren().toString());
        assertEquals("['']", poset.getNonParents().toString());

        final String ab = "'ab'";
        poset.add(ab);
        printValidate(poset);
        assertEquals(3, poset.size());
        assertEquals("[]", poset.getChildren(empty).toString());
        assertEquals("['ab']", poset.getParents(empty).toString());
        assertEquals("['ab']", poset.getChildren(abcd).toString());
        assertEquals("[]", poset.getParents(abcd).toString());
        assertEquals("['']", poset.getChildren(ab).toString());
        assertEquals("['abcd']", poset.getParents(ab).toString());

        // "bcd" is child of "abcd" and parent of ""
        final String bcd = "'bcd'";
        poset.add(bcd);
        printValidate(poset);
        assertTrue(poset.isValid(false));
        assertEquals("['']", poset.getChildren(bcd).toString());
        assertEquals("['abcd']", poset.getParents(bcd).toString());
        assertEquals("['ab', 'bcd']", poset.getChildren(abcd).toString());

        buf.setLength(0);
        poset.out(buf);
        TestContext.assertEqualsVerbose(
            "PartiallyOrderedSet size: 4 elements: {\n"
            + "  'abcd' parents: [] children: ['ab', 'bcd']\n"
            + "  'ab' parents: ['abcd'] children: ['']\n"
            + "  'bcd' parents: ['abcd'] children: ['']\n"
            + "  '' parents: ['ab', 'bcd'] children: []\n"
            + "}",
            buf.toString());

        final String b = "'b'";

        // ancestors of an element not in the set
        assertEqualsList("['ab', 'abcd', 'bcd']", poset.getAncestors(b));

        poset.add(b);
        printValidate(poset);
        assertEquals("['abcd']", poset.getNonChildren().toString());
        assertEquals("['']", poset.getNonParents().toString());
        assertEquals("['']", poset.getChildren(b).toString());
        assertEqualsList("['ab', 'bcd']", poset.getParents(b));
        assertEquals("['']", poset.getChildren(b).toString());
        assertEquals("['ab', 'bcd']", poset.getChildren(abcd).toString());
        assertEquals("['b']", poset.getChildren(bcd).toString());
        assertEquals("['b']", poset.getChildren(ab).toString());
        assertEqualsList("['ab', 'abcd', 'bcd']", poset.getAncestors(b));

        // descendants and ancestors of an element with no descendants
        assertEquals("[]", poset.getDescendants(empty).toString());
        assertEqualsList(
            "['ab', 'abcd', 'b', 'bcd']",
            poset.getAncestors(empty));

        // some more ancestors of missing elements
        assertEqualsList("['abcd']", poset.getAncestors("'ac'"));
        assertEqualsList("[]", poset.getAncestors("'z'"));
        assertEqualsList("['ab', 'abcd']", poset.getAncestors("'a'"));
    }
    @Test
    public void testPosetTricky() {
        PartiallyOrderedSet<String> poset =
            new PartiallyOrderedSet<String>(stringSubsetOrdering);

        // A tricky little poset with 4 elements:
        // {a <= ab and ac, b < ab, ab, ac}
        poset.clear();
        poset.add("'a'");
        printValidate(poset);
        poset.add("'b'");
        printValidate(poset);
        poset.add("'ac'");
        printValidate(poset);
        poset.add("'ab'");
        printValidate(poset);
    }
    @Test
    public void testPosetBits() {
        final PartiallyOrderedSet<Integer> poset =
            new PartiallyOrderedSet<Integer>(isBitSuperset);
        poset.add(2112); // {6, 11} i.e. 64 + 2048
        poset.add(2240); // {6, 7, 11} i.e. 64 + 128 + 2048
        poset.add(2496); // {6, 7, 8, 11} i.e. 64 + 128 + 256 + 2048
        printValidate(poset);
        poset.remove(2240);
        printValidate(poset);
        poset.add(2240); // {6, 7, 11} i.e. 64 + 128 + 2048
        printValidate(poset);
    }

    @Test
    public void testPosetBitsRemoveParent() {
        final PartiallyOrderedSet<Integer> poset =
            new PartiallyOrderedSet<Integer>(isBitSuperset);
        poset.add(66); // {bit 2, bit 6}
        poset.add(68); // {bit 3, bit 6}
        poset.add(72); // {bit 4, bit 6}
        poset.add(64); // {bit 6}
        printValidate(poset);
        poset.remove(64); // {bit 6}
        printValidate(poset);
    }

    @Test
    public void testMondrian2628() {
        PartiallyOrderedSet<Integer> integers =
            new PartiallyOrderedSet<Integer>(isDivisorWithNulls,
              range(1, 1000));
        // Null elements can't be added to the poset.
        assertFalse(integers.add(null));
        // Ancestors list cannot have null elements.
        for (int i = 1; i < 1000; i++) {
            assertFalse(integers.getAncestors(i).contains(null),
        	    "Ancestor list of " + i + " has null elements.");
        }
    }

    @Test
    public void testDivisorPoset() {
        PartiallyOrderedSet<Integer> integers =
            new PartiallyOrderedSet<Integer>(isDivisor, range(1, 1000));
        assertEquals(
            "[1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 24, 30, 40, 60]",
            new TreeSet<Integer>(integers.getDescendants(120)).toString());
        assertEquals(
            "[240, 360, 480, 600, 720, 840, 960]",
            new TreeSet<Integer>(integers.getAncestors(120)).toString());
        assertTrue(integers.getDescendants(1).isEmpty());
        assertEquals(
            998,
            integers.getAncestors(1).size());
        assertTrue(integers.isValid(true));
    }

    @Test
    public void testDivisorSeries() {
        checkPoset(isDivisor, debug, range(1, SCALE * 3), false);
    }

    @Test
    public void testDivisorRandom() {
        boolean ok = false;
        try {
            checkPoset(
                isDivisor, debug, random(random, SCALE, SCALE * 3), false);
            ok = true;
        } finally {
            if (!ok) {
                System.out.println("Random seed: " + seed);
            }
        }
    }

    @Test
    public void testDivisorRandomWithRemoval() {
        boolean ok = false;
        try {
            checkPoset(
                isDivisor, debug, random(random, SCALE, SCALE * 3), true);
            ok = true;
        } finally {
            if (!ok) {
                System.out.println("Random seed: " + seed);
            }
        }
    }

    @Test
    public void testDivisorInverseSeries() {
        checkPoset(isDivisorInverse, debug, range(1, SCALE * 3), false);
    }

    @Test
    public void testDivisorInverseRandom() {
        boolean ok = false;
        try {
            checkPoset(
                isDivisorInverse, debug, random(random, SCALE, SCALE * 3),
                false);
            ok = true;
        } finally {
            if (!ok) {
                System.out.println("Random seed: " + seed);
            }
        }
    }

    @Test
    public void testDivisorInverseRandomWithRemoval() {
        boolean ok = false;
        try {
            checkPoset(
                isDivisorInverse, debug, random(random, SCALE, SCALE * 3),
                true);
            ok = true;
        } finally {
            if (!ok) {
                System.out.println("Random seed: " + seed);
            }
        }
    }

    @Test
    public void testSubsetSeries() {
        checkPoset(isBitSubset, debug, range(1, SCALE / 2), false);
    }

    @Test
    public void testSubsetRandom() {
        boolean ok = false;
        try {
            checkPoset(
                isBitSubset, debug, random(random, SCALE / 4, SCALE), false);
            ok = true;
        } finally {
            if (!ok) {
                System.out.println("Random seed: " + seed);
            }
        }
    }

    private <E> void printValidate(PartiallyOrderedSet<E> poset) {
        if (debug) {
            dump(poset);
        }
        assertTrue(poset.isValid(debug));
    }

    public void checkPoset(
        PartiallyOrderedSet.Ordering<Integer> ordering,
        boolean debug,
        Iterable<Integer> generator,
        boolean remove)
    {
        final PartiallyOrderedSet<Integer> poset =
            new PartiallyOrderedSet<Integer>(ordering);
        int n = 0;
        int z = 0;
        if (debug) {
            dump(poset);
        }
        for (int i : generator) {
            if (remove && z++ % 2 == 0) {
                if (debug) {
                    System.out.println("remove " + i);
                }
                poset.remove(i);
                if (debug) {
                    dump(poset);
                }
                continue;
            }
            if (debug) {
                System.out.println("add " + i);
            }
            poset.add(i);
            if (debug) {
                dump(poset);
            }
            assertEquals(++n, poset.size());
            if (i < 100) {
                if (!poset.isValid(false)) {
                    dump(poset);
                }
                assertTrue(poset.isValid(true));
            }
        }
        assertTrue(poset.isValid(true));

        final StringBuilder buf = new StringBuilder();
        poset.out(buf);
        assertTrue(buf.length() > 0);
    }

    private <E> void dump(PartiallyOrderedSet<E> poset) {
        final StringBuilder buf = new StringBuilder();
        poset.out(buf);
        System.out.println(buf);
    }

    private static Collection<Integer> range(
        final int start, final int end)
    {
        return new AbstractList<Integer>() {
            @Override
            public Integer get(int index) {
                return start + index;
            }

            @Override
            public int size() {
                return end - start;
            }
        };
    }

    private static Iterable<Integer> random(
        Random random, final int size, final int max)
    {
        final Set<Integer> set = new LinkedHashSet<Integer>();
        while (set.size() < size) {
            set.add(random.nextInt(max) + 1);
        }
        return set;
    }

    private static void assertEqualsList(String expected, List<String> ss) {
        assertEquals(
            expected,
            new TreeSet<String>(ss).toString());
    }
}

// End PartiallyOrderedSetTest.java
