/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import junit.framework.TestCase;
import org.apache.commons.collections.ComparatorUtils;
import org.apache.commons.collections.comparators.ReverseComparator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * <code>PartialSortTest</code> is a unit test for the partial-sort algorithm
 * {@link FunUtil#partialSort}, which supports MDX functions like TopCount and
 * BottomCount. No MDX here; there are tests of TopCount etc in FunctionTest.
 *
 * @author Marc Berkowitz
 * @since Nov 2008
 * @version $Id$
 */
public class PartialSortTest extends TestCase
{
    final Random random = new Random();

    // subroutines

    // returns a new array of random integers
    private Integer[] newRandomIntegers(int length, int minValue, int maxValue)
    {
        final int delta = maxValue - minValue;
        Integer[] vec = new Integer[length];
        for (int i = 0; i < length; i++) {
            vec[i] = minValue + random.nextInt(delta);
        }
        return vec;
    }

    // calls partialSort() for natural ascending or descending order
    private void doPartialSort(Object[] items, boolean descending, int limit)
    {
        Comparator comp = ComparatorUtils.naturalComparator();
        if (descending) {
            comp = ComparatorUtils.reversedComparator(comp);
        }
        FunUtil.partialSort(items, comp, limit);
    }

    // predicate: checks that results have been partially sorted;
    // simplest case, on int[]
    private static boolean isPartiallySorted(
        int[] vec, int limit, boolean descending)
    {
        // elements {0 <= i < limit} are sorted
        for (int i = 1; i < limit; i++) {
            int delta = vec[i] - vec[i - 1];
            if (descending) {
                if (delta > 0) {
                    return false;
                }
            } else {
                if (delta < 0) {
                    return false;
                }
            }
        }
        // elements {limit <= i} are just bigger or smaller than the
        // bound vec[limit - 1];
        int bound = vec[limit - 1];
        for (int i = limit; i < vec.length; i++) {
            int delta = vec[i] - bound;
            if (descending) {
                if (delta > 0) {
                    return false;
                }
            } else {
                if (delta < 0) {
                    return false;
                }
            }
        }
        return true;
    }

    // same predicate generalized: uses natural comparison
    private static <T extends Comparable> boolean isPartiallySorted(
        T [] vec, int limit, boolean descending)
    {
        return isPartiallySorted(
            vec, limit, ComparatorUtils.naturalComparator(), descending);
    }

    // same predicate generalized: uses a given Comparator
    private static  <T>  boolean isPartiallySorted(
        T[] vec, int limit, Comparator<? super T> order, boolean descending)
    {
        return isPartiallySorted(vec, limit, order, descending, null);
    }

    // Same predicate generalized: uses two Comparators, a sort key
    // (ascending or descending), and a tie-breaker (always
    // ascending). This is a contrivance to verify a stable partial
    // sort, on a special input array.
    private static <T>  boolean isPartiallySorted(
        T[] vec, int limit,
        Comparator<? super T> order, boolean descending,
        Comparator<? super T> tieBreaker)
    {
        // elements {0 <= i < limit} are sorted
        for (int i = 1; i < limit; i++) {
            int delta = order.compare(vec[i], vec[i - 1]);
            if (delta == 0) {
                if (tieBreaker != null
                    && tieBreaker.compare(vec[i],  vec[i - 1]) < 0)
                {
                    return false;
                }
            } else if (descending) {
                if (delta > 0) {
                    return false;
                }
            } else {
                if (delta < 0) {
                    return false;
                }
            }
        }

        // elements {limit <= i} are just bigger or smaller than the
        // bound vec[limit - 1];
        T bound = vec[limit - 1];
        for (int i = limit; i < vec.length; i++) {
            int delta = order.compare(vec[i], bound);
            if (descending) {
                if (delta > 0) {
                    return false;
                }
            } else {
                if (delta < 0) {
                    return false;
                }
            }
        }
        return true;
    }

    // validate the predicate isPartiallySorted()
    public void testPredicate1() {
        int errct = 0;
        int size = 10 * 1000;
        int[] vec = new int[size];

        // all sorted, ascending
        int key = 0;
        for (int i = 0; i < size; i++) {
            vec[i] = key;
            key += i % 3;
        }
        if (isPartiallySorted(vec, size, true)) {
            errct++;
        }
        if (!isPartiallySorted(vec, size, false)) {
            errct++;
        }

        // partially sorted, ascending
        key = 0;
        int limit = 2000;
        for (int i = 0; i < limit; i++) {
            vec[i] = key;
            key += i % 3;
        }
        for (int i = limit; i < size; i++) {
            vec[i] = 2 * key + random.nextInt(1000);
        }
        if (isPartiallySorted(vec, limit, true)) {
            errct++;
        }
        if (!isPartiallySorted(vec, limit, false)) {
            errct++;
        }

        // all sorted, descending;
        key = 2 * size;
        for (int i = 0; i < size; i++) {
            vec[i] = key;
            key -= i % 3;
        }
        if (!isPartiallySorted(vec, size, true)) {
            errct++;
        }
        if (isPartiallySorted(vec, size, false)) {
            errct++;
        }

        // partially sorted, descending
        key = 2 * size;
        limit = 2000;
        for (int i = 0; i < limit; i++) {
            vec[i] = key;
            key -= i % 3;
        }
        for (int i = limit; i < size; i++) {
            vec[i] = key - random.nextInt(size);
        }
        if (!isPartiallySorted(vec, limit, true)) {
            errct++;
        }
        if (isPartiallySorted(vec, limit, false)) {
            errct++;
        }

        assertTrue(errct == 0);
    }

    // same as testPredicate() but boxed
    public void testPredicate2() {
        int errct = 0;
        int size = 10 * 1000;
        Integer[] vec = new Integer[size];
        Random random = new Random();

        // all sorted, ascending
        int key = 0;
        for (int i = 0; i < size; i++) {
            vec[i] = key;
            key += i % 3;
        }
        if (isPartiallySorted(vec, size, true)) {
            errct++;
        }
        if (!isPartiallySorted(vec, size, false)) {
            errct++;
        }

        // partially sorted, ascending
        key = 0;
        int limit = 2000;
        for (int i = 0; i < limit; i++) {
            vec[i] = key;
            key += i % 3;
        }
        for (int i = limit; i < size; i++) {
            vec[i] = 2 * key + random.nextInt(1000);
        }
        if (isPartiallySorted(vec, limit, true)) {
            errct++;
        }
        if (!isPartiallySorted(vec, limit, false)) {
            errct++;
        }

        // all sorted, descending;
        key = 2 * size;
        for (int i = 0; i < size; i++) {
            vec[i] = key;
            key -= i % 3;
        }
        if (!isPartiallySorted(vec, size, true)) {
            errct++;
        }
        if (isPartiallySorted(vec, size, false)) {
            errct++;
        }

        // partially sorted, descending
        key = 2 * size;
        limit = 2000;
        for (int i = 0; i < limit; i++) {
            vec[i] = key;
            key -= i % 3;
        }
        for (int i = limit; i < size; i++) {
            vec[i] = key - random.nextInt(size);
        }
        if (!isPartiallySorted(vec, limit, true)) {
            errct++;
        }
        if (isPartiallySorted(vec, limit, false)) {
            errct++;
        }

        assertTrue(errct == 0);
    }

    public void testQuick() {
        final int length = 40;
        final int limit = 4;
        Integer vec[] = newRandomIntegers(length, 0, length);
        // sort descending
        doPartialSort(vec, true, limit);
        assertTrue(isPartiallySorted(vec, limit, true));
    }

    public void testOnAlreadySorted() {
        final int length = 200;
        final int limit = 8;
        Integer vec[] = new Integer[length];
        for (int i = 0; i < length; i++) {
            vec[i] = i;
        }
        // sort ascending
        doPartialSort(vec, false, limit);
        assertTrue(isPartiallySorted(vec, limit, false));
    }

    public void testOnAlreadyReverseSorted() {
        final int length = 200;
        final int limit = 8;
        Integer vec[] = new Integer[length];
        for (int i = 0; i < length; i++) {
            vec[i] = length - i;
        }
        // sort ascending
        doPartialSort(vec, false, limit);
        assertTrue(isPartiallySorted(vec, limit, false));
    }

    // tests partial sort on arras of random integers
    private void randomIntegerTests(int length, int limit) {
        Integer vec[] = newRandomIntegers(length, 0, length);
        // sort descending
        doPartialSort(vec, true, limit);
        assertTrue(isPartiallySorted(vec, limit, true));

        // sort ascending
        vec = newRandomIntegers(length, 0, length);
        doPartialSort(vec, false, limit);
        assertTrue(isPartiallySorted(vec, limit, false));

        // both again with a wider range of values
        vec = newRandomIntegers(length, 10, 4 * length);
        doPartialSort(vec, true, limit);
        assertTrue(isPartiallySorted(vec, limit, true));

        vec = newRandomIntegers(length, 10, 4 * length);
        doPartialSort(vec, false, limit);
        assertTrue(isPartiallySorted(vec, limit, false));

        // and again with a narrower range values
        vec = newRandomIntegers(length, 0, length / 10);
        doPartialSort(vec, true, limit);
        assertTrue(isPartiallySorted(vec, limit, true));

        vec = newRandomIntegers(length, 0, length / 10);
        doPartialSort(vec, false, limit);
        assertTrue(isPartiallySorted(vec, limit, false));
    }

    // test correctness
    public void testOnRandomIntegers() {
        randomIntegerTests(100, 20);
        randomIntegerTests(50000, 10);
        randomIntegerTests(50000, 500);
        randomIntegerTests(50000, 12000);
    }

    // test with large vector
    public void testOnManyRandomIntegers() {
        randomIntegerTests(1000 * 1000, 5000);
        randomIntegerTests(1000 * 1000, 10);
    }

    public void longTest() {
        int count = 128;
        while (count-- > 0) {
            testOnRandomIntegers();
        }
    }



    // Test stable partial sort, Test input; a vector of itens with an explicit
    // index; sort should not pernute the index.
    static class Item {
        final int index;
        final int key;
        Item(int index, int key) {
            this.index = index;
            this.key = key;
        }

        static final Comparator<Item> byIndex = new Comparator<Item>() {
            public int compare(Item x, Item y) {
                return x.index - y.index;
            }
        };
        static final Comparator<Item> byKey = new Comparator<Item>() {
            public int compare(Item x, Item y) {
                return x.key - y.key;
            }
        };

        // returns true iff VEC is partially sorted 1st by key (ascending or
        // descending), 2nd by index (always ascending)
        static boolean isStablySorted(Item[] vec, int limit, boolean desc) {
            return isPartiallySorted(vec, limit, byKey, desc, byIndex);
        }
    }

    // returns a new array of partially sorted Items
    private Item[] newPartlySortedItems(int length, int limit, boolean desc) {
        Item[] vec = new Item[length];
        int factor = desc? -1 : 1;
        int key = desc ? (2 * length) : 0;
        int i;
        for (i = 0; i < limit; i++) {
            vec[i] = new Item(i, key);
            key += factor * (i % 3);    // stutter
        }
        for (; i < length; i++) {
            vec[i] = new Item(i, key + factor * random.nextInt(length));
        }
        return vec;
    }

    // returns a new array of random Items
    private Item[] newRandomItems(int length, int minKey, int maxKey) {
        final int delta = maxKey - minKey;
        Item[] vec = new Item[length];
        for (int i = 0; i < length; i++) {
            vec[i] = new Item(i, minKey + random.nextInt(delta));
        }
        return vec;
    }

    // just glue
    private Item[] doStablePartialSort(Item[] vec, boolean desc, int limit) {
        Comparator<Item> comp = Item.byKey;
        if (desc) {
            comp = new ReverseComparator(comp);
        }
        List<Item> sorted =
            FunUtil.stablePartialSort(Arrays.asList(vec), comp, limit);
        return sorted.toArray(new Item[0]);
    }

    public void testPredicateIsStablySorted() {
        Item[] vec = newPartlySortedItems(24, 4, false);
        assertTrue(Item.isStablySorted(vec, 4,  false));
        assertFalse(Item.isStablySorted(vec, 4, true));

        vec = newPartlySortedItems(24, 8, true);
        assertTrue(Item.isStablySorted(vec, 4,  true));
        assertFalse(Item.isStablySorted(vec, 4, false));

        vec = newPartlySortedItems(1000, 100, true);
        assertTrue(Item.isStablySorted(vec, 100, true));
        assertTrue(Item.isStablySorted(vec, 20,  true));
        assertTrue(Item.isStablySorted(vec,  4,  true));
    }


    public void testStableQuick() {
        final int length = 40;
        final int limit = 4;
        Item vec[] = newRandomItems(length, 0, length);
        // sort descending
        vec = doStablePartialSort(vec, true, limit);
        assertTrue(Item.isStablySorted(vec, limit, true));
    }

    // tests stable partial sort on arras of random Items
    private void randomItemTests(int length, int limit) {
        Item vec[] = newRandomItems(length, 0, length);
        // sort descending
        vec = doStablePartialSort(vec, true, limit);
        assertTrue(Item.isStablySorted(vec, limit, true));

        // sort ascending
        vec = newRandomItems(length, 0, length);
        vec = doStablePartialSort(vec, false, limit);
        assertTrue(Item.isStablySorted(vec, limit, false));

        // both again with a wider range of values
        vec = newRandomItems(length, 10, 4 * length);
        vec = doStablePartialSort(vec, true, limit);
        assertTrue(Item.isStablySorted(vec, limit, true));

        vec = newRandomItems(length, 10, 4 * length);
        vec = doStablePartialSort(vec, false, limit);
        assertTrue(Item.isStablySorted(vec, limit, false));

        // and again with a narrower range values
        vec = newRandomItems(length, 0, length / 10);
        vec = doStablePartialSort(vec, true, limit);
        assertTrue(Item.isStablySorted(vec, limit, true));

        vec = newRandomItems(length, 0, length / 10);
        vec = doStablePartialSort(vec, false, limit);
        assertTrue(Item.isStablySorted(vec, limit, false));
    }

    public void testStableOnRandomItems() {
        randomItemTests(100, 20);
        randomItemTests(50000, 10);
        randomItemTests(50000, 500);
        randomItemTests(50000, 12000);
    }


    // Compares elapsed time of full sort (mergesort), partial sort, and stable
    // partial sort on the same input set.
    private void speedTest(int length, int limit) {
        System.out.println("sorting the max " + limit + " of " + length + " random Integers");

        // random input, 3 copies
        // repeated keys
        Integer[] vec1 = newRandomIntegers(length, 0, length / 5);
        Integer[] vec2 = new Integer[length];
        Integer[] vec3 = new Integer[length];
        System.arraycopy(vec1, 0, vec2, 0, length);
        System.arraycopy(vec1, 0, vec3, 0, length);

        // full sort vec1
        long now = System.currentTimeMillis();
        Arrays.sort(vec1);
        long dt = System.currentTimeMillis() - now;
        System.out.println(" full mergesort took " + dt + " msecs");

        // partial sort vec2
        now = System.currentTimeMillis();
        doPartialSort(vec2, true, limit);
        dt = System.currentTimeMillis() - now;
        System.out.println(" partial quicksort took " + dt + " msecs");

        // stable partial sort vec3
        Comparator comp =
            new ReverseComparator(ComparatorUtils.naturalComparator());
        List<Integer> vec3List = Arrays.asList(vec3);
        now = System.currentTimeMillis();
        FunUtil.stablePartialSort(vec3List, comp, limit);
        dt = System.currentTimeMillis() - now;
        System.out.println(" stable partial quicksort took " + dt + " msecs");
    }


    // compare speed on different sizes of input
    public void _testSpeed() {
        speedTest(60, 2);               // tiny
        speedTest(600, 12);             // small
        speedTest(600, 200);
        speedTest(16000, 4);            // medium
        speedTest(16000, 160);
        speedTest(400 * 400, 4);          // large
        // speedTest(1600 * 1600, 4);        // very large needs bigger heap
    }
}
// End PartialSortTest.java
