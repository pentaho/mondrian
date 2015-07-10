/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.util;

import junit.framework.TestCase;

import java.util.*;

/**
 * Test case for {@link ObjectPool}.
 *
 * @author Richard Emberson
 */
public class ObjectPoolTest extends TestCase {
    public ObjectPoolTest() {
        super();
    }
    public ObjectPoolTest(String name) {
        super(name);
    }

    static class KeyValue {
        long key;
        Object value;
        KeyValue(long key, Object value) {
            this.key = key;
            this.value = value;
        }
        public int hashCode() {
            return (int)(key ^ (key >>> 32));
        }
        public boolean equals(Object o) {
            return (o instanceof KeyValue)
                ? (((KeyValue) o).key == this.key)
                : false;
        }
        public String toString() {
            return value.toString();
        }
    }

    public void testString() throws Exception {
        ObjectPool<String> strings = new ObjectPool<String>();
        int nos = 100000;
        String[] ss1 = genStringsArray(nos);
        for (int i = 0; i < nos; i++) {
            strings.add(ss1[i]);
        }
        assertEquals("size not equal", nos, strings.size());

        // second array of strings, same as the first but different objects
        String[] ss2 = genStringsArray(nos);
        for (int i = 0; i < nos; i++) {
            String s = strings.add(ss2[i]);
            assertEquals("string not equal: " + s, s, ss2[i]);
            // REVIEW jvs 16-Jan-2008:  This failed for me when
            // I ran with a 1GB JVM heap size on JDK 1.5, probably
            // because of interning (I tried changing genStringsList to add a
            // gratuitous String constructor call, but that did not help).  If
            // there's a reason this test is on strings explicitly, then
            // this needs to stay disabled; if the datatype can be changed
            // to something which doesn't have any magic interning, then
            // it can be re-enabled.  This probably explains the
            // Util.PreJdk15 "unknown reasons" above.
            /*
            assertFalse("same object", (s == ss2[i]));
            */
        }

        strings.clear();
        assertEquals("size not equal", 0, strings.size());

        nos = 25;
        ss1 = genStringsArray(nos);
        for (int i = 0; i < nos; i++) {
            strings.add(ss1[i]);
        }
        assertEquals("size not equal", nos, strings.size());

        List<String> l = genStringsList(nos);
        Iterator<String> it = strings.iterator();
        while (it.hasNext()) {
            String s = it.next();
            l.remove(s);
        }
        assertTrue("list not empty", l.isEmpty());
    }

    public void testKeyValue() throws Exception {
        ObjectPool<KeyValue> op = new ObjectPool<KeyValue>();
        int nos = 100000;
        KeyValue[] kv1 = genKeyValueArray(nos);
        for (int i = 0; i < nos; i++) {
            op.add(kv1[i]);
        }
        assertEquals("size not equal", nos, op.size());

        // second array of KeyValues, same as the first but different objects
        KeyValue[] kv2 = genKeyValueArray(nos);
        for (int i = 0; i < nos; i++) {
            KeyValue kv = op.add(kv2[i]);
            assertEquals("KeyValue not equal: " + kv, kv, kv2[i]);
            assertFalse("same object", (kv == kv2[i]));
        }

        op.clear();
        assertEquals("size not equal", 0, op.size());

        nos = 25;
        kv1 = genKeyValueArray(nos);
        for (int i = 0; i < nos; i++) {
            op.add(kv1[i]);
        }
        assertEquals("size not equal", nos, op.size());

        List<KeyValue> l = genKeyValueList(nos);
        Iterator<KeyValue> it = op.iterator();
        while (it.hasNext()) {
            KeyValue kv = it.next();
            l.remove(kv);
        }
        assertTrue("list not empty", l.isEmpty());
    }

    /**
     * Tests ObjectPools containing large numbers of integer and string keys,
     * and makes sure they return the same results as HashSet. Optionally
     * measures performance.
     */
    public void testLarge() {
        // Some typical results (2.4 GHz Intel dual-core).

        // Key type:        Integer               String
        // Implementation:  ObjectPool    HashSet ObjectPool    HashSet
        //                  ========== ========== ========== ==========
        // With density=0.01, 298,477 distinct entries, 7,068 hits
        // 300,000 adds        221 ms      252 ms     293 ms    1013 ms
        // 700,000 gets        164 ms      148 ms     224 ms     746 ms
        //
        // With density=0.5, 236,022 distinct entries, 275,117 hits
        // 300,000 adds        175 ms      250 ms     116 ms     596 ms
        // 700,000 gets        147 ms      176 ms     190 ms     757 ms
        //
        // With density=0.999, 189,850 distinct entries, 442,618 hits
        // 300,000 adds        128 ms      185 ms      99 ms     614 ms
        // 700,000 gets        133 ms      184 ms     130 ms     830 ms

        checkLargeMulti(300000, 0.01,  700000, 298477, 7068);
        checkLargeMulti(300000, 0.5,   700000, 236022, 275117);
        checkLargeMulti(300000, 0.999, 700000, 189850, 442618);
    }

    private void checkLargeMulti(
        int entryCount,
        double density,
        int retrieveCount,
        int expectedDistinct,
        int expectedHits)
    {
        checkLarge(
            true, true, entryCount, density, retrieveCount,
            expectedDistinct, expectedHits);
        checkLarge(
            false, true, entryCount, density, retrieveCount,
            expectedDistinct, expectedHits);
        checkLarge(
            false, true, entryCount, density, retrieveCount,
            expectedDistinct, expectedHits);
        checkLarge(
            false, false, entryCount, density, retrieveCount,
            expectedDistinct, expectedHits);
    }

    private void checkLarge(
        boolean usePool,
        boolean intKey,
        int entryCount,
        double density,
        int retrieveCount,
        int expectedDistinct,
        int expectedHits)
    {
        final boolean print = false;
        final long t1 = System.currentTimeMillis();
        assert density > 0 && density <= 1;
        int space = (int) (entryCount / density);
        ObjectPool<Object> objectPool = new ObjectPool<Object>();
        HashSet<Object> set = new HashSet<Object>();
        Random random = new Random(1234);
        int distinctCount = 0;
        final String longString =
            "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyy";
        for (int i = 0; i < entryCount; i++) {
            final Object key = intKey
                ? random.nextInt(space)
                : longString + random.nextInt(space);
            if (usePool) {
                if (objectPool.add(key) != null) {
                    ++distinctCount;
                }
            } else {
                if (set.add(key)) {
                    ++distinctCount;
                }
            }
        }
        final long t2 = System.currentTimeMillis();
        int hitCount = 0;
        for (int i = 0; i < retrieveCount; i++) {
            final Object key = intKey
                ? random.nextInt(space)
                : longString + random.nextInt(space);
            if (usePool) {
                if (objectPool.contains(key)) {
                    ++hitCount;
                }
            } else {
                if (set.contains(key)) {
                    ++hitCount;
                }
            }
        }
        final long t3 = System.currentTimeMillis();
        if (usePool) {
// todo:           assertEquals(expectedDistinct, objectPool.size());
            distinctCount = objectPool.size();
        } else {
            assertEquals(expectedDistinct, set.size());
        }
        if (print) {
            System.out.println(
                "Using " + (usePool ? "ObjectPool" : "HashSet")
                    + ", density=" + density
                    + ", " + distinctCount + " distinct entries, "
                    + hitCount + " hits");
            System.out.println(
                entryCount + " adds took " + (t2 - t1) + " milliseconds");
            System.out.println(
                retrieveCount + " gets took " + (t3 - t2) + " milliseconds");
        }
        assertEquals(expectedDistinct, distinctCount);
        assertEquals(expectedHits, hitCount);
    }

    /////////////////////////////////////////////////////////////////////////
    // helpers
    /////////////////////////////////////////////////////////////////////////
    private static String[] genStringsArray(int nos) {
        List l = genStringsList(nos);
        return (String[]) l.toArray(new String[l.size()]);
    }
    private static List<String> genStringsList(int nos) {
        List<String> l = new ArrayList<String>(nos);
        for (int i = 0; i < nos; i++) {
            l.add(Integer.valueOf(i).toString());
        }
        return l;
    }
    private static KeyValue[] genKeyValueArray(int nos) {
        List<KeyValue> l = genKeyValueList(nos);
        return l.toArray(new KeyValue[l.size()]);
    }
    private static List<KeyValue> genKeyValueList(int nos) {
        List<KeyValue> l = new ArrayList<KeyValue>(nos);
        for (int i = 0; i < nos; i++) {
            l.add(new KeyValue(i, Integer.valueOf(i)));
        }
        return l;
    }

}

// End ObjectPoolTest.java
