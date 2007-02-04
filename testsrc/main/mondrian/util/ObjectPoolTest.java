/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.util;

import mondrian.olap.Util;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

/**
 * Test case for {@link ObjectPool}.
 *
 * @version $Id$
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
        // for reasons unknown this fails with java4
        if (Util.PreJdk15) {
            return;
        }
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
            assertEquals("string not equal: " +s, s, ss2[i]);
	    assertFalse("same object", (s == ss2[i]));
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
            assertEquals("KeyValue not equal: " +kv, kv, kv2[i]);
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
            l.add(new Integer(i).toString());
        }
        return l;
    }
    private static KeyValue[] genKeyValueArray(int nos) {
        List<KeyValue> l = genKeyValueList(nos);
        return (KeyValue[]) l.toArray(new KeyValue[l.size()]);
    }
    private static List<KeyValue> genKeyValueList(int nos) {
        List<KeyValue> l = new ArrayList<KeyValue>(nos);
        for (int i = 0; i < nos; i++) {
            l.add(new KeyValue((long)i, new Integer(i)));
        }
        return l;
    }

}

// End ObjectPoolTest.java
