/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import junit.framework.TestCase;
import mondrian.olap.Util;

/**
 * Unit test for {@link BitKey}.
 *
 * @author Richard Emberson
 * @version $Id$
 */
public class BitKeyTest extends TestCase {
    public BitKeyTest(String name) {
        super(name);
    }

    /**
     * Test that negative size throws IllegalArgumentException.
     *
     */
    public void testBadSize() {
        int size = -1;
        boolean gotException = false;
        BitKey bitKey = null;
        try {
            bitKey = BitKey.Factory.makeBitKey(size);
            Util.discard(bitKey);
        } catch (IllegalArgumentException e) {
            gotException = true;
        }
        assertTrue("BitKey negative size " + size, (gotException));

        size = -10;
        gotException = false;
        try {
            bitKey = BitKey.Factory.makeBitKey(size);
        } catch (IllegalArgumentException e) {
            gotException = true;
        }
        assertTrue("BitKey negative size " + size, (gotException));
    }

    /**
     * Test that non-negative sizes do not throw IllegalArgumentException
     */
    public void testGoodSize() {
        int size = 0;
        boolean gotException = false;
        BitKey bitKey = null;
        try {
            bitKey = BitKey.Factory.makeBitKey(size);
            Util.discard(bitKey);
        } catch (IllegalArgumentException e) {
            gotException = true;
        }
        assertTrue("BitKey size " +size, (! gotException));

        size = 1;
        gotException = false;
        try {
            bitKey = BitKey.Factory.makeBitKey(size);
        } catch (IllegalArgumentException e) {
            gotException = true;
        }
        assertTrue("BitKey size " +size, (! gotException));

        size = 10;
        gotException = false;
        try {
            bitKey = BitKey.Factory.makeBitKey(size);
        } catch (IllegalArgumentException e) {
            gotException = true;
        }
        assertTrue("BitKey size " +size, (! gotException));
    }

    /**
     * Test that the implementation object returned is expected type.
     */
    public void testSizeTypes() {
        int size = 0;
        BitKey bitKey = BitKey.Factory.makeBitKey(size);
        assertTrue("BitKey size " +size+ " not BitKey.Small",
            (bitKey.getClass() == BitKey.Small.class));

        size = 63;
        bitKey = BitKey.Factory.makeBitKey(size);
        assertTrue("BitKey size " +size+ " not BitKey.Small",
            (bitKey.getClass() == BitKey.Small.class));

        size = 64;
        bitKey = BitKey.Factory.makeBitKey(size);
        assertTrue("BitKey size " +size+ " not BitKey.Small",
            (bitKey.getClass() == BitKey.Small.class));

        size = 65;
        bitKey = BitKey.Factory.makeBitKey(size);
        assertTrue("BitKey size " +size+ " not BitKey.Mid128",
            (bitKey.getClass() == BitKey.Mid128.class));

        size = 127;
        bitKey = BitKey.Factory.makeBitKey(size);
        assertTrue("BitKey size " +size+ " not BitKey.Mid128",
            (bitKey.getClass() == BitKey.Mid128.class));

        size = 128;
        bitKey = BitKey.Factory.makeBitKey(size);
        assertTrue("BitKey size " +size+ " not BitKey.Mid128",
            (bitKey.getClass() == BitKey.Mid128.class));

        size = 129;
        bitKey = BitKey.Factory.makeBitKey(size);
        assertTrue("BitKey size " +size+ " not BitKey.Big",
            (bitKey.getClass() == BitKey.Big.class));

        size = 1280;
        bitKey = BitKey.Factory.makeBitKey(size);
        assertTrue("BitKey size " +size+ " not BitKey.Big",
            (bitKey.getClass() == BitKey.Big.class));
    }
    /**
     * Test for equals and not equals
     */
    public void testEquals() {
        int[][] positionsArray0 = {
            { 0, 1, 2, 3, },
            { 3, 17, 33, 63 },
            { 1, 2, 3, 20, 21, 33, 61, 62, 63 },
        };
        doTestEquals(0, 0, positionsArray0);
        doTestEquals(0, 64, positionsArray0);
        doTestEquals(64, 0, positionsArray0);
        doTestEquals(0, 128, positionsArray0);
        doTestEquals(128, 0, positionsArray0);
        doTestEquals(64, 128, positionsArray0);
        doTestEquals(128, 64, positionsArray0);

        int[][] positionsArray1 = {
            { 0, 1, 2, 3, },
            { 3, 17, 33, 63 },
            { 1, 2, 3, 20, 21, 33, 61, 62, 63 },
            { 1, 2, 3, 20, 21, 33, 61, 62, 55, 56, 127 },
        };
        doTestEquals(65, 65, positionsArray1);
        doTestEquals(65, 128, positionsArray1);
        doTestEquals(128, 65, positionsArray1);
        doTestEquals(128, 128, positionsArray1);

        int[][] positionsArray2 = {
            { 0, 1, 2, 3, },
            { 1, 2, 3, 20, 21, 33, 61, 62, 55, 56, 127, 128 },
            { 1, 2, 499},
            { 1, 2, 200, 300, 499},
        };
        doTestEquals(500, 500, positionsArray2);
        doTestEquals(500, 700, positionsArray2);
        doTestEquals(700, 500, positionsArray2);
        doTestEquals(700, 700, positionsArray2);
    }
    /**
     * Test for not equals and not equals
     */
    public void testNotEquals() {
        int[] positions0 = {
            0, 1, 2, 3, 4
        };
        int[] positions1 = {
            0, 1, 2, 3,
        };
        doTestNotEquals(0, positions0, 0, positions1);
        doTestNotEquals(0, positions1, 0, positions0);
        doTestNotEquals(0, positions0, 64, positions1);
        doTestNotEquals(0, positions1, 64, positions0);
        doTestNotEquals(64, positions0, 0, positions1);
        doTestNotEquals(64, positions1, 0, positions0);
        doTestNotEquals(0, positions0, 128, positions1);
        doTestNotEquals(128, positions1, 0, positions0);
        doTestNotEquals(64, positions0, 128, positions1);
        doTestNotEquals(128, positions1, 64, positions0);
        doTestNotEquals(128, positions0, 128, positions1);
        doTestNotEquals(128, positions1, 128, positions0);

        int[] positions2 = {
            0, 1,
        };
        int[] positions3 = {
            0, 1, 113
        };
        doTestNotEquals(0, positions2, 64, positions3);
        doTestNotEquals(64, positions3, 0, positions2);

        int[] positions4 = {
            0, 1, 100, 121
        };
        int[] positions5 = {
            0, 1, 100, 121, 200
        };
        doTestNotEquals(64, positions4, 300, positions5);
        doTestNotEquals(300, positions5, 64, positions4);

        int[] positions6 = {
            0, 1, 100, 121, 200,
        };
        int[] positions7 = {
            0, 1, 100, 121, 130, 200,
        };
        doTestNotEquals(200, positions6, 300, positions7);
        doTestNotEquals(300, positions7, 200, positions6);
    }

    /**
     * Test that after clear the internal values are 0.
     */
    public void testClear() {
        BitKey bitKey_0 = BitKey.Factory.makeBitKey(0);
        BitKey bitKey_64 = BitKey.Factory.makeBitKey(64);
        BitKey bitKey_128 = BitKey.Factory.makeBitKey(128);

        int size0 = 20;
        int[] positions0 = {
            0, 1, 2, 3, 4
        };
        BitKey bitKey0 = makeAndSet(size0, positions0);
        bitKey0.clear();

        assertTrue("BitKey 0 not equals after clear to 0",
                (bitKey0.equals(bitKey_0)));
        assertTrue("BitKey 0 not equals after clear to 64",
                (bitKey0.equals(bitKey_64)));
        assertTrue("BitKey 0 not equals after clear to 128",
                (bitKey0.equals(bitKey_128)));

        int size1 = 34;
        int[] positions1 = {
            0, 1, 2, 3, 4, 45, 67
        };
        BitKey bitKey1 = makeAndSet(size1, positions1);
        bitKey1.clear();

        assertTrue("BitKey 1 not equals after clear to 0",
                (bitKey1.equals(bitKey_0)));
        assertTrue("BitKey 1 not equals after clear to 64",
                (bitKey1.equals(bitKey_64)));
        assertTrue("BitKey 1 not equals after clear to 128",
                (bitKey1.equals(bitKey_128)));

        int[] positions2 = {
            0, 1, 2, 3, 4, 45, 67, 213, 333
        };
        BitKey bitKey2 = makeAndSet(size1, positions2);
        bitKey2.clear();

        assertTrue("BitKey 2 not equals after clear to 0",
                (bitKey2.equals(bitKey_0)));
        assertTrue("BitKey 2 not equals after clear to 64",
                (bitKey2.equals(bitKey_64)));
        assertTrue("BitKey 2 not equals after clear to 128",
                (bitKey2.equals(bitKey_128)));
    }

    /**
     * This test is one BitKey is a subset of another.
     */
    public void testIsSuperSetOf() {
        int size0 = 20;
        int[] positions0 = {
            0, 2, 3, 4, 23, 30
        };
        BitKey bitKey0 = makeAndSet(size0, positions0);

        int size1 = 20;
        int[] positions1 = {
            0, 2, 23
        };
        BitKey bitKey1 = makeAndSet(size1, positions1);

        assertTrue("BitKey 1 not subset of 0",
                (bitKey0.isSuperSetOf(bitKey1)));

        assertTrue("BitKey 0 is subset of 1",
                (! bitKey1.isSuperSetOf(bitKey0)));

        int size2 = 65;
        int[] positions2 = {
            0, 1, 2, 3, 4, 23, 30, 113
        };
        BitKey bitKey2 = makeAndSet(size2, positions2);

        assertTrue("BitKey 0 not subset of 2",
                (bitKey2.isSuperSetOf(bitKey0)));
        assertTrue("BitKey 1 not subset of 2",
                (bitKey2.isSuperSetOf(bitKey1)));

        assertTrue("BitKey 2 is subset of 0",
                (! bitKey0.isSuperSetOf(bitKey2)));
        assertTrue("BitKey 2 is subset of 1",
                (! bitKey1.isSuperSetOf(bitKey2)));


        int size3 = 213;
        int[] positions3 = {
            0, 1, 2, 3, 4, 23, 30, 113, 145, 233, 234
        };
        BitKey bitKey3 = makeAndSet(size3, positions3);

        assertTrue("BitKey 0 not subset of 3",
                (bitKey3.isSuperSetOf(bitKey0)));
        assertTrue("BitKey 1 not subset of 3",
                (bitKey3.isSuperSetOf(bitKey1)));
        assertTrue("BitKey 2 not subset of 3",
                (bitKey3.isSuperSetOf(bitKey2)));

        assertTrue("BitKey 3 is subset of 0",
                (! bitKey0.isSuperSetOf(bitKey3)));
        assertTrue("BitKey 3 is subset of 1",
                (! bitKey1.isSuperSetOf(bitKey3)));
        assertTrue("BitKey 3 is subset of 2",
                (! bitKey2.isSuperSetOf(bitKey3)));
    }

    /**
     * This test or-ing BitKeys
     */
    public void testOr() {
        int size0 = 40;
        int[] positions0 = { 0 };
        BitKey bitKey0 = makeAndSet(size0, positions0);
        int[] positions1 = { 1 };
        BitKey bitKey1 = makeAndSet(size0, positions1);

        BitKey bitKey = bitKey0.or(bitKey1);

        assertTrue("BitKey position 0 not set",
                (bitKey.isSetByPos(0)));
        assertTrue("BitKey position 1 not set",
                (bitKey.isSetByPos(1)));

        int[] positions2 = { 0, 1, 10, 20 };
        BitKey bitKey2 = makeAndSet(size0, positions2);
        int[] positions3 = { 1, 2, 10, 11 };
        BitKey bitKey3 = makeAndSet(size0, positions3);

        bitKey = bitKey2.or(bitKey3);

        assertTrue("BitKey position 0 not set",
                (bitKey.isSetByPos(0)));
        assertTrue("BitKey position 1 not set",
                (bitKey.isSetByPos(1)));
        assertTrue("BitKey position 2 not set",
                (bitKey.isSetByPos(2)));
        assertTrue("BitKey position 10 not set",
                (bitKey.isSetByPos(10)));
        assertTrue("BitKey position 11 not set",
                (bitKey.isSetByPos(11)));
        assertTrue("BitKey position 20 not set",
                (bitKey.isSetByPos(20)));

        int size1 = 100;
        int[] positions4 = { 0, 1, 10, 20 };
        BitKey bitKey4 = makeAndSet(size0, positions4);
        int[] positions5 = { 1, 2, 10, 65, 66 };
        BitKey bitKey5 = makeAndSet(size1, positions5);

        bitKey = bitKey4.or(bitKey5);

        assertTrue("BitKey position 0 not set",
                (bitKey.isSetByPos(0)));
        assertTrue("BitKey position 1 not set",
                (bitKey.isSetByPos(1)));
        assertTrue("BitKey position 2 not set",
                (bitKey.isSetByPos(2)));
        assertTrue("BitKey position 10 not set",
                (bitKey.isSetByPos(10)));
        assertTrue("BitKey position 20 not set",
                (bitKey.isSetByPos(20)));
        assertTrue("BitKey position 65 not set",
                (bitKey.isSetByPos(65)));
        assertTrue("BitKey position 66 not set",
                (bitKey.isSetByPos(66)));

        bitKey = bitKey5.or(bitKey4);

        assertTrue("BitKey position 0 not set",
                (bitKey.isSetByPos(0)));
        assertTrue("BitKey position 1 not set",
                (bitKey.isSetByPos(1)));
        assertTrue("BitKey position 10 not set",
                (bitKey.isSetByPos(10)));
        assertTrue("BitKey position 20 not set",
                (bitKey.isSetByPos(20)));
        assertTrue("BitKey position 65 not set",
                (bitKey.isSetByPos(65)));
        assertTrue("BitKey position 66 not set",
                (bitKey.isSetByPos(66)));

        int[] positions6 = { 0, 1, 10, 20, 64, 65, 66 };
        BitKey bitKey6 = makeAndSet(size1, positions6);
        int[] positions7 = { 1, 2, 10, 65, 66 };
        BitKey bitKey7 = makeAndSet(size1, positions7);

        bitKey = bitKey6.or(bitKey7);

        assertTrue("BitKey position 0 not set",
                (bitKey.isSetByPos(0)));
        assertTrue("BitKey position 1 not set",
                (bitKey.isSetByPos(1)));
        assertTrue("BitKey position 10 not set",
                (bitKey.isSetByPos(10)));
        assertTrue("BitKey position 20 not set",
                (bitKey.isSetByPos(20)));
        assertTrue("BitKey position 64 not set",
                (bitKey.isSetByPos(64)));
        assertTrue("BitKey position 65 not set",
                (bitKey.isSetByPos(65)));
        assertTrue("BitKey position 66 not set",
                (bitKey.isSetByPos(66)));

        int size2 = 400;
        int[] positions8 = { 0, 1, 10, 20 };
        BitKey bitKey8 = makeAndSet(size0, positions8);
        int[] positions9 = { 1, 2, 10, 165, 366 };
        BitKey bitKey9 = makeAndSet(size2, positions9);

        bitKey = bitKey8.or(bitKey9);

        assertTrue("BitKey position 0 not set",
                (bitKey.isSetByPos(0)));
        assertTrue("BitKey position 1 not set",
                (bitKey.isSetByPos(1)));
        assertTrue("BitKey position 2 not set",
                (bitKey.isSetByPos(2)));
        assertTrue("BitKey position 10 not set",
                (bitKey.isSetByPos(10)));
        assertTrue("BitKey position 20 not set",
                (bitKey.isSetByPos(20)));
        assertTrue("BitKey position 165 not set",
                (bitKey.isSetByPos(165)));
        assertTrue("BitKey position 366 not set",
                (bitKey.isSetByPos(366)));

        bitKey = bitKey9.or(bitKey8);

        assertTrue("BitKey position 0 not set",
                (bitKey.isSetByPos(0)));
        assertTrue("BitKey position 1 not set",
                (bitKey.isSetByPos(1)));
        assertTrue("BitKey position 2 not set",
                (bitKey.isSetByPos(2)));
        assertTrue("BitKey position 10 not set",
                (bitKey.isSetByPos(10)));
        assertTrue("BitKey position 20 not set",
                (bitKey.isSetByPos(20)));
        assertTrue("BitKey position 165 not set",
                (bitKey.isSetByPos(165)));
        assertTrue("BitKey position 366 not set",
                (bitKey.isSetByPos(366)));

        int[] positions10 = { 0, 1, 10, 20, 100 };
        BitKey bitKey10 = makeAndSet(size1, positions10);
        int[] positions11 = { 1, 2, 10, 165, 366 };
        BitKey bitKey11 = makeAndSet(size2, positions11);

        bitKey = bitKey10.or(bitKey11);

        assertTrue("BitKey position 0 not set",
                (bitKey.isSetByPos(0)));
        assertTrue("BitKey position 1 not set",
                (bitKey.isSetByPos(1)));
        assertTrue("BitKey position 2 not set",
                (bitKey.isSetByPos(2)));
        assertTrue("BitKey position 10 not set",
                (bitKey.isSetByPos(10)));
        assertTrue("BitKey position 20 not set",
                (bitKey.isSetByPos(20)));
        assertTrue("BitKey position 100 not set",
                (bitKey.isSetByPos(100)));
        assertTrue("BitKey position 165 not set",
                (bitKey.isSetByPos(165)));
        assertTrue("BitKey position 366 not set",
                (bitKey.isSetByPos(366)));

        bitKey = bitKey11.or(bitKey10);

        assertTrue("BitKey position 0 not set",
                (bitKey.isSetByPos(0)));
        assertTrue("BitKey position 1 not set",
                (bitKey.isSetByPos(1)));
        assertTrue("BitKey position 2 not set",
                (bitKey.isSetByPos(2)));
        assertTrue("BitKey position 10 not set",
                (bitKey.isSetByPos(10)));
        assertTrue("BitKey position 20 not set",
                (bitKey.isSetByPos(20)));
        assertTrue("BitKey position 100 not set",
                (bitKey.isSetByPos(100)));
        assertTrue("BitKey position 165 not set",
                (bitKey.isSetByPos(165)));
        assertTrue("BitKey position 366 not set",
                (bitKey.isSetByPos(366)));

        int[] positions12 = { 0, 1, 10, 20, 100, 165, 367 };
        BitKey bitKey12 = makeAndSet(size2, positions12);
        int[] positions13 = { 1, 2, 10, 165, 366 };
        BitKey bitKey13 = makeAndSet(size2, positions13);

        bitKey = bitKey12.or(bitKey13);

        assertTrue("BitKey position 0 not set",
                (bitKey.isSetByPos(0)));
        assertTrue("BitKey position 1 not set",
                (bitKey.isSetByPos(1)));
        assertTrue("BitKey position 2 not set",
                (bitKey.isSetByPos(2)));
        assertTrue("BitKey position 10 not set",
                (bitKey.isSetByPos(10)));
        assertTrue("BitKey position 20 not set",
                (bitKey.isSetByPos(20)));
        assertTrue("BitKey position 100 not set",
                (bitKey.isSetByPos(100)));
        assertTrue("BitKey position 165 not set",
                (bitKey.isSetByPos(165)));
        assertTrue("BitKey position 366 not set",
                (bitKey.isSetByPos(366)));
        assertTrue("BitKey position 367 not set",
                (bitKey.isSetByPos(367)));

        bitKey = bitKey13.or(bitKey12);

        assertTrue("BitKey position 0 not set",
                (bitKey.isSetByPos(0)));
        assertTrue("BitKey position 1 not set",
                (bitKey.isSetByPos(1)));
        assertTrue("BitKey position 2 not set",
                (bitKey.isSetByPos(2)));
        assertTrue("BitKey position 10 not set",
                (bitKey.isSetByPos(10)));
        assertTrue("BitKey position 20 not set",
                (bitKey.isSetByPos(20)));
        assertTrue("BitKey position 100 not set",
                (bitKey.isSetByPos(100)));
        assertTrue("BitKey position 165 not set",
                (bitKey.isSetByPos(165)));
        assertTrue("BitKey position 366 not set",
                (bitKey.isSetByPos(366)));
        assertTrue("BitKey position 367 not set",
                (bitKey.isSetByPos(367)));
    }





    private void doTestEquals(int size0, int size1, int[][] positionsArray) {
        for (int i = 0; i < positionsArray.length; i++) {
            int[] positions = positionsArray[i];

            BitKey bitKey0 = makeAndSet(size0, positions);
            BitKey bitKey1 = makeAndSet(size1, positions);

            assertTrue("BitKey not equals size0=" +size0+
                ", size1=" +size1+
                ", i=" +i,
                (bitKey0.equals(bitKey1)));

        }
    }
    private void doTestNotEquals(int size0, int[] positions0,
                                 int size1, int[] positions1) {
        BitKey bitKey0 = makeAndSet(size0, positions0);
        BitKey bitKey1 = makeAndSet(size1, positions1);

        assertTrue("BitKey not equals size0=" +size0+
                ", size1=" +size1,
                (! bitKey0.equals(bitKey1)));
    }

    private BitKey makeAndSet(int size, int[] positions) {
        BitKey bitKey = BitKey.Factory.makeBitKey(size);

        for (int i = 0; i < positions.length; i++) {
            bitKey.setByPos(positions[i]);
        }
        return bitKey;
    }
}
