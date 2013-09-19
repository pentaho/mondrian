/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.util;

import java.util.*;

/**
 * A list that, given a collection of elements, contains every possible
 * combination of those objects (also known as the
 * <a href="http://en.wikipedia.org/wiki/Power_set">power set</a> of those
 * objects).
 *
 * @author LBoudreau
 * @author jhyde
 */
public class CombiningGenerator<E> extends AbstractList<List<E>> {

    private final E[] elements;
    private final int size;

    /**
     * Creates a CombiningGenerator.
     *
     * @param elements Elements to iterate over
     */
    public CombiningGenerator(Collection<E> elements) {
        //noinspection unchecked
        this.elements = (E[]) elements.toArray(new Object[elements.size()]);
        if (elements.size() > 31) {
            // No point having a list with more than 2^31 elements; you can't
            // address it using a java (signed) int value. I suppose you could
            // have an iterator that is larger... but it would take a long time
            // to iterate it.
            throw new IllegalArgumentException("too many elements");
        }
        size = 1 << this.elements.length;
    }

    /**
     * Creates a CombiningGenerator, inferring the type from the argument.
     *
     * @param elements Elements to iterate over
     * @param <T> Element type
     * @return Combing generator containing the power set
     */
    public static <T> CombiningGenerator<T> of(Collection<T> elements) {
        return new CombiningGenerator<T>(elements);
    }

    @Override
    public List<E> get(final int index) {
        final int size = Integer.bitCount(index);
        return new AbstractList<E>() {
            public E get(int index1) {
                if (index1 < 0 || index1 >= size) {
                    throw new IndexOutOfBoundsException();
                }
                int i = nth(index1, index);
                return elements[i];
            }

            public int size() {
                return size;
            }
        };
    }

    /**
     * Returns the position of the {@code seek}th bit set in {@code b}.
     * For example,
     * nth(0, 1) returns 0;
     * nth(0, 2) returns 1;
     * nth(0, 4) returns 2;
     * nth(7, 255) returns 7.
     *
     * <p>Careful. If there are not that many bits set, this method never
     * returns.</p>
     *
     * @param seek Bit to seek
     * @param b Integer in which to seek bits
     * @return Ordinal of the given set bit
     */
    private static int nth(int seek, int b) {
        int i = 0, c = 0;
        for (;;) {
            if ((b & 1) == 1) {
                if (c++ == seek) {
                    return i;
                }
            }
            ++i;
            b >>= 1;
        }
    }

    public int size() {
        return size;
    }

    /**
     * Ad hoc test. See also UtilTest.testCombiningGenerator.
     *
     * @param args ignored
     */
    public static void main(String[] args) {
        List<String> seed = new ArrayList<String>();
        for (int i = 0; i < 8; i++) {
            seed.add(String.valueOf(i));
        }
        List<List<String>> result = new CombiningGenerator<String>(seed);
        for (List<String> i : result) {
            for (Object o : i) {
                System.out.print("|");
                System.out.print(String.valueOf(o));
            }
            System.out.println("|");
        }
    }
}

// End CombiningGenerator.java
