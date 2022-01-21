/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Unit-test for FilteredIterable
 *
 * @author jlopez, lcanals, Stefan Bischof
 * @since May, 2008
 */
public class FilteredIterableTest{
    public FilteredIterableTest() {
    }

    @Test
    public void testEmptyList() throws Exception {
        final List<Integer> base = new ArrayList<Integer>();
        for (int i = 0; i < 10; i++) {
            base.add(i);
        }

        final List<Integer> empty =
            new FilteredIterableList<Integer>(
                base,
                new FilteredIterableList.Filter<Integer>() {
                    public boolean accept(final Integer i) {
                        return false;
                    }
                });
        for (final Integer x : empty) {
            fail("All elements should have been filtered");
        }
    }

    @Test
    public void testGetter() throws Exception {
        final List<Integer> base = new ArrayList<Integer>();
        for (int i = 0; i < 10; i++) {
            base.add(i);
        }

        final List<Integer> empty =
            new FilteredIterableList<Integer>(
                base,
                new FilteredIterableList.Filter<Integer>() {
                    public boolean accept(final Integer i) {
                        return i < 2;
                    }
                });
        for (int i = 0; i < 2; i++) {
            assertEquals(Integer.valueOf(i), empty.get(i));
        }
    }

    @Test
    public void test2Elements() throws Exception {
        final List<Integer> base = new ArrayList<Integer>();
        for (int i = 0; i < 2; i++) {
            base.add(i);
        }

        final List<Integer> identical =
            new FilteredIterableList<Integer>(
                base,
                new FilteredIterableList.Filter<Integer>() {
                    public boolean accept(final Integer i) {
                        return true;
                    }
                });
        assertFalse(identical.isEmpty());
        assertNotNull(identical.get(0));
        int k = 0;
        for (final Integer i : identical) {
            assertEquals(i, identical.get(k));
            k++;
        }
    }
}

// End FilteredIterableTest.java
