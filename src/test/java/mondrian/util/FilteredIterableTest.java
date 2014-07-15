/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.test.FoodMartTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit-test for FilteredIterable
 *
 * @author jlopez, lcanals
 * @since May, 2008
 */
public class FilteredIterableTest extends FoodMartTestCase {
    public FilteredIterableTest() {
    }

    public FilteredIterableTest(String name) {
        super(name);
    }

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
            assertEquals(new Integer(i), empty.get(i));
        }
    }

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
