/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


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
