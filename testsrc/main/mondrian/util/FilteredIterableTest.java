/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import mondrian.calc.ResultStyle;
import mondrian.olap.*;
import mondrian.olap.*;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.test.DiffRepository;

import java.util.*;
import java.lang.ref.*;

/**
 * Unit-test for FilteredIterable
 *
 * @author jlopez, lcanals
 * @version $Id$
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
        for(int i=0; i<10; i++) {
            base.add(i);
        }

        final List<Integer> empty = new FilteredIterableList<Integer>(
                base, new FilteredIterableList.Filter<Integer>() {
                    public boolean accept(final Integer i) {
                        return false;
                    }
                });
        for(final Integer x : empty) {
            fail("All elements should have been filtered");
        }
    }


    public void testGetter() throws Exception {
        final List<Integer> base = new ArrayList<Integer>();
        for(int i=0; i<10; i++) {
            base.add(i);
        }

        final List<Integer> empty = new FilteredIterableList<Integer>(
                base, new FilteredIterableList.Filter<Integer>() {
                    public boolean accept(final Integer i) {
                        return i<2;
                    }
                });
        for(int i=0; i<2; i++) {
            assertEquals(new Integer(i), empty.get(i));
        }
    }

    public void test2Elements() throws Exception {
        final List<Integer> base = new ArrayList<Integer>();
        for(int i=0; i<2; i++) {
            base.add(i);
        }

        final List<Integer> identical = new FilteredIterableList<Integer>(
                base, new FilteredIterableList.Filter<Integer>() {
                    public boolean accept(final Integer i) {
                        return true;
                    }
                });
        assertFalse(identical.isEmpty());
        assertNotNull(identical.get(0));
        int k = 0;
        for(final Integer i : identical) {
            assertEquals(i, identical.get(k));
            k++;
        }
    }


}

// End FilteredIterableTest.java
