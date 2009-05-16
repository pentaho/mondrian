/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/
package mondrian.test.clearview;

import junit.framework.TestSuite;
import junit.framework.Test;
import mondrian.test.DiffRepository;

/**
 * <code>MemHungryTest</code> is a test suite which tests
 * complex queries against the FoodMart database. MDX queries and their
 * expected results are maintained separately in MemHungryTest.ref.xml file.
 * If you would prefer to see them as inlined Java string literals, run
 * ant target "generateDiffRepositoryJUnit" and then use
 * file MemHungryTestJUnit.java which will be generated in this directory.
 *
 * @author Khanh Vu
 * @version $Id$
 */
public class MemHungryTest extends ClearViewBase {

    public MemHungryTest() {
        super();
    }

    public MemHungryTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(MemHungryTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), MemHungryTest.class);
    }
}
// End MemHungryTest.java
