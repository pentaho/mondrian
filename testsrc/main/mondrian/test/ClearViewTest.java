/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.test;

import mondrian.olap.*;

import java.util.*;

import junit.framework.*;

/**
 * <code>ClearViewTest</code> is a test case which tests complex queries
 * against the FoodMart database.  MDX queries and their expected results are
 * maintained separately in ClearViewTest.ref.xml.  If you would
 * prefer to see them as inlined Java string literals, run
 * ant target "generateDiffRepositoryJUnit" and then use
 * class ClearViewJUnit which will be generated in this directory.
 *
 * @author John Sichi
 * @author Richard Emberson
 *
 * @since Jan 25, 2007
 * @version $Id$
 */
public class ClearViewTest extends FoodMartTestCase {

    public ClearViewTest() {
        super();
    }
    
    public ClearViewTest(String name) {
        super(name);
    }

    private static DiffRepository getDiffRepos() {
        return DiffRepository.lookup(ClearViewTest.class);
    }

    // implement TestCase
    protected void setUp() throws Exception {
        DiffRepository diffRepos = getDiffRepos();
        diffRepos.setCurrentTestCaseName(getName());
    }
    
    // implement TestCase
    protected void tearDown() throws Exception {
        DiffRepository diffRepos = getDiffRepos();
        diffRepos.setCurrentTestCaseName(null);
    }
    
    // implement TestCase
    public static TestSuite suite() {
        TestSuite suite = new TestSuite();

        DiffRepository diffRepos = getDiffRepos();

        for (String name : diffRepos.getTestCaseNames()) {
            suite.addTest(new ClearViewTest(name));
        }

        return suite;
    }

    // implement TestCase
    protected void runTest() throws Exception {
        DiffRepository diffRepos = getDiffRepos();
        TestContext testContext = getTestContext();
        String mdx = diffRepos.expand(null, "${mdx}");
        String result = Util.nl + testContext.toString(
            testContext.executeQuery(mdx));
        diffRepos.assertEquals("result", "${result}", result);
    }
}

// End ClearViewTest.java
