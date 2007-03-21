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
package mondrian.test.clearview;

import mondrian.olap.*;

import java.util.*;

import junit.framework.*;

import mondrian.test.*;

import java.lang.reflect.*;

/**
 * <code>ClearViewBase</code> is the base class to build test cases which test
 * queries against the FoodMart database. A concrete sub class and
 * a ref.xml file will be needed for each test suites to be added. MDX queries
 * and their expected results are maintained separately in *.ref.xml files.
 * If you would prefer to see them as inlined Java string literals, run
 * ant target "generateDiffRepositoryJUnit" and then use
 * files *JUnit.java which will be generated in this directory.
 *
 * @author John Sichi
 * @author Richard Emberson
 * @author Khanh Vu
 *
 * @since Jan 25, 2007
 * @version $Id$
 */
public abstract class ClearViewBase extends FoodMartTestCase {

    public ClearViewBase() {
        super();
    }
    
    public ClearViewBase(String name) {
        super(name);
    }

    protected abstract DiffRepository getDiffRepos();

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
    public static TestSuite constructSuite(
        DiffRepository diffRepos,
        Class clazz) 
    {
        TestSuite suite = new TestSuite();
        Class[] types = new Class[] { String.class };

        for (String name : diffRepos.getTestCaseNames()) {
            try {
                Constructor cons = clazz.getConstructor(types);
                Object[] args = new Object[] { name };
                suite.addTest((Test) cons.newInstance(args));
            } catch (Exception e) {
                throw new Error(e.getMessage());
            }
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

// End ClearViewBase.java
