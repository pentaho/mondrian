/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2003-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.test.clearview;

import mondrian.olap.*;
import mondrian.rolap.BatchTestCase;

import junit.framework.*;

import mondrian.test.*;
import mondrian.spi.Dialect;

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
public abstract class ClearViewBase extends BatchTestCase {

    public ClearViewBase() {
        super();
    }

    public ClearViewBase(String name) {
        super(name);
    }

    public abstract DiffRepository getDiffRepos();

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

        // add calculated member to a cube if specified in the xml file
        String cubeName = diffRepos.expand(null, "${modifiedCubeName}").trim();
        if (! (cubeName.equals("")
            || cubeName.equals("${modifiedCubeName}")))
        {
            String customDimensions = diffRepos.expand(
                null, "${customDimensions}");
            customDimensions =
                (! (customDimensions.equals("")
                    || customDimensions.equals("${customDimensions}")))
                ? customDimensions : null;
            String calculatedMembers = diffRepos.expand(
                null, "${calculatedMembers}");
            calculatedMembers =
                (! (calculatedMembers.equals("")
                    || calculatedMembers.equals("${calculatedMembers}")))
                ? calculatedMembers : null;
            String namedSets = diffRepos.expand(
                null, "${namedSets}");
            namedSets =
                (! (namedSets.equals("")
                    || namedSets.equals("${namedSets}")))
                ? namedSets : null;
            testContext = testContext.createSubstitutingCube(
                cubeName, customDimensions, calculatedMembers, namedSets);
        }

        // Set some properties to match the way we configure them
        // for ClearView.
        boolean origExpandNonNative =
            MondrianProperties.instance().ExpandNonNative.get();
        MondrianProperties.instance().ExpandNonNative.set(true);

        try {
            String mdx = diffRepos.expand(null, "${mdx}");
            String result = Util.nl + testContext.toString(
                testContext.executeQuery(mdx));
            diffRepos.assertEquals("result", "${result}", result);
        } finally {
            MondrianProperties.instance().ExpandNonNative.set(
                origExpandNonNative);
        }
    }

    protected void assertQuerySql(boolean flushCache)
        throws Exception
    {
        DiffRepository diffRepos = getDiffRepos();

        if (buildSqlPatternArray() == null) {
            return;
        }

        super.assertQuerySqlOrNot(
            getTestContext(),
            diffRepos.expand(null, "${mdx}"),
            buildSqlPatternArray(),
            false,
            false,
            flushCache);
    }

    protected void assertNoQuerySql(boolean flushCache)
        throws Exception
    {
        DiffRepository diffRepos = getDiffRepos();

        if (buildSqlPatternArray() == null) {
            return;
        }

        super.assertQuerySqlOrNot(
            getTestContext(),
            diffRepos.expand(null, "${mdx}"),
            buildSqlPatternArray(),
            true,
            false,
            flushCache);
    }

    private SqlPattern[] buildSqlPatternArray() {
        DiffRepository diffRepos = getDiffRepos();
        Dialect d = getTestContext().getDialect();
        Dialect.DatabaseProduct dialect = d.getDatabaseProduct();
        String testCaseName = getName();
        String sql = diffRepos.get(
            testCaseName, "expectedSql", dialect.name());
        if (sql != null) {
            sql = sql.replaceAll("[ \t\n\f\r]+", " ").trim();
            SqlPattern[] patterns = {
                new SqlPattern(dialect, sql, null)
            };
            return patterns;
        }
        return null;
    }
}

// End ClearViewBase.java
