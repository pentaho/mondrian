/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.test.clearview;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.BatchTestCase;
import mondrian.spi.Dialect;
import mondrian.test.*;

import junit.framework.Test;
import junit.framework.TestSuite;

import java.lang.reflect.Constructor;

/**
 * <code>ClearViewBase</code> is the base class to build test cases which test
 * queries against the FoodMart database. A concrete sub class and
 * a ref.xml file will be needed for each test suites to be added. MDX queries
 * and their expected results are maintained separately in *.ref.xml files.
 *
 * @author John Sichi
 * @author Richard Emberson
 * @author Khanh Vu
 *
 * @since Jan 25, 2007
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
        super.setUp();
        DiffRepository diffRepos = getDiffRepos();
        diffRepos.setCurrentTestCaseName(getName());
    }

    // implement TestCase
    protected void tearDown() throws Exception {
        DiffRepository diffRepos = getDiffRepos();
        diffRepos.setCurrentTestCaseName(null);
        super.tearDown();
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
            String measures = diffRepos.expand(
                null, "${measures}");
            measures =
                (! (measures.equals("")
                    || measures.equals("${measures}")))
                ? measures : null;
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
            testContext = TestContext.instance().createSubstitutingCube(
                cubeName, customDimensions, measures, calculatedMembers,
                namedSets);
        }

        // Set some properties to match the way we configure them
        // for ClearView.
        propSaver.set(propSaver.props.ExpandNonNative, true);

        String mdx = diffRepos.expand(null, "${mdx}");
        String result = Util.nl + TestContext.toString(
            testContext.executeQuery(mdx));
        diffRepos.assertEquals("result", "${result}", result);
    }

    protected void assertQuerySql(boolean flushCache)
        throws Exception
    {
        DiffRepository diffRepos = getDiffRepos();

        if (buildSqlPatternArray() == null) {
            return;
        }

        assertQuerySqlOrNot(
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
            return new SqlPattern[]{
                new SqlPattern(dialect, sql, null)
            };
        }
        return null;
    }
}

// End ClearViewBase.java
