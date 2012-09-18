/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.MondrianProperties;
import mondrian.spi.Dialect;

import org.apache.log4j.Logger;

/**
 * Holder for constants which indicate whether particular issues have been
 * fixed. Reference one of those constants in your code, and it is clear which
 * code can be enabled when the bug is fixed. Generally a constant is removed
 * when its bug is fixed.
 *
 * <h3>Cleanup items</h3>
 *
 * The following is a list of cleanup items. They are not bugs per se:
 * functionality is not wrong, just the organization of the code. If they were
 * bugs, they would be in jira. It makes sense to have the list here, so that
 * referenced class, method and variable names show up as uses in code searches.
 *
 * <dl>
 *
 * <dt>Obsolete {@link mondrian.olap.Id.Segment}</dt>
 * <dd>Replace it by {@link org.olap4j.mdx.IdentifierSegment}. Likewise
 * {@link mondrian.olap.Id.Quoting} with {@link org.olap4j.mdx.Quoting}.
 * Should wait until after the mondrian 4 'big bang', because there are ~300
 * uses of Segment in the code.</dd>
 *
 * </dl>
 *
 * @author jhyde
 * @since Oct 11, 2006
 */
public class Bug {
    /**
     * Whether Mondrian is 100% compatible with Microsoft Analysis Services
     * 2005. We know that it is not, so this constant is {@code false}.
     *
     * <p>Use this
     * field to flag test cases whose behavior is intentionally different from
     * SSAS. If the behavior is <em>un</em>intentionally different and something
     * we want to fix, log a bug, add a new {@code BugMondrianXxxFixed} constant
     * to this class, and make the test case conditional on that constant
     * instead.
     *
     * <p>See also the property
     * {@link mondrian.olap.MondrianProperties#SsasCompatibleNaming},
     * which allows the user to choose certain behaviors which are compatible
     * with SSAS 2005 but incompatible with Mondrian's previous behavior.
     */
    public static final boolean Ssas2005Compatible = false;

    /**
     * If you want to tag a piece of code in mondrian that needs to be changed
     * when we upgrade to a future version of olap4j, reference this function.
     * It will always return false.
     */
    public static boolean olap4jUpgrade(String reason) {
        return false;
    }

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-229">MONDRIAN-229,
     * "NON EMPTY when hierarchy's default member is not 'all'"</a>
     * is fixed.
     */
    public static final boolean BugMondrian229Fixed = false;

    // Properties relating to checkin 7641.
    // This is part of the junit test Checkin_7641 that
    // shows that there is a difference when the default
    // member is not the one used in an axis.
    // When Checkin 7641 is resolved, then this System property access and
    // boolean should go away.
    // (What's the bug associated with this??)

    public static final boolean Checkin7641UseOptimizer = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-207">MONDRIAN-207,
     * "IS EMPTY and IS NULL"</a> is fixed.
     */
    public static final boolean BugMondrian207Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-446">bug MONDRIAN-446,
     * "Make Native NonEmpty consistant with MSAS"</a>
     * is fixed.
     */
    public static final boolean BugMondrian446Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-313">bug MONDRIAN-313,
     * "Predicate references RolapStar.Column when used in AggStar"</a>
     * is fixed.
     */
    public static final boolean BugMondrian313Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-314">bug MONDRIAN-314,
     * "Predicate sometimes has null RolapStar.Column"</a>
     * is fixed.
     */
    public static final boolean BugMondrian314Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-328">bug MONDRIAN-328,
     * "CrossJoin no empty optimizer eliminates calculated member"</a>
     * is fixed.
     */
    public static final boolean BugMondrian328Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-361">bug MONDRIAN-361,
     * "Aggregate Tables not working with Shared Dimensions"</a>
     * is fixed.
     *
     */
    public static final boolean BugMondrian361Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-441">bug MONDRIAN-441,
     * "Parent-child hierarchies: &lt;Join&gt; used in dimension"</a>
     * is fixed.
     */
    public static final boolean BugMondrian441Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-486">bug MONDRIAN-486,
     * "HighCardinalityTest test cases disabled"</a>
     * is fixed.
     */
    public static final boolean BugMondrian486Fixed = false;

    /**
     * Whether bug <a href="http://jira.pentaho.com/browse/MONDRIAN-495">
     * MONDRIAN-495, "Table filter concept does not support dialects."</a>
     * is fixed.
     */
    public static final boolean BugMondrian495Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-503">bug MONDRIAN-503,
     * "RolapResultTest disabled"</a>
     * is fixed.
     */
    public static final boolean BugMondrian503Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-555">bug MONDRIAN-555,
     * "Compound slicer counts cells twice in certain cases"</a> is fixed.
     * If a set in the slicer contains the same member more than once, or more
     * generally, if the regions overlap, then mondrian counts the overlaps
     * twice, whereas SSAS 2005 does not.
     */
    public static final boolean BugMondrian555Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-584">bug MONDRIAN-584,
     * "Native evaluation returns enumerated members in the wrong order"</a>
     * is fixed. A query that includes { Gender.M, Gender.F } should return
     * results where the Gender.M values are returned before the Gender.F
     * values.
     */
    public static final boolean BugMondrian584Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-641">bug MONDRIAN-641,
     * "Large NON EMPTY result performs poorly with ResultStyle.ITERABLE"</a>
     */
    public static final boolean BugMondrian641Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-648">bug MONDRIAN-648,
     * "AS operator has lower precedence than required by MDX specification"</a>
     * is fixed.
     */
    public static final boolean BugMondrian648Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-687">bug MONDRIAN-687,
     * "Format treats negative numbers differently than SSAS"</a>
     * is fixed.
     */
    public static final boolean BugMondrian687Fixed = false;

    /**
     * Whether bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-747">bug MONDRIAN-747,
     * "When joining a shared dimension into a cube at a level
     * other than its leaf level, Mondrian gives wrong results"</a> is fixed.
     */
    public static final boolean BugMondrian747Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-759">bug MONDRIAN-759,
     * "use dynamic parameters and PreparedStatement for frequently executed SQL
     * patterns"</a> is fixed.
     */
    public static final boolean BugMondrian759Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-785">bug MONDRIAN-785,
     * "Native evaluation does not respect ordering"</a> is fixed.
     */
    public static final boolean BugMondrian785Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1001">bug MONDRIAN-1001,
     * "Tests disabled due to property trigger issues"</a> is fixed.
     */
    public static final boolean BugMondrian1001Fixed = false;

    /**
     * Whether RolapCubeMember and RolapMember have been fully segregated; any
     * piece of code should be working with one or the other, not both.
     */
    public static final boolean BugSegregateRolapCubeMemberFixed = false;

    /**
     * Returns whether to avoid a test because the memory monitor may cause it
     * to fail.
     *
     * <p>Some tests fail if memory monitor is switched on, and Access and
     * Derby tend to use a lot of memory because they are embedded.
     *
     * @param dialect Dialect
     * @return Whether to avoid a test
     */
    public static boolean avoidMemoryOverflow(Dialect dialect) {
        return dialect.getDatabaseProduct() == Dialect.DatabaseProduct.ACCESS
            && MondrianProperties.instance().MemoryMonitor.get();
    }

    /**
     * Returns true if we are running against
     * {@link mondrian.spi.Dialect.DatabaseProduct#LUCIDDB} and we wish to
     * avoid slow tests.
     *
     * <p>This is because some tests involving parent-child hierarchies are
     * very slow. If we are running performance tests (indicated by the
     * {@code mondrian.test.PerforceTest} logger set at
     * {@link org.apache.log4j.Level#DEBUG} or higher), we expect the suite to
     * take a long time, so we enable the tests.
     *
     * <p>Fixing either {@link #BugMondrian759Fixed MONDRIAN-759} or
     * <a href="http://issues.eigenbase.org/browse/FRG-400">FRG-400, "rewrite
     * statements containing literals to use internally-managed dynamic
     * parameters instead"</a> would solve the problem.
     *
     * @return Whether we are running LucidDB and we wish to avoid slow tests.
     */
    public static boolean avoidSlowTestOnLucidDB(Dialect dialect) {
        return
            !BugMondrian759Fixed
            && dialect.getDatabaseProduct() == Dialect.DatabaseProduct.LUCIDDB
            && !Logger.getLogger("mondrian.test.PerformanceTest")
                .isDebugEnabled();
    }
}

// End Bug.java
