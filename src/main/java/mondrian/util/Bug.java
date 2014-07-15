/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.*;
import mondrian.rolap.RolapSchemaLoader;
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
     */
    public static final boolean Ssas2005Compatible = false;

    /**
     * If you want to tag a piece of code in mondrian that needs to be changed
     * when we upgrade to a future version of olap4j, reference this function.
     * It will always return false.
     */
    public static boolean olap4jUpgrade(String reason) {
        Util.discard(reason);
        return false;
    }

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-229">MONDRIAN-229,
     * "NON EMPTY when hierarchy's default member is not 'all'"</a>
     * is fixed.
     */
    public static final boolean BugMondrian229Fixed = false;

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
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-675">bug MONDRIAN-687,
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
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-941">bug MONDRIAN-951,
     * "Support for member unique names containing primary keys (e.g.
     * [Customer].[Customer Id].&amp;1234)"</a> is fixed.
     */
    public static final boolean BugMondrian951Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1001">bug MONDRIAN-1001,
     * "Tests disabled due to property trigger issues"</a> is fixed.
     */
    public static final boolean BugMondrian1001Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1315">MONDRIAN-1315</a>
     * is fixed.
     */
    public static final boolean BugMondrian1315Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1173">bug MONDRIAN-1173,
     * "Revisit member ordinals in mondrian version 4"</a>
     * is fixed.
     *
     * <p>In a few cases, members come out sorted wrong. Not assigning ordinal
     * correctly?</p>
     */
    public static final boolean BugMondrian1173Fixed = false;

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

    // Mondrian 4 tasks

    private static final boolean Enable = false;

    /**
     * Tests that call TestContext.createSubstitutingCube. 66 uses.
     */
    public static final boolean ModifiedSchema = Enable;

    /**
     * Cube [Store] not found
     */
    public static final boolean CubeStoreFeature = Enable;

     /**
     * Layout wrong cardinality (error 'invalid key ... for level')
     */
     public static final boolean LayoutWrongCardinalty = Enable;

    /**
     * Cube [Sales Ragged] not found
     */
    public static final boolean CubeRaggedFeature = Enable;

    /**
     * Visual totals:
     *  NPE in RolapSchemaReader.getMemberChildren
     *  or NPE in Key.isValid
     */
    public static final boolean VisualTotalsFixed = Enable;

    /**
     * UnsupportedOperationException in getBaseStarKeyColumn
     */
    public static final boolean BaseStarKeyColumnFixed = Enable;

    /**
     * Whether the bug is fixed that causes upgrade of virtual cubes to fail
     * if calculated members or sets depend on dimensions and members in their
     * base cube that are not exposed in the virtual cube.
     */
    public static final boolean VirtualCubeConversionMissesHiddenFixed = true;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1324">bug MONDRIAN-1324,
     * Using the same table in multiple dimensions yields incorrect results
     * </a> is fixed.
     */
    public static final boolean BugMondrian1324Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1329">bug MONDRIAN-1329,
     * Level Types for TimeHalfYears do not work
     * </a> is fixed.
     */
    public static final boolean BugMondrian1329Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1330">bug MONDRIAN-1330,
     * NumberFormatException when using ColumnDef with binary value
     * </a> is fixed.
     */
    public static final boolean BugMondrian1330Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1332">bug MONDRIAN-1333,
     * Levels should be functionally dependent on the level immediately below
     * </a> is fixed.
     */
    public static final boolean BugMondrian1333Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1281">bug MONDRIAN-1281,
     * "custom role injection" </a> is fixed.
     */
    public static final boolean BugMondrian1281Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1335">bug MONDRIAN-1335,
     * 3-way snowflake yielding incorrect results
     * </a> is fixed.
     */
    public static final boolean BugMondrian1335Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1416">bug MONDRIAN-1416,
     * Inline calculated members defined as a child of an existing member
     * cannot be referenced in the mdx query
     * </a> is fixed.
     */
    public static final boolean BugMondrian1420Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1420">bug MONDRIAN-1420,
     * Add support for native evaluation of compound slicers to Mondrian 4.0
     * </a> is fixed.
     */
    public static final boolean BugMondrian1416Fixed = false;

    /**
     * Whether
     * <a href="https://sourceforge.net/p/olap4j/feature-requests/31/">bug
     * OLAP4J-31, "Schema and Catalog should implement MetadataElement"</a> is
     * fixed.
     */
    public static final boolean BugOlap4j31Fixed = false;

    /**
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1338">Bug 1338</a><br/>
     * Mondrian isn't optimizing member fetching according to query constraints
     */
    public static final boolean FetchMembersOptimizationFixed = Enable;

    /**
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1337">Bug 1337</a><br/>
     * Handling of ReferenceLink schema elements is not yet implemented
     * in {@link RolapSchemaLoader}
     */
    public static final boolean ReferenceLinkNotImplementedFixed = Enable;

    /**
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1339">Bug 1339</a><br/>
     * Childless snowflake members are always being filtered irregardless of
     * FilterChildlessSnowflakeMembers's value
     */
    public static final boolean ShowChildlessSnowflakeMembersFixed = Enable;

    /**
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1340">Bug 1340</a><br/>
     * MemberCacheHelper#mapMemberToChildren not populated when reading level
     * members due to SqlTupleReader.Target#members never being set
     */
    public static final boolean PopulateChildrenCacheOnLevelMembersFixed =
        Enable;

    /**
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1375">Bug 1375:</a><br/>
     * AggregationManager and native set functions not using measure groups
     * for aggregates in lagunitas.
     */
    public static final boolean BugMondrian1375Fixed = Enable;


    /**
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1372">Bug 1372:</a><br/>
     * SqlTupleReader doesn't use aggregate tables in lagunitas
     */
    public static final boolean BugMondrian1372Fixed = Enable;
}

// End Bug.java
