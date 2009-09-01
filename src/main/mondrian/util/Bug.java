/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import mondrian.olap.MondrianProperties;
import mondrian.spi.Dialect;

/**
 * Holder for constants which indicate whether particular issues have been
 * fixed. Reference one of those constants in your code, and it is clear which
 * code can be enabled when the bug is fixed. Generally a constant is removed
 * when its bug is fixed.
 *
 * @author jhyde
 * @version $Id$
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
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-229">MONDRIAN-229, "NON EMPTY when hierarchy's default member is not 'all'"</a>
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
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-207">MONDRIAN-207, "IS EMPTY and IS NULL"</a>
     * is fixed.
     */
    public static final boolean BugMondrian207Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-446">bug MONDRIAN-446, "Make Native NonEmpty consistant with MSAS"</a>
     * is fixed.
     */
    public static final boolean BugMondrian446Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-313">bug MONDRIAN-313, "Predicate references RolapStar.Column when used in AggStar"</a>
     * is fixed.
     */
    public static final boolean BugMondrian313Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-314">bug MONDRIAN-314, "Predicate sometimes has null RolapStar.Column"</a>
     * is fixed.
     */
    public static final boolean BugMondrian314Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-328">bug MONDRIAN-328, "CrossJoin no empty optimizer eliminates calculated member"</a>
     * is fixed.
     */
    public static final boolean BugMondrian328Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-361">bug MONDRIAN-361, "Aggregate Tables not working with Shared Dimensions"</a>
     * is fixed.
     *
     */
    public static final boolean BugMondrian361Fixed = false;

    /**
     * Whether
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-486">bug MONDRIAN-486, "HighCardinalityTest test cases disabled"</a>
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
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-503">bug MONDRIAN-503, "RolapResultTest disabled"</a>
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
}

// End Bug.java
