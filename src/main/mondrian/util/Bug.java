/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import mondrian.rolap.sql.SqlQuery;
import mondrian.olap.MondrianProperties;
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
     * Whether
     * <a href="http://sourceforge.net/tracker/index.php?func=detail&aid=1574942&group_id=35302&atid=414613">bug 1574942, "NON EMPTY when hierarchy's default member is not 'all'"</a>
     * is fixed.
     */
    public static final boolean Bug1574942Fixed = false;

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
     * <a href="http://sourceforge.net/tracker/index.php?func=detail&aid=1530543&group_id=35302&atid=414613">bug 1530543, "IS EMPTY and IS NULL"</a>
     * is fixed.
     */
    public static final boolean Bug1530543Fixed = false;

    /**
     * Whether
     * <a href="http://sourceforge.net/tracker/index.php?func=detail&aid=1722959&group_id=35302&atid=414613">bug 1722959, "NON EMPTY &lt;Level&gt;.MEMBERS fails if nonempty.enable=false"</a>
     * is fixed.
     */
    public static final boolean Bug1722959Fixed = false;

    /**
     * Whether
     * <a href="http://sourceforge.net/tracker/index.php?func=detail&aid=1767775&group_id=35302&atid=414613">bug 1767775, "Predicate references RolapStar.Column when used in AggStar"</a>
     * is fixed.
     */
    public static final boolean Bug1767775Fixed = false;

    /**
     * Whether
     * <a href="http://sourceforge.net/tracker/index.php?func=detail&aid=1767779&group_id=35302&atid=414613">bug 1767779, "Predicate sometimes has null RolapStar.Column"</a>
     * is fixed.
     */
    public static final boolean Bug1767779Fixed = false;

    /**
     * Whether
     * <a href="http://sourceforge.net/tracker/index.php?func=detail&aid=1791609&group_id=35302&atid=414613">bug 1791609, "CrossJoin no empty optimizer eliminates calculated member"</a>
     * is fixed.
     */
    public static final boolean Bug1791609Fixed = false;
    
    /**
     * Returns whether to avoid a test because the memory monitor may cause it
     * to fail.
     *
     * <p>Some tests fail if memory monitor is switched on, and Access and
     * Derby tend to use a lot of memory because they are embedded.
     */
    public static boolean avoidMemoryOverflow(SqlQuery.Dialect dialect) {
        return dialect.isAccess() &&
            MondrianProperties.instance().MemoryMonitor.get();
    }
}

// End Bug.java
