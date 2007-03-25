/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.List;

/**
 * Condition which constrains a set of values of a single
 * {@link mondrian.rolap.RolapStar.Column} or a set of columns.
 *
 * <p>For example, the predicate
 * <code>Range([Time].[1997].[Q3], [Time].[1998].[Q2])</code>
 * constrains the <code>year</code> and <code>quarter</code> columns:
 *
 * <blockquote><code>
 * &nbsp;&nbsp;((year = 1997 and quarter >= 'Q3')<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;or (year > 1997))<br/>
 * and ((year = 1998 and quarter <= 'Q2')<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;or (year < 1998))</code></blockquote>
 *
 * @author jhyde
 * @version $Id$
 * @since Jan 15, 2007
 */
public interface StarPredicate {
    /**
     * Returns a list of constrained columns.
     *
     * @return List of constrained columns
     */
    public List<RolapStar.Column> getConstrainedColumnList();

    /**
     * Appends a description of this predicate to a <code>StringBuilder</code>.
     * For example:<ul>
     * <li>=any</li>
     * <li>=5</li>
     * <li>in (2, 4, 6)</li>
     * </ul>
     *
     * @param buf Builder to append to
     */
    public abstract void describe(StringBuilder buf);

    /**
     * Evaluates a constraint against a list of values.
     *
     * <p>If one of the values is {@link #WILDCARD}, returns true if constraint is
     * true for all possible values of that column.
     *
     * @param valueList List of values, one for each constrained column
     * @return Whether constraint holds for given set of values
     */
    public boolean evaluate(List<Object> valueList);

    /**
     * Returns whether this Predicate has the same constraining effect as the
     * other constraint. This is weaker than {@link Object#equals(Object)}: it
     * is possible for two different members to constrain the same column in the
     * same way.
     */
    boolean equalConstraint(StarPredicate that);

    /**
     * Returns the logical inverse of this Predicate. The result is a Predicate
     * which holds whenever this predicate holds but the other does not.
     *
     * @pre predicate != null
     */
    StarPredicate minus(StarPredicate predicate);

    /**
     * Wildcard value for {@link #evaluate(java.util.List)}.
     */
    Object WILDCARD = new Object();
}

// End StarPredicate.java
