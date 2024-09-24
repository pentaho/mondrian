/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package mondrian.rolap;

import mondrian.rolap.sql.SqlQuery;

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
     * Returns a bitmap of constrained columns to speed up comparison
     * @return bitmap representing all constraining columns.
     */
    public BitKey getConstrainedColumnBitKey();

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
     *
     * @param that Other predicate
     * @return whether the other predicate is equivalent
     */
    boolean equalConstraint(StarPredicate that);

    /**
     * Returns the logical inverse of this Predicate. The result is a Predicate
     * which holds whenever this predicate holds but the other does not.
     *
     * @pre predicate != null
     * @param predicate Predicate
     * @return Combined predicate
     */
    StarPredicate minus(StarPredicate predicate);

    /**
     * Returns this union of this Predicate with another. The result is a
     * Predicate which holds whenever either predicate holds.
     *
     * @pre predicate != null
     * @param predicate Predicate
     * @return Combined predicate
     */
    StarPredicate or(StarPredicate predicate);

    /**
     * Returns this intersection of this Predicate with another. The result is a
     * Predicate which holds whenever both predicates hold.
     *
     * @pre predicate != null
     * @param predicate Predicate
     * @return Combined predicate
     */
    StarPredicate and(StarPredicate predicate);

    /**
     * Wildcard value for {@link #evaluate(java.util.List)}.
     */
    Object WILDCARD = new Object();

    void toSql(SqlQuery sqlQuery, StringBuilder buf);
}

// End StarPredicate.java
