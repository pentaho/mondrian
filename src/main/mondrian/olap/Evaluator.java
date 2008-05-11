/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 27 July, 2001
*/

package mondrian.olap;

import mondrian.calc.ParameterSlot;
import java.util.List;
import java.util.Locale;
import java.util.Date;

/**
 * An <code>Evaluator</code> holds the context necessary to evaluate an
 * expression.
 *
 * @author jhyde
 * @since 27 July, 2001
 * @version $Id$
 */
public interface Evaluator {

    /**
     * Returns the current cube.
     */
    Cube getCube();

    /**
     * Returns the current query.
     */
    Query getQuery();

    /**
     * Returns the start time of the current query.
     */
    Date getQueryStartTime();

    /**
     * Creates a new Evaluator with each given member overriding the context of
     * the current Evaluator for its dimension. Other dimensions retain the
     * same context as this Evaluator.
     *
     * <p>You can retrieve this Evaluator by calling the new Evaluator's
     * {@link #pop()} method, but it is not necessary to call <code>pop</code>.
     *
     * @param members Array of members to add to the context
     * @return Evaluator with each given member overriding the state of the
     *   current Evaluator for its dimension
     */
    Evaluator push(Member[] members);

    /**
     * Creates a new Evaluator with the same context as this evaluator.
     * Equivalent to {@link #push(Member[]) push(new Member[0])}.
     *
     * <p>This method is typically called before evaluating an expression which
     * is known to corrupt the evaluation context.
     *
     * <p>You can retrieve this Evaluator by calling the new Evaluator's
     * {@link #pop()} method, but it is not necessary to call <code>pop</code>.
     *
     * @return Evaluator with each given member overriding the state of the
     *   current Evaluator for its dimension
     */
    Evaluator push();

    /**
     * Creates a new Evaluator with the same context except for one member.
     * Equivalent to
     * {@link #push(Member[]) push(new Member[] &#124;member&#125;)}.
     *
     * <p>You can retrieve this Evaluator by calling the new Evaluator's
     * {@link #pop()} method, but it is not necessary to call <code>pop</code>.
     *
     * @param member Member to add to the context
     * @return Evaluator with each given member overriding the state of the
     *   current Evaluator for its dimension
     */
    Evaluator push(Member member);

    /**
     * Restores previous evaluator.
     */
    Evaluator pop();

    /**
     * Makes <code>member</code> the current member of its dimension. Returns
     * the previous context.
     *
     * @pre member != null
     * @post return != null
     */
    Member setContext(Member member);

    void setContext(List<Member> memberList);

    void setContext(Member[] members);

    Member getContext(Dimension dimension);

    /**
     * Calculates and returns the value of the cell at the current context.
     */
    Object evaluateCurrent();

    /**
     * Returns the format string for this cell. This is computed by evaluating
     * the format expression in the current context, and therefore different
     * cells may have different format strings.
     */
    public String getFormatString();

    /**
     * Formats a value as a string according to the current context's
     * format.
     */
    String format(Object o);

    /**
     * Formats a value as a string according to the current context's
     * format, using a given format string.
     */
    String format(Object o, String formatString);

    /**
     * Returns number of ancestor evaluators. Used to check for infinite
     * loops.
     *
     * @post return getParent() == null ? 0 : getParent().getDepth() + 1
     */
    int getDepth();

    /**
     * Returns parent evaluator.
     */
    Evaluator getParent();

    /**
     * Returns the connection's locale.
     */
    Locale getConnectionLocale();

    /**
     * Retrieves the value of property <code>name</code>. If more than one
     * member in the current context defines that property, the one with the
     * highest solve order has precedence.
     *
     * <p>If the property is not defined, default value is returned.
     */
    Object getProperty(String name, Object defaultValue);

    /**
     * Returns a {@link SchemaReader} appropriate for the current
     * access-control context.
     */
    SchemaReader getSchemaReader();

    /**
     * Simple caching of the result of an <code>Exp</code>. The
     * key for the cache consists of all members of the current
     * context that <code>exp</code> depends on. Members of
     * independent dimensions are not part of the key.
     *
     * @see mondrian.calc.Calc#dependsOn
     */
    Object getCachedResult(ExpCacheDescriptor key);

    /**
     * Returns true for an axis that is NON EMPTY.
     *
     * <p>May be used by expression
     * evaluators to optimize their result. For example, a top-level crossjoin
     * may be optimized by removing all non-empty set elements before
     * performing the crossjoin. This is possible because of the identity
     *
     * <blockquote><code>nonempty(crossjoin(a, b)) ==
     * nonempty(crossjoin(nonempty(a), nonempty(b));</code></blockquote>
     */
    boolean isNonEmpty();

    /**
     * Sets whether an expression evaluation should filter out empty cells.
     * Allows expressions to modify non empty flag to evaluate their children.
     */
    void setNonEmpty(boolean nonEmpty);

    /**
     * Creates an exception which indicates that an error has occurred during
     * the runtime evaluation of a function. The caller should then throw that
     * exception.
     */
    RuntimeException newEvalException(Object context, String s);

    /**
     * Evaluates a named set.
     */
    Object evaluateNamedSet(String name, Exp exp);

    /**
     * Returns an array of the members which make up the current context.
     */
    Member[] getMembers();

    /**
     * Returns the number of times that this evaluator has told a lie when
     * retrieving cell values.
     */
    int getMissCount();

    /**
     * Returns the value of a parameter, evaluating its default value if it is
     * not set.
     */
    Object getParameterValue(ParameterSlot slot);

    /**
     * @return the iteration length of the current context
     */
    int getIterationLength();

    /**
     * Sets the iteration length for the current evaluator context
     *
     * @param length length to be set
     */
    void setIterationLength(int length);

    /**
     * @return true if evaluating axes
     */
    boolean isEvalAxes();

    /**
     * Indicate whether the evaluator is evaluating the axes
     *
     * @param evalAxes true if evaluating axes
     */
    void setEvalAxes(boolean evalAxes);

    /**
     * Returns a new Aggregator whose aggregation context adds a given list of
     * tuples, and whose dimensional context is the same as this
     * Aggregator.
     *
     * @param list List of tuples
     * @return Aggregator with <code>list</code> added to its aggregation
     *   context
     */
    Evaluator pushAggregation(List<Member[]> list);

    /**
     * Checks if unrelated dimensions to the measure in the current context
     * should be ignored
     * @return boolean
     */
    boolean shouldIgnoreUnrelatedDimensions();

    /**
     * Returns the base (non-virtual) cube that the current measure in the
     * context belongs to.
     * @return Cube
     */
    Cube getMeasureCube();

    /**
     * If IgnoreMeasureForNonJoiningDimension is set to true and one or more
     * members are on unrelated dimension for the measure in current context
     * then returns true.
     * @param members
     * dimensions for the members need to be checked whether
     * related or unrelated
     * @return boolean
     */
    boolean needToReturnNullForUnrelatedDimension(Member[] members);
}

// End Evaluator.java
