/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import mondrian.calc.ParameterSlot;
import mondrian.calc.TupleIterable;
import mondrian.rolap.RolapMeasureGroup;

import java.util.*;

/**
 * An <code>Evaluator</code> holds the context necessary to evaluate an
 * expression.
 *
 * @author jhyde
 * @since 27 July, 2001
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
     * Creates a savepoint encapsulating the current state of the evalutor.
     * You can restore the evaluator to this state by calling
     * {@link #restore(int)} with the value returned by this method.
     *
     * <p>This method is typically called before evaluating an expression which
     * is known to corrupt the evaluation context.
     *
     * <p>Multiple savepoints may be active at the same time for the same
     * evaluator. And, it is allowable to restore to the save savepoint more
     * than once (or not at all). However, when you have rolled back to a
     * particular savepoint you may not restore to a later savepoint.
     *
     * @return Evaluator with each given member overriding the state of the
     *   current Evaluator for its hierarchy
     */
    int savepoint();

    /**
     * Creates a new Evaluator with each given member overriding the context of
     * the current Evaluator for its hierarchy. Other hierarchies retain the
     * same context as this Evaluator.
     *
     * <p>In mondrian-3.3 and later, a more efficient way to save the state of
     * an evaluator is to call {@link #savepoint} followed by
     * {@link #restore(int)}. We recommend using those methods.
     *
     * @param members Array of members to add to the context
     * @return Evaluator with each given member overriding the state of the
     *   current Evaluator for its hierarchy
     *
     * @deprecated Use {@link #savepoint()} followed by
     *   {@link #setContext(Member[])}; will be removed in mondrian-4
     */
    Evaluator push(Member[] members);

    /**
     * Creates a new Evaluator with the same context as this evaluator.
     *
     * <p>This method is typically called before evaluating an expression which
     * may corrupt the evaluation context.
     *
     * <p>In mondrian-3.3 and later, a more efficient way to save the state of
     * an evaluator is to call {@link #savepoint} followed by
     * {@link #restore(int)}. We recommend using those methods most of the time.
     *
     * <p>However, it makes sense to use this method in the constructor of an
     * iterator. It allows the iterator to modify its evaluation context without
     * affecting the evaluation context of the calling code. This behavior
     * cannot be achieved using {@code savepoint}.
     *
     * @return Evaluator with each given member overriding the state of the
     *   current Evaluator for its hierarchy
     */
    Evaluator push();

    /**
     * Creates a new Evaluator with the same context except for one member.
     *
     * <p>This method is typically called before evaluating an expression which
     * may corrupt the evaluation context.
     *
     * <p>In mondrian-3.3 and later, a more efficient way to save the state of
     * an evaluator is to call {@link #savepoint} followed by
     * {@link #restore(int)}. We recommend using those methods.
     *
     * @param member Member to add to the context
     * @return Evaluator with each given member overriding the state of the
     *   current Evaluator for its hierarchy
     *
     * @deprecated Use {@link #savepoint()} followed by
     *   {@link #setContext(Member)}; will be removed in mondrian-4
     */
    Evaluator push(Member member);

    /**
     * Creates a new evaluator with the same state except nonEmpty property
     *
     * <p>In mondrian-3.3 and later, a more efficient way to save the state of
     * an evaluator is to call {@link #savepoint} followed by
     * {@link #restore(int)}. We recommend using those methods.
     *
     * @deprecated Use {@link #savepoint()} followed by
     *     {@link #setNonEmpty(boolean)}; will be removed in mondrian-4
     */
    Evaluator push(boolean nonEmpty);

    /**
     * Creates a new evaluator with the same state except nonEmpty
     * and nativeEnabled properties.
     *
     * <p>In mondrian-3.3 and later, a more efficient way to save the state of
     * an evaluator is to call {@link #savepoint} followed by
     * {@link #restore(int)}. We recommend using those methods.
     *
     * @deprecated Use {@link #savepoint()} followed by
     *     {@link #setNonEmpty(boolean)} and
     *     {@link #setNativeEnabled(boolean)}; will be removed in mondrian-4.
     */
    Evaluator push(boolean nonEmpty, boolean nativeEnabled);

    /**
     * Restores previous evaluator.
     *
     * @param savepoint Savepoint returned by {@link #savepoint()}
     */
    void restore(int savepoint);

    /**
     * Makes <code>member</code> the current member of its hierarchy.
     *
     * @param member  New member
     *
     * @return Previous member of this hierarchy
     */
    Member setContext(Member member);

    /**
     * Makes <code>member</code> the current member of its hierarchy.
     *
     * <p>If {@code safe}, checks whether this is the first time that
     * a member of this hierarchy has been changed since {@link #savepoint()}
     * was called. If so, saves the previous member. If {@code safe} is false,
     * never saves the previous member.
     *
     * <p>Use {@code safe = false} only if you are sure that the context has
     * been set before. For example,
     *
     * <blockquote>
     * <code>int n = 0;<br/>
     * for (Member member : members) {<br/>
     * &nbsp;&nbsp;evaluator.setContext(member, n++ &gt; 0);<br/>
     * }<br/></code></blockquote>
     *
     * @param member  New member
     * @param safe    Whether to store the member of this hierarchy that was
     *                current last time that {@link #savepoint()} was called.
     */
    void setContext(Member member, boolean safe);

    /**
     * Sets the context to a list of members.
     *
     * <p>Equivalent to
     *
     * <blockquote><code>for (Member member : memberList) {<br/>
     * &nbsp;&nbsp;setContext(member);<br/>
     * }<br/></code></blockquote>
     *
     * @param memberList List of members
     */
    void setContext(List<Member> memberList);

    /**
     * Sets the context to a list of members, optionally skipping the check
     * whether it is necessary to store the previous member of each hierarchy.
     *
     * <p>Equivalent to
     *
     * <blockquote><code>for (Member member : memberList) {<br/>
     * &nbsp;&nbsp;setContext(member, safe);<br/>
     * }<br/></code></blockquote>
     *
     * @param memberList List of members
     * @param safe    Whether to store the member of each hierarchy that was
     *                current last time that {@link #savepoint()} was called.
     */
    void setContext(List<Member> memberList, boolean safe);

    /**
     * Sets the context to an array of members.
     *
     * <p>Equivalent to
     *
     * <blockquote><code>for (Member member : memberList) {<br/>
     * &nbsp;&nbsp;setContext(member);<br/>
     * }<br/></code></blockquote>
     *
     * @param members Array of members
     */
    void setContext(Member[] members);

    /**
     * Sets the context to an array of members, optionally skipping the check
     * whether it is necessary to store the previous member of each hierarchy.
     *
     * <p>Equivalent to
     *
     * <blockquote><code>for (Member member : memberList) {<br/>
     * &nbsp;&nbsp;setContext(member, safe);<br/>
     * }<br/></code></blockquote>
     *
     * @param members Array of members
     * @param safe    Whether to store the member of each hierarchy that was
     *                current last time that {@link #savepoint()} was called.
     */
    void setContext(Member[] members, boolean safe);

    Member getContext(Hierarchy hierarchy);

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
     * Obsolete method.
     *
     * @deprecated Will be removed in mondrian-4
     */
    int getDepth();

    /**
     * Returns parent evaluator.
     *
     * @deprecated Will be removed in mondrian-4
     */
    Evaluator getParent();

    /**
     * Returns the connection's locale.
     */
    Locale getConnectionLocale();

    /**
     * Retrieves the value of a given property. If more than one
     * member in the current context defines that property, the one with the
     * highest solve order has precedence.
     *
     * <p>If the property is not defined, default value is returned.
     */
    Object getProperty(Property property, Object defaultValue);

    /**
     * Returns a {@link SchemaReader} appropriate for the current
     * access-control context.
     */
    SchemaReader getSchemaReader();

    /**
     * Simple caching of the result of an <code>Exp</code>. The
     * key for the cache consists of all members of the current
     * context that <code>exp</code> depends on. Members of
     * independent hierarchies are not part of the key.
     *
     * @see mondrian.calc.Calc#dependsOn(Hierarchy)
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
     * Returns an evaluator for a named set.
     *
     * @param namedSet Named set
     * @param create Whether to create evaluator if not found
     * @return Evaluator of named set
     */
    NamedSetEvaluator getNamedSetEvaluator(NamedSet namedSet, boolean create);

    /**
     * Returns an array of the members which make up the current context.
     */
    Member[] getMembers();

    /**
     * Returns an array of the non-All members which make up the current
     * context.
     *
     * <p>Notes:<ul>
     * <li>The 0th element is a measure, but otherwise the order of the
     *     members is unspecified.
     * <li>No hierarchy occurs more than once.
     * <li>In rare circumstances, some of the members may be an 'All' member.
     * <li>The list may contain calculated members.
     * </ul>
     */
    Member[] getNonAllMembers();

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
     * tuples, and whose evaluation context is the same as this
     * Aggregator.
     *
     * @param list List of tuples
     * @return Aggregator with <code>list</code> added to its aggregation
     *   context
     */
    Evaluator pushAggregation(List<List<Member>> list);

    /**
     * Returns whether hierarchies unrelated to the measure in the current
     * context should be forced to their default member (usually, but not
     * always, their 'all' member) during aggregation.
     *
     * @return whether hierarchies unrelated to the measure in the current
     *     context should be ignored
     */
    boolean shouldIgnoreUnrelatedDimensions();

    /**
     * Returns the measure group that the current measure in the
     * context belongs to.
     *
     * @return Measure group
     */
    RolapMeasureGroup getMeasureGroup();

    /**
     * Returns whether it is necessary to check whether to return null for
     * an unrelated dimension. If false, we never need to check: we can assume
     * that {@link #needToReturnNullForUnrelatedDimension(mondrian.olap.Member[])}
     * will always return false.
     *
     * @return whether it is necessary to check whether to return null for
     * an unrelated dimension
     */
    boolean mightReturnNullForUnrelatedDimension();

    /**
     * If IgnoreMeasureForNonJoiningDimension is set to true and one or more
     * members are on unrelated dimension for the measure in current context
     * then returns true.
     *
     * <p>You must not call this method unless
     * {@link #mightReturnNullForUnrelatedDimension()} has returned true.
     *
     * @param members Dimensions for the members need to be checked whether
     *     related or unrelated
     *
     * @return boolean
     */
    boolean needToReturnNullForUnrelatedDimension(Member[] members);

    /**
     * Returns whether native evaluation is enabled in this context.
     *
     * @return whether native evaluation is enabled in this context
     */
    boolean nativeEnabled();

    /**
     * Sets whether native evaluation should be used.
     *
     * @param nativeEnabled Whether native evaluation should be used
     */
   void setNativeEnabled(boolean nativeEnabled);

    /**
     * Returns whether the current context is an empty cell.
     *
     * @return Whether the current context is an empty cell
     */
    boolean currentIsEmpty();

    /**
     * Returns the member that was the current evaluation context for a
     * particular hierarchy before the most recent change in context.
     *
     * @param hierarchy Hierarchy
     * @return Previous context member for given hierarchy
     */
    Member getPreviousContext(Hierarchy hierarchy);

    /**
     * Returns the query timing context for this execution.
     *
     * @return query timing context
     */
    QueryTiming getTiming();

    /**
     * Interface for evaluating a particular named set.
     */
    interface NamedSetEvaluator {
        /**
         * Returns an iterator over the tuples of the named set. Applicable if
         * the named set is a set of tuples.
         *
         * <p>The iterator from this iterable maintains the current ordinal
         * property required for the methods {@link #currentOrdinal()} and
         * {@link #currentTuple()}.
         *
         * @return Iterable over the tuples of the set
         */
        TupleIterable evaluateTupleIterable();

        /**
         * Returns the ordinal of the current member or tuple in the named set.
         *
         * @return Ordinal of the current member or tuple in the named set
         */
        int currentOrdinal();

        /**
         * Returns the current member in the named set.
         *
         * <p>Applicable if the named set is a set of members.
         *
         * @return Current member
         */
        Member currentMember();

        /**
         * Returns the current tuple in the named set.
         *
         * <p>Applicable if the named set is a set of tuples.
         *
         * @return Current tuple.
         */
        Member[] currentTuple();
    }
}

// End Evaluator.java
