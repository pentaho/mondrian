/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.util.Format;

import java.util.*;

/**
 * <code>RolapEvaluator</code> evaluates expressions in a dimensional
 * environment.
 *
 * <p>The context contains a member (which may be the default member)
 * for every dimension in the current cube. Certain operations, such as
 * evaluating a calculated member or a tuple, change the current context. The
 * evaluator's {@link #push} method creates a clone of the current evaluator
 * so that you can revert to the original context once the operation has
 * completed.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapEvaluator implements Evaluator
{
	RolapCube cube;
	RolapConnection connection;
	RolapMember[] currentMembers;
	Evaluator parent;
	CellReader cellReader;
	int depth;
	
	Map expResultCache;

	RolapEvaluator(RolapCube cube, RolapConnection connection)
	{
		this.cube = cube;
		this.connection = connection;
		SchemaReader scr = connection.getSchemaReader();
		RolapDimension[] dimensions = (RolapDimension[]) cube.getDimensions();
		currentMembers = new RolapMember[dimensions.length];
		for (int i = 0; i < dimensions.length; i++) {
			final RolapDimension dimension = dimensions[i];
			final int ordinal = dimension.getOrdinal(cube);
			final Hierarchy hier = dimension.getHierarchy();
			currentMembers[ordinal] = (RolapMember) scr.getHierarchyDefaultMember(hier);
		}
		this.parent = null;
		this.depth = 0;
		this.cellReader = null; // we expect client to set it
		this.expResultCache = new HashMap();
	}

	private RolapEvaluator(
		RolapCube cube, RolapConnection connection,
		RolapMember[] currentMembers, RolapEvaluator parent)
	{
		this.cube = cube;
		this.connection = connection;
		this.currentMembers = currentMembers;
		this.parent = parent;
		this.depth = parent.getDepth() + 1;
		this.cellReader = parent.cellReader;
		int memberCount = currentMembers.length;
		if (depth > memberCount) {
			if (depth % memberCount == 0) {
				checkRecursion();
			}
		}
		this.expResultCache = parent.expResultCache;
	}

	public Cube getCube() {
		return cube;
	}

	public int getDepth() {
		return depth;
	}

	public Evaluator getParent() {
		return parent;
	}

	public SchemaReader getSchemaReader() {
		return connection.getSchemaReader();
	}

	public Evaluator push(Member[] members) {
		final RolapEvaluator evaluator = _push();
		evaluator.setContext(members);
		return evaluator;
	}

	public Evaluator push(Member member) {
		final RolapEvaluator evaluator = _push();
		evaluator.setContext(member);
		return evaluator;
	}

	public Evaluator push() {
		return _push();
	}

	private final RolapEvaluator _push() {
		RolapMember[] cloneCurrentMembers = (RolapMember[]) this.currentMembers.clone();
		return new RolapEvaluator(cube, connection, cloneCurrentMembers, this);
	}

	public Evaluator pop() {
		return parent;
	}
	public Object xx(Literal literal) {
		return literal.getValue();
	}
	public Object xx(Parameter parameter) {
		return parameter.getExp().evaluate(this);
	}
	public Object xx(FunCall funCall) {
		FunDef funDef = funCall.getFunDef();
		return funDef.evaluate(this, funCall.args);
	}
	public Object xx(Id id) {
		throw new Error("unsupported");
	}
	public Object xx(OlapElement mdxElement) {
		return mdxElement;
	}
	public Member setContext(Member member)
	{
		RolapMember m = (RolapMember) member;
		int ordinal = m.getDimension().getOrdinal(cube);
		RolapMember previous = currentMembers[ordinal];
		currentMembers[ordinal] = m;
		return previous;
	}
	public void setContext(Member[] members)
	{
		for (int i = 0; i < members.length; i++) {
			setContext(members[i]);
		}
	}
	public Member getContext(Dimension dimension)
	{
		return currentMembers[dimension.getOrdinal(cube)];
	}
	public Object evaluateCurrent()
	{
		int minSolve = Integer.MAX_VALUE;
		RolapMember minSolveMember = null;
		for (int i = 0, count = currentMembers.length; i < count; i++) {
			if (currentMembers[i].isCalculated()) {
				int solve = currentMembers[i].getSolveOrder();
				if (solve < minSolve) {
					minSolve = solve;
					minSolveMember = currentMembers[i];
				}
			}
		}
		if (minSolve < Integer.MAX_VALUE) {
			// There is at least one calculated member. Expand the first one
			// with the lowest solve order.
			RolapMember defaultMember = (RolapMember)
					minSolveMember.getHierarchy().getDefaultMember();
			Util.assertTrue(
					defaultMember != minSolveMember,
					"default member must not be calculated");
			Evaluator evaluator = push(defaultMember);
//			((RolapEvaluator) evaluator).cellReader = new CachingCellReader(cellReader);
			return minSolveMember.getExpression().evaluateScalar(evaluator);
		}
		return cellReader.get(this);
	}
	/**
	 * Makes sure that there is no evaluator with identical context on the
	 * stack.
	 *
	 * @throws mondrian.olap.fun.MondrianEvaluationException if there is a loop
	 */
	void checkRecursion() {
		outer:
		for (Evaluator eval = getParent(); eval != null; eval = eval.getParent()) {
			for (int i = 0; i < currentMembers.length; i++) {
				RolapMember member = currentMembers[i];
				Member parentMember = eval.getContext(member.getDimension());
				if (member != parentMember) {
					continue outer;
				}
				throw FunUtil.newEvalException(
						null, "infinite loop: context is " + getContextString());
			}
		}
	}

	private String getContextString() {
		StringBuffer sb = new StringBuffer("(");
		for (int j = 0; j < currentMembers.length; j++) {
			RolapMember m = currentMembers[j];
			if (j > 0) {
				sb.append(", ");
			}
			sb.append(m.getUniqueName());
		}
		sb.append(")");
		return sb.toString();
	}

	public Object getProperty(String name)
	{
		Object o = null;
		int maxSolve = Integer.MIN_VALUE;
		for (int i = 0; i < currentMembers.length; i++) {
			RolapMember member = currentMembers[i];
			Object p = member.getPropertyValue(name);
			if (p != null) {
				int solve = member.getSolveOrder();
				if (solve > maxSolve) {
					o = p;
					maxSolve = solve;
				}
			}
		}
		return o;
	}
    /**
     * Returns the format string for this cell. This is computed by evaluating
     * the format expression in the current context, and therefore different
     * cells may have different format strings.
     *
     * @post return != null
     */
	String getFormatString()
	{
		Exp formatExp = (Exp) getProperty(Property.PROPERTY_FORMAT_EXP);
		if (formatExp == null) {
			return "Standard";
		}
		Object o = formatExp.evaluate(this);
		return o.toString();
	}
	private Format getFormat()
	{
		String formatString = getFormatString();
		return Format.get(formatString, connection.getLocale());
	}

	/**
	 * Converts a value of this member into a string according to this member's
	 * format specification.
	 **/
	String format(Evaluator evaluator, Object o)
	{
		return getFormat().format(o);
	}

	public String format(Object o) {
		if (o == Util.nullValue) {
			Format format = getFormat();
			return format.format(null);
		} else if (o instanceof Throwable) {
			return "#ERR: " + o.toString();
		} else if (o instanceof String) {
			return (String) o;
		} else {
			Format format = getFormat();
			return format.format(o);
		}
	}

	/**
	 * <code>CachingCellReader</code> stores the results of cell-requests in a
	 * cache.
	 *
	 * <p>The caching strategy is naive. It caches in a <em>static object</em>, and
	 * never flushes its cache. We should cache sets of members -- say, those with
	 * the same dimensionality -- in the aggregation manager.
	 *
	 * <p>We also ought to be able to control on a per-measure basis whether to do
	 * caching.
	 */
	private static class CachingCellReader implements CellReader {
		private final CellReader cellReader;
		private static final HashMap cellCache = new HashMap();

		CachingCellReader(CellReader cellReader) {
			this.cellReader = cellReader;
		}
		public Object get(Evaluator evaluator) {
			final RolapMember[] currentMembers = ((RolapEvaluator) evaluator).currentMembers;
			final List key = Arrays.asList(currentMembers);
			Object value = cellCache.get(key);
			if (value == null) {
				value = cellReader.get(evaluator);
				if (value != RolapUtil.valueNotReadyException) {
					cellCache.put(key, value);
				}
			}
			return value;
		}
	}

	private Object getExpResultCacheKey(Exp exp) {
		List key = new ArrayList();
		key.add(exp);
		for (int i = 0; i < currentMembers.length; i++) {
			Dimension dim = currentMembers[i].getDimension();
			if (exp.dependsOn(dim))
				key.add(currentMembers[i]);
		}
		return key;
	}

	public Object getCachedResult(Exp exp) {
		Object key = getExpResultCacheKey(exp);
		return expResultCache.get(key);
	}

	public void setCachedResult(Exp exp, Object result) {
		Object key = getExpResultCacheKey(exp);
		expResultCache.put(key, result);
	}

	public void clearExpResultCache() {
		expResultCache.clear();
	}
	
	private boolean nonEmpty;
	public boolean isNonEmpty() {
		return nonEmpty;
	}
	public void setNonEmpty(boolean b) {
		nonEmpty = b;
	}

}

// End RolapEvaluator.java
