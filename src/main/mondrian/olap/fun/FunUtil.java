/*
// $Id$
// (C) Copyright 2002 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import mondrian.olap.*;
import mondrian.test.FoodMartTestCase;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * <code>FunUtil</code> contains a set of methods useful within the
 * <code>mondrian.olap.fun</code> package.
 **/
public class FunUtil extends Util {
	static final String nl = System.getProperty("line.separator");

	public static RuntimeException newEvalException(
			FunDef funDef, String message) {
		return new MondrianEvaluationException(message);
	}

	static Exp getArgNoEval(Exp[] args, int index) {
		return args[index];
	}

	static Object getArg(Evaluator evaluator, Exp[] args, int index) {
		return getArg(evaluator, args, index, null);
	}

	static Object getArg(
			Evaluator evaluator, Exp[] args, int index, Object defaultValue) {
		if (index >= args.length) {
			return defaultValue;
		}
		Exp exp = args[index];
		return exp.evaluate(evaluator);
	}

	static String getStringArg(
			Evaluator evaluator, Exp[] args, int index, String defaultValue) {
		return (String) getArg(evaluator, args, index, defaultValue);
	}

	static boolean getBooleanArg(Evaluator evaluator, Exp[] args, int index) {
		Object o = getArg(evaluator, args, index);
		return ((Boolean) o).booleanValue();
	}

	static int getIntArg(Evaluator evaluator, Exp[] args, int index) {
		Object o = args[index].evaluateScalar(evaluator);
		if (o instanceof Number) {
			return ((Number) o).intValue();
		} else {
			// we need to handle String("5.0")
			String s = o.toString();
			double d = Double.valueOf(s).doubleValue();
			return (int) d;
		}
	}

	static BigDecimal getDecimalArg(
			Evaluator evaluator, Exp[] args, int index) {
		Object o = args[index].evaluateScalar(evaluator);
		if (o instanceof BigDecimal) {
			return (BigDecimal) o;
		} else if (o instanceof BigInteger) {
			return new BigDecimal((BigInteger) o);
		} else if (o instanceof Number) {
			return new BigDecimal(((Number) o).doubleValue());
		} else {
			throw Util.newInternal(
					"arg " + o + " cannot be converted to BigDecimal");
		}
	}

	static Double getDoubleArg(Evaluator evaluator, Exp[] args, int index) {
		Object o = args[index].evaluateScalar(evaluator);
		if (o instanceof Double) {
			return (Double) o;
		} else if (o instanceof Number) {
			return new Double(((Number) o).doubleValue());
		} else if (o instanceof Throwable) {
			return new Double(Double.NaN);
		} else if (o instanceof Util.NullCellValue) {
			return new Double(0);
		} else {
			throw Util.newInternal(
					"arg " + o + " cannot be converted to Double");
		}
	}

	static Member getMemberArg(
			Evaluator evaluator, Exp[] args, int index, boolean fail) {
		if (index >= args.length) {
			if (fail) {
				throw Util.getRes().newInternal("missing member argument");
			} else {
				return null;
			}
		}
		Exp arg = args[index];
		Object o = arg.evaluate(evaluator);
        if (true) {
            return (Member) o;
        }
		if (o instanceof Member) {
			return (Member) o;
		} else if (o instanceof Hierarchy) {
			return evaluator.getContext(
					((Hierarchy) o).getDimension());
		} else if (o instanceof Dimension) {
			return evaluator.getContext((Dimension) o);
		} else {
			throw Util.getRes().newInternal("expecting a member, got " + o);
		}
	}

	static Member[] getTupleArg(
			Evaluator evaluator, Exp[] args, int index) {
		Exp arg = args[index];
		Object o = arg.evaluate(evaluator);
		return (Member[]) o;
	}

	static Level getLevelArg(
			Evaluator evaluator, Exp[] args, int index, boolean fail) {
		if (index >= args.length) {
			if (fail) {
				throw Util.getRes().newInternal("missing level argument");
			} else {
				return null;
			}
		}
		Exp arg = args[index];
		Object o = arg.evaluate(evaluator);
		return (Level) o;
	}

	static Hierarchy getHierarchyArg(
			Evaluator evaluator, Exp[] args, int index, boolean fail) {
		if (index >= args.length) {
			if (fail) {
				throw Util.getRes().newInternal("missing hierarchy argument");
			} else {
				return null;
			}
		}
		Exp arg = args[index];
		Object o = arg.evaluate(evaluator);
        if (true) {
            return (Hierarchy) o;
        }
		if (o instanceof Member) {
			return ((Member) o).getHierarchy();
		} else if (o instanceof Level) {
			return ((Level) o).getHierarchy();
		} else if (o instanceof Hierarchy) {
			return (Hierarchy) o;
		} else if (o instanceof Dimension) {
			return ((Dimension) o).getHierarchies()[0];
		} else {
			throw Util.getRes().newInternal("expecting a hierarchy, got " + o);
		}
	}

	static Dimension getDimensionArg(
			Evaluator evaluator, Exp[] args, int index, boolean fail) {
		if (index >= args.length) {
			if (fail) {
				throw Util.getRes().newInternal("missing dimension argument");
			} else {
				return null;
			}
		}
		Exp arg = args[index];
		Object o = arg.evaluate(evaluator);
        if (true) {
            return (Dimension) o;
        }
		if (o instanceof Member) {
			return ((Member) o).getDimension();
		} else if (o instanceof Level) {
			return ((Level) o).getDimension();
		} else if (o instanceof Hierarchy) {
			return ((Hierarchy) o).getDimension();
		} else if (o instanceof Dimension) {
			return (Dimension) o;
		} else {
			throw Util.getRes().newInternal("expecting a dimension, got " + o);
		}
	}

	static Vector toVector(Object[] array) {
		Vector vector = new Vector();
		return addArray(vector, array);
	}

	static Vector addArray(Vector vector, Object[] array) {
		for (int i = 0; i < array.length; i++) {
			vector.addElement(array[i]);
		}
		return vector;
	}

	static HashSet toHashSet(Vector vector) {
		HashSet set = new HashSet();
		for (int i = 0, count = vector.size(); i < count; i++) {
			set.add(vector.elementAt(i));
		}
		return set;
	}

	static Vector addMembers(Vector vector, Hierarchy hierarchy) {
		Level[] levels = hierarchy.getLevels();
		for (int i = 0; i < levels.length; i++) {
			addMembers(vector, levels[i]);
		}
		return vector;
	}

	static Vector addMembers(Vector vector, Level level) {
		Member[] members = level.getMembers();
		return addArray(vector, members);
	}

	/**
	 * Returns whether <code>m0</code> is an ancestor of <code>m1</code>.
	 *
	 * @param strict if true, a member is not an ancestor of itself
	 **/
	static boolean isAncestorOf(Member m0, Member m1, boolean strict) {
		if (strict) {
			if (m1 == null) {
				return false;
			}
			m1 = m1.getParentMember();
		}
		while (m1 != null) {
			if (m1 == m0) {
				return true;
			}
			m1 = m1.getParentMember();
		}
		return false;
	}

	/**
	 * @pre exp != null
	 */
	static HashMap evaluateMembers(
			Evaluator evaluator, ExpBase exp, Vector members, boolean parentsToo) {
		Member[] constantTuple = exp.isConstantTuple();
		if (constantTuple == null) {
			return _evaluateMembers(evaluator.push(), exp, members, parentsToo);
		} else {
			// exp is constant -- add it to the context before the loop, rather
			// than at every step
			return evaluateMembers(evaluator.push(constantTuple), members, parentsToo);
		}
	}

	private static HashMap _evaluateMembers(
			Evaluator evaluator, ExpBase exp, Vector members, boolean parentsToo) {
		HashMap mapMemberToValue = new HashMap();
		for (int i = 0, count = members.size(); i < count; i++) {
			Member member = (Member) members.elementAt(i);
			while (true) {
				evaluator.setContext(member);
				Object result = exp.evaluateScalar(evaluator);
				mapMemberToValue.put(member, result);
				if (!parentsToo) {
					break;
				}
				member = member.getParentMember();
				if (member == null) {
					break;
				}
				if (mapMemberToValue.containsKey(member)) {
					break;
				}
			}
		}
		return mapMemberToValue;
	}

	static HashMap evaluateMembers(Evaluator evaluator, Vector members, boolean parentsToo) {
		HashMap mapMemberToValue = new HashMap();
		for (int i = 0, count = members.size(); i < count; i++) {
			Member member = (Member) members.elementAt(i);
			while (true) {
				evaluator.setContext(member);
				Object result = evaluator.evaluateCurrent();
				mapMemberToValue.put(member, result);
				if (!parentsToo) {
					break;
				}
				member = member.getParentMember();
				if (member == null) {
					break;
				}
				if (mapMemberToValue.containsKey(member)) {
					break;
				}
			}
		}
		return mapMemberToValue;
	}

	static void sort(
			Evaluator evaluator, Vector members, ExpBase exp, boolean desc,
			boolean brk) {
		if (members.isEmpty()) {
			return;
		}
		Object first = members.elementAt(0);
		Comparator comparator;
		if (first instanceof Member) {
			final boolean parentsToo = !brk;
			HashMap mapMemberToValue = evaluateMembers(evaluator, exp, members, parentsToo);
			if (brk) {
				comparator = new BreakMemberComparator(mapMemberToValue);
			} else {
				comparator = new HierarchicalMemberComparator(mapMemberToValue);
			}
		} else {
			Util.assertTrue(first instanceof Member[]);
			final int arity = ((Member[]) first).length;
			if (brk) {
				comparator = new BreakArrayComparator(evaluator, exp, arity);
			} else {
				comparator = new HierarchicalArrayComparator(evaluator, exp, arity);
			}
		}
		if (desc) {
			comparator = new ReverseComparator(comparator);
		}
		sort(comparator, members);
	}

	static int compareValues(Object value0, Object value1) {
		if (value0 == value1) {
			return 0;
		} else if (value0 == Util.nullValue) {
			return 1; // null == +infinity
		} else if (value1 == Util.nullValue) {
			return -1; // null == +infinity
		} else if (value0 instanceof String) {
			return ((String) value0).compareTo((String) value1);
		} else if (value0 instanceof Number) {
			return FunUtil.sign(((Number) value0).doubleValue(), ((Number) value1).doubleValue());
		} else {
			throw Util.newInternal("cannot compare " + value0);
		}
	}

	static void sort(Comparator comparator, Vector vector) {
		Object[] objects = new Object[vector.size()];
		vector.copyInto(objects);
		Arrays.sort(objects, comparator);
		for (int i = 0; i < vector.size(); i++) {
			vector.setElementAt(objects[i], i);
		}
	}

	/**
	 * Turns the mapped values into relative values (percentages) for easy
	 * use by the general topOrBottom function. This might also be a useful
	 * function in itself.
	 */
	static void toPercent (Vector members, HashMap mapMemberToValue) {
		double total = 0;
		int numMembers = members.size();
		for (int i = 0; i < numMembers; i++) {
			Object o = mapMemberToValue.get(members.elementAt(i));
			if (o instanceof Number) {
				total += ((Number) o).doubleValue();
			}
		}
		for (int i = 0; i < numMembers; i++) {
			Object member = members.elementAt(i);
			Object o = mapMemberToValue.get(member);
			if (o instanceof Number) {
				double d = ((Number) o).doubleValue();
				mapMemberToValue.put(member, new Double(d / total * 100));
			}
		}

	}

	/**
	 * Handles TopSum, TopPercent, BottomSum, BottomPercent by
	 * evaluating members, sorting appropriately, and returning a
	 * truncated vector of members
	 */
	static Object topOrBottom (Evaluator evaluator, Vector members, ExpBase exp, boolean isTop, boolean isPercent, double target) {
		final boolean brk = true,
			desc = isTop;
		HashMap mapMemberToValue = evaluateMembers(evaluator, exp, members, false);
		Comparator comparator;
		if (brk) {
			comparator = new BreakMemberComparator(mapMemberToValue);
		} else {
			comparator = new HierarchicalMemberComparator(mapMemberToValue);
		}
		if (isTop) {
			comparator = new ReverseComparator(comparator);
		}
		sort(comparator, members);
		if (isPercent) {
			toPercent(members, mapMemberToValue);
		}
		double runningTotal = 0;
		for (int i = 0, numMembers = members.size(); i < numMembers; i++) {
			if (runningTotal >= target) {
				members.setSize(i);
				break;
			}
			Object o = mapMemberToValue.get(members.elementAt(i));
			if (o instanceof Number) {
				runningTotal += ((Number) o).doubleValue();
			} else if (o instanceof Exception) {
				// ignore the error
			} else {
				throw Util.newInternal("got " + o + " when expecting Number");
			}
		}
		return members;
	}

	static class SetWrapper {
		Vector v = new Vector();
		public int errorCount = 0, nullCount = 0;
	}

	static Object median(Evaluator evaluator, Vector members, ExpBase exp) {
		SetWrapper sw = evaluateSet(evaluator, members, exp);
		if (sw.errorCount > 0) {
			return new Double(Double.NaN);
		} else if (sw.v.size() == 0) {
			return Util.nullValue;
		}
		double[] asArray = new double[sw.v.size()];
		for (int i = 0; i < asArray.length; i++) {
			asArray[i] = ((Double) sw.v.elementAt(i)).doubleValue();
		}
		Arrays.sort(asArray);
		int median = (int) Math.floor(asArray.length / 2);
		return new Double(asArray[median]);
	}

	static Object min(Evaluator evaluator, Vector members, ExpBase exp) {
		SetWrapper sw = evaluateSet(evaluator, members, exp);
		if (sw.errorCount > 0) {
			return new Double(Double.NaN);
		} else if (sw.v.size() == 0) {
			return Util.nullValue;
		}
		else {
			double min = Double.MAX_VALUE;
			for (int i = 0; i < sw.v.size(); i++) {
				double iValue = ((Double) sw.v.elementAt(i)).doubleValue();
				if (iValue < min) { min = iValue; }
			}
			return new Double(min);
		}
	}

	static Object max(Evaluator evaluator, Vector members, ExpBase exp) {
		SetWrapper sw = evaluateSet(evaluator, members, exp);
		if (sw.errorCount > 0) {
			return new Double(Double.NaN);
		} else if (sw.v.size() == 0) {
			return Util.nullValue;
		}
		else {
			double max = Double.MIN_VALUE;
			for (int i = 0; i < sw.v.size(); i++) {
				double iValue = ((Double) sw.v.elementAt(i)).doubleValue();
				if (iValue > max) { max = iValue; }
			}
			return new Double(max);
		}
	}

	static Object avg(Evaluator evaluator, Vector members, ExpBase exp) {
		SetWrapper sw = evaluateSet(evaluator, members, exp);
		if (sw.errorCount > 0) {
			return new Double(Double.NaN);
		} else if (sw.v.size() == 0) {
			return Util.nullValue;
		}
		else {
			double sum = 0.0;
			for (int i = 0; i < sw.v.size(); i++) {
				sum += ((Double) sw.v.elementAt(i)).doubleValue();
			}
			//todo: should look at context and optionally include nulls
			return new Double(sum / sw.v.size());
		}
	}

	static Object sum(Evaluator evaluator, Vector members, ExpBase exp) {
		SetWrapper sw = evaluateSet(evaluator, members, exp);
		if (sw.errorCount > 0) {
			if (false) {
				return new MondrianEvaluationException(
						sw.errorCount + " error(s) while computing sum");
			}
			return new Double(Double.NaN);
		} else if (sw.v.size() == 0) {
			return Util.nullValue;
		}
		else {
			double sum = 0.0;
			for (int i = 0; i < sw.v.size(); i++) {
				sum += ((Double) sw.v.elementAt(i)).doubleValue();
			}
			return new Double(sum);
		}
	}

	/**
	 * Evluates <code>exp</code> over <code>members</code> to generate a
	 * <code>Vector</code> of <code>SetWrapper</code>, which contains a
	 * <code>Double</code> value and meta information, unlike
	 * <code>evaluateMembers</code>, which only produces values
	 */
	static SetWrapper evaluateSet(Evaluator evaluator, Vector members, ExpBase exp) {
		// todo: treat constant exps as evaluateMembers() does
		SetWrapper retval = new SetWrapper();
		for (int i = 0, count = members.size(); i < count; i++) {
			Member member = (Member) members.elementAt(i);
			evaluator.setContext(member);
			Object o = null;
			if (exp != null) {
				o = exp.evaluateScalar(evaluator);
			}
			else { //is this right?
				evaluator.setContext(member);
				o = evaluator.evaluateCurrent();
			}

			if (o == null || o == Util.nullValue) {
				retval.nullCount++;
			} else if (o instanceof Throwable) {
				// Carry on summing, so that if we are running in a
				// BatchingCellReader, we find out all the dependent cells we
				// need
				retval.errorCount++;
			} else if (o instanceof Double) {
				retval.v.add(o);
			} else if (o instanceof Number) {
				retval.v.add(new Double(((BigDecimal) o).doubleValue()));
			} else {
				retval.v.add(o);
			}
		}
		return retval;
	}

	static int sign(double d) {
		return d == 0 ? 0 :
				d < 0 ? -1 :
				1;
	}

	static int sign(double d1, double d2) {
		return d1 == d2 ? 0 :
				d1 < d2 ? -1 :
				1;
	}

	static Vector periodsToDate(
			Evaluator evaluator, Level level, Member member) {
		if (member == null) {
			member = evaluator.getContext(
					level.getHierarchy().getDimension());
		}
		Member[] members = level.getPeriodsToDate(member);
		return toVector(members);
	}

	/**
	 * Helper for <code>OpeningPeriod</code> and <code>ClosingPeriod</code>.
	 */
	static Object openClosingPeriod(FunDef funDef, Member member, Level level) {
		if (member.getHierarchy() != level.getHierarchy()) {
			throw newEvalException(
					funDef,
					"member '" + member +
					"' must be in same hierarchy as level '" + level + "'");
		}
		if (member.getLevel().getDepth() > level.getDepth()) {
			return member.getHierarchy().getNullMember();
		}
		// Expand member to its children, until we get to the right
		// level. We assume that all children are in the same
		// level.
		final Hierarchy hierarchy = member.getHierarchy();
		Member[] children = {member};
		while (children.length > 0 &&
				children[0].getLevel().getDepth() <
				level.getDepth()) {
			children = hierarchy.getChildMembers(children);
		}
		return children[children.length - 1];
	}

	/**
	 * Adds a test case for each method of this object whose signature looks
	 * like 'public void testXxx()'.
	 */
	public void addTests(TestSuite suite) {
		addTests(this, suite);
	}

	/**
	 * Adds a test case for each method in an object whose signature looks
	 * like 'public void testXxx({@link TestCase})'.
	 */
	public static void addTests(Object o, TestSuite suite) {
		String testName = Util.getProperties().getProperty(
				"mondrian.test.Name");
		for (Class clazz = o.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
			Method[] methods = clazz.getDeclaredMethods();
			for (int i = 0; i < methods.length; i++) {
				Method method = methods[i];
				String methodName = method.getName();
				if (methodName.startsWith("test") &&
						Modifier.isPublic(method.getModifiers()) &&
						method.getParameterTypes().length == 1 &&
						TestCase.class.isAssignableFrom(
								method.getParameterTypes()[0]) &&
						method.getReturnType() == Void.TYPE) {
					if (testName != null &&
							methodName.indexOf(testName) < 0) {
						continue;
					}
					suite.addTest(new MethodCallTestCase(
							clazz.getName() + "." + method.getName(), o, method));
				}
			}
		}
	}

	private static class MethodCallTestCase extends FoodMartTestCase {
		Object o;
		Object[] args;
		Method method;

		MethodCallTestCase(String name, Object o, Method method) {
			super(name);
			this.o = o;
			this.args = new Object[]{this};
			this.method = method;
		}

		protected void runTest() throws Throwable {
			method.invoke(o, args);
		}
	}
}

abstract class MemberComparator implements Comparator {
	Map mapMemberToValue;

	MemberComparator(Map mapMemberToValue) {
		this.mapMemberToValue = mapMemberToValue;
	}

	// implement Comparator
	public int compare(Object o1, Object o2) {
		Member m1 = (Member) o1,
				m2 = (Member) o2;
		int c = compareInternal(m1, m2);
		if (false) {
			System.out.println(
					"compare " +
					m1.getUniqueName() + "(" + mapMemberToValue.get(m1) + "), " +
					m2.getUniqueName() + "(" + mapMemberToValue.get(m2) + ")" +
					" yields " + c);
		}
		return c;
	}

	protected abstract int compareInternal(Member m1, Member m2);

	protected int compareByValue(Member m1, Member m2) {
		Object value0 = mapMemberToValue.get(m1),
				value1 = mapMemberToValue.get(m2);
		return FunUtil.compareValues(value0, value1);
	}

	protected int compareHierarchicallyButSiblingsByValue(Member m1, Member m2) {
		if (m1 == m2) {
			return 0;
		}
		while (true) {
			int levelDepth1 = m1.getLevel().getDepth(),
				levelDepth2 = m2.getLevel().getDepth();
			if (levelDepth1 < levelDepth2) {
				m2 = m2.getParentMember();
				if (m1 == m2) {
					return -1;
				}
			} else if (levelDepth1 > levelDepth2) {
				m1 = m1.getParentMember();
				if (m1 == m2) {
					return 1;
				}
			} else {
				Member prev1 = m1, prev2 = m2;
				m1 = m1.getParentMember();
				m2 = m2.getParentMember();
				if (m1 == m2) {
					// including case where both parents are null
					return compareByValue(prev1, prev2);
				}
			}
		}
	}
}

class HierarchicalMemberComparator extends MemberComparator {
	HierarchicalMemberComparator(Map mapMemberToValue) {
		super(mapMemberToValue);
	}

	protected int compareInternal(Member m1, Member m2) {
		return compareHierarchicallyButSiblingsByValue(m1, m2);
	}
}

class BreakMemberComparator extends MemberComparator {
	BreakMemberComparator(Map mapMemberToValue) {
		super(mapMemberToValue);
	}

	protected int compareInternal(Member m1, Member m2) {
		return compareByValue(m1, m2);
	}
}

/**
 * Compares tuples, which are represented as arrays of {@link Member}s.
 */
abstract class ArrayComparator implements Comparator {
	Evaluator evaluator;
	Exp exp;
	int arity;
	ArrayComparator(Evaluator evaluator, Exp exp, int arity) {
		this.evaluator = evaluator;
		this.exp = exp;
		this.arity = arity;
	}

	private static String toString(Member[] a) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < a.length; i++) {
			Member member = a[i];
			if (i > 0) {
				sb.append(",");
			}
			sb.append(member.getUniqueName());
		}
		return sb.toString();
	}
	public int compare(Object o1, Object o2) {
		final Member[] a1 = (Member[]) o1;
		final Member[] a2 = (Member[]) o2;
		final int c = compare(a1, a2);
		if (false) {
			System.out.println(
					"compare {" + toString(a1)+ "}, {" + toString(a2) + "}" +
					" yields " + c);
		}
		return c;
	}
	protected abstract int compare(Member[] a1, Member[] a2);
}

class HierarchicalArrayComparator extends ArrayComparator {
	HierarchicalArrayComparator(Evaluator evaluator, Exp exp, int arity) {
		super(evaluator, exp, arity);
	}
	protected int compare(Member[] a1, Member[] a2) {
		for (int i = 0; i < arity; i++) {
			Member m1 = a1[i],
					m2 = a2[i];
			int c = compareHierarchicallyButSiblingsByValue(m1, m2);
			if (c != 0) {
				return c;
			}
			// compareHierarchicallyButSiblingsByValue imposes a total order
			Util.assertTrue(m1 == m2);
			evaluator.setContext(m1);
		}
		return 0;
	}
	protected int compareHierarchicallyButSiblingsByValue(Member m1, Member m2) {
		if (m1 == m2) {
			return 0;
		}
		while (true) {
			int levelDepth1 = m1.getLevel().getDepth(),
				levelDepth2 = m2.getLevel().getDepth();
			if (levelDepth1 < levelDepth2) {
				m2 = m2.getParentMember();
				if (m1 == m2) {
					return -1;
				}
			} else if (levelDepth1 > levelDepth2) {
				m1 = m1.getParentMember();
				if (m1 == m2) {
					return 1;
				}
			} else {
				Member prev1 = m1, prev2 = m2;
				m1 = m1.getParentMember();
				m2 = m2.getParentMember();
				if (m1 == m2) {
					// including case where both parents are null
					int c = compareByValue(prev1, prev2);
					if (c != 0) {
						return c;
					}
					// todo: use ordinal not caption
					return FunUtil.compareValues(prev1.getCaption(), prev2.getCaption());
				}
			}
		}
	}
	private int compareByValue(Member m1, Member m2) {
		int c;
		Member old = evaluator.setContext(m1);
		Object v1 = exp.evaluateScalar(evaluator);
		evaluator.setContext(m2);
		Object v2 = exp.evaluateScalar(evaluator);
		// important to restore the evaluator state -- and this is faster
		// than calling evaluator.push()
		evaluator.setContext(old);
		c = FunUtil.compareValues(v1, v2);
		return c;
	}
}

class BreakArrayComparator extends ArrayComparator {
	BreakArrayComparator(Evaluator evaluator, Exp exp, int arity) {
		super(evaluator, exp, arity);
	}

	protected int compare(Member[] a1, Member[] a2) {
		evaluator.setContext(a1);
		Object v1 = exp.evaluateScalar(evaluator);
		evaluator.setContext(a2);
		Object v2 = exp.evaluateScalar(evaluator);
		return FunUtil.compareValues(v1, v2);
	}
}

class ReverseComparator implements Comparator {
	Comparator comparator;
	ReverseComparator(Comparator comparator) {
		this.comparator = comparator;
	}

	public int compare(Object o1, Object o2) {
		int c = comparator.compare(o1, o2);
		return -c;
	}
}

// End FunUtil.java
