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
import mondrian.test.TestContext;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
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
		if (o instanceof Integer) {
			return ((Integer) o).intValue();
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
		if (o instanceof Double) {
			return new BigDecimal(((Double) o).doubleValue());
		} else {
			return (BigDecimal) o;
		}
	}

	static Double getDoubleArg(Evaluator evaluator, Exp[] args, int index) {
		Object o = args[index].evaluateScalar(evaluator);
		if (o instanceof BigDecimal) {
			return new Double(((BigDecimal) o).doubleValue());
		} else if (o instanceof Throwable) {
			return new Double(Double.NaN);
		} else if (o instanceof Util.NullCellValue) {
			return new Double(0);
		} else {
			return (Double) o;
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

	static HashMap evaluateMembers(
			Evaluator evaluator, ExpBase exp, Vector members) {
		//if (exp == null) { //needed?
		//	return evaluateMembers(evaluator.push(), members);
		//}
		//else {			
			Member[] constantTuple = exp.isConstantTuple();
			if (constantTuple == null) {
				return _evaluateMembers(evaluator.push(), exp, members);
			} else {
				// exp is constant -- add it to the context before the loop, rather
				// than at every step
				return evaluateMembers(evaluator.push(constantTuple), members);
			}
		//}
	}

	private static HashMap _evaluateMembers(
			Evaluator evaluator, ExpBase exp, Vector members) {
		HashMap mapMemberToValue = new HashMap();
		for (int i = 0, count = members.size(); i < count; i++) {
			Member member = (Member) members.elementAt(i);
			evaluator.setContext(member);
			Object o = exp.evaluate(evaluator);
			Object result;
			if (o instanceof Member) {
				evaluator.setContext((Member) o);
				result = evaluator.evaluateCurrent();
			} else if (o instanceof Member[]) {
				evaluator.setContext((Member[]) o);
				result = evaluator.evaluateCurrent();
			} else {
				result = o;
			}
			mapMemberToValue.put(member, result);
		}
		return mapMemberToValue;
	}

	static HashMap evaluateMembers(Evaluator evaluator, Vector members) {
		HashMap mapMemberToValue = new HashMap();
		for (int i = 0, count = members.size(); i < count; i++) {
			Member member = (Member) members.elementAt(i);
			evaluator.setContext(member);
			Object result = evaluator.evaluateCurrent();
			mapMemberToValue.put(member, result);
		}
		return mapMemberToValue;
	}

	static void sort(
			Evaluator evaluator, Vector members, ExpBase exp, boolean desc,
			boolean brk) {
		HashMap mapMemberToValue = evaluateMembers(evaluator, exp, members);
		Comparator comparator = new MemberComparator(
				mapMemberToValue, desc, brk);
		sort(comparator, members);
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
			if (o instanceof Double) {
				total += ((Double) o).doubleValue();
			}
		}
		for (int i = 0; i < numMembers; i++) {
			Object o = mapMemberToValue.get(members.elementAt(i));
			if (o instanceof Double) {
				mapMemberToValue.put(members.elementAt(i), new Double(((Double) o).doubleValue() / total * 100));
			}
		}
		
	}

	/**
	 * Handles TopSum, TopPercent, BottomSum, BottomPercent by
	 * evaluating members, sorting appropriately, and returning a 
	 * truncated vector of members
	 */
	static Object topOrBottom (Evaluator evaluator, Vector members, ExpBase exp, boolean isTop, boolean isPercent, double target) {
		HashMap mapMemberToValue = evaluateMembers(evaluator, exp, members);
		Comparator comparator = new MemberComparator(
				mapMemberToValue, isTop, true);
		sort(comparator, members);
		if (isPercent) {
			toPercent(members, mapMemberToValue);
		}
		int numMembers = members.size();
		double runningTotal = 0; int i = 0;
		for (; (i < numMembers) && (runningTotal < target); i++) {
			Object o = mapMemberToValue.get(members.elementAt(i));
			//todo: figure out why we have non-doubles, add error handling	
			if (o instanceof Double) {
				runningTotal += ((Double) o).doubleValue();
			}
		}
		members.setSize(i);
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
			double max = 0.0;
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
			} else if (o instanceof BigDecimal) {
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

	static Vector periodsToDate(
			Evaluator evaluator, Level level, Member member) {
		if (member == null) {
			member = evaluator.getContext(
					level.getHierarchy().getDimension());
		}
		Member[] members = level.getPeriodsToDate(member);
		return toVector(members);
	}

	static class MemberComparator implements Comparator {
		Map mapMemberToValue;
		boolean desc;
		boolean brk;

		MemberComparator(Map mapMemberToValue, boolean desc, boolean brk) {
			this.mapMemberToValue = mapMemberToValue;
			this.desc = desc;
			this.brk = brk;
		}

		// implement Comparator
		public int compare(Object o, Object p) {
			Member member0 = (Member) o,
					member1 = (Member) p;
			int c = compareInternal(member0, member1);
			return desc ? -c : c;
		}

		private int compareInternal(Member member0, Member member1) {
			int c;
			if (!brk) {
				c = member0.compareHierarchically(member1);
				if (c != 0) {
					return c;
				}
			}
			Object value0 = mapMemberToValue.get(member0),
					value1 = mapMemberToValue.get(member1);
			if (value0 == value1) {
				return 0;
			} else if (value0 == Util.nullValue) {
				return 1; // null == +infinity
			} else if (value1 == Util.nullValue) {
				return -1; // null == +infinity
			} else if (value0 instanceof String) {
				return ((String) value0).compareTo((String) value1);
			} else if (value0 instanceof Double) {
				return sign(((Double) value0).doubleValue() - ((Double) value1).doubleValue());
			} else if (value0 instanceof BigDecimal) {
				return ((BigDecimal) value0).compareTo((BigDecimal) value1);
			} else {
				throw getRes().newInternal("cannot compare " + value0);
			}
		}
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
