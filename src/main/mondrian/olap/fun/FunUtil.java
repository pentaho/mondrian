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

	static Hashtable toHashtable(Vector vector) {
		Hashtable hashtable = new Hashtable();
		for (int i = 0, count = vector.size(); i < count; i++) {
			Object o = vector.elementAt(i);
			hashtable.put(o, o);
		}
		return hashtable;
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

	static HashSet toHashSet(Vector v) {
		HashSet set = new HashSet();
		Enumeration e = v.elements();
		while (e.hasMoreElements()) {
			set.add(e.nextElement());
		}
		return set;
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

	static Hashtable evaluateMembers(
			Evaluator evaluator, ExpBase exp, Vector members) {
		Member[] constantTuple = exp.isConstantTuple();
		if (constantTuple == null) {
			return _evaluateMembers(evaluator.push(new Member[0]), exp, members);
		} else {
			// exp is constant -- add it to the context before the loop, rather
			// than at every step
			return evaluateMembers(evaluator.push(constantTuple), members);
		}
	}

	private static Hashtable _evaluateMembers(
			Evaluator evaluator, ExpBase exp, Vector members) {
		Hashtable mapMemberToValue = new Hashtable();
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

	static Hashtable evaluateMembers(Evaluator evaluator, Vector members) {
		Hashtable mapMemberToValue = new Hashtable();
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
		Hashtable mapMemberToValue = evaluateMembers(evaluator, exp, members);
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

	static Object sum(Evaluator evaluator, Vector members, ExpBase exp) {
		// todo: treat constant exps as evaluateMembers() does
		double sum = 0;
		int valueCount = 0, errorCount = 0;
		for (int i = 0, count = members.size(); i < count; i++) {
			Member member = (Member) members.elementAt(i);
			evaluator.setContext(member);
			Object o = exp.evaluateScalar(evaluator);
			if (o == null || o == Util.nullValue) {
			} else if (o instanceof Throwable) {
				// Carry on summing, so that if we are running in a
				// BatchingCellReader, we find out all the dependent cells we
				// need
				errorCount++;
			} else if (o instanceof BigDecimal) {
				valueCount++;
				sum += ((BigDecimal) o).doubleValue();
			} else {
				valueCount++;
				sum += ((Double) o).doubleValue();
			}
		}
		if (errorCount > 0) {
			if (false) {
				return new MondrianEvaluationException(
						errorCount + " error(s) while computing sum");
			}
			return new Double(Double.NaN);
		} else if (valueCount == 0) {
			return Util.nullValue;
		} else {
			return new Double(sum);
		}
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
		Hashtable mapMemberToValue;
		boolean desc;
		boolean brk;

		MemberComparator(
				Hashtable mapMemberToValue, boolean desc, boolean brk) {
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
		Member[] children = {member};
		while (children.length > 0 &&
				children[0].getLevel().getDepth() <
				level.getDepth()) {
			children = member.getCube().getMemberChildren(children);
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
