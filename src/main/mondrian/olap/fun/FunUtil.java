/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.Set;

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
		return getArgNoEval(args, index, null);
	}

	static Exp getArgNoEval(Exp[] args, int index, Exp defaultValue) {
		if (index >= args.length)
			return defaultValue;
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

	/** Returns an argument whose value is a literal. Unlike the other
	 * <code>get<i>Xxx</i>Arg</code> methods, an evalutor is not required,
	 * and hence this can be called at resolve-time. */
	static String getLiteralArg(Exp[] args, int i, String defaultValue, String[] allowedValues, FunDef funDef) {
		if (i >= args.length) {
			if (defaultValue == null) {
				throw newEvalException(funDef, "Required argument is missing");
			} else {
				return defaultValue;
			}
		}
		Exp arg = args[i];
		if (!(arg instanceof Literal) ||
				arg.getType() != Category.Symbol) {
			throw newEvalException(funDef, "Expected a symbol, found '" + arg + "'");
		}
		String s = (String) ((Literal) arg).getValue();
		StringBuffer sb = new StringBuffer();
		for (int j = 0; j < allowedValues.length; j++) {
			String allowedValue = allowedValues[j];
			if (allowedValue.equalsIgnoreCase(s)) {
				return allowedValue;
			}
			if (j > 0) {
				sb.append(", ");
			}
			sb.append(allowedValue);
		}
		throw newEvalException(funDef, "Allowed values are: {" + sb + "}");
	}

	/** Returns the ordinal of a literal argument. If the argument does not
	 * belong to the supplied enumeration, returns -1. */
	static int getLiteralArg(Exp[] args, int i, int defaultValue, EnumeratedValues allowedValues, FunDef funDef) {
		final String literal = getLiteralArg(args, i, allowedValues.getName(defaultValue), allowedValues.getNames(), funDef);
		if (literal == null) {
			return -1;
		}
		return allowedValues.getOrdinal(literal);
	}

	static boolean getBooleanArg(Evaluator evaluator, Exp[] args, int index) {
		Object o = getArg(evaluator, args, index);
		return ((Boolean) o).booleanValue();
	}

	static int getIntArg(Evaluator evaluator, Exp[] args, int index) {
		Object o = getScalarArg(evaluator, args, index);
		if (o instanceof Number) {
			return ((Number) o).intValue();
		} else if (o instanceof RuntimeException) {
            return 0;
        } else {
			// we need to handle String("5.0")
			String s = o.toString();
			double d = Double.valueOf(s).doubleValue();
			return (int) d;
		}
	}

	static Object getScalarArg(Evaluator evaluator, Exp[] args, int index) {
		return args[index].evaluateScalar(evaluator);
	}

	static BigDecimal getDecimalArg(Evaluator evaluator, Exp[] args, int index) {
		Object o = getScalarArg(evaluator, args, index);
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


	private static final Double nullValue = new Double(0);

	static Double getDoubleArg(Evaluator evaluator, Exp[] args, int index) {
		return getDoubleArg(evaluator, args, index, nullValue);
	}

	static Double getDoubleArg(Evaluator evaluator, Exp[] args, int index, Double nullValue) {
		Object o = getScalarArg(evaluator, args, index);
		if (o instanceof Double) {
			return (Double) o;
		} else if (o instanceof Number) {
			return new Double(((Number) o).doubleValue());
		} else if (o instanceof Throwable) {
			return new Double(Double.NaN);
		} else if (o == null || o == Util.nullValue) {
			return nullValue;
		} else {
			throw Util.newInternal("arg " + o + " cannot be converted to Double");
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

	/**
	 * Throws an error if the expressions don't have the same hierarchy.
	 * @param left
	 * @param right
	 * @throws MondrianEvaluationException if expressions don't have the same
	 *     hierarchy
	 */
	static void checkCompatible(Exp left, Exp right, FunDef funDef) {
		final Hierarchy hierarchy = left.getHierarchy();
		final Hierarchy hierarchy2 = right.getHierarchy();
		if (hierarchy != hierarchy2 && hierarchy != null && hierarchy2 != null) {
			throw newEvalException(funDef, "Expressions must have the same hierarchy");
		}
	}

	/** @deprecated */
	static SchemaReader getSchemaReader() {
		return null;
	}

	/** Adds every element of <code>right</code> to <code>left</code>. **/
	static void add(Vector left, Vector right) {
		if (right == null) {
			return;
		}
		for (int i = 0, n = right.size(); i < n; i++) {
			final Object o = right.elementAt(i);
			left.addElement(o);
		}
	}

	/** Adds every element of <code>right</code> which is not in <code>set</code>
	 * to both <code>set</code> and <code>left</code>. **/
	static void addUnique(List left, List right, Set set) {
		if (right == null) {
			return;
		}
		for (int i = 0, n = right.size(); i < n; i++) {
			Object o = right.get(i),
					p = o;
			if (o instanceof Object[]) {
				p = new ArrayHolder((Object[]) o);
			}
			if (set.add(p)) {
				left.add(o);
			}
		}
	}

	static Boolean toBoolean(boolean b) {
		return b ? Boolean.TRUE : Boolean.FALSE;
	}

	static HashSet toHashSet(Vector vector) {
		HashSet set = new HashSet();
		for (int i = 0, count = vector.size(); i < count; i++) {
			set.add(vector.elementAt(i));
		}
		return set;
	}

	static List addMembers(SchemaReader schemaReader, List members, Hierarchy hierarchy) {
		Level[] levels = schemaReader.getHierarchyLevels(hierarchy); // only accessible levels
		for (int i = 0; i < levels.length; i++) {
			addMembers(schemaReader, members, levels[i]);
		}
		return members;
	}

	static List addMembers(SchemaReader schemaReader, List members, Level level) {
		Member[] levelMembers = schemaReader.getLevelMembers(level);
		addAll(members, levelMembers);
		return members;
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
			Evaluator evaluator, ExpBase exp, List members, boolean parentsToo) {
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
			Evaluator evaluator, ExpBase exp, List members, boolean parentsToo) {
		HashMap mapMemberToValue = new HashMap();
		for (int i = 0, count = members.size(); i < count; i++) {
			Member member = (Member) members.get(i);
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

	static HashMap evaluateMembers(Evaluator evaluator, List members, boolean parentsToo) {
		HashMap mapMemberToValue = new HashMap();
		for (int i = 0, count = members.size(); i < count; i++) {
			Member member = (Member) members.get(i);
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
			Evaluator evaluator, List members, ExpBase exp, boolean desc,
			boolean brk) {
		if (members.isEmpty()) {
			return;
		}
		Object first = members.get(0);
		Comparator comparator;
		if (first instanceof Member) {
			final boolean parentsToo = !brk;
			HashMap mapMemberToValue = evaluateMembers(evaluator, exp, members, parentsToo);
			if (brk) {
				comparator = new BreakMemberComparator(mapMemberToValue, desc);
			} else {
				comparator = new HierarchicalMemberComparator(mapMemberToValue, desc);
			}
		} else {
			Util.assertTrue(first instanceof Member[]);
			final int arity = ((Member[]) first).length;
			if (brk) {
				comparator = new BreakArrayComparator(evaluator, exp, arity);
				if (desc) {
					comparator = new ReverseComparator(comparator);
				}
			} else {
				comparator = new HierarchicalArrayComparator(evaluator, exp, arity, desc);
			}
		}
		Collections.sort(members, comparator);
	}

	static void hierarchize(List members, boolean post) {
		if (members.isEmpty()) {
			return;
		}
		Object first = members.get(0);
		Comparator comparator;
		if (first instanceof Member) {
			comparator = new HierarchizeComparator(post);
		} else {
			Util.assertTrue(first instanceof Member[]);
			final int arity = ((Member[]) first).length;
			comparator = new HierarchizeArrayComparator(arity, post);
		}
		Collections.sort(members, comparator);
	}

	static int compareValues(Object value0, Object value1) {
		if (value0 == value1)
			return 0;
		// null is less than anything else
		if (value0 == null)
			return -1;
		if (value1 == null)
			return 1;
		if (value0 instanceof RuntimeException ||
			value1 instanceof RuntimeException) {
			// one of the values is not in cache; continue as best as we can
			return 0;
		} else if (value0 == Util.nullValue) {
			return -1; // null == -infinity
		} else if (value1 == Util.nullValue) {
			return 1; // null == -infinity
		} else if (value0 instanceof String) {
			return ((String) value0).compareTo((String) value1);
		} else if (value0 instanceof Number) {
			return FunUtil.sign(((Number) value0).doubleValue(), ((Number) value1).doubleValue());
		} else {
			throw Util.newInternal("cannot compare " + value0);
		}
	}

	/**
	 * Turns the mapped values into relative values (percentages) for easy
	 * use by the general topOrBottom function. This might also be a useful
	 * function in itself.
	 */
	static void toPercent (List members, HashMap mapMemberToValue) {
		double total = 0;
		int numMembers = members.size();
		for (int i = 0; i < numMembers; i++) {
			Object o = mapMemberToValue.get(members.get(i));
			if (o instanceof Number) {
				total += ((Number) o).doubleValue();
			}
		}
		for (int i = 0; i < numMembers; i++) {
			Object member = members.get(i);
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
	static Object topOrBottom (Evaluator evaluator, List members, ExpBase exp, boolean isTop, boolean isPercent, double target) {
		HashMap mapMemberToValue = evaluateMembers(evaluator, exp, members, false);
		Comparator comparator = new BreakMemberComparator(mapMemberToValue, isTop);
		Collections.sort(members, comparator);
		if (isPercent) {
			toPercent(members, mapMemberToValue);
		}
		double runningTotal = 0;
		for (int i = 0, numMembers = members.size(); i < numMembers; i++) {
			if (runningTotal >= target) {
				members = members.subList(0, i);
				break;
			}
			Object o = mapMemberToValue.get(members.get(i));
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
		ArrayList v = new ArrayList();
		public int errorCount = 0, nullCount = 0;

		//private double avg = Double.NaN;
		//todo: parameterize inclusion of nulls
		//by making this a method of the SetWrapper, we can cache the result
		//this allows its reuse in Correlation
//		public double getAverage() {
//			if (avg == Double.NaN) {
//				double sum = 0.0;
//				for (int i = 0; i < resolvers.size(); i++) {
//					sum += ((Double) resolvers.elementAt(i)).doubleValue();
//				}
//				//todo: should look at context and optionally include nulls
//				avg = sum / resolvers.size();
//			}
//			return avg;
//		}
	}

	static Object median(Evaluator evaluator, List members, ExpBase exp) {
		SetWrapper sw = evaluateSet(evaluator, members, exp);
		if (sw.errorCount > 0) {
			return new Double(Double.NaN);
		} else if (sw.v.size() == 0) {
			return Util.nullValue;
		}
		double[] asArray = new double[sw.v.size()];
		for (int i = 0; i < asArray.length; i++) {
			asArray[i] = ((Double) sw.v.get(i)).doubleValue();
		}
		Arrays.sort(asArray);
		int median = (int) Math.floor(asArray.length / 2);
		return new Double(asArray[median]);
	}

    /**
     * Returns the member which lies upon a particular quartile according to a
     * given expression.
     *
     * @param evaluator Evaluator
     * @param members List of members
     * @param exp Expression to rank members
     * @param range Quartile (1, 2 or 3)
     *
     * @pre range >= 1 && range <= 3
     */
    static Object quartile(Evaluator evaluator, List members, ExpBase exp, int range) {
        Util.assertPrecondition(range >= 1 && range <= 3, "range >= 1 && range <= 3");
        SetWrapper sw = evaluateSet(evaluator, members, exp);
        if (sw.errorCount > 0) {
            return new Double(Double.NaN);
        } else if (sw.v.size() == 0) {
            return Util.nullValue;
        }
        double[] asArray = new double[sw.v.size()];
        for (int i = 0; i < asArray.length; i++) {
            asArray[i] = ((Double) sw.v.get(i)).doubleValue();
        }
        Arrays.sort(asArray);
        // get a quartile, median is a second q
        double dm = (asArray.length * range) / 4;
        int median = (int) Math.floor(dm);
        if (dm == median && median < asArray.length - 1) {
            //have more elements
            return new Double((asArray[median] + asArray[median+1])/2);
        } else {
            return new Double(asArray[median]);
        }
    }

	public static Object min(Evaluator evaluator, List members, Exp exp) {
		SetWrapper sw = evaluateSet(evaluator, members, (ExpBase) exp);
		if (sw.errorCount > 0) {
			return new Double(Double.NaN);
		} else if (sw.v.size() == 0) {
			return Util.nullValue;
		}
		else {
			double min = Double.MAX_VALUE;
			for (int i = 0; i < sw.v.size(); i++) {
				double iValue = ((Double) sw.v.get(i)).doubleValue();
				if (iValue < min) { min = iValue; }
			}
			return new Double(min);
		}
	}

	public static Object max(Evaluator evaluator, List members, Exp exp) {
		SetWrapper sw = evaluateSet(evaluator, members, (ExpBase) exp);
		if (sw.errorCount > 0) {
			return new Double(Double.NaN);
		} else if (sw.v.size() == 0) {
			return Util.nullValue;
		}
		else {
			double max = Double.MIN_VALUE;
			for (int i = 0; i < sw.v.size(); i++) {
				double iValue = ((Double) sw.v.get(i)).doubleValue();
				if (iValue > max) { max = iValue; }
			}
			return new Double(max);
		}
	}

	static Object var(Evaluator evaluator, List members, ExpBase exp, boolean biased) {
		SetWrapper sw = evaluateSet(evaluator, members, exp);
		return _var(sw, biased);
	}

	private static Object _var(SetWrapper sw, boolean biased) {
		if (sw.errorCount > 0) {
			return new Double(Double.NaN);
		} else if (sw.v.size() == 0) {
			return Util.nullValue;
		}
		else {
			double stdev = 0.0;
			double avg = _avg(sw);
			for (int i = 0; i < sw.v.size(); i++) {
				stdev += Math.pow((((Double) sw.v.get(i)).doubleValue() - avg),2);
			}
			int n = sw.v.size();
			if (!biased) { n--; }
			return new Double(stdev / n);
		}
	}

	static Object correlation(Evaluator evaluator, List members, ExpBase exp1, ExpBase exp2) {
		SetWrapper sw1 = evaluateSet(evaluator, members, exp1);
		SetWrapper sw2 = evaluateSet(evaluator, members, exp2);
		Object covar = _covariance(sw1, sw2, false);
		Object var1 = _var(sw1, false); //this should be false, yes?
		Object var2 = _var(sw2, false);
		if ((covar instanceof Double) && (var1 instanceof Double) && (var2 instanceof Double)) {
			return new Double(((Double) covar).doubleValue() /
				Math.sqrt(((Double) var1).doubleValue() * ((Double) var2).doubleValue()));
		}
		else {
			return Util.nullValue;
		}
	}

	static Object covariance(Evaluator evaluator, List members, ExpBase exp1, ExpBase exp2, boolean biased) {
		SetWrapper sw1 = evaluateSet(evaluator.push(), members, exp1);
		SetWrapper sw2 = evaluateSet(evaluator.push(), members, exp2);
		//todo: because evaluateSet does not add nulls to the SetWrapper, this solution may
		//lead to mismatched vectors and is therefore not robust
		return _covariance(sw1, sw2, biased);
	}


	private static Object _covariance(SetWrapper sw1, SetWrapper sw2, boolean biased) {
		if (sw1.v.size() != sw2.v.size()) {
			return Util.nullValue;
		}
		double avg1 = _avg(sw1);
		double avg2 = _avg(sw2);
		double covar = 0.0;
		for (int i = 0; i < sw1.v.size(); i++) {
			//all of this casting seems inefficient - can we make SetWrapper contain an array of double instead?
			double diff1 = (((Double) sw1.v.get(i)).doubleValue() - avg1);
			double diff2 = (((Double) sw2.v.get(i)).doubleValue() - avg2);
			covar += (diff1 * diff2);
		}
		int n = sw1.v.size();
		if (!biased) { n--; }
		return new Double(covar / n);
	}

	static Object stdev(Evaluator evaluator, List members, ExpBase exp, boolean biased) {
		Object o = var(evaluator, members, exp, biased);
		if (o instanceof Double) {
			return new Double(Math.sqrt(((Double) o).doubleValue()));
		} else {
			return o;
		}
	}

	public static Object avg(Evaluator evaluator, List members, Exp exp) {
		SetWrapper sw = evaluateSet(evaluator, members, (ExpBase) exp);
		if (sw.errorCount > 0) {
			return new Double(Double.NaN);
		} else if (sw.v.size() == 0) {
			return Util.nullValue;
		}
		else {
			return new Double(_avg(sw));
		}
	}

	//todo: parameterize inclusion of nulls
	//also, maybe make _avg a method of setwrapper, so we can cache the result (i.e. for correl)
	private static double _avg(SetWrapper sw) {
		double sum = 0.0;
		for (int i = 0; i < sw.v.size(); i++) {
			sum += ((Double) sw.v.get(i)).doubleValue();
		}
		//todo: should look at context and optionally include nulls
		return sum / sw.v.size();
	}

	public static Object sum(Evaluator evaluator, List members, Exp exp) {
		SetWrapper sw = evaluateSet(evaluator, members, (ExpBase) exp);
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
				sum += ((Double) sw.v.get(i)).doubleValue();
			}
			return new Double(sum);
		}
	}

	public static Object count(Evaluator evaluator, List members, boolean includeEmpty) {
		if (includeEmpty) {
			return new Double(members.size());
		} else {
			int retval = 0;
			for (int i = 0; i < members.size(); i++) {
				final Object member = members.get(i);
                if (member instanceof Member) {
                    evaluator.setContext((Member) member);
                } else {
                    evaluator.setContext((Member[]) member);
                }
                Object o = evaluator.evaluateCurrent();
				if (o != Util.nullValue && o != null) {
					retval++;
				}
			}
			return new Double(retval);
		}
	}

	/**
	 * Evaluates <code>exp</code> (if defined) over <code>members</code> to
	 * generate a <code>Vector</code> of <code>SetWrapper</code>, which contains
	 * a <code>Double</code> value and meta information, unlike
	 * <code>evaluateMembers</code>, which only produces values
	 *
	 * @pre exp != null
	 */
	static SetWrapper evaluateSet(Evaluator evaluator, List members, ExpBase exp) {
		Util.assertPrecondition(exp != null, "exp != null");
		// todo: treat constant exps as evaluateMembers() does
		SetWrapper retval = new SetWrapper();
		for (Iterator it = members.iterator(); it.hasNext();) {
			Object obj = it.next();
			if (obj instanceof Member[])
				evaluator.setContext((Member[])obj);
			else
				evaluator.setContext((Member)obj);
			Object o = exp.evaluateScalar(evaluator);
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
				retval.v.add(new Double(((Number) o).doubleValue()));
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

	static List periodsToDate(
			Evaluator evaluator, Level level, Member member) {
		if (member == null) {
			member = evaluator.getContext(
					level.getHierarchy().getDimension());
		}
		Member m = member;
		while (m != null) {
			if (m.getLevel() == level) {
				break;
			}
			m = m.getParentMember();
		}
		// If m == null, then "level" was lower than member's level.
		// periodsToDate( [Time].[Quarter], [Time].[1997] is valid,
		//  but will return an empty List
		ArrayList members = new ArrayList();
		if (m != null)
			evaluator.getSchemaReader().getMemberRange(level, m, member, members);
		return members;
	}

	static List memberRange(Evaluator evaluator, Member startMember, Member endMember) {
		final Level level = startMember.getLevel();
		assertTrue(level == endMember.getLevel());
		ArrayList members = new ArrayList();
		evaluator.getSchemaReader().getMemberRange(level, startMember, endMember, members);
		if (members.isEmpty()) {
			// The result is empty, so maybe the members are reversed. This is
			// cheaper than comparing the members before we call getMemberRange.
			evaluator.getSchemaReader().getMemberRange(level, endMember, startMember, members);
		}
		return members;
	}

	/**
	 * Helper for <code>OpeningPeriod</code> and <code>ClosingPeriod</code>.
	 */
	static Object openClosingPeriod(Evaluator evaluator, FunDef funDef, Member member, Level level) {
        if (level == null) {
            return member.getHierarchy().getNullMember();
        }
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
				children[0].getLevel().getDepth() < level.getDepth()) {
			children = evaluator.getSchemaReader().getMemberChildren(children);
		}
		return children[children.length - 1];
	}

	static boolean equals(Member m1, Member m2) {
		return m1 == null ?
				m2 == null :
				m1.equals(m2);
	}

	static int compareHierarchically(Member m1, Member m2, boolean post) {
		if (equals(m1, m2)) {
			return 0;
		}
		while (true) {
			int levelDepth1 = m1.getLevel().getDepth(),
					levelDepth2 = m2.getLevel().getDepth();
			if (levelDepth1 < levelDepth2) {
				m2 = m2.getParentMember();
				if (equals(m1, m2)) {
					return post ? 1 : -1;
				}
			} else if (levelDepth1 > levelDepth2) {
				m1 = m1.getParentMember();
				if (equals(m1, m2)) {
					return post ? -1 : 1;
				}
			} else {
				Member prev1 = m1, prev2 = m2;
				m1 = m1.getParentMember();
				m2 = m2.getParentMember();
				if (equals(m1, m2)) {
					return prev1.compareTo(prev2);
				}
			}
		}
	}
	/**
	 * Adds a test case for each method of this object whose signature looks
	 * like 'public void testXxx()'.
	 */
//	public void addTests(TestSuite suite, Pattern pattern) {
//		addTests(this, suite, pattern);
//	}

//	/**
//	 * Adds a test case for each method in an object whose signature looks
//	 * like 'public void testXxx({@link TestCase})'.
//	 */
//	public static void addTests(Object o, TestSuite suite, Pattern pattern) {
//		for (Class clazz = o.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
//			Method[] methods = clazz.getDeclaredMethods();
//			for (int i = 0; i < methods.length; i++) {
//				Method method = methods[i];
//				String methodName = method.getName();
//				if (methodName.startsWith("test") &&
//						Modifier.isPublic(method.getModifiers()) &&
//						method.getParameterTypes().length == 1 &&
//						TestCase.class.isAssignableFrom(
//								method.getParameterTypes()[0]) &&
//						method.getReturnType() == Void.TYPE) {
//                    String fullMethodName = clazz.getName() + "." + method.getName();
//                    if (pattern == null || pattern.matcher(fullMethodName).matches()) {
//                        suite.addTest(new MethodCallTestCase(
//                            fullMethodName, o, method));
//                    }
//				}
//			}
//		}
//	}

}

abstract class MemberComparator implements Comparator {
	Map mapMemberToValue;
	private boolean desc;

	MemberComparator(Map mapMemberToValue, boolean desc) {
		this.mapMemberToValue = mapMemberToValue;
		this.desc = desc;
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
		Object value1 = mapMemberToValue.get(m1),
				value2 = mapMemberToValue.get(m2);
		final int c = FunUtil.compareValues(value1, value2);
		return desc ? -c : c;
	}

	protected int compareHierarchicallyButSiblingsByValue(Member m1, Member m2) {
		if (FunUtil.equals(m1, m2)) {
			return 0;
		}
		while (true) {
			int levelDepth1 = m1.getLevel().getDepth(),
					levelDepth2 = m2.getLevel().getDepth();
			if (levelDepth1 < levelDepth2) {
				m2 = m2.getParentMember();
				if (FunUtil.equals(m1, m2)) {
					return -1;
				}
			} else if (levelDepth1 > levelDepth2) {
				m1 = m1.getParentMember();
				if (FunUtil.equals(m1, m2)) {
					return 1;
				}
			} else {
				Member prev1 = m1, prev2 = m2;
				m1 = m1.getParentMember();
				m2 = m2.getParentMember();
				if (FunUtil.equals(m1, m2)) {
					// including case where both parents are null
					return compareByValue(prev1, prev2);
				}
			}
		}
	}
}

class HierarchicalMemberComparator extends MemberComparator {
	HierarchicalMemberComparator(Map mapMemberToValue, boolean desc) {
		super(mapMemberToValue, desc);
	}

	protected int compareInternal(Member m1, Member m2) {
		return compareHierarchicallyButSiblingsByValue(m1, m2);
	}
}

class BreakMemberComparator extends MemberComparator {
	BreakMemberComparator(Map mapMemberToValue, boolean desc) {
		super(mapMemberToValue, desc);
	}

	protected int compareInternal(Member m1, Member m2) {
		return compareByValue(m1, m2);
	}
}

/**
 * Compares tuples, which are represented as arrays of {@link Member}s.
 */
abstract class ArrayComparator implements Comparator {
	int arity;

	ArrayComparator(int arity) {
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

/**
 * Extension to {@link ArrayComparator} which compares tuples by evaluating an
 * expression.
 */
abstract class ArrayExpComparator extends ArrayComparator {
	Evaluator evaluator;
	Exp exp;

	ArrayExpComparator(Evaluator evaluator, Exp exp, int arity) {
		super(arity);
		this.evaluator = evaluator;
		this.exp = exp;
	}

}

class HierarchicalArrayComparator extends ArrayExpComparator {
	private boolean desc;

	HierarchicalArrayComparator(Evaluator evaluator, Exp exp, int arity, boolean desc) {
		super(evaluator, exp, arity);
		this.desc = desc;
	}
	protected int compare(Member[] a1, Member[] a2) {
		int c = 0;
		evaluator = evaluator.push();
		for (int i = 0; i < arity; i++) {
			Member m1 = a1[i],
					m2 = a2[i];
			c = compareHierarchicallyButSiblingsByValue(m1, m2);
			if (c != 0) {
				break;
			}
			// compareHierarchicallyButSiblingsByValue imposes a total order
			//Util.assertTrue(m1 == m2);
      Util.assertTrue(m1.equals(m2));
			evaluator.setContext(m1);
		}
		evaluator = evaluator.pop();
		return c;
	}
	protected int compareHierarchicallyButSiblingsByValue(Member m1, Member m2) {
		if (FunUtil.equals(m1, m2)) {
			return 0;
		}
		while (true) {
			int levelDepth1 = m1.getLevel().getDepth(),
					levelDepth2 = m2.getLevel().getDepth();
			if (levelDepth1 < levelDepth2) {
				m2 = m2.getParentMember();
				if (FunUtil.equals(m1, m2)) {
					return -1;
				}
			} else if (levelDepth1 > levelDepth2) {
				m1 = m1.getParentMember();
				if (FunUtil.equals(m1, m2)) {
					return 1;
				}
			} else {
				Member prev1 = m1, prev2 = m2;
				m1 = m1.getParentMember();
				m2 = m2.getParentMember();
				if (FunUtil.equals(m1, m2)) {
					// including case where both parents are null
					int c = compareByValue(prev1, prev2);
					if (c == 0) {
						c = prev1.compareTo(prev2);
					}
					return desc ? -c : c;
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

class BreakArrayComparator extends ArrayExpComparator {
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

/**
 * Compares arrays of {@link Member}s so as to convert them into hierarchical
 * order. Applies lexicographic order to the array.
 */
class HierarchizeArrayComparator extends ArrayComparator {
	private boolean post;

	HierarchizeArrayComparator(int arity, boolean post) {
		super(arity);
		this.post = post;
	}

	protected int compare(Member[] a1, Member[] a2) {
		for (int i = 0; i < arity; i++) {
			Member m1 = a1[i],
					m2 = a2[i];
			int c = FunUtil.compareHierarchically(m1, m2, post);
			if (c != 0) {
				return c;
			}
			// compareHierarchically imposes a total order
			//Util.assertTrue(m1 == m2);
      Util.assertTrue(m1.equals(m2));
		}
		return 0;
	}
}

/**
 * Compares {@link Member}s so as to arrage them in prefix or postfix
 * hierarchical order.
 */
class HierarchizeComparator implements Comparator {
	private boolean post;

	HierarchizeComparator(boolean post) {
		this.post = post;
	}
	public int compare(Object o1, Object o2) {
		return FunUtil.compareHierarchically((Member) o1, (Member) o2, post);
	}
}

/**
 * Reverses the order of a {@link Comparator}.
 */
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

/**
 * Holds an array, so that {@link #equals} and {@link #hashCode} work.
 */
class ArrayHolder {
	private Object[] a;

	ArrayHolder(Object[] a) {
		this.a = a;
	}

	public int hashCode() {
		int h = 0;
		for (int i = 0; i < a.length; i++) {
			Object o = a[i];
			h = (h << 4) ^ o.hashCode();
		}
		return h;
	}

	public boolean equals(Object o) {
		return o instanceof ArrayHolder &&
				equals(a, ((ArrayHolder) o).a);
	}

	private static boolean equals(Object[] a1, Object[] a2) {
		if (a1.length != a2.length) {
			return false;
		}
		for (int i = 0; i < a1.length; i++) {
			if (!a1[i].equals(a2[i])) {
				return false;
			}
		}
		return true;
	}
}

// End FunUtil.java
