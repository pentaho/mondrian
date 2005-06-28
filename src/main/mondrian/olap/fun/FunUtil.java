/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.TypeUtil;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.Set;

/**
 * <code>FunUtil</code> contains a set of methods useful within the
 * <code>mondrian.olap.fun</code> package.
 **/
public class FunUtil extends Util {

    static final String[] emptyStringArray = new String[0];

    /**
     * Creates an exception which indicates that an error has occurred while
     * executing a given function.
     */
    public static RuntimeException newEvalException(
            FunDef funDef,
            String message) {
        Util.discard(funDef); // TODO: use this
        return new MondrianEvaluationException(message);
    }

    static Exp getArgNoEval(Exp[] args, int index) {
        return getArgNoEval(args, index, null);
    }

    static Exp getArgNoEval(
            Exp[] args,
            int index,
            Exp defaultValue) {
        return (index >= args.length)
            ? defaultValue
            : args[index];
    }

    static Object getArg(
            Evaluator evaluator,
            Exp[] args,
            int index) {
        return getArg(evaluator, args, index, null);
    }

    static Object getArg(
            Evaluator evaluator,
            Exp[] args,
            int index,
            Object defaultValue) {
        return (index >= args.length)
            ? defaultValue
            : args[index].evaluate(evaluator);
    }

    static String getStringArg(
            Evaluator evaluator,
            Exp[] args,
            int index,
            String defaultValue) {
        return (String) getArg(evaluator, args, index, defaultValue);
    }

    /** Returns an argument whose value is a literal. Unlike the other
     * <code>get<i>Xxx</i>Arg</code> methods, an evalutor is not required,
     * and hence this can be called at resolve-time. */
    static String getLiteralArg(
            Exp[] args,
            int i,
            String defaultValue,
            String[] allowedValues,
            FunDef funDef) {
        if (i >= args.length) {
            if (defaultValue == null) {
                throw newEvalException(funDef, "Required argument is missing");
            } else {
                return defaultValue;
            }
        }
        Exp arg = args[i];
        if (!(arg instanceof Literal) ||
                arg.getCategory() != Category.Symbol) {
            throw newEvalException(funDef, "Expected a symbol, found '" + arg + "'");
        }
        String s = (String) ((Literal) arg).getValue();
        StringBuffer sb = new StringBuffer(64);
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

    /**
     * Returns the ordinal of a literal argument. If the argument does not
     * belong to the supplied enumeration, returns -1.
     */
    static int getLiteralArg(
            Exp[] args,
            int i,
            int defaultValue,
            EnumeratedValues allowedValues,
            FunDef funDef) {
        final String literal = getLiteralArg(
                args,
                i,
                allowedValues.getName(defaultValue),
                allowedValues.getNames(),
                funDef);
        return (literal == null)
            ? -1
            : allowedValues.getOrdinal(literal);
    }

    /**
     * returns defaultValue, if the expression can not be evaluated because
     * some required operands have not been loaded from the database yet.
     */
    static boolean getBooleanArg(
            Evaluator evaluator,
            Exp[] args,
            int index,
            boolean defaultValue) {
        Object o = getArg(evaluator, args, index);
        return (o == null)
            ? defaultValue
            : ((Boolean) o).booleanValue();
    }

    /**
     * returns null, if the expression can not be evaluated because
     * some required operands have not been loaded from the database yet.
     */
    static Boolean getBooleanArg(Evaluator evaluator, Exp[] args, int index) {
        Object o = getArg(evaluator, args, index);
        return (Boolean) o;
    }

    static int getIntArg(
            Evaluator evaluator,
            Exp[] args,
            int index) {
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

    static Object getScalarArg(
            Evaluator evaluator,
            Exp[] args,
            int index) {
        return args[index].evaluateScalar(evaluator);
    }

    static BigDecimal getDecimalArg(
            Evaluator evaluator,
            Exp[] args,
            int index) {
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

    protected static Double getDoubleArg(
            Evaluator evaluator,
            Exp[] args,
            int index) {
        return getDoubleArg(evaluator, args, index, nullValue);
    }

    static Double getDoubleArg(
            Evaluator evaluator,
            Exp[] args,
            int index,
            Double nullValue) {
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
            Evaluator evaluator,
            Exp[] args,
            int index,
            boolean fail) {
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
            return evaluator.getContext(((Hierarchy) o).getDimension());
        } else if (o instanceof Dimension) {
            return evaluator.getContext((Dimension) o);
        } else {
            throw Util.getRes().newInternal("expecting a member, got " + o);
        }
    }

    /**
     * Evaluates and returns the <code>index</code>th argument, which must
     * be a tuple.
     *
     * @see #getTupleOrMemberArg
     *
     * @param evaluator Evaluation context
     * @param args The arguments to the function call
     * @param index Ordinal of the argument we are seeking
     * @return A tuple, represented as usual by an array of {@link Member}
     *   objects.
     */
    public static Member[] getTupleArg(
            Evaluator evaluator,
            Exp[] args,
            int index) {
        Exp arg = args[index];
        Object o = arg.evaluate(evaluator);
        return (Member[]) o;
    }

    /**
     * Evaluates and returns the <code>index</code>th argument, which we
     * expect to be either a member or a tuple, as a tuple. If the argument
     * is a member, converts it into a tuple with one member.
     *
     * @see #getTupleArg
     *
     * @param evaluator Evaluation context
     * @param args The arguments to the function call
     * @param index Ordinal of the argument we are seeking
     * @return A tuple, represented as usual by an array of {@link Member}
     *   objects.
     *
     * @throws ArrayIndexOutOfBoundsException if <code>index</code> is out of
     *   range
     */
    public static Member[] getTupleOrMemberArg(
            Evaluator evaluator,
            Exp[] args,
            int index) {
        Exp arg = args[index];
        Object o0 = arg.evaluate(evaluator);
        if (o0 instanceof Member[]) {
            return (Member[]) o0;
        } else if (o0 instanceof Member) {
            return new Member[] { (Member)o0 };
        } else {
            throw Util.newInternal("Expected tuple or member, got " + o0);
        }
    }

    static Level getLevelArg(Evaluator evaluator,
                             Exp[] args,
                             int index,
                             boolean fail) {
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
            Evaluator evaluator,
            Exp[] args,
            int index,
            boolean fail) {
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
            Evaluator evaluator,
            Exp[] args,
            int index,
            boolean fail) {
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
        final Type leftType = TypeUtil.stripSetType(left.getTypeX());
        final Type rightType = TypeUtil.stripSetType(right.getTypeX());
        if (!TypeUtil.isUnionCompatible(leftType, rightType)) {
            throw newEvalException(funDef, "Expressions must have the same hierarchy");
        }
    }

    /**
     * Returns <code>true</code> if the mask in <code>flag</code> is set.
     * @param value The value to check.
     * @param mask The mask within value to look for.
     * @param strict If <code>true</code> all the bits in mask must be set. If
     * <code>false</code> the method will return <code>true</code> if any of the
     * bits in <code>mask</code> are set.
     * @return <code>true</code> if the correct bits in <code>mask</code> are set.
     */
    static boolean checkFlag(int value, int mask, boolean strict) {
        return (strict)
            ? ((value & mask) == mask)
            : ((value & mask) != 0);
    }

    /**
     * Adds every element of <code>right</code> which is not in <code>set</code>
     * to both <code>set</code> and <code>left</code>.
     **/
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

    static List addMembers(
            SchemaReader schemaReader,
            List members,
            Hierarchy hierarchy) {
        // only add accessible levels
        Level[] levels = schemaReader.getHierarchyLevels(hierarchy);
        for (int i = 0; i < levels.length; i++) {
            addMembers(schemaReader, members, levels[i]);
        }
        return members;
    }

    static List addMembers(
            SchemaReader schemaReader,
            List members,
            Level level) {
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
    static Map evaluateMembers(Evaluator evaluator,
                               ExpBase exp,
                               List members,
                               boolean parentsToo) {
        Member[] constantTuple = exp.isConstantTuple();
        return (constantTuple == null)
            ? _evaluateMembers(evaluator.push(), exp, members, parentsToo)
            // exp is constant -- add it to the context before the loop, rather
            // than at every step
            : evaluateMembers(evaluator.push(constantTuple),
                members, parentsToo);
    }

    private static Map _evaluateMembers(Evaluator evaluator,
                                        ExpBase exp,
                                        List members,
                                        boolean parentsToo) {
        Map mapMemberToValue = new HashMap();
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

    static Map evaluateMembers(Evaluator evaluator,
                               List members,
                               boolean parentsToo) {
        Map mapMemberToValue = new HashMap();
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

    static void sort(Evaluator evaluator,
                     List members,
                     ExpBase exp,
                     boolean desc, boolean brk) {
        if (members.isEmpty()) {
            return;
        }
        Object first = members.get(0);
        Comparator comparator;
        if (first instanceof Member) {
            final boolean parentsToo = !brk;
            Map mapMemberToValue = evaluateMembers(evaluator, exp, members, parentsToo);
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

    static int sign(double d) {
        return (d == 0)
            ? 0
            : (d < 0)
                ? -1
                : 1;
    }

    static int compareValues(double d1, double d2) {
        return (d1 == d2)
            ? 0
            : (d1 < d2)
                ? -1
                : 1;
    }

    static int compareValues(int i, int j) {
        return (i == j)
            ? 0
            : (i < j)
                ? -1
                : 1;
    }

    /**
     * Compares two cell values.
     *
     * <p>Nulls compare last, exceptions (including the
     * object which indicates the the cell is not in the cache yet) next,
     * then numbers and strings are compared by value.
     *
     * @param value0 First cell value
     * @param value1 Second cell value
     * @return -1, 0, or 1, depending upon whether first cell value is less
     *   than, equal to, or greater than the second
     */
    static int compareValues(Object value0, Object value1) {
        if (value0 == value1) {
            return 0;
        }
        // null is less than anything else
        if (value0 == null) {
            return -1;
        }
        if (value1 == null) {
            return 1;
        }
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
            return FunUtil.compareValues(
                    ((Number) value0).doubleValue(),
                    ((Number) value1).doubleValue());
        } else {
            throw Util.newInternal("cannot compare " + value0);
        }
    }

    /**
     * Turns the mapped values into relative values (percentages) for easy
     * use by the general topOrBottom function. This might also be a useful
     * function in itself.
     */
    static void toPercent(List members, Map mapMemberToValue) {
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
                mapMemberToValue.put(
                    member,
                    new Double(d / total * (double) 100));
            }
        }

    }

    /**
     * Handles TopSum, TopPercent, BottomSum, BottomPercent by
     * evaluating members, sorting appropriately, and returning a
     * truncated list of members
     */
    static Object topOrBottom(Evaluator evaluator,
                              List members,
                              ExpBase exp,
                              boolean isTop,
                              boolean isPercent,
                              double target) {
        Map mapMemberToValue = evaluateMembers(evaluator, exp, members, false);
        Comparator comparator = new BreakMemberComparator(mapMemberToValue, isTop);
        Collections.sort(members, comparator);
        if (isPercent) {
            toPercent(members, mapMemberToValue);
        }
        double runningTotal = 0;
        int numMembers = members.size();
        int nullCount = 0;
        for (int i = 0; i < numMembers; i++) {
            if (runningTotal >= target) {
                members = members.subList(0, i);
                break;
            }
            Object o = mapMemberToValue.get(members.get(i));
            if (o instanceof Number) {
                runningTotal += ((Number) o).doubleValue();
            } else if (o instanceof Exception) {
                // ignore the error
            } else if (o instanceof Util.NullCellValue) {
                nullCount++;
            } else {
                throw Util.newInternal("got " + o + " when expecting Number");
            }
        }

        // MSAS exhibits the following behavior. If the value of all members is
        // null, then the first (or last) member of the set is returned for percent
        // operations.
        if ((numMembers > 0) && isPercent && (nullCount == numMembers)) {
            return (isTop)
                ? members.subList(0, 1)
                : members.subList(numMembers - 1, numMembers);
        }
        return members;
    }

    /**
     * Decodes the syntactic type of an operator.
     *
     * @param flags A encoded string which represents an operator signature,
     *   as used by the <code>flags</code> parameter used to construct a
     *   {@link FunDefBase}.
     *
     * @return A {@link Syntax}
     */
    public static Syntax decodeSyntacticType(String flags) {
        char c = flags.charAt(0);
        switch (c) {
        case 'p':
            return Syntax.Property;
        case 'f':
            return Syntax.Function;
        case 'm':
            return Syntax.Method;
        case 'i':
            return Syntax.Infix;
        case 'P':
            return Syntax.Prefix;
        case 'I':
            return Syntax.Internal;
        default:
            throw newInternal(
                    "unknown syntax code '" + c + "' in string '" + flags + "'");
        }
    }

    /**
     * Decodes the signature of a function into a category code which describes
     * the return type of the operator.
     *
     * <p>For example, <code>decodeReturnType("fnx")</code> returns
     * <code>{@link Category#Numeric}</code>, indicating this function has a
     * numeric return value.
     *
     * @param flags The signature of an operator,
     *   as used by the <code>flags</code> parameter used to construct a
     *   {@link FunDefBase}.
     *
     * @return An array {@link Category} codes.
     */
    public static int decodeReturnType(String flags) {
        final int returnType = decodeType(flags, 1);
        if ((returnType & Category.Mask) != returnType) {
            throw newInternal("bad return code flag in flags '" + flags + "'");
        }
        return returnType;
    }

    /**
     * Decodes the <code>offset</code>th character of an encoded method
     * signature into a type category.
     *
     * <p>The codes are:
     * <table border="1">
     *
     * <tr><td>a</td><td>{@link Category#Array}</td></tr>
     *
     * <tr><td>d</td><td>{@link Category#Dimension}</td></tr>
     *
     * <tr><td>h</td><td>{@link Category#Hierarchy}</td></tr>
     *
     * <tr><td>l</td><td>{@link Category#Level}</td></tr>
     *
     * <tr><td>b</td><td>{@link Category#Logical}</td></tr>
     *
     * <tr><td>m</td><td>{@link Category#Member}</td></tr>
     *
     * <tr><td>N</td><td>Constant {@link Category#Numeric}</td></tr>
     *
     * <tr><td>n</td><td>{@link Category#Numeric}</td></tr>
     *
     * <tr><td>x</td><td>{@link Category#Set}</td></tr>
     *
     * <tr><td>#</td><td>Constant {@link Category#String}</td></tr>
     *
     * <tr><td>S</td><td>{@link Category#String}</td></tr>
     *
     * <tr><td>t</td><td>{@link Category#Tuple}</td></tr>
     *
     * <tr><td>v</td><td>{@link Category#Value}</td></tr>
     *
     * <tr><td>y</td><td>{@link Category#Symbol}</td></tr>
     *
     * </table>
     *
     * @param flags Encoded signature string
     * @param offset 0-based offset of character within string
     * @return A {@link Category}
     */
    public static int decodeType(String flags, int offset) {
        char c = flags.charAt(offset);
        switch (c) {
        case 'a':
            return Category.Array;
        case 'd':
            return Category.Dimension;
        case 'h':
            return Category.Hierarchy;
        case 'l':
            return Category.Level;
        case 'b':
            return Category.Logical;
        case 'm':
            return Category.Member;
        case 'N':
            return Category.Numeric | Category.Constant;
        case 'n':
            return Category.Numeric;
        case 'I':
            return Category.Numeric | Category.Integer | Category.Constant;
        case 'i':
            return Category.Numeric | Category.Integer;
        case 'x':
            return Category.Set;
        case '#':
            return Category.String | Category.Constant;
        case 'S':
            return Category.String;
        case 't':
            return Category.Tuple;
        case 'v':
            return Category.Value;
        case 'y':
            return Category.Symbol;
        default:
            throw newInternal(
                    "unknown type code '" + c + "' in string '" + flags + "'");
        }
    }

    /**
     * Decodes a string of parameter types into an array of type codes.
     *
     * <p>Each character is decoded using {@link #decodeType(String, int)}.
     * For example, <code>decodeParameterTypes("nx")</code> returns
     * <code>{{@link Category#Numeric}, {@link Category#Set}}</code>.
     *
     * @param flags The signature of an operator,
     *   as used by the <code>flags</code> parameter used to construct a
     *   {@link FunDefBase}.
     *
     * @return An array {@link Category} codes.
     */
    public static int[] decodeParameterTypes(String flags) {
        int[] parameterTypes = new int[flags.length() - 2];
        for (int i = 0; i < parameterTypes.length; i++) {
            parameterTypes[i] = decodeType(flags, i + 2);
        }
        return parameterTypes;
    }

    /**
     * Sorts an array of values.
     */
    public static void sortValuesDesc(Object[] values) {
        Arrays.sort(values, DescendingValueComparator.instance);
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

        /*
         * The median is defined as the value that has exactly the same
         * number of entries before it in the sorted list as after.
         * So, if the number of entries in the list is odd, the
         * median is the entry at (length-1)/2 (using zero-based indexes).
         * If the number of entries is even, the median is defined as the
         * arithmetic mean of the two numbers in the middle of the list, or
         * (entries[length/2 - 1] + entries[length/2]) / 2.
         */
        int length = asArray.length;
        Double result = ((length & 1) == 1)
            // The length is odd. Note that length/2 is an integer expression,
            // and it's positive so we save ourselves a divide...
            ? new Double(asArray[length >> 1])
            : new Double((asArray[(length >> 1) - 1] + asArray[length >> 1]) / 2.0);

        return result;
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
    static Object quartile(Evaluator evaluator,
                           List members,
                           ExpBase exp,
                           int range) {
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
        return ((dm == median) && (median < asArray.length - 1))
            //have more elements
            ? new Double((asArray[median] + asArray[median+1])/2)
            : new Double(asArray[median]);
    }

    public static Object min(Evaluator evaluator, List members, Exp exp) {
        SetWrapper sw = evaluateSet(evaluator, members, (ExpBase) exp);
        if (sw.errorCount > 0) {
            return new Double(Double.NaN);
        } else if (sw.v.size() == 0) {
            return Util.nullValue;
        } else {
            double min = Double.MAX_VALUE;
            for (int i = 0; i < sw.v.size(); i++) {
                double iValue = ((Double) sw.v.get(i)).doubleValue();
                if (iValue < min) {
                    min = iValue;
                }
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
        } else {
            double max = Double.MIN_VALUE;
            for (int i = 0; i < sw.v.size(); i++) {
                double iValue = ((Double) sw.v.get(i)).doubleValue();
                if (iValue > max) {
                    max = iValue;
                }
            }
            return new Double(max);
        }
    }

    static Object var(Evaluator evaluator,
                      List members,
                      ExpBase exp,
                      boolean biased) {
        SetWrapper sw = evaluateSet(evaluator, members, exp);
        return _var(sw, biased);
    }

    private static Object _var(SetWrapper sw, boolean biased) {
        if (sw.errorCount > 0) {
            return new Double(Double.NaN);
        } else if (sw.v.size() == 0) {
            return Util.nullValue;
        } else {
            double stdev = 0.0;
            double avg = _avg(sw);
            for (int i = 0; i < sw.v.size(); i++) {
                stdev += Math.pow((((Double) sw.v.get(i)).doubleValue() - avg),2);
            }
            int n = sw.v.size();
            if (!biased) {
                n--;
            }
            return new Double(stdev / (double) n);
        }
    }

    static Object correlation(Evaluator evaluator,
                              List members,
                              ExpBase exp1,
                              ExpBase exp2) {
        SetWrapper sw1 = evaluateSet(evaluator, members, exp1);
        SetWrapper sw2 = evaluateSet(evaluator, members, exp2);
        Object covar = _covariance(sw1, sw2, false);
        Object var1 = _var(sw1, false); //this should be false, yes?
        Object var2 = _var(sw2, false);
        if ((covar instanceof Double) &&
            (var1 instanceof Double) &&
            (var2 instanceof Double)) {
            return new Double(((Double) covar).doubleValue() /
                Math.sqrt(((Double) var1).doubleValue() *
                          ((Double) var2).doubleValue()));
        } else {
            return Util.nullValue;
        }
    }

    static Object covariance(Evaluator evaluator, List members, ExpBase exp1, ExpBase exp2, boolean biased) {
        SetWrapper sw1 = evaluateSet(evaluator.push(), members, exp1);
        SetWrapper sw2 = evaluateSet(evaluator.push(), members, exp2);
        // todo: because evaluateSet does not add nulls to the SetWrapper, this
        // solution may lead to mismatched lists and is therefore not robust
        return _covariance(sw1, sw2, biased);
    }


    private static Object _covariance(SetWrapper sw1,
                                      SetWrapper sw2,
                                      boolean biased) {
        if (sw1.v.size() != sw2.v.size()) {
            return Util.nullValue;
        }
        double avg1 = _avg(sw1);
        double avg2 = _avg(sw2);
        double covar = 0.0;
        for (int i = 0; i < sw1.v.size(); i++) {
            //all of this casting seems inefficient - can we make SetWrapper
            //contain an array of double instead?
            double diff1 = (((Double) sw1.v.get(i)).doubleValue() - avg1);
            double diff2 = (((Double) sw2.v.get(i)).doubleValue() - avg2);
            covar += (diff1 * diff2);
        }
        int n = sw1.v.size();
        if (!biased) {
            n--;
        }
        return new Double(covar / (double) n);
    }

    static Object stdev(Evaluator evaluator,
                        List members,
                        ExpBase exp,
                        boolean biased) {
        Object o = var(evaluator, members, exp, biased);
        return (o instanceof Double)
            ? new Double(Math.sqrt(((Double) o).doubleValue()))
            : o;
    }

    public static Object avg(Evaluator evaluator, List members, Exp exp) {
        SetWrapper sw = evaluateSet(evaluator, members, (ExpBase) exp);
        return (sw.errorCount > 0)
            ? new Double(Double.NaN)
            : (sw.v.size() == 0)
                ? Util.nullValue
                : new Double(_avg(sw));
    }

    //todo: parameterize inclusion of nulls
    //also, maybe make _avg a method of setwrapper, so we can cache the result (i.e. for correl)
    private static double _avg(SetWrapper sw) {
        double sum = 0.0;
        for (int i = 0; i < sw.v.size(); i++) {
            sum += ((Double) sw.v.get(i)).doubleValue();
        }
        //todo: should look at context and optionally include nulls
        return sum / (double) sw.v.size();
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
        } else {
            double sum = 0.0;
            for (int i = 0; i < sw.v.size(); i++) {
                sum += ((Double) sw.v.get(i)).doubleValue();
            }
            return new Double(sum);
        }
    }

    public static Object count(Evaluator evaluator,
                               List members,
                               boolean includeEmpty) {
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
     * generate a {@link List} of {@link SetWrapper} objects, which contains
     * a {@link Double} value and meta information, unlike
     * {@link #evaluateMembers}, which only produces values.
     *
     * @pre exp != null
     */
    static SetWrapper evaluateSet(Evaluator evaluator,
                                  List members,
                                  ExpBase exp) {
        Util.assertPrecondition(exp != null, "exp != null");

        // todo: treat constant exps as evaluateMembers() does
        SetWrapper retval = new SetWrapper();
        for (Iterator it = members.iterator(); it.hasNext();) {
            Object obj = it.next();
            if (obj instanceof Member[]) {
                evaluator.setContext((Member[])obj);
            } else {
                evaluator.setContext((Member)obj);
            }
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

    /**
     * This evaluates one or more ExpBases against the member list returning
     * a SetWrapper array. Where this differs very significantly from the
     * above evaluateSet methods is how it count null values and Throwables;
     * this method adds nulls to the SetWrapper Vector rather than not adding
     * anything - as the above method does. The impact of this is that if, for
     * example, one was creating a list of x,y values then each list will have
     * the same number of values (though some might be null) - this allows
     * higher level code to determine how to handle the lack of data rather than
     * having a non-equal number (if one is plotting x,y values it helps to
     * have the same number and know where a potential gap is the data is.
     *
     * @param evaluator
     * @param members
     * @param exps
     * @return
     */
    static SetWrapper[] evaluateSet(Evaluator evaluator,
                                    List members,
                                    ExpBase[] exps) {
        Util.assertPrecondition(exps != null, "exps != null");

        // todo: treat constant exps as evaluateMembers() does
        SetWrapper[] retvals = new SetWrapper[exps.length];
        for (int i = 0; i < exps.length; i++) {
            retvals[i] = new SetWrapper();
        }
        for (Iterator it = members.iterator(); it.hasNext();) {
            Object obj = it.next();
            if (obj instanceof Member[]) {
                evaluator.setContext((Member[])obj);
            } else {
                evaluator.setContext((Member)obj);
            }
            for (int i = 0; i < exps.length; i++) {
                ExpBase exp = exps[i];
                SetWrapper retval = retvals[i];
                Object o = exp.evaluateScalar(evaluator);
                if (o == null || o == Util.nullValue) {
                    retval.nullCount++;
                    retval.v.add(null);
                } else if (o instanceof Throwable) {
                    // Carry on summing, so that if we are running in a
                    // BatchingCellReader, we find out all the dependent cells
                    // we
                    // need
                    retval.errorCount++;
                    retval.v.add(null);
                } else if (o instanceof Double) {
                    retval.v.add(o);
                } else if (o instanceof Number) {
                    retval.v.add(new Double(((Number) o).doubleValue()));
                } else {
                    retval.v.add(o);
                }
            }
        }
        return retvals;
    }

    static List periodsToDate(
            Evaluator evaluator,
            Level level,
            Member member) {
        if (member == null) {
            member = evaluator.getContext(level.getHierarchy().getDimension());
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
        List members = new ArrayList();
        if (m != null) {
            // e.g. m is [Time].[1997] and member is [Time].[1997].[Q1].[3]
            // we now have to make m to be the first member of the range,
            // so m becomes [Time].[1997].[Q1].[1]
            SchemaReader reader = evaluator.getSchemaReader();
            m = Util.getFirstDescendantOnLevel(reader, m, member.getLevel());
            reader.getMemberRange(level, m, member, members);
        }
        return members;
    }

    static List memberRange(Evaluator evaluator,
                            Member startMember,
                            Member endMember) {
        final Level level = startMember.getLevel();
        assertTrue(level == endMember.getLevel());
        List members = new ArrayList();
        evaluator.getSchemaReader().getMemberRange(level,
            startMember, endMember, members);

        if (members.isEmpty()) {
            // The result is empty, so maybe the members are reversed. This is
            // cheaper than comparing the members before we call getMemberRange.
            evaluator.getSchemaReader().getMemberRange(level,
                endMember, startMember, members);
        }
        return members;
    }

    /**
     * Returns the member under ancestorMember having the same relative position
     * under member's parent.
     * <p>For exmaple, cousin([Feb 2001], [Q3 2001]) is [August 2001].
     * @param schemaReader The reader to use
     * @param member The member for which we'll find the cousin.
     * @param ancestorMember The cousin's ancestor.
     * @return The child of <code>ancestorMember</code> in the same position under
     * <code>ancestorMember</code> as <code>member</code> is under its parent.
     */
    static Member cousin(SchemaReader schemaReader,
                         Member member,
                         Member ancestorMember) {
        if (ancestorMember.isNull()) {
            return ancestorMember;
        }
        if (member.getHierarchy() != ancestorMember.getHierarchy()) {
            throw MondrianResource.instance().newCousinHierarchyMismatch(
                member.getUniqueName(), ancestorMember.getUniqueName());
        }
        if (member.getLevel().getDepth() < ancestorMember.getLevel().getDepth()) {
            return member.getHierarchy().getNullMember();
        }

        Member cousin = cousin2(schemaReader, member, ancestorMember);
        if (cousin == null) {
            cousin = member.getHierarchy().getNullMember();
        }

        return cousin;
    }

    static private Member cousin2(SchemaReader schemaReader,
                                  Member member1,
                                  Member member2) {
        if (member1.getLevel() == member2.getLevel()) {
            return member2;
        }
        Member uncle = cousin2(schemaReader, member1.getParentMember(), member2);
        if (uncle == null) {
            return null;
        }
        int ordinal = Util.getMemberOrdinalInParent(schemaReader, member1);
        Member[] cousins = schemaReader.getMemberChildren(uncle);
        if (cousins.length <= ordinal) {
            return null;
        }
        return cousins[ordinal];
    }

    /**
     * Returns the ancestor of <code>member</code> at the given level
     * or distance. It is assumed that any error checking required
     * has been done prior to calling this function.
     * <p>This method takes into consideration the fact that there
     * may be intervening hidden members between <code>member</code>
     * and the ancestor. If <code>targetLevel</code> is not null, then
     * the method will only return a member if the level at
     * <code>distance</code> from the member is actually the
     * <code>targetLevel</code> specified.
     * @param evaluator The evaluation context
     * @param member The member for which the ancestor is to be found
     * @param distance The distance up the chain the ancestor is to
     * be found.
     * @param targetLevel The desired targetLevel of the ancestor. If <code>null</code>,
     * then the distance completely determines the desired ancestor.
     * @return The ancestor member, or <code>null</code> if no such
     * ancestor exists.
     */
    static Member ancestor(Evaluator evaluator,
                           Member member,
                           int distance,
                           Level targetLevel) {
        if ((targetLevel != null) &&
            (member.getHierarchy() != targetLevel.getHierarchy())) {
            throw MondrianResource.instance().newMemberNotInLevelHierarchy(
                member.getUniqueName(), targetLevel.getUniqueName());
        }

        if (distance == 0) {
            /*
            * Shortcut if there's nowhere to go.
            */
            return member;
        } else if (distance < 0) {
            /*
            * Can't go backwards.
            */
            return member.getHierarchy().getNullMember();
        }

        Member[] ancestors = member.getAncestorMembers();
        final SchemaReader schemaReader = evaluator.getSchemaReader();

        Member result = member.getHierarchy().getNullMember();

        searchLoop:
        for (int i = 0; i < ancestors.length; i++) {
            final Member ancestorMember = ancestors[i];

            if (targetLevel != null) {
                if (ancestorMember.getLevel() == targetLevel) {
                    if (schemaReader.isVisible(ancestorMember)) {
                        result = ancestorMember;
                        break searchLoop;
                    } else {
                        result = member.getHierarchy().getNullMember();
                        break searchLoop;
                    }
                }
            } else {
                if (schemaReader.isVisible(ancestorMember)) {
                    distance--;

                    //
                    // Make sure that this ancestor is really on the right
                    // targetLevel. If a targetLevel was specified and at least
                    // one of the ancestors was hidden, this this algorithm goes
                    // too far up the ancestor list. It's not a problem, except
                    // that we need to check if it's happened and return the
                    // hierarchy's null member instead.
                    //
                    // For example, consider what happens with
                    // Ancestor([Store].[Israel].[Haifa], [Store].[Store State]).
                    // The distance from [Haifa] to [Store State] is 1, but that
                    // lands us at the country targetLevel, which is clearly
                    // wrong.
                    //
                    if (distance == 0) {
                        if (targetLevel == null || ancestorMember.getLevel() == targetLevel) {
                            result = ancestorMember;
                            break searchLoop;
                        } else {
                            result = member.getHierarchy().getNullMember();
                            break searchLoop;
                        }
                    }
                }
            }
        }

        return result;
    }

    /**
     * Compares a pair of members according to their positions in a
     * prefix-order (or postfix-order, if <code>post</code> is true) walk
     * over a hierarchy.
     *
     * @param m1 First member
     * @param m2 Second member
     * @param post Whether to sort in postfix order. If true, a parent will
     *   sort immediately after its last child. If false, a parent will sort
     *   immediately before its first child.
     * @return -1 if m1 collates before m2,
     *   0 if m1 equals m2,
     *   1 if m1 collates after m2
     */
    public static int compareHierarchically(Member m1,
                                            Member m2,
                                            boolean post) {
        if (equals(m1, m2)) {
            return 0;
        }
        while (true) {
            int depth1 = m1.getDepth();
            int depth2 = m2.getDepth();
            if (depth1 < depth2) {
                m2 = m2.getParentMember();
                if (equals(m1, m2)) {
                    return post ? 1 : -1;
                }
            } else if (depth1 > depth2) {
                m1 = m1.getParentMember();
                if (equals(m1, m2)) {
                    return post ? -1 : 1;
                }
            } else {
                Member prev1 = m1;
                Member prev2 = m2;
                m1 = m1.getParentMember();
                m2 = m2.getParentMember();
                if (equals(m1, m2)) {
                    return compareSiblingMembers(prev1, prev2);
                }
            }
        }
    }

    /**
     * Compares two members which are known to have the same parent.
     *
     * First, compare by ordinal.
     * This is only valid now we know they're siblings, because
     * ordinals are only unique within a parent.
     * If the dimension does not use ordinals, both ordinals
     * will be -1.
     *
     * <p>If the ordinals do not differ, compare using regular member
     * comparison.
     *
     * @param m1 First member
     * @param m2 Second member
     * @return -1 if m1 collates less than m2,
     *   1 if m1 collates after m2,
     *   0 if m1 == m2.
     */
    static int compareSiblingMembers(Member m1, Member m2) {
        final int ordinal1 = m1.getOrdinal();
        final int ordinal2 = m2.getOrdinal();
        return (ordinal1 == ordinal2)
            ? m1.compareTo(m2)
            : (ordinal1 < ordinal2)
                ? -1
                : 1;
/*
        if (ordinal1 == ordinal2) {
            ;
        } else if (ordinal1 < ordinal2) {
            return -1;
        } else if (ordinal1 > ordinal2) {
            return 1;
        }
        return m1.compareTo(m2);
*/
    }

    /**
     * Evaluates the OPENINGPERIOD (or, CLOSINGPERIOD, if
     * <code>isOpeningPeriod</code> is false) function.
     */
    static Object openingClosingPeriod(
            Evaluator evaluator,
            Exp[] args,
            boolean isOpeningPeriod)
    {
        Member member;
        Level level;

        //
        // If the member argument is present, use it. Otherwise default
        // to the time dimension's current member.
        //
        if (args.length == 2) {
            member = getMemberArg(evaluator, args, 1, false);
        } else {
            member = evaluator.getContext(evaluator.getCube().getTimeDimension());
        }

        //
        // If the level argument is present, use it. Otherwise use the level
        // immediately after that of the member argument.
        //
        if (args.length >= 1) {
            level = getLevelArg(evaluator,  args, 0, false);
        } else {
            int targetDepth = member.getLevel().getDepth() + 1;
            Level[] levels = member.getHierarchy().getLevels();

            if (levels.length <= targetDepth) {
                return member.getHierarchy().getNullMember();
            }
            level = levels[targetDepth];
        }

        //
        // Make sure the member and the level come from the same hierarchy.
        //
        if (!member.getHierarchy().equals(level.getHierarchy())) {
            throw MondrianResource.instance().newFunctionMbrAndLevelHierarchyMismatch(
                    isOpeningPeriod ? "OpeningPeriod" : "ClosingPeriod",
                    level.getHierarchy().getUniqueName(),
                    member.getHierarchy().getUniqueName());
        }

        //
        // Shortcut if the level is above the member.
        //
        if (level.getDepth() < member.getLevel().getDepth()) {
            return member.getHierarchy().getNullMember();
        }

        //
        // Shortcut if the level is the same as the member
        //
        if (level == member.getLevel()) {
            return member;
        }

        return getDescendant(evaluator.getSchemaReader(), member, level,
            isOpeningPeriod);
    }

    /**
     * Returns the first or last descendant of the member at the target level.
     * This method is the implementation of both OpeningPeriod and ClosingPeriod.
     * @param schemaReader The schema reader to use to evaluate the function.
     * @param member The member from which the descendant is to be found.
     * @param targetLevel The level to stop at.
     * @param returnFirstDescendant Flag indicating whether to return the first
     * or last descendant of the member.
     * @return A member.
     * @pre member.getLevel().getDepth() < level.getDepth();
     */
    private static Member getDescendant(SchemaReader schemaReader,
            Member member, Level targetLevel, boolean returnFirstDescendant) {
        Member[] children;

        final int targetLevelDepth = targetLevel.getDepth();
        assertPrecondition(member.getLevel().getDepth() < targetLevelDepth,
                "member.getLevel().getDepth() < targetLevel.getDepth()");

        for (;;) {
            children = schemaReader.getMemberChildren(member);

            if (children.length == 0) {
                return targetLevel.getHierarchy().getNullMember();
            }

            member = children[returnFirstDescendant ? 0 : (children.length - 1)];

            if (member.getLevel().getDepth() == targetLevelDepth) {
                if (member.isHidden()) {
                    return member.getHierarchy().getNullMember();
                } else {
                    return member;
                }
            }
        }
    }
    /**
     * Adds a test case for each method of this object whose signature looks
     * like 'public void testXxx()'.
     */
//  public void addTests(TestSuite suite, Pattern pattern) {
//      addTests(this, suite, pattern);
//  }

//  /**
//   * Adds a test case for each method in an object whose signature looks
//   * like 'public void testXxx({@link TestCase})'.
//   */
//  public static void addTests(Object o, TestSuite suite, Pattern pattern) {
//      for (Class clazz = o.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
//          Method[] methods = clazz.getDeclaredMethods();
//          for (int i = 0; i < methods.length; i++) {
//              Method method = methods[i];
//              String methodName = method.getName();
//              if (methodName.startsWith("test") &&
//                      Modifier.isPublic(method.getModifiers()) &&
//                      method.getParameterTypes().length == 1 &&
//                      TestCase.class.isAssignableFrom(
//                              method.getParameterTypes()[0]) &&
//                      method.getReturnCategory() == Void.TYPE) {
//                    String fullMethodName = clazz.getName() + "." + method.getName();
//                    if (pattern == null || pattern.matcher(fullMethodName).matches()) {
//                        suite.addTest(new MethodCallTestCase(
//                            fullMethodName, o, method));
//                    }
//              }
//          }
//      }
//  }

    // Inner classes


    private static abstract class MemberComparator implements Comparator {
        private static final Logger LOGGER =
                Logger.getLogger(MemberComparator.class);
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
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        "compare " +
                        m1.getUniqueName() +
                        "(" + mapMemberToValue.get(m1) + "), " +
                        m2.getUniqueName() +
                        "(" + mapMemberToValue.get(m2) + ")" +
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
                int depth1 = m1.getDepth(),
                        depth2 = m2.getDepth();
                if (depth1 < depth2) {
                    m2 = m2.getParentMember();
                    if (Util.equals(m1, m2)) {
                        return -1;
                    }
                } else if (depth1 > depth2) {
                    m1 = m1.getParentMember();
                    if (Util.equals(m1, m2)) {
                        return 1;
                    }
                } else {
                    Member prev1 = m1, prev2 = m2;
                    m1 = m1.getParentMember();
                    m2 = m2.getParentMember();
                    if (Util.equals(m1, m2)) {
                        // including case where both parents are null
                        int c = compareByValue(prev1, prev2);
                        if (c != 0) {
                            return c;
                        }
                        // prev1 and prev2 are siblings.
                        // Order according to hierarchy, if the values do not differ.
                        // Needed to have a consistent sort if members with equal (null!)
                        //  values are compared.
                        c = FunUtil.compareSiblingMembers(prev1, prev2);
                        return c;
                    }
                }
            }
        }
    }

    private static class HierarchicalMemberComparator
            extends MemberComparator {
        HierarchicalMemberComparator(Map mapMemberToValue, boolean desc) {
            super(mapMemberToValue, desc);
        }

        protected int compareInternal(Member m1, Member m2) {
            return compareHierarchicallyButSiblingsByValue(m1, m2);
        }
    }

    private static class BreakMemberComparator extends MemberComparator {
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
    private static abstract class ArrayComparator implements Comparator {
        private static final Logger LOGGER =
                Logger.getLogger(ArrayComparator.class);
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
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        "compare {" + toString(a1)+ "}, {" + toString(a2) +
                        "} yields " + c);
            }
            return c;
        }

        protected abstract int compare(Member[] a1, Member[] a2);
    }

    /**
     * Extension to {@link ArrayComparator} which compares tuples by evaluating
     * an expression.
     */
    private static abstract class ArrayExpComparator
            extends ArrayComparator {
        Evaluator evaluator;
        final Exp exp;

        ArrayExpComparator(Evaluator evaluator, Exp exp, int arity) {
            super(arity);
            this.evaluator = evaluator;
            this.exp = exp;
        }

    }

    private static class HierarchicalArrayComparator
            extends ArrayExpComparator {
        private final boolean desc;

        HierarchicalArrayComparator(
                Evaluator evaluator, Exp exp, int arity, boolean desc) {
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

        protected int compareHierarchicallyButSiblingsByValue(
                Member m1, Member m2) {
            if (FunUtil.equals(m1, m2)) {
                return 0;
            }
            while (true) {
                int depth1 = m1.getDepth(),
                        depth2 = m2.getDepth();
                if (depth1 < depth2) {
                    m2 = m2.getParentMember();
                    if (FunUtil.equals(m1, m2)) {
                        return -1;
                    }
                } else if (depth1 > depth2) {
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
                            c = FunUtil.compareSiblingMembers(prev1, prev2);
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

    private static class BreakArrayComparator extends ArrayExpComparator {
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
    private static class HierarchizeArrayComparator extends ArrayComparator {
        private final boolean post;

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
    private static class HierarchizeComparator implements Comparator {
        private final boolean post;

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
    private static class ReverseComparator implements Comparator {
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
    protected static class ArrayHolder {
        private Object[] a;

        ArrayHolder(Object[] a) {
            this.a = a;
        }

        public int hashCode() {
            int h = 0;
            for (int i = 0; i < a.length; i++) {
                Object o = a[i];
                int rotated = (h << 4) | ((h >> 28) & 0xf);
                h = rotated ^ o.hashCode();
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

    static class SetWrapper {
        List v = new ArrayList();
        public int errorCount = 0, nullCount = 0;

        //private double avg = Double.NaN;
        //todo: parameterize inclusion of nulls
        //by making this a method of the SetWrapper, we can cache the result
        //this allows its reuse in Correlation
//      public double getAverage() {
//          if (avg == Double.NaN) {
//              double sum = 0.0;
//              for (int i = 0; i < resolvers.size(); i++) {
//                  sum += ((Double) resolvers.elementAt(i)).doubleValue();
//              }
//              //todo: should look at context and optionally include nulls
//              avg = sum / (double) resolvers.size();
//          }
//          return avg;
//      }
    }

    /**
     * Compares cell values, so that larger values compare first.
     *
     * <p>Nulls compare last, exceptions (including the
     * object which indicates the the cell is not in the cache yet) next,
     * then numbers and strings are compared by value.
     */
    private static class DescendingValueComparator implements Comparator {
        /**
         * The singleton.
         */
        static final DescendingValueComparator instance =
                new DescendingValueComparator();

        public int compare(Object o1, Object o2) {
            return - compareValues(o1, o2);
        }
    }
}

// End FunUtil.java
