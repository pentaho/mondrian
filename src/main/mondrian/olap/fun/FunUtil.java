/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler.ResultStyle;
import mondrian.calc.DoubleCalc;
import mondrian.mdx.*;

import org.apache.log4j.Logger;

import java.util.*;
import java.util.Set;
import java.io.PrintWriter;

/**
 * <code>FunUtil</code> contains a set of methods useful within the
 * <code>mondrian.olap.fun</code> package.
 *
 * @author jhyde
 * @version $Id$
 * @since 1.0
 */
public class FunUtil extends Util {

    static final String[] emptyStringArray = new String[0];
    private static final boolean debug = false;
    public static final NullMember NullMember = new NullMember();

    /**
     * Special value which indicates that a <code>double</code> computation
     * has returned the MDX null value. See {@link DoubleCalc}.
     */
    public static final double DoubleNull = 0.000000012345;

    /**
     * Special value which indicates that a <code>double</code> computation
     * has returned the MDX EMPTY value. See {@link DoubleCalc}.
     */
    public static final double DoubleEmpty = -0.000000012345;
    
    /**
     * Special value which indicates that an <code>int</code> computation
     * has returned the MDX null value. See {@link mondrian.calc.IntegerCalc}.
     */
    public static final int IntegerNull = Integer.MIN_VALUE + 1;

    /**
     * Null value in three-valued boolean logic.
     * Actually, a placeholder until we actually implement 3VL.
     */
    public static final boolean BooleanNull = false;

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

    /**
     * Creates an exception which indicates that an error has occurred while
     * executing a given function.
     */
    public static RuntimeException newEvalException(Throwable throwable) {
        return new MondrianEvaluationException(throwable.getMessage());
    }

    public static final boolean isMemberType(Calc calc) {
        Type type = calc.getType();
        return (type instanceof SetType) &&
          (((SetType) type).getElementType() instanceof MemberType);
    }

    public static final void checkIterListResultStyles(Calc calc) {
        switch (calc.getResultStyle()) {
        case ITERABLE :
        case LIST :
        case MUTABLE_LIST :
            break;
        default :
            throw ResultStyleException.generateBadType(
                new ResultStyle[] {
                    ResultStyle.ITERABLE,
                    ResultStyle.LIST,
                    ResultStyle.MUTABLE_LIST
                },
                calc.getResultStyle());
        }
    }
    public static final void checkListResultStyles(Calc calc) {
        switch (calc.getResultStyle()) {
        case LIST :
        case MUTABLE_LIST :
            break;
        default :
            throw ResultStyleException.generateBadType(
                new ResultStyle[] {
                    ResultStyle.LIST,
                    ResultStyle.MUTABLE_LIST
                },
                calc.getResultStyle());
        }
    }


    /**
     * Returns an argument whose value is a literal.
     */
    static String getLiteralArg(
            ResolvedFunCall call,
            int i,
            String defaultValue,
            String[] allowedValues) {
        if (i >= call.getArgCount()) {
            if (defaultValue == null) {
                throw newEvalException(call.getFunDef(),
                        "Required argument is missing");
            } else {
                return defaultValue;
            }
        }
        Exp arg = call.getArg(i);
        if (!(arg instanceof Literal) ||
                arg.getCategory() != Category.Symbol) {
            throw newEvalException(call.getFunDef(),
                    "Expected a symbol, found '" + arg + "'");
        }
        String s = (String) ((Literal) arg).getValue();
        StringBuilder sb = new StringBuilder(64);
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
        throw newEvalException(call.getFunDef(),
                "Allowed values are: {" + sb + "}");
    }

    /**
     * Returns the ordinal of a literal argument. If the argument does not
     * belong to the supplied enumeration, returns -1.
     */
    static <E extends Enum<E>> E getLiteralArg(
            ResolvedFunCall call,
            int i,
            E defaultValue,
            Class<E> allowedValues) {
        if (i >= call.getArgCount()) {
            if (defaultValue == null) {
                throw newEvalException(call.getFunDef(),
                        "Required argument is missing");
            } else {
                return defaultValue;
            }
        }
        Exp arg = call.getArg(i);
        if (!(arg instanceof Literal) ||
                arg.getCategory() != Category.Symbol) {
            throw newEvalException(call.getFunDef(),
                    "Expected a symbol, found '" + arg + "'");
        }
        String s = (String) ((Literal) arg).getValue();
        for (E e : allowedValues.getEnumConstants()) {
            if (e.name().equalsIgnoreCase(s)) {
                return e;
            }
        }
        StringBuilder buf = new StringBuilder(64);
        int k = 0;
        for (E e : allowedValues.getEnumConstants()) {
            if (k++ > 0) {
                buf.append(", ");
            }
            buf.append(e.name());
        }
        throw newEvalException(call.getFunDef(),
                "Allowed values are: {" + buf + "}");
    }

    /**
     * Throws an error if the expressions don't have the same hierarchy.
     * @param left
     * @param right
     * @throws MondrianEvaluationException if expressions don't have the same
     *     hierarchy
     */
    static void checkCompatible(Exp left, Exp right, FunDef funDef) {
        final Type leftType = TypeUtil.stripSetType(left.getType());
        final Type rightType = TypeUtil.stripSetType(right.getType());
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
     */
    static void addUnique(List left, List right, Set set) {
        assert left != null;
        assert right != null;
        if (right.isEmpty()) {
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

    static List<Member> addMembers(
            SchemaReader schemaReader,
            List<Member> members,
            Hierarchy hierarchy) {
        // only add accessible levels
        Level[] levels = schemaReader.getHierarchyLevels(hierarchy);
        for (int i = 0; i < levels.length; i++) {
            addMembers(schemaReader, members, levels[i]);
        }
        return members;
    }

    static List<Member> addMembers(
            SchemaReader schemaReader,
            List<Member> members,
            Level level) {
        Member[] levelMembers = schemaReader.getLevelMembers(level, true);
        addAll(members, levelMembers);
        return members;
    }

    /**
     * Removes every member from a list which is calculated.
     * The list must not be null, and must consist only of members.
     */
    static void removeCalculatedMembers(List<Member> memberList)
    {
        for (int i = 0; i < memberList.size(); i++) {
            Member member = (Member) memberList.get(i);
            if (member.isCalculated()) {
                memberList.remove(i);
                --i;
            }
        }
    }

    /**
     * Returns whether <code>m0</code> is an ancestor of <code>m1</code>.
     *
     * @param strict if true, a member is not an ancestor of itself
     */
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
     * For each member in a list, evaluate an expression and create a map
     * from members to values.
     *
     * <p>If the list contains tuples, use
     * {@link #evaluateTuples(mondrian.olap.Evaluator, mondrian.calc.Calc, java.util.List)}.
     *
     * @param evaluator Evaluation context
     * @param exp Expression to evaluate
     * @param members List of members
     * @param parentsToo If true, evaluate the expression for all ancestors
     *            of the members as well
     *
     * @pre exp != null
     * @pre exp.getType() instanceof ScalarType
     */
    static Map<Member, Object> evaluateMembers(
            Evaluator evaluator,
            Calc exp,
            List<Member> members,
            boolean parentsToo) {
        // RME
        evaluator= evaluator.push();

        assert exp.getType() instanceof ScalarType;
        Map<Member, Object> mapMemberToValue = new HashMap<Member, Object>();
        for (int i = 0, count = members.size(); i < count; i++) {
            Member member = members.get(i);
            while (true) {
                evaluator.setContext(member);
                Object result = exp.evaluate(evaluator);
                if (result == null) {
                    result = Util.nullValue;
                }
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

    /**
     * For each tuple in a list, evaluates an expression and creates a map
     * from tuples to values.
     *
     * @param evaluator Evaluation context
     * @param exp Expression to evaluate
     * @param members List of members (or List of Member[] tuples)
     *
     * @pre exp != null
     * @pre exp.getType() instanceof ScalarType
     */
    static Map<Object, Object> evaluateTuples(
            Evaluator evaluator,
            Calc exp,
            List<Member[]> members) {
        // RME
        evaluator= evaluator.push();

        assert exp.getType() instanceof ScalarType;
        Map<Object, Object> mapMemberToValue = new HashMap<Object, Object>();
        for (int i = 0, count = members.size(); i < count; i++) {
            Member[] tuples = members.get(i);
            evaluator.setContext(tuples);
            Object result = exp.evaluate(evaluator);
            if (result == null) {
                result = Util.nullValue;
            }
            mapMemberToValue.put(new Member.ArrayEquals(tuples), result);
        }
        return mapMemberToValue;
    }

    static Map<Member, Object> evaluateMembers(
            Evaluator evaluator,
            List<Member> members,
            boolean parentsToo) {
        Map<Member, Object> mapMemberToValue = new HashMap<Member, Object>();
        for (int i = 0, count = members.size(); i < count; i++) {
            Member member = members.get(i);
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

    /**
     * Helper function to sortMembers a list of members according to an expression.
     *
     * <p>NOTE: This function does not preserve the contents of the validator.
     */
    static void sortMembers(
            Evaluator evaluator,
            List<Member> members,
            Calc exp,
            boolean desc,
            boolean brk) {
        if (members.isEmpty()) {
            return;
        }
        Object first = members.get(0);
        Comparator comparator;
        Map<Member, Object> mapMemberToValue;
        if (first instanceof Member) {
            final boolean parentsToo = !brk;
            mapMemberToValue = evaluateMembers(evaluator, exp, members, parentsToo);
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
        if (debug) {
            final PrintWriter pw = new PrintWriter(System.out);
            for (int i = 0; i < members.size(); i++) {
                Object o = members.get(i);
                pw.print(i);
                pw.print(": ");
                if (mapMemberToValue != null) {
                    pw.print(mapMemberToValue.get(o));
                    pw.print(": ");
                }
                pw.println(o);
            }
            pw.flush();
        }
    }

    /**
     * Helper function to sortMembers a list of members according to an expression.
     *
     * <p>NOTE: This function does not preserve the contents of the validator.
     */
    static void sortTuples(
        Evaluator evaluator,
        List<Member[]> tuples,
        Calc exp,
        boolean desc,
        boolean brk,
        int arity)
    {
        if (tuples.isEmpty()) {
            return;
        }
        Comparator comparator;
        if (brk) {
            comparator = new BreakArrayComparator(evaluator, exp, arity);
            if (desc) {
                comparator = new ReverseComparator(comparator);
            }
        } else {
            comparator = new HierarchicalArrayComparator(evaluator, exp, arity, desc);
        }
        Collections.sort(tuples, comparator);
        if (debug) {
            final PrintWriter pw = new PrintWriter(System.out);
            for (int i = 0; i < tuples.size(); i++) {
                Object o = tuples.get(i);
                pw.print(i);
                pw.print(": ");
                pw.println(o);
            }
            pw.flush();
        }
    }

    public static void hierarchize(List members, boolean post) {
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

    /**
     * Compares double-precision values according to MDX semantics.
     *
     * <p>MDX requires a total order:
     * <pre>
     *    -inf &lt; NULL &lt; ... &lt; -1 &lt; ... &lt; 0 &lt; ... &lt; NaN &lt; +inf
     * </pre>
     * but this is different than Java semantics, specifically with regard
     * to {@link Double#NaN}.
     */
    static int compareValues(double d1, double d2) {
        if (Double.isNaN(d1)) {
            if (d2 == Double.POSITIVE_INFINITY) {
                return -1;
            } else if (Double.isNaN(d2)) {
                return 0;
            } else {
                return 1;
            }
        } else if (Double.isNaN(d2)) {
            if (d1 == Double.POSITIVE_INFINITY) {
                return 1;
            } else {
                return -1;
            }
        } else if (d1 == d2) {
            return 0;
        } else if (d1 == FunUtil.DoubleNull) {
            if (d2 == Double.NEGATIVE_INFINITY) {
                return 1;
            } else {
                return -1;
            }
        } else if (d2 == FunUtil.DoubleNull) {
            if (d1 == Double.NEGATIVE_INFINITY) {
                return -1;
            } else {
                return 1;
            }
        } else if (d1 < d2) {
            return -1;
        } else {
            return 1;
        }
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
    static void toPercent(List members, Map mapMemberToValue, boolean isMember) {
        double total = 0;
        int memberCount = members.size();
        for (int i = 0; i < memberCount; i++) {
            Object o = (isMember)
                    ? mapMemberToValue.get(members.get(i))
                    : mapMemberToValue.get(
                        new Member.ArrayEquals(members.get(i)));
            if (o instanceof Number) {
                total += ((Number) o).doubleValue();
            }
        }
        for (int i = 0; i < memberCount; i++) {
            Object mo = members.get(i);
            Object o = (isMember)
                    ? mapMemberToValue.get(mo)
                    : mapMemberToValue.get(new Member.ArrayEquals(mo));
            if (o instanceof Number) {
                double d = ((Number) o).doubleValue();
                if (isMember) {
                    mapMemberToValue.put(
                        mo,
                        d / total * (double) 100);
                } else {
                    mapMemberToValue.put(
                        new Member.ArrayEquals(mo),
                        d / total * (double) 100);
                }
            }
        }
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
        case 'Q':
            return Syntax.Postfix;
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
    public static int decodeReturnCategory(String flags) {
        final int returnCategory = decodeCategory(flags, 1);
        if ((returnCategory & Category.Mask) != returnCategory) {
            throw newInternal("bad return code flag in flags '" + flags + "'");
        }
        return returnCategory;
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
    public static int decodeCategory(String flags, int offset) {
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
        case 'U':
            return Category.Null;
        default:
            throw newInternal(
                    "unknown type code '" + c + "' in string '" + flags + "'");
        }
    }

    /**
     * Decodes a string of parameter types into an array of type codes.
     *
     * <p>Each character is decoded using {@link #decodeCategory(String, int)}.
     * For example, <code>decodeParameterTypes("nx")</code> returns
     * <code>{{@link Category#Numeric}, {@link Category#Set}}</code>.
     *
     * @param flags The signature of an operator,
     *   as used by the <code>flags</code> parameter used to construct a
     *   {@link FunDefBase}.
     *
     * @return An array {@link Category} codes.
     */
    public static int[] decodeParameterCategories(String flags) {
        int[] parameterCategories = new int[flags.length() - 2];
        for (int i = 0; i < parameterCategories.length; i++) {
            parameterCategories[i] = decodeCategory(flags, i + 2);
        }
        return parameterCategories;
    }

    /**
     * Sorts an array of values.
     */
    public static void sortValuesDesc(Object[] values) {
        Arrays.sort(values, DescendingValueComparator.instance);
    }

    /**
     * Binary searches an array of values.
     */
    public static int searchValuesDesc(Object[] values, Object value) {
        return Arrays.binarySearch(
                values, value, DescendingValueComparator.instance);
    }

    static Object median(Evaluator evaluator, List members, Calc exp) {
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
    protected static double quartile(
            Evaluator evaluator,
            List members,
            Calc exp,
            int range) {
        Util.assertPrecondition(range >= 1 && range <= 3, "range >= 1 && range <= 3");

        SetWrapper sw = evaluateSet(evaluator, members, exp);
        if (sw.errorCount > 0) {
            return Double.NaN;
        } else if (sw.v.size() == 0) {
            return DoubleNull;
        }

        double[] asArray = new double[sw.v.size()];
        for (int i = 0; i < asArray.length; i++) {
            asArray[i] = ((Double) sw.v.get(i)).doubleValue();
        }

        Arrays.sort(asArray);
        // get a quartile, median is a second q
        double dm = (asArray.length * range) / 4;
        int median = (int) Math.floor(dm);
        return dm == median && median < asArray.length - 1 ?
                (asArray[median] + asArray[median + 1]) / 2 :
                asArray[median];
    }

    public static Object min(Evaluator evaluator, List members, Calc calc) {
        SetWrapper sw = evaluateSet(evaluator, members, calc);
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

    public static Object max(Evaluator evaluator, List members, Calc exp) {
        SetWrapper sw = evaluateSet(evaluator, members, exp);
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

    static Object var(
            Evaluator evaluator,
            List members,
            Calc exp,
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

    static double correlation(
            Evaluator evaluator,
            List memberList,
            Calc exp1,
            Calc exp2) {
        SetWrapper sw1 = evaluateSet(evaluator, memberList, exp1);
        SetWrapper sw2 = evaluateSet(evaluator, memberList, exp2);
        Object covar = _covariance(sw1, sw2, false);
        Object var1 = _var(sw1, false); //this should be false, yes?
        Object var2 = _var(sw2, false);
        if ((covar instanceof Double) &&
            (var1 instanceof Double) &&
            (var2 instanceof Double)) {
            return ((Double) covar).doubleValue() /
                    Math.sqrt(((Double) var1).doubleValue() *
                    ((Double) var2).doubleValue());
        } else {
            return DoubleNull;
        }
    }

    static Object covariance(Evaluator evaluator, List members,
            Calc exp1, Calc exp2, boolean biased) {
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

    static Object stdev(
            Evaluator evaluator,
            List members,
            Calc exp,
            boolean biased) {
        Object o = var(evaluator, members, exp, biased);
        return (o instanceof Double)
            ? new Double(Math.sqrt(((Double) o).doubleValue()))
            : o;
    }

    public static Object avg(Evaluator evaluator, List members, Calc calc) {
        SetWrapper sw = evaluateSet(evaluator, members, calc);
        return (sw.errorCount > 0) ?
                new Double(Double.NaN) :
                (sw.v.size() == 0) ?
                Util.nullValue :
                new Double(_avg(sw));
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

    public static Object sum(Evaluator evaluator, List members, Calc exp) {
        double d = sumDouble(evaluator, members, exp);
        return d == DoubleNull ? Util.nullValue : new Double(d);
    }

    public static double sumDouble(Evaluator evaluator, List members, Calc exp) {
        SetWrapper sw = evaluateSet(evaluator, members, exp);
        if (sw.errorCount > 0) {
            return Double.NaN;
        } else if (sw.v.size() == 0) {
            return DoubleNull;
        } else {
            double sum = 0.0;
            for (int i = 0; i < sw.v.size(); i++) {
                sum += ((Double) sw.v.get(i)).doubleValue();
            }
            return sum;
        }
    }
    public static double sumDouble(Evaluator evaluator, Iterable iterable, Calc exp) {
        SetWrapper sw = evaluateSet(evaluator, iterable, exp);
        if (sw.errorCount > 0) {
            return Double.NaN;
        } else if (sw.v.size() == 0) {
            return DoubleNull;
        } else {
            double sum = 0.0;
            for (int i = 0; i < sw.v.size(); i++) {
                sum += ((Double) sw.v.get(i)).doubleValue();
            }
            return sum;
        }
    }
    public static int count(
            Evaluator evaluator,
            Iterable iterable,
            boolean includeEmpty) {
        if (iterable == null) {
            return 0;
        }
        if (includeEmpty) {
            if (iterable instanceof Collection) {
                return ((Collection) iterable).size();
            } else {
                int retval = 0;
                Iterator it = iterable.iterator();
                while (it.hasNext()) {
                    // must get the next one
                    it.next();
                    retval++;
                }
                return retval;
            }
        } else {
            int retval = 0;
            for (Object object: iterable) {
                if (object instanceof Member) {
                    evaluator.setContext((Member) object);
                } else {
                    evaluator.setContext((Member[]) object);
                }
                Object o = evaluator.evaluateCurrent();
                if (o != Util.nullValue && o != null) {
                    retval++;
                }
            }
            return retval;
        }
    }

/*
    public static int countOld(
            Evaluator evaluator,
            List members,
            boolean includeEmpty) {
        if (members == null) {
System.out.println("FunUtil.count List: null 0");
            return 0;
        }
        if (includeEmpty) {
System.out.println("FunUtil.count List: "+members.size());
            return members.size();
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
System.out.println("FunUtil.count List: "+retval);
            return retval;
        }
    }
    public static int countIterable(
            Evaluator evaluator,
            Iterable iterable,
            boolean includeEmpty) {
        if (iterable == null) {
System.out.println("FunUtil.countIterable Iterable: null 0");
            return 0;
        }
        int retval = 0;
        Iterator it = iterable.iterator();
        while (it.hasNext()) {
            final Object member = it.next();
            if (member instanceof Member) {
                evaluator.setContext((Member) member);
            } else if (member instanceof Member[]) {
                evaluator.setContext((Member[]) member);
            }
            if (includeEmpty) {
                retval++;
            } else {
                Object o = evaluator.evaluateCurrent();
                if (o != Util.nullValue && o != null) {
                    retval++;
                }
            }
        }
System.out.println("FunUtil.countIterable Iterable: "+retval);
        return retval;
    }
*/

    /**
     * Evaluates <code>exp</code> (if defined) over <code>members</code> to
     * generate a {@link List} of {@link SetWrapper} objects, which contains
     * a {@link Double} value and meta information, unlike
     * {@link #evaluateMembers}, which only produces values.
     *
     * @pre exp != null
     */
    static SetWrapper evaluateSet(
            Evaluator evaluator,
            Iterable members,
            Calc calc) {
        Util.assertPrecondition(members != null, "members != null");
        Util.assertPrecondition(calc != null, "calc != null");
        Util.assertPrecondition(calc.getType() instanceof ScalarType);

        // todo: treat constant exps as evaluateMembers() does
        SetWrapper retval = new SetWrapper();
        for (Iterator it = members.iterator(); it.hasNext();) {
            Object obj = it.next();
            if (obj instanceof Member[]) {
                evaluator.setContext((Member[])obj);
            } else {
                evaluator.setContext((Member)obj);
            }
            Object o = calc.evaluate(evaluator);
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
                retval.v.add(((Number) o).doubleValue());
            } else {
                retval.v.add(o);
            }
        }
        return retval;
    }

    /**
     * Evaluates one or more expressions against the member list returning
     * a SetWrapper array. Where this differs very significantly from the
     * above evaluateSet methods is how it count null values and Throwables;
     * this method adds nulls to the SetWrapper Vector rather than not adding
     * anything - as the above method does. The impact of this is that if, for
     * example, one was creating a list of x,y values then each list will have
     * the same number of values (though some might be null) - this allows
     * higher level code to determine how to handle the lack of data rather than
     * having a non-equal number (if one is plotting x,y values it helps to
     * have the same number and know where a potential gap is the data is.
     */
    static SetWrapper[] evaluateSet(
            Evaluator evaluator,
            List members,
            DoubleCalc[] calcs,
            boolean isTuples) {
        Util.assertPrecondition(calcs != null, "calcs != null");

        // todo: treat constant exps as evaluateMembers() does
        SetWrapper[] retvals = new SetWrapper[calcs.length];
        for (int i = 0; i < calcs.length; i++) {
            retvals[i] = new SetWrapper();
        }
        for (int j = 0; j < members.size(); j++) {
            if (isTuples) {
                evaluator.setContext((Member[]) members.get(j));
            } else {
                evaluator.setContext((Member) members.get(j));
            }
            for (int i = 0; i < calcs.length; i++) {
                DoubleCalc calc = calcs[i];
                SetWrapper retval = retvals[i];
                double o = calc.evaluateDouble(evaluator);
                if (o == FunUtil.DoubleNull) {
                    retval.nullCount++;
                    retval.v.add(null);
                } else {
                    retval.v.add(o);
                }
                // TODO: If the expression yielded an error, carry on
                // summing, so that if we are running in a
                // BatchingCellReader, we find out all the dependent cells
                // we need
            }
        }
        return retvals;
    }

    static List<Member> periodsToDate(
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
        List<Member> members = new ArrayList<Member>();
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

    static List<Member> memberRange(
        Evaluator evaluator,
        Member startMember,
        Member endMember)
    {
        final Level level = startMember.getLevel();
        assertTrue(level == endMember.getLevel());
        List<Member> members = new ArrayList<Member>();
        evaluator.getSchemaReader().getMemberRange(
            level, startMember, endMember, members);

        if (members.isEmpty()) {
            // The result is empty, so maybe the members are reversed. This is
            // cheaper than comparing the members before we call getMemberRange.
            evaluator.getSchemaReader().getMemberRange(
                level, endMember, startMember, members);
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
            throw MondrianResource.instance().CousinHierarchyMismatch.ex(
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
            throw MondrianResource.instance().MemberNotInLevelHierarchy.ex(
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
     * @param post Whether to sortMembers in postfix order. If true, a parent will
     *   sortMembers immediately after its last child. If false, a parent will sortMembers
     *   immediately before its first child.
     * @return -1 if m1 collates before m2,
     *   0 if m1 equals m2,
     *   1 if m1 collates after m2
     */
    public static int compareHierarchically(
            Member m1,
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
    public static int compareSiblingMembers(Member m1, Member m2) {
        // calculated members collate after non-calculated
        final boolean calculated1 = m1.isCalculatedInQuery();
        final boolean calculated2 = m2.isCalculatedInQuery();
        if (calculated1) {
            if (!calculated2) {
                return 1;
            }
        } else {
            if (calculated2) {
                return -1;
            }
        }
        final int ordinal1 = m1.getOrdinal();
        final int ordinal2 = m2.getOrdinal();
        return (ordinal1 == ordinal2) ?
                m1.compareTo(m2) :
                (ordinal1 < ordinal2) ?
                -1 :
                1;
    }

    /**
     * Returns whether we can convert an argument to a parameter tyoe.
     * @param fromExp argument type
     * @param to   parameter type
     * @param conversionCount in/out count of number of conversions performed;
     *             is incremented if the conversion is non-trivial (for
     *             example, converting a member to a level).
     */
    public static boolean canConvert(
            Exp fromExp,
            int to,
            int[] conversionCount) {
        int from = fromExp.getCategory();
        if (from == to) {
            return true;
        }
        switch (from) {
        case Category.Array:
            return false;
        case Category.Dimension:
            // Seems funny that you can 'downcast' from a dimension, doesn't
            // it? But we add an implicit 'CurrentMember', for example,
            // '[Time].PrevMember' actually means
            // '[Time].CurrentMember.PrevMember'.
            switch (to) {
            case Category.Member:
            case Category.Tuple:
                // It's easier to convert dimension to member than dimension
                // to hierarchy or level.
                conversionCount[0]++;
                return true;
            case Category.Hierarchy:
            case Category.Level:
                conversionCount[0] += 2;
                return true;
            default:
                return false;
            }
        case Category.Hierarchy:
            switch (to) {
            case Category.Dimension:
            case Category.Member:
            case Category.Tuple:
                conversionCount[0]++;
                return true;
            default:
                return false;
            }
        case Category.Level:
            switch (to) {
            case Category.Dimension:
                // It's more difficult to convert to a dimension than a
                // hierarchy. For example, we want '[Store City].CurrentMember'
                // to resolve to <Hierarchy>.CurrentMember rather than
                // <Dimension>.CurrentMember.
                conversionCount[0] += 2;
                return true;
            case Category.Hierarchy:
                conversionCount[0]++;
                return true;
            default:
                return false;
            }
        case Category.Logical:
            return false;
        case Category.Member:
            switch (to) {
            case Category.Dimension:
            case Category.Hierarchy:
            case Category.Level:
            case Category.Tuple:
                conversionCount[0]++;
                return true;
            case (Category.Numeric | Category.Expression):
                // We assume that members are numeric, so a cast to a numeric
                // expression is less expensive than a conversion to a string
                // expression.
                conversionCount[0]++;
                return true;
            case Category.Value:
            case (Category.String | Category.Expression):
                conversionCount[0] += 2;
                return true;
            default:
                return false;
            }
        case Category.Numeric | Category.Constant:
            return to == Category.Value ||
                to == Category.Numeric;
        case Category.Numeric:
            return to == Category.Value ||
                to == Category.Integer ||
                to == (Category.Integer | Category.Constant) ||
                to == (Category.Numeric | Category.Constant);
        case Category.Integer:
            return to == Category.Value ||
                to == (Category.Integer | Category.Constant) ||
                to == Category.Numeric ||
                to == (Category.Numeric | Category.Constant);
        case Category.Set:
            return false;
        case Category.String | Category.Constant:
            return to == Category.Value ||
                to == Category.String;
        case Category.String:
            return to == Category.Value ||
                to == (Category.String | Category.Constant);
        case Category.Tuple:
            return to == Category.Value ||
                to == Category.Numeric;
        case Category.Value:
            return false;
        case Category.Symbol:
            return false;
        case Category.Null:
            return  to == Category.Numeric;
        default:
            throw newInternal("unknown category " + from);
        }
    }

    /**
     * Returns whether one of the members in a tuple is null.
     */
    static boolean tupleContainsNullMember(Member[] tuple) {
        for (int i = 0; i < tuple.length; i++) {
            Member member = tuple[i];
            if (member.isNull()) {
                return true;
            }
        }
        return false;
    }

    public static Member[] makeNullTuple(final TupleType tupleType) {
        Member[] members = new Member[tupleType.elementTypes.length];
        for (int i = 0; i < tupleType.elementTypes.length; i++) {
            MemberType type = (MemberType) tupleType.elementTypes[i];
            members[i] = makeNullMember(type);
        }
        return members;
    }

    static Member makeNullMember(
            MemberType memberType) {
        Hierarchy hierarchy = memberType.getHierarchy();
        if (hierarchy == null) {
            return NullMember;
        }
        return hierarchy.getNullMember();
    }

    /**
     * Validates the arguments to a function and resolves the function.
     *
     * @param validator validator used to validate function arguments and
     * resolve the function
     * @param args arguments to the function
     * @param newArgs returns the resolved arguments to the function
     * @param name function name
     * @param syntax syntax style used to invoke function
     *
     * @return resolved function definition
     */
    public static FunDef resolveFunArgs(
        Validator validator, Exp[] args, Exp[] newArgs, String name,
        Syntax syntax) {

        Query query = validator.getQuery();
        Cube cube = null;
        if (query != null) {
            cube = query.getCube();
        }
        for (int i = 0; i < args.length; i++) {
            newArgs[i] = validator.validate(args[i], false);
        }
        final FunTable funTable = validator.getFunTable();
        FunDef funDef = funTable.getDef(newArgs, validator, name, syntax);

        // If the first argument to a function is either:
        // 1) the measures dimension or
        // 2) a measures member where the function returns another member or
        //    a set,
        // then these are functions that dynamically return one or more
        // members ofthe measures dimension.  In that case, we cannot use
        // native cross joins because the functions need to be executed to
        // determine the resultant measures.
        //
        // As a result, we disallow functions like AllMembers applied on the
        // Measures dimension as well as functions like the range operator,
        // siblings, and lag, when the argument is a measure member.
        // However, we do allow functions like isEmpty, rank, and topPercent.
        // Also, the set function is ok since it just enumerates its
        // arguments.
        if (!(funDef instanceof SetFunDef) && query != null &&
            query.nativeCrossJoinVirtualCube())
        {
            int[] paramCategories = funDef.getParameterCategories();
            if (paramCategories.length > 0 &&
                ((paramCategories[0] == Category.Dimension &&
                    newArgs[0] instanceof DimensionExpr &&
                    ((DimensionExpr) newArgs[0]).getDimension().
                        getOrdinal(cube) == 0) ||
                (paramCategories[0] == Category.Member &&
                    newArgs[0] instanceof MemberExpr &&
                    ((MemberExpr) newArgs[0]).getMember().getDimension().
                        getOrdinal(cube) == 0 &&
                    (funDef.getReturnCategory() == Category.Member ||
                        funDef.getReturnCategory() == Category.Set))))
            {
                query.setVirtualCubeNonNativeCrossJoin();
            }
        }

        return funDef;
    }

    static void appendTuple(StringBuilder buf, Member[] members) {
        buf.append("(");
        for (int j = 0; j < members.length; j++) {
            if (j > 0) {
                buf.append(", ");
            }
            Member member = members[j];
            buf.append(member.getUniqueName());
        }
        buf.append(")");
    }

    /**
     * Returns whether two tuples are equal.
     *
     * <p>The members are allowed to be in different positions. For example,
     * <blockquote>
     * <code>([Gender].[M], [Store].[USA]) IS ([Store].[USA], [Gender].[M])</code>
     * </blockquote>
     * returns <code>true</code>.
     */
    static boolean equalTuple(Member[] members0, Member[] members1) {
        final int count = members0.length;
        if (count != members1.length) {
            return false;
        }
        outer:
        for (int i = 0; i < count; i++) {
            // First check the member at the corresponding ordinal. It is more
            // likely to be there.
            final Member member0 = members0[i];
            if (member0.equals(members1[i])) {
                continue;
            }
            // Look for this member in other positions.
            // We can assume that the members in members0 are distinct (because
            // they belong to different dimensions), so this test is valid.
            for (int j = 0; j < count; j++) {
                if (i != j && member0.equals(members1[j])) {
                    continue outer;
                }
            }
            // This member of members0 does not occur in any position of
            // members1. The tuples are not equal.
            return false;
        }
        return true;
    }

    static FunDef createDummyFunDef(
        Resolver resolver,
        int returnCategory,
        Exp[] args)
    {
        final int[] argCategories = ExpBase.getTypes(args);
        return new FunDefBase(resolver, returnCategory, argCategories) {};
    }

    // Inner classes


    private static abstract class MemberComparator implements Comparator {
        private static final Logger LOGGER =
                Logger.getLogger(MemberComparator.class);
        Map<Member, Object> mapMemberToValue;
        private boolean desc;

        MemberComparator(Map<Member, Object> mapMemberToValue, boolean desc) {
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
                        // Needed to have a consistent sortMembers if members with equal (null!)
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
        HierarchicalMemberComparator(Map<Member, Object> mapMemberToValue, boolean desc) {
            super(mapMemberToValue, desc);
        }

        protected int compareInternal(Member m1, Member m2) {
            return compareHierarchicallyButSiblingsByValue(m1, m2);
        }
    }

    private static class BreakMemberComparator extends MemberComparator {
        BreakMemberComparator(Map<Member, Object> mapMemberToValue, boolean desc) {
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
            StringBuilder sb = new StringBuilder();
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
        final Calc calc;

        ArrayExpComparator(Evaluator evaluator, Calc calc, int arity) {
            super(arity);
            this.evaluator = evaluator;
            this.calc = calc;
        }

    }

    private static class HierarchicalArrayComparator
            extends ArrayExpComparator {
        private final boolean desc;

        HierarchicalArrayComparator(
                Evaluator evaluator, Calc calc, int arity, boolean desc) {
            super(evaluator, calc, arity);
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
            Object v1 = calc.evaluate(evaluator);
            evaluator.setContext(m2);
            Object v2 = calc.evaluate(evaluator);
            // important to restore the evaluator state -- and this is faster
            // than calling evaluator.push()
            evaluator.setContext(old);
            c = FunUtil.compareValues(v1, v2);
            return c;
        }
    }

    private static class BreakArrayComparator extends ArrayExpComparator {
        BreakArrayComparator(Evaluator evaluator, Calc calc, int arity) {
            super(evaluator, calc, arity);
        }

        protected int compare(Member[] a1, Member[] a2) {
            evaluator.setContext(a1);
            Object v1 = calc.evaluate(evaluator);
            evaluator.setContext(a2);
            Object v2 = calc.evaluate(evaluator);
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

    /**
     * Null member of unknown hierarchy.
     */
    private static class NullMember implements Member {
        public Member getParentMember() {
            throw new UnsupportedOperationException();
        }

        public Level getLevel() {
            throw new UnsupportedOperationException();
        }

        public Hierarchy getHierarchy() {
            throw new UnsupportedOperationException();
        }

        public String getParentUniqueName() {
            throw new UnsupportedOperationException();
        }

        public MemberType getMemberType() {
            throw new UnsupportedOperationException();
        }

        public void setName(String name) {
            throw new UnsupportedOperationException();
        }

        public boolean isAll() {
            return false;
        }

        public boolean isMeasure() {
            throw new UnsupportedOperationException();
        }

        public boolean isNull() {
            return true;
        }

        public boolean isChildOrEqualTo(Member member) {
            throw new UnsupportedOperationException();
        }

        public boolean isCalculated() {
            throw new UnsupportedOperationException();
        }

        public int getSolveOrder() {
            throw new UnsupportedOperationException();
        }

        public Exp getExpression() {
            throw new UnsupportedOperationException();
        }

        public Member[] getAncestorMembers() {
            throw new UnsupportedOperationException();
        }

        public boolean isCalculatedInQuery() {
            throw new UnsupportedOperationException();
        }

        public Object getPropertyValue(String propertyName) {
            throw new UnsupportedOperationException();
        }

        public Object getPropertyValue(String propertyName, boolean matchCase) {
            throw new UnsupportedOperationException();
        }

        public String getPropertyFormattedValue(String propertyName) {
            throw new UnsupportedOperationException();
        }

        public void setProperty(String name, Object value) {
            throw new UnsupportedOperationException();
        }

        public Property[] getProperties() {
            throw new UnsupportedOperationException();
        }

        public int getOrdinal() {
            throw new UnsupportedOperationException();
        }

        public boolean isHidden() {
            throw new UnsupportedOperationException();
        }

        public int getDepth() {
            throw new UnsupportedOperationException();
        }

        public Member getDataMember() {
            throw new UnsupportedOperationException();
        }

        public String getUniqueName() {
            throw new UnsupportedOperationException();
        }

        public String getName() {
            throw new UnsupportedOperationException();
        }

        public String getDescription() {
            throw new UnsupportedOperationException();
        }

        public OlapElement lookupChild(SchemaReader schemaReader, String s) {
            throw new UnsupportedOperationException();
        }

        public OlapElement lookupChild(
            SchemaReader schemaReader, String s, MatchType matchType) {
            throw new UnsupportedOperationException();
        }

        public String getQualifiedName() {
            throw new UnsupportedOperationException();
        }

        public String getCaption() {
            throw new UnsupportedOperationException();
        }

        public Dimension getDimension() {
            throw new UnsupportedOperationException();
        }

        public int compareTo(Object o) {
            throw new UnsupportedOperationException();
        }
    }

    public static void main(String[] args) {
        System.out.println("done");
    }
}

// End FunUtil.java
