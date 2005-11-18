/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.olap.type.DimensionType;
import mondrian.resource.MondrianResource;

import java.util.*;
import java.io.PrintWriter;

/**
 * Abstract implementation of {@link FunTable}.
 *
 * <p>The derived class must implement {@link #defineFunctions()} to define
 * each function which will be recognized by this table. This method is called
 * from the constructor, after which point, no further functions can be added.
 */
public abstract class FunTableImpl implements FunTable {
    /**
     * Maps the upper-case name of a function plus its
     * {@link mondrian.olap.Syntax} to an array of
     * {@link mondrian.olap.Validator} objects for that name.
     */
    protected final Map mapNameToResolvers = new HashMap();
    private final HashSet reservedWords = new HashSet();
    private final HashSet propertyWords = new HashSet();
    protected static final Resolver[] emptyResolverArray = new Resolver[0];
    /** used during initialization **/
    protected final List resolvers = new ArrayList();
    protected final List funInfoList = new ArrayList();

    protected FunTableImpl() {
    }

    /**
     * Initializes the function table.
     */
    public void init() {
        defineFunctions();
        organizeFunctions();
    }

    protected static String makeResolverKey(String name, Syntax syntax) {
        return name.toUpperCase() + "$" + syntax;
    }

    protected void define(FunDef funDef) {
        define(new SimpleResolver(funDef));
    }

    protected void define(Resolver resolver) {
        addFunInfo(resolver);
        if (resolver.getSyntax() == Syntax.Property) {
            defineProperty(resolver.getName());
        }
        resolvers.add(resolver);
        final String[] reservedWords = resolver.getReservedWords();
        for (int i = 0; i < reservedWords.length; i++) {
            String reservedWord = reservedWords[i];
            defineReserved(reservedWord);
        }
    }

    protected void addFunInfo(Resolver resolver) {
        this.funInfoList.add(FunInfo.make(resolver));
    }

    public Exp createValueFunCall(Exp exp, Validator validator) {
        final Type type = exp.getTypeX();
        if (type instanceof ScalarType) {
            return exp;
        }
        if (!TypeUtil.canEvaluate(type)) {
            String exprString = Util.unparse(exp);
            throw MondrianResource.instance().MdxMemberExpIsSet.ex(exprString);
        }
        if (type instanceof MemberType) {
            return new MemberScalarExp(exp);
        } else if (type instanceof DimensionType ||
                type instanceof HierarchyType) {
            exp = new FunCall(
                    "CurrentMember",
                    Syntax.Property,
                    new Exp[]{exp});
            exp = exp.accept(validator);
            return new MemberScalarExp(exp);
        } else if (type instanceof TupleType) {
            if (exp instanceof FunCall) {
                FunCall call = (FunCall) exp;
                if (call.getFunDef() instanceof TupleFunDef) {
                    return new MemberListScalarExp(call.getArgs());
                }
            }
            return new TupleScalarExp(exp);
        } else {
            throw Util.newInternal("Unknown type " + type);
        }
    }

    /**
     * Creates an expression which will yield the current value of the current
     * measure.
     */
    static Exp createValueFunCall() {
        return new ScalarExp();
    }

    public FunDef getDef(FunCall call, Validator validator) {
        String key = makeResolverKey(call.getFunName(), call.getSyntax());

        // Resolve function by its upper-case name first.  If there is only one
        // function with that name, stop immediately.  If there is more than
        // function, use some custom method, which generally involves looking
        // at the type of one of its arguments.
        String signature = call.getSyntax().getSignature(call.getFunName(),
                Category.Unknown, ExpBase.getTypes(call.getArgs()));
        Resolver[] resolvers = (Resolver[]) mapNameToResolvers.get(key);
        if (resolvers == null) {
            resolvers = emptyResolverArray;
        }

        int[] conversionCount = new int[] {0};
        int minConversions = Integer.MAX_VALUE;
        int matchCount = 0;
        FunDef matchDef = null;
        for (int i = 0; i < resolvers.length; i++) {
            conversionCount[0] = 0;
            FunDef def = resolvers[i].resolve(
                    call.getArgs(), validator, conversionCount);
            if (def != null) {
                int conversions = conversionCount[0];
                if (conversions < minConversions) {
                    minConversions = conversions;
                    matchCount = 1;
                    matchDef = def;
                } else if (conversions == minConversions) {
                    matchCount++;
                } else {
                    // ignore this match -- it required more coercions than
                    // other overloadings we've seen
                }
            }
        }
        switch (matchCount) {
        case 0:
            throw MondrianResource.instance().NoFunctionMatchesSignature.ex(
                    signature);
        case 1:
            final String matchKey = makeResolverKey(matchDef.getName(),
                    matchDef.getSyntax());
            Util.assertTrue(matchKey.equals(key), matchKey);
            return matchDef;
        default:
            throw MondrianResource.instance().MoreThanOneFunctionMatchesSignature.ex(signature);
        }
    }

    public boolean requiresExpression(
            FunCall call,
            int k,
            Validator validator) {
        final FunDef funDef = call.getFunDef();
        if (funDef != null) {
            final int[] parameterTypes = funDef.getParameterTypes();
            return parameterTypes[k] != Category.Set;
        }
        // The function call has not been resolved yet. In fact, this method
        // may have been invoked while resolving the child. Consider this:
        //   CrossJoin([Measures].[Unit Sales] * [Measures].[Store Sales])
        //
        // In order to know whether to resolve '*' to the multiplication
        // operator (which returns a scalar) or the crossjoin operator (which
        // returns a set) we have to know what kind of expression is expected.
        String key = makeResolverKey(call.getFunName(), call.getSyntax());
        Resolver[] resolvers = (Resolver[]) mapNameToResolvers.get(key);
        if (resolvers == null) {
            resolvers = emptyResolverArray;
        }
        for (int i = 0; i < resolvers.length; i++) {
            Resolver resolver2 = resolvers[i];
            if (!resolver2.requiresExpression(k)) {
                // This resolver accepts a set in this argument position,
                // therefore we don't REQUIRE a scalar expression.
                return false;
            }
        }
        return true;
    }

    public List getReservedWords() {
        return new ArrayList(reservedWords);
    }

    public boolean isReserved(String s) {
        return reservedWords.contains(s.toUpperCase());
    }

    /**
     * Defines a reserved word.
     * @see #isReserved
     */
    protected void defineReserved(String s) {
        reservedWords.add(s.toUpperCase());
    }

    public List getResolvers() {
        final List list = new ArrayList();
        final Collection c = mapNameToResolvers.values();
        for (Iterator iterator = c.iterator(); iterator.hasNext();) {
            Resolver[] resolvers = (Resolver[]) iterator.next();
            for (int i = 0; i < resolvers.length; i++) {
                Resolver resolver = resolvers[i];
                list.add(resolver);
            }
        }
        return list;
    }

    public boolean isProperty(String s) {
        return propertyWords.contains(s.toUpperCase());
    }

    /**
     * Defines a word matching a property function name.
     * @see #isProperty
     */
    protected void defineProperty(String s) {
        propertyWords.add(s.toUpperCase());
    }

    public List getFunInfoList() {
        return Collections.unmodifiableList(this.funInfoList);
    }

    /**
     * Indexes the collection of functions.
     */
    protected void organizeFunctions() {
        Collections.sort(funInfoList);
        // Map upper-case function names to resolvers.
        for (int i = 0, n = resolvers.size(); i < n; i++) {
            Resolver resolver = (Resolver) resolvers.get(i);
            String key = makeResolverKey(resolver.getName(), resolver.getSyntax());
            final Object value = mapNameToResolvers.get(key);
            if (value instanceof Resolver[]) {
                continue; // has already been converted
            }
            List v2 = (List) value;
            if (v2 == null) {
                v2 = new ArrayList();
                mapNameToResolvers.put(key, v2);
            }
            v2.add(resolver);
        }
        // Convert the Lists into arrays.
        for (Iterator keys = mapNameToResolvers.keySet().iterator(); keys.hasNext();) {
            String key = (String) keys.next();
            final Object value = mapNameToResolvers.get(key);
            if (value instanceof Resolver[]) {
                continue; // has already been converted
            }
            List v2 = (List) value;
            mapNameToResolvers.put(key, v2.toArray(new Resolver[v2.size()]));
        }
    }

    /**
     * This method is called from the constructor, to define the set of
     * functions and reserved words recognized.
     *
     * <p>Each function is declared by calling {@link #define}. Each reserved
     * word is declared by calling {@link #defineReserved(String)}.
     *
     * <p>Derived class can override this method to add more functions.
     **/
    protected abstract void defineFunctions();

    /**
     * Wrapper which evaluates an expression to a tuple, sets the current
     * context from that tuple, and converts it to a scalar expression.
     */
    public static class TupleScalarExp extends ExpBase {
        private final Exp exp;

        public TupleScalarExp(Exp exp) {
            this.exp = exp;
            assert exp.getTypeX() instanceof TupleType;
        }

        public Object[] getChildren() {
            return new Object[] {exp};
        }

        public void unparse(PrintWriter pw) {
            exp.unparse(pw);
        }

        public Object clone() {
            return this;
        }

        public int getCategory() {
            return exp.getCategory();
        }

        public Type getTypeX() {
            return new ScalarType();
        }

        public Exp accept(Validator validator) {
            final Exp exp2 = validator.validate(exp, false);
            if (exp2 == exp) {
                //return this;
            }
            final FunTable funTable = validator.getFunTable();
            return funTable.createValueFunCall(exp2, validator);
        }

        public boolean dependsOn(Dimension dimension) {
            // The value at the current context by definition depends upon
            // all dimensions.
            return true;
        }

        public Object evaluate(Evaluator evaluator) {
            return exp.evaluateScalar(evaluator);
        }
    }

    /**
     * Wrapper which evaluates an expression to a dimensional context and
     * converts it to a scalar expression.
     */
    public static class MemberScalarExp extends ExpBase {
        private final Exp exp;

        public MemberScalarExp(Exp exp) {
            this.exp = exp;
        }

        public Object[] getChildren() {
            return new Object[] {exp};
        }

        public void unparse(PrintWriter pw) {
            exp.unparse(pw);
        }

        public Object clone() {
            return this;
        }

        public int getCategory() {
            return exp.getCategory();
        }

        public Type getTypeX() {
            return new ScalarType();
        }

        public Exp accept(Validator validator) {
            final Exp exp2 = validator.validate(exp, false);
            if (exp2 == exp) {
                return this;
            }
            final FunTable funTable = validator.getFunTable();
            return funTable.createValueFunCall(exp2, validator);
        }

        public boolean dependsOn(Dimension dimension) {
            // If the expression has type dimension
            // but does not depend on dimension
            // then this expression does not dimension on dimension.
            // Otherwise it depends on everything.
            final Type type = exp.getTypeX();
            if (type.usesDimension(dimension)) {
                return exp.dependsOn(dimension);
            } else {
                return true;
            }
        }

        public Object evaluate(Evaluator evaluator) {
            final Member member = (Member) exp.evaluate(evaluator);
            if (member == null ||
                    member.isNull()) {
                return null;
            }
            Member old = evaluator.setContext(member);
            Object value = evaluator.evaluateCurrent();
            evaluator.setContext(old);
            return value;
        }
    }

    /**
     * An expression which yields the current value of the current member.
     */
    public static class ScalarExp extends ExpBase {

        public ScalarExp() {
        }

        public void unparse(PrintWriter pw) {
            pw.print("$Value()");
        }

        public Object clone() {
            return this;
        }

        public int getCategory() {
            return Category.Numeric;
        }

        public Type getTypeX() {
            return new NumericType();
        }

        public Exp accept(Validator validator) {
            return this;
        }

        public boolean dependsOn(Dimension dimension) {
            // The value at the current context by definition depends upon
            // all dimensions.
            return true;
        }

        public Object evaluate(Evaluator evaluator) {
            return evaluator.evaluateCurrent();
        }
    }

    /**
     * An expression which evaluates a list of members, sets the context to
     * these members, then evaluates the current measure as a scalar
     * expression.
     *
     * <p>A typical expression which would be evaluated in this way is:
     * <blockquote><code>WITH MEMBER [Measures].[Female Sales] AS
     * ' ( [Measures].[Unit Sales], [Gender].[F] ) '</code></blockquote>
     *
     * @see TupleScalarExp
     */
    public static class MemberListScalarExp extends ExpBase {
        private final Exp[] exps;

        public MemberListScalarExp(Exp[] exps) {
            this.exps = exps;
            for (int i = 0; i < exps.length; i++) {
                assert exps[i].getTypeX() instanceof MemberType;
            }
        }

        public void unparse(PrintWriter pw) {
            unparseList(pw, exps, "(", ", ", ")");
        }

        public Object[] getChildren() {
            return exps;
        }

        public Object clone() {
            return this;
        }

        public int getCategory() {
            return Category.Numeric;
        }

        public Type getTypeX() {
            return new NumericType();
        }

        public Exp accept(Validator validator) {
            return this;
        }

        public boolean dependsOn(Dimension dimension) {
            // This expression depends upon dimension
            // if none of the sub-expressions returns a member of dimension
            // or if one of the sub-expressions is dependent upon dimension.
            //
            // Examples:
            //
            //   ( [Gender].[M], [Marital Status].CurrentMember )
            //
            // does not depend upon [Gender], because one of the members is
            // of the [Gender] dimension, yet none of the expressions depends
            // upon [Gender].
            //
            //   ( [Store].[USA], [Marital Status].CurrentMember )
            //
            // depends upon [Gender], because none of the members is of
            // the [Gender] dimension.
            boolean uses = false;
            for (int i = 0; i < exps.length; i++) {
                Exp exp = exps[i];
                if (exp.dependsOn(dimension)) {
                    return true;
                }
                final Type type = exp.getTypeX();
                if (type.usesDimension(dimension)) {
                    uses = true;
                }
            }
            return !uses;
        }

        public Object evaluate(Evaluator evaluator) {
            Evaluator evaluator2 = evaluator.push();
            for (int i = 0; i < exps.length; i++) {
                Exp exp = exps[i];
                final Member member = (Member) exp.evaluate(evaluator);
                evaluator2.setContext(member);
            }
            return evaluator2.evaluateCurrent();
        }
    }
}

// End FunTableImpl.java
