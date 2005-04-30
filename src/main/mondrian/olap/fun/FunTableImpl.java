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

import java.util.*;

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

    /**
     * Converts an argument to a parameter type.
     */
    public Exp convert(Exp fromExp, int to, Validator resolver) {
        Exp exp = convert_(fromExp, to);
        if (exp == null) {
            throw Util.newInternal("cannot convert " + fromExp + " to " + to);
        }
        return resolver.validate(exp);
    }

    private static Exp convert_(Exp fromExp, int to) {
        int from = fromExp.getCategory();
        if (from == to) {
            return fromExp;
        }
        switch (from) {
        case Category.Array:
            return null;
        case Category.Dimension:
            // Seems funny that you can 'downcast' from a dimension, doesn't
            // it? But we add an implicit 'CurrentMember', for example,
            // '[Time].PrevMember' actually means
            // '[Time].CurrentMember.PrevMember'.
            switch (to) {
            case Category.Hierarchy:
                // "<Dimension>.CurrentMember.Hierarchy"
                return new FunCall(
                        "Hierarchy", Syntax.Property, new Exp[]{
                        new FunCall(
                                "CurrentMember",
                                Syntax.Property, new Exp[]{fromExp}
                        )}
                );
            case Category.Level:
                // "<Dimension>.CurrentMember.Level"
                return new FunCall(
                        "Level", Syntax.Property, new Exp[]{
                        new FunCall(
                                "CurrentMember",
                                Syntax.Property, new Exp[]{fromExp}
                        )}
                );
            case Category.Member:
            case Category.Tuple:
                // "<Dimension>.CurrentMember"
                return new FunCall(
                        "CurrentMember",
                        Syntax.Property,
                        new Exp[]{fromExp});
            default:
                return null;
            }
        case Category.Hierarchy:
            switch (to) {
            case Category.Dimension:
                // "<Hierarchy>.Dimension"
                return new FunCall(
                        "Dimension",
                        Syntax.Property,
                        new Exp[]{fromExp});
            default:
                return null;
            }
        case Category.Level:
            switch (to) {
            case Category.Dimension:
                // "<Level>.Dimension"
                return new FunCall(
                        "Dimension",
                        Syntax.Property,
                        new Exp[] {fromExp});
            case Category.Hierarchy:
                // "<Level>.Hierarchy"
                return new FunCall(
                        "Hierarchy",
                        Syntax.Property,
                        new Exp[] {fromExp});
            default:
                return null;
            }
        case Category.Logical:
            return null;
        case Category.Member:
            switch (to) {
            case Category.Dimension:
                // "<Member>.Dimension"
                return new FunCall(
                        "Dimension",
                        Syntax.Property,
                        new Exp[] {fromExp});
            case Category.Hierarchy:
                // "<Member>.Hierarchy"
                return new FunCall(
                        "Hierarchy",
                        Syntax.Property,
                        new Exp[]{fromExp});
            case Category.Level:
                // "<Member>.Level"
                return new FunCall(
                        "Level",
                        Syntax.Property,
                        new Exp[]{fromExp});
            case Category.Tuple:
                // Conversion to tuple is trivial: a member is a
                // one-dimensional tuple already.
                return fromExp;
            case Category.Numeric | Category.Constant:
            case Category.String | Category.Constant: //todo: assert is a string member
                // "<Member>.Value"
                return new FunCall(
                        "Value",
                        Syntax.Property,
                        new Exp[]{fromExp});
            case Category.Value:
            case Category.Numeric:
            case Category.String:
                return fromExp;
            default:
                return null;
            }
        case Category.Numeric | Category.Constant:
            switch (to) {
            case Category.Value:
            case Category.Numeric:
                return fromExp;
            default:
                return null;
            }
        case Category.Numeric:
            switch (to) {
            case Category.Value:
                return fromExp;
            case Category.Numeric | Category.Constant:
                return new FunCall(
                        "_Value",
                        Syntax.Function,
                        new Exp[] {fromExp});
            default:
                return null;
            }
        case Category.Set:
            return null;
        case Category.String | Category.Constant:
            switch (to) {
            case Category.Value:
            case Category.String:
                return fromExp;
            default:
                return null;
            }
        case Category.String:
            switch (to) {
            case Category.Value:
                return fromExp;
            case Category.String | Category.Constant:
                return new FunCall(
                        "_Value",
                        Syntax.Function,
                        new Exp[] {fromExp});
            default:
                return null;
            }
        case Category.Tuple:
            switch (to) {
            case Category.Value:
                return fromExp;
            case Category.Numeric:
            case Category.String:
                return new FunCall(
                        "_Value",
                        Syntax.Function,
                        new Exp[] {fromExp});
            default:
                return null;
            }
        case Category.Value:
            return null;
        case Category.Symbol:
            return null;
        default:
            throw Util.newInternal("unknown category " + from);
        }
    }

    /**
     * Returns whether we can convert an argument to a parameter tyoe.
     * @param fromExp argument type
     * @param to   parameter type
     * @param conversionCount in/out count of number of conversions performed;
     *             is incremented if the conversion is non-trivial (for
     *             example, converting a member to a level).
     *
     * @see mondrian.olap.FunTable#convert
     */
    static boolean canConvert(Exp fromExp, int to, int[] conversionCount) {
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
            if (to == Category.Hierarchy ||
                    to == Category.Level ||
                    to == Category.Member ||
                    to == Category.Tuple) {
                conversionCount[0]++;
                return true;
            } else {
                return false;
            }
        case Category.Hierarchy:
            if (to == Category.Dimension) {
                conversionCount[0]++;
                return true;
            } else {
                return false;
            }
        case Category.Level:
            if (to == Category.Dimension ||
                    to == Category.Hierarchy) {
                conversionCount[0]++;
                return true;
            } else {
                return false;
            }
        case Category.Logical:
            return false;
        case Category.Member:
            if (to == Category.Dimension ||
                    to == Category.Hierarchy ||
                    to == Category.Level ||
                    to == Category.Tuple) {
                conversionCount[0]++;
                return true;
            } else if (to == (Category.Numeric | Category.Expression)) {
                // We assume that members are numeric, so a cast to a numeric
                // expression is less expensive than a conversion to a string
                // expression.
                conversionCount[0]++;
                return true;
            } else if (to == Category.Value ||
                    to == (Category.String | Category.Expression)) {
                conversionCount[0] += 2;
                return true;
            } else {
                return false;
            }
        case Category.Numeric | Category.Constant:
            return to == Category.Value ||
                to == Category.Numeric;
        case Category.Numeric:
            return to == Category.Value ||
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
        default:
            throw Util.newInternal("unknown category " + from);
        }
    }

    public FunDef getDef(FunCall call, Validator resolver) {
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

        int[] conversionCount = new int[1];
        int minConversions = Integer.MAX_VALUE;
        int matchCount = 0;
        FunDef matchDef = null;
        for (int i = 0; i < resolvers.length; i++) {
            conversionCount[0] = 0;
            FunDef def = resolvers[i].resolve(call.getArgs(), conversionCount);
            if (def != null) {
                if (def.getReturnCategory() == Category.Set &&
                        resolver.requiresExpression()) {
                    continue;
                }
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
            throw MondrianResource.instance().newNoFunctionMatchesSignature(
                    signature);
        case 1:
            final String matchKey = makeResolverKey(matchDef.getName(),
                    matchDef.getSyntax());
            Util.assertTrue(matchKey.equals(key), matchKey);
            return matchDef;
        default:
            throw MondrianResource.instance()
                    .newMoreThanOneFunctionMatchesSignature(signature);
        }
    }

    public boolean requiresExpression(FunCall call, int k,
            Validator resolver) {
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
}

// End FunTableImpl.java