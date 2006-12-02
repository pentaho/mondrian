/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.mdx.UnresolvedFunCall;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;

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
    protected final Map<String, List<Resolver>> mapNameToResolvers =
        new HashMap<String, List<Resolver>>();
    private final Set<String> reservedWords = new HashSet<String>();
    private final Set<String> propertyWords = new HashSet<String>();
    /** used during initialization */
    protected final List<Resolver> resolverList = new ArrayList<Resolver>();
    protected final List<FunInfo> funInfoList = new ArrayList<FunInfo>();

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
        resolverList.add(resolver);
        final String[] reservedWords = resolver.getReservedWords();
        for (String reservedWord : reservedWords) {
            defineReserved(reservedWord);
        }
    }

    protected void addFunInfo(Resolver resolver) {
        this.funInfoList.add(FunInfo.make(resolver));
    }

    public FunDef getDef(
            Exp[] args, Validator validator, String funName, Syntax syntax) {
        String key = makeResolverKey(funName, syntax);

        // Resolve function by its upper-case name first.  If there is only one
        // function with that name, stop immediately.  If there is more than
        // function, use some custom method, which generally involves looking
        // at the type of one of its arguments.
        String signature = syntax.getSignature(funName,
                Category.Unknown, ExpBase.getTypes(args));
        List<Resolver> resolvers = mapNameToResolvers.get(key);
        if (resolvers == null) {
            resolvers = Collections.emptyList();
        }

        int[] conversionCount = new int[] {0};
        int minConversions = Integer.MAX_VALUE;
        int matchCount = 0;
        FunDef matchDef = null;
        for (Resolver resolver : resolvers) {
            conversionCount[0] = 0;
            FunDef def = resolver.resolve(args, validator, conversionCount);
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
            UnresolvedFunCall call,
            int k,
            Validator validator) {
        // The function call has not been resolved yet. In fact, this method
        // may have been invoked while resolving the child. Consider this:
        //   CrossJoin([Measures].[Unit Sales] * [Measures].[Store Sales])
        //
        // In order to know whether to resolve '*' to the multiplication
        // operator (which returns a scalar) or the crossjoin operator (which
        // returns a set) we have to know what kind of expression is expected.
        String key = makeResolverKey(call.getFunName(), call.getSyntax());
        List<Resolver> resolvers = mapNameToResolvers.get(key);
        if (resolvers == null) {
            resolvers = Collections.emptyList();
        }
        for (Resolver resolver2 : resolvers) {
            if (!resolver2.requiresExpression(k)) {
                // This resolver accepts a set in this argument position,
                // therefore we don't REQUIRE a scalar expression.
                return false;
            }
        }
        return true;
    }

    public List<String> getReservedWords() {
        return new ArrayList<String>(reservedWords);
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

    public List<Resolver> getResolvers() {
        final List<Resolver> list = new ArrayList<Resolver>();
        for (List<Resolver> resolvers : mapNameToResolvers.values()) {
            list.addAll(resolvers);
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

    public List<FunInfo> getFunInfoList() {
        return Collections.unmodifiableList(this.funInfoList);
    }

    /**
     * Indexes the collection of functions.
     */
    protected void organizeFunctions() {
        Collections.sort(funInfoList);
        // Map upper-case function names to resolvers.
        for (Resolver resolver : resolverList) {
            String key = makeResolverKey(resolver.getName(),
                resolver.getSyntax());
            List<Resolver> list = mapNameToResolvers.get(key);
            if (list == null) {
                list = new ArrayList<Resolver>();
                mapNameToResolvers.put(key, list);
            }
            list.add(resolver);
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
     */
    protected abstract void defineFunctions();
}

// End FunTableImpl.java
