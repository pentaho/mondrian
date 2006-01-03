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
    protected final Map mapNameToResolvers = new HashMap();
    private final HashSet reservedWords = new HashSet();
    private final HashSet propertyWords = new HashSet();
    protected static final Resolver[] emptyResolverArray = new Resolver[0];
    /** used during initialization **/
    protected final List resolverList = new ArrayList();
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
        resolverList.add(resolver);
        final String[] reservedWords = resolver.getReservedWords();
        for (int i = 0; i < reservedWords.length; i++) {
            String reservedWord = reservedWords[i];
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
            FunDef def = resolvers[i].resolve(args, validator, conversionCount);
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
        for (int i = 0, n = resolverList.size(); i < n; i++) {
            Resolver resolver = (Resolver) resolverList.get(i);
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
