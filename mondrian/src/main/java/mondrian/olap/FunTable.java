/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap;

import mondrian.olap.fun.FunInfo;
import mondrian.olap.fun.Resolver;

import java.util.List;

/**
 * List of all MDX functions.
 *
 * <p>A function table can resolve a function call, using a particular
 * {@link Syntax} and set of arguments, to a
 * function definition ({@link FunDef}).</p>
 *
 * @author jhyde, 3 March, 2002
 */
public interface FunTable {
    /**
     * Returns whether a string is a reserved word.
     */
    boolean isReserved(String s);

    /**
     * Returns whether a string is a property-style (postfix)
     * operator. This is used during parsing to disambiguate
     * functions from unquoted member names.
     */
    boolean isProperty(String s);

    /**
     * Returns a list of words ({@link String}) which may not be used as
     * identifiers.
     */
    List<String> getReservedWords();

    /**
     * Returns a list of {@link mondrian.olap.fun.Resolver} objects.
     */
    List<Resolver> getResolvers();

    /**
     * Returns a list of resolvers for an operator with a given name and syntax.
     * Never returns null; if there are no resolvers, returns the empty list.
     *
     * @param name Operator name
     * @param syntax Operator syntax
     * @return List of resolvers for the operator
     */
    List<Resolver> getResolvers(
        String name,
        Syntax syntax);

    /**
     * Returns a list of {@link mondrian.olap.fun.FunInfo} objects.
     */
    List<FunInfo> getFunInfoList();

    /**
     * This method is called from the constructor, to define the set of
     * functions and reserved words recognized.
     *
     * <p>The implementing class calls {@link Builder} methods to declare
     * functions and reserved words.
     *
     * <p>Derived class can override this method to add more functions. It must
     * call the base method.
     *
     * @param builder Builder
     */
    void defineFunctions(Builder builder);

    /**
     * Builder that assists with the construction of a function table by
     * providing callbacks to define functions.
     *
     * <p>An implementation of {@link mondrian.olap.FunTable} must register all
     * of its functions and operators by making callbacks during its
     * {@link mondrian.olap.FunTable#defineFunctions(mondrian.olap.FunTable.Builder)}
     * method.
     */
    public interface Builder {
        /**
         * Defines a function.
         *
         * @param funDef Function definition
         */
        void define(FunDef funDef);

        /**
         * Defines a resolver that will resolve overloaded function calls to
         * function definitions.
         *
         * @param resolver Function call resolver
         */
        void define(Resolver resolver);

        /**
         * Defines a function info that is not matchd by an actual function.
         * The function will be implemented via implicit conversions, but
         * we still want the function info to appear in the metadata.
         *
         * @param funInfo Function info
         */
        void define(FunInfo funInfo);

        /**
         * Defines a reserved word.
         *
         * @param keyword Reserved word
         *
         * @see mondrian.olap.FunTable#isReserved
         */
        void defineReserved(String keyword);
    }
}

// End FunTable.java
