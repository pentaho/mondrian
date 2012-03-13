/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.util.Pair;

import java.util.*;

/**
 * Abstract implementation of {@link FunTable}.
 *
 * <p>The derived class must implement
 * {@link #defineFunctions(mondrian.olap.FunTable.Builder)} to define
 * each function which will be recognized by this table. This method is called
 * from the constructor, after which point, no further functions can be added.
 */
public abstract class FunTableImpl implements FunTable {
    /**
     * Maps the upper-case name of a function plus its
     * {@link mondrian.olap.Syntax} to an array of
     * {@link Resolver} objects for that name.
     */
    private Map<Pair<String, Syntax>, List<Resolver>> mapNameToResolvers;
    private Set<String> reservedWordSet;
    private List<String> reservedWordList;
    private Set<String> propertyWords;
    private List<FunInfo> funInfoList;

    /**
     * Creates a FunTableImpl.
     */
    protected FunTableImpl() {
    }

    /**
     * Initializes the function table.
     */
    public final void init() {
        final BuilderImpl builder = new BuilderImpl();
        defineFunctions(builder);
        builder.organizeFunctions();

        // Copy information out of builder into this.
        this.funInfoList = Collections.unmodifiableList(builder.funInfoList);
        this.mapNameToResolvers =
            Collections.unmodifiableMap(builder.mapNameToResolvers);
        this.reservedWordSet = builder.reservedWords;
        final String[] reservedWords =
            builder.reservedWords.toArray(
                new String[builder.reservedWords.size()]);
        Arrays.sort(reservedWords);
        this.reservedWordList =
            Collections.unmodifiableList(Arrays.asList(reservedWords));
        this.propertyWords = Collections.unmodifiableSet(builder.propertyWords);
    }

    /**
     * Creates a key to look up an operator in the resolver map. The key
     * consists of the uppercase function name and the syntax.
     *
     * @param name Function/operator name
     * @param syntax Syntax
     * @return Key
     */
    private static Pair<String, Syntax> makeResolverKey(
        String name,
        Syntax syntax)
    {
        return new Pair<String, Syntax>(name.toUpperCase(), syntax);
    }

    public List<String> getReservedWords() {
        return reservedWordList;
    }

    public boolean isReserved(String s) {
        return reservedWordSet.contains(s.toUpperCase());
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

    public List<FunInfo> getFunInfoList() {
        return funInfoList;
    }

    public List<Resolver> getResolvers(String name, Syntax syntax) {
        Pair<String, Syntax> key = makeResolverKey(name, syntax);
        List<Resolver> resolvers = mapNameToResolvers.get(key);
        if (resolvers == null) {
            resolvers = Collections.emptyList();
        }
        return resolvers;
    }

    /**
     * Implementation of {@link mondrian.olap.FunTable.Builder}.
     * Functions are added to lists each time {@link #define(Resolver)} is
     * called, then {@link #organizeFunctions()} sorts and indexes the map.
     */
    private class BuilderImpl implements Builder {
        private final List<Resolver> resolverList = new ArrayList<Resolver>();
        private final List<FunInfo> funInfoList = new ArrayList<FunInfo>();
        private final Map<Pair<String, Syntax>, List<Resolver>>
            mapNameToResolvers =
            new HashMap<Pair<String, Syntax>, List<Resolver>>();
        private final Set<String> reservedWords = new HashSet<String>();
        private final Set<String> propertyWords = new HashSet<String>();

        public void define(FunDef funDef) {
            define(new SimpleResolver(funDef));
        }

        public void define(Resolver resolver) {
            funInfoList.add(FunInfo.make(resolver));
            if (resolver.getSyntax() == Syntax.Property) {
                propertyWords.add(resolver.getName().toUpperCase());
            }
            resolverList.add(resolver);
            final String[] reservedWords = resolver.getReservedWords();
            for (String reservedWord : reservedWords) {
                defineReserved(reservedWord);
            }
        }

        public void define(FunInfo funInfo) {
            funInfoList.add(funInfo);
        }

        public void defineReserved(String s) {
            reservedWords.add(s.toUpperCase());
        }

        /**
         * Indexes the collection of functions.
         */
        protected void organizeFunctions() {
            Collections.sort(funInfoList);

            // Map upper-case function names to resolvers.
            final List<List<Resolver>> nonSingletonResolverLists =
                new ArrayList<List<Resolver>>();
            for (Resolver resolver : resolverList) {
                Pair<String, Syntax> key =
                    makeResolverKey(
                        resolver.getName(),
                        resolver.getSyntax());
                List<Resolver> list = mapNameToResolvers.get(key);
                if (list == null) {
                    list = new ArrayList<Resolver>();
                    mapNameToResolvers.put(key, list);
                }
                list.add(resolver);
                if (list.size() == 2) {
                    nonSingletonResolverLists.add(list);
                }
            }

            // Sort lists by signature (skipping singleton lists)
            final Comparator<Resolver> comparator =
                new Comparator<Resolver>() {
                    public int compare(Resolver o1, Resolver o2) {
                        return o1.getSignature().compareTo(o2.getSignature());
                    }
                };
            for (List<Resolver> resolverList : nonSingletonResolverLists) {
                Collections.sort(resolverList, comparator);
            }
        }
    }
}

// End FunTableImpl.java
