/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.type.*;

import java.util.*;

/**
 * Definition of the <code>Extract</code> MDX function.
 *
 * <p>Syntax:
 * <blockquote><code>Extract(&lt;Set&gt;, &lt;Hierarchy&gt;[,
 * &lt;Hierarchy&gt;...])</code></blockquote>
 *
 * @author jhyde
 * @since Jun 10, 2007
 */
class ExtractFunDef extends FunDefBase {
    static final ResolverBase Resolver = new ResolverBase(
        "Extract",
        "Extract(<Set>, <Hierarchy>[, <Hierarchy>...])",
        "Returns a set of tuples from extracted hierarchy elements. The opposite of Crossjoin.",
        Syntax.Function)
    {
        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            if (args.length < 2) {
                return null;
            }
            if (!validator.canConvert(0, args[0], Category.Set, conversions)) {
                return null;
            }
            for (int i = 1; i < args.length; ++i) {
                if (!validator.canConvert(
                        0, args[i], Category.Hierarchy, conversions))
                {
                    return null;
                }
            }

            // Find the dimensionality of the set expression.

            // Form a list of ordinals of the hierarchies being extracted.
            // For example, in
            //   Extract(X.Members * Y.Members * Z.Members, Z, X)
            // the hierarchy ordinals are X=0, Y=1, Z=2, and the extracted
            // ordinals are {2, 0}.
            //
            // Each hierarchy extracted must exist in the LHS,
            // and no hierarchy may be extracted more than once.
            List<Integer> extractedOrdinals = new ArrayList<Integer>();
            final List<Hierarchy> extractedHierarchies =
                new ArrayList<Hierarchy>();
            findExtractedHierarchies(
                args, extractedHierarchies, extractedOrdinals);
            int[] parameterTypes = new int[args.length];
            parameterTypes[0] = Category.Set;
            Arrays.fill(
                parameterTypes, 1, parameterTypes.length, Category.Hierarchy);
            return new ExtractFunDef(this, Category.Set, parameterTypes);
        }
    };

    private ExtractFunDef(
        Resolver resolver, int returnType, int[] parameterTypes)
    {
        super(resolver, returnType, parameterTypes);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        final List<Hierarchy> extractedHierarchies =
            new ArrayList<Hierarchy>();
        final List<Integer> extractedOrdinals = new ArrayList<Integer>();
        findExtractedHierarchies(args, extractedHierarchies, extractedOrdinals);
        if (extractedHierarchies.size() == 1) {
            return new SetType(
                MemberType.forHierarchy(
                    extractedHierarchies.get(0)));
        } else {
            List<Type> typeList = new ArrayList<Type>();
            for (Hierarchy extractedHierarchy : extractedHierarchies) {
                typeList.add(
                    MemberType.forHierarchy(
                        extractedHierarchy));
            }
            return new SetType(
                new TupleType(
                    typeList.toArray(new Type[typeList.size()])));
        }
    }

    private static void findExtractedHierarchies(
        Exp[] args,
        List<Hierarchy> extractedHierarchies,
        List<Integer> extractedOrdinals)
    {
        SetType type = (SetType) args[0].getType();
        final List<Hierarchy> hierarchies;
        if (type.getElementType() instanceof TupleType) {
            hierarchies = ((TupleType) type.getElementType()).getHierarchies();
        } else {
            hierarchies = Collections.singletonList(type.getHierarchy());
        }
        for (Hierarchy hierarchy : hierarchies) {
            if (hierarchy == null) {
                throw new RuntimeException(
                    "hierarchy of argument not known");
            }
        }

        for (int i = 1; i < args.length; i++) {
            Exp arg = args[i];
            Hierarchy extractedHierarchy = null;
            if (arg instanceof HierarchyExpr) {
                HierarchyExpr hierarchyExpr = (HierarchyExpr) arg;
                extractedHierarchy = hierarchyExpr.getHierarchy();
            } else if (arg instanceof DimensionExpr) {
                DimensionExpr dimensionExpr = (DimensionExpr) arg;
                extractedHierarchy =
                    dimensionExpr.getDimension().getHierarchy();
            }
            if (extractedHierarchy == null) {
                throw new RuntimeException("not a constant hierarchy: " + arg);
            }
            int ordinal = hierarchies.indexOf(extractedHierarchy);
            if (ordinal == -1) {
                throw new RuntimeException(
                    "hierarchy "
                    + extractedHierarchy.getUniqueName()
                    + " is not a hierarchy of the expression " + args[0]);
            }
            if (extractedOrdinals.indexOf(ordinal) >= 0) {
                throw new RuntimeException(
                    "hierarchy "
                    + extractedHierarchy.getUniqueName()
                    + " is extracted more than once");
            }
            extractedOrdinals.add(ordinal);
            extractedHierarchies.add(extractedHierarchy);
        }
    }

    private static int[] toIntArray(List<Integer> integerList) {
        final int[] ints = new int[integerList.size()];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = integerList.get(i);
        }
        return ints;
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        List<Hierarchy> extractedHierarchyList = new ArrayList<Hierarchy>();
        List<Integer> extractedOrdinalList = new ArrayList<Integer>();
        findExtractedHierarchies(
            call.getArgs(),
            extractedHierarchyList,
            extractedOrdinalList);
        Util.assertTrue(
            extractedOrdinalList.size() == extractedHierarchyList.size());
        Exp arg = call.getArg(0);
        final ListCalc listCalc = compiler.compileList(arg, false);
        int inArity = arg.getType().getArity();
        final int outArity = extractedOrdinalList.size();
        if (inArity == 1) {
            // LHS is a set of members, RHS is the same hierarchy. Extract boils
            // down to eliminating duplicate members.
            Util.assertTrue(outArity == 1);
            return new DistinctFunDef.CalcImpl(call, listCalc);
        }
        final int[] extractedOrdinals = toIntArray(extractedOrdinalList);
        return new AbstractListCalc(call, new Calc[]{listCalc}) {
            public TupleList evaluateList(Evaluator evaluator) {
                TupleList result = TupleCollections.createList(outArity);
                TupleList list = listCalc.evaluateList(evaluator);
                Set<List<Member>> emittedTuples = new HashSet<List<Member>>();
                for (List<Member> members : list.project(extractedOrdinals)) {
                    if (emittedTuples.add(members)) {
                        result.add(members);
                    }
                }
                return result;
            }
        };
    }
}

// End ExtractFunDef.java
