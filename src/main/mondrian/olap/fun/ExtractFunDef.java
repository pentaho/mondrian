/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.mdx.DimensionExpr;
import mondrian.olap.*;
import mondrian.olap.type.*;

import java.util.*;

/**
 * Definition of the <code>Extract</code> MDX function.
 *
 * <p>Syntax:
 * <blockquote><code>Extract(&lt;Set&gt;, &lt;Dimension&gt;[,
 * &lt;Dimension&gt;...])</code></blockquote>
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 10, 2007
 */
class ExtractFunDef extends FunDefBase {
    static final ResolverBase Resolver = new ResolverBase(
        "Extract",
        "Extract(<Set>, <Dimension>[, <Dimension>...])",
        "Returns a set of tuples from extracted dimension elements. The opposite of Crossjoin.",
        Syntax.Function) {
        public FunDef resolve(
            Exp[] args, Validator validator, int[] conversionCount) {
            if (args.length < 2) {
                return null;
            }
            if (!validator.canConvert(
                args[0], Category.Set, conversionCount)) {
                return null;
            }
            for (int i = 1; i < args.length; ++i) {
                if (!validator.canConvert(
                    args[i], Category.Dimension, conversionCount)) {
                    return null;
                }
            }

            // Find the dimensionality of the set expression.

            // Form a list of ordinals of the dimensions being extracted.
            // For example, in
            //   Extract(X.Members * Y.Members * Z.Members, Z, X)
            // the dimension ordinals are X=0, Y=1, Z=2, and the extracted
            // ordinals are {2, 0}.
            //
            // Each dimension extracted must exist in the LHS,
            // and no dimension may be extracted more than once.
            List<Integer> extractedOrdinals = new ArrayList<Integer>();
            final List<Dimension> extractedDimensions = new ArrayList<Dimension>();
            findExtractedDimensions(args, extractedDimensions, extractedOrdinals);
            int[] parameterTypes = new int[args.length];
            parameterTypes[0] = Category.Set;
            Arrays.fill(parameterTypes, 1, parameterTypes.length, Category.Dimension);
            return new ExtractFunDef(this, Category.Set, parameterTypes);
        }
    };

    private ExtractFunDef(
        Resolver resolver, int returnType, int[] parameterTypes)
    {
        super(resolver, returnType, parameterTypes);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        final List<Dimension> extractedDimensions =
            new ArrayList<Dimension>();
        final List<Integer> extractedOrdinals = new ArrayList<Integer>();
        findExtractedDimensions(args, extractedDimensions, extractedOrdinals);
        if (extractedDimensions.size() == 1) {
            return new SetType(
                new MemberType(extractedDimensions.get(0), null, null, null));
        } else {
            List<Type> typeList = new ArrayList<Type>();
            for (Dimension extractedDimension : extractedDimensions) {
                typeList.add(
                    new MemberType(
                        extractedDimension, null, null, null));
            }
            return new SetType(
                new TupleType(
                    typeList.toArray(new Type[typeList.size()])));
        }
    }

    private static void findExtractedDimensions(
        Exp[] args,
        List<Dimension> extractedDimensions,
        List<Integer> extractedOrdinals)
    {
        SetType type = (SetType) args[0].getType();
        final List<Dimension> dimensions = new ArrayList<Dimension>();
        if (type.getElementType() instanceof TupleType) {
            for (Type elementType : ((TupleType) type
                .getElementType()).elementTypes) {
                Dimension dimension = elementType.getDimension();
                if (dimension == null) {
                    throw new RuntimeException("dimension of argument not known");
                }
                dimensions.add(dimension);
            }
        } else {
            Dimension dimension = type.getDimension();
            if (dimension == null) {
                throw new RuntimeException("dimension of argument not known");
            }
            dimensions.add(dimension);
        }

        for (int i = 1; i < args.length; i++) {
            Exp arg = args[i];
            if (arg instanceof DimensionExpr) {
                DimensionExpr dimensionExpr = (DimensionExpr) arg;
                final Dimension extractedDimension =
                    dimensionExpr.getDimension();
                int ordinal = dimensions.indexOf(extractedDimension);
                if (ordinal == -1) {
                    throw new RuntimeException(
                        "dimension " +
                            extractedDimension.getUniqueName() +
                            " is not a dimension of the expression " + args[0]);
                }
                if (extractedOrdinals.indexOf(ordinal) >= 0) {
                    throw new RuntimeException(
                        "dimension " +
                            extractedDimension.getUniqueName() +
                            " is extracted more than once");
                }
                extractedOrdinals.add(ordinal);
                extractedDimensions.add(extractedDimension);
            } else {
                throw new RuntimeException("not a constant dimension: " + arg);
            }
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
        List<Dimension> extractedDimensionList = new ArrayList<Dimension>();
        List<Integer> extractedOrdinalList = new ArrayList<Integer>();
        findExtractedDimensions(
            call.getArgs(),
            extractedDimensionList,
            extractedOrdinalList);
        Util.assertTrue(
            extractedOrdinalList.size() == extractedDimensionList.size());
        Exp arg = call.getArg(0);
        final TupleListCalc listCalc =
            (TupleListCalc) compiler.compileList(arg, false);
        int inArity = ((SetType) arg.getType()).getArity();
        final int outArity = extractedOrdinalList.size();
        if (inArity == 1) {
            // LHS is a set of members, RHS is the same dimension. Extract boils
            // down to eliminating duplicate members.
            Util.assertTrue(outArity == 1);
            return new DistinctFunDef.CalcImpl(call, listCalc);
        }
        final int[] extractedOrdinals = toIntArray(extractedOrdinalList);
        if (outArity == 1) {
            return new AbstractListCalc(call, new Calc[] {listCalc}) {
                public List evaluateList(Evaluator evaluator) {
                    List<Member> result = new ArrayList<Member>();
                    List<Member[]> list = listCalc.evaluateTupleList(evaluator);
                    Set<Member> emittedMembers = new HashSet<Member>();
                    for (Member[] members : list) {
                        Member outMember = members[extractedOrdinals[0]];
                        if (emittedMembers.add(outMember)) {
                            result.add(outMember);
                        }
                    }
                    return result;
                }
            };
        } else {
            return new AbstractListCalc(call, new Calc[] {listCalc}) {
                public List evaluateList(Evaluator evaluator) {
                    List<Member[]> result = new ArrayList<Member[]>();
                    List<Member[]> list = listCalc.evaluateTupleList(evaluator);
                    Set<List<Member>> emittedTuples = new HashSet<List<Member>>();
                    for (Member[] members : list) {
                        Member[] outMembers = new Member[outArity];
                        for (int i = 0; i < outMembers.length; i++) {
                            outMembers[i] = members[extractedOrdinals[i]];
                        }
                        final List<Member> outTuple = Arrays.asList(outMembers);
                        if (emittedTuples.add(outTuple)) {
                            result.add(outMembers);
                        }
                    }
                    return result;
                }
            };
        }
    }
}

// End ExtractFunDef.java
