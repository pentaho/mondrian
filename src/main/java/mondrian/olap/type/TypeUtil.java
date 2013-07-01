/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.type;

import mondrian.mdx.UnresolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.fun.Resolver;

import java.util.*;

/**
 * Utility methods relating to types.
 *
 * @author jhyde
 * @since Feb 17, 2005
 */
public class TypeUtil {
    /**
     * Given a set type, returns the element type. Or its element type, if it
     * is a set type. And so on.
     *
     * @param type Type
     * @return underlying element type which is not a set type
     */
    public static Type stripSetType(Type type) {
        while (type instanceof SetType) {
            type = ((SetType) type).getElementType();
        }
        return type;
    }

    /**
     * Converts a type to a member or tuple type.
     * If it cannot, returns null.
     *
     * @param type Type
     * @return member or tuple type
     */
    public static Type toMemberOrTupleType(Type type) {
        type = stripSetType(type);
        if (type instanceof TupleType) {
            return type;
        } else {
            return toMemberType(type);
        }
    }

    /**
     * Converts a type to a member type.
     * If it is a set, strips the set.
     * If it is a member type, returns the type unchanged.
     * If it is a dimension, hierarchy or level type, converts it to
     * a member type.
     * If it is a tuple, number, string, or boolean, returns null.
     *
     * @param type Type
     * @return type as a member type
     */
    public static MemberType toMemberType(Type type) {
        type = stripSetType(type);
        if (type instanceof MemberType) {
            return (MemberType) type;
        } else if (type instanceof DimensionType
            || type instanceof HierarchyType
            || type instanceof LevelType)
        {
            return MemberType.forType(type);
        } else {
            return null;
        }
    }

    /**
     * Returns whether this type is union-compatible with another.
     * In general, to be union-compatible, types must have the same
     * dimensionality.
     *
     * @param type1 First type
     * @param type2 Second type
     * @return whether types are union-compatible
     */
    public static boolean isUnionCompatible(Type type1, Type type2) {
        if (type1 instanceof TupleType) {
            TupleType tupleType1 = (TupleType) type1;
            if (type2 instanceof TupleType) {
                TupleType tupleType2 = (TupleType) type2;
                if (tupleType1.elementTypes.length
                    == tupleType2.elementTypes.length)
                {
                    for (int i = 0; i < tupleType1.elementTypes.length; i++) {
                        if (!isUnionCompatible(
                                tupleType1.elementTypes[i],
                                tupleType2.elementTypes[i]))
                        {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        } else {
            final MemberType memberType1 = toMemberType(type1);
            if (memberType1 == null) {
                return false;
            }
            final MemberType memberType2 = toMemberType(type2);
            if (memberType2 == null) {
                return false;
            }
            final Hierarchy hierarchy1 = memberType1.getHierarchy();
            final Hierarchy hierarchy2 = memberType2.getHierarchy();
            return equal(hierarchy1, hierarchy2);
        }
    }

    /**
     * Returns whether two hierarchies are equal.
     *
     * @param hierarchy1 First hierarchy
     * @param hierarchy2 Second hierarchy
     * @return Whether hierarchies are equal
     */
    private static boolean equal(
        final Hierarchy hierarchy1,
        final Hierarchy hierarchy2)
    {
        return hierarchy1 == null
            || hierarchy2 == null
            || hierarchy2.getUniqueName().equals(
                hierarchy1.getUniqueName());
    }

    /**
     * Returns whether a value of a given type can be evaluated to a scalar
     * value.
     *
     * <p>The rules are as follows:<ul>
     * <li>Clearly boolean, numeric and string expressions can be evaluated.
     * <li>Member and tuple expressions can be interpreted as a scalar value.
     *     The expression is evaluated to establish the context where a measure
     *     can be evaluated.
     * <li>Hierarchy and dimension expressions are implicitly
     *     converted into the current member, and evaluated as above.
     * <li>Level expressions cannot be evaluated
     * <li>Cube and Set (even sets with a single member) cannot be evaluated.
     * </ul>
     *
     * @param type Type
     * @return Whether an expression of this type can be evaluated to yield a
     *   scalar value.
     */
    public static boolean canEvaluate(Type type) {
        return ! (type instanceof SetType
                  || type instanceof CubeType
                  || type instanceof LevelType);
    }

    /**
     * Returns whether a type is a set type.
     *
     * @param type Type
     * @return Whether a value of this type can be evaluated to yield a set.
     */
    public static boolean isSet(Type type) {
        return type instanceof SetType;
    }

    public static boolean couldBeMember(Type type) {
        return type instanceof MemberType
            || type instanceof HierarchyType
            || type instanceof DimensionType;
    }

    /**
     * Converts a {@link Type} value to a {@link Category} ordinal.
     *
     * @param type Type
     * @return category ordinal
     */
    public static int typeToCategory(Type type) {
        if (type instanceof NullType) {
            return Category.Null;
        } else if (type instanceof EmptyType) {
            return Category.Empty;
        } else if (type instanceof DateTimeType) {
            return Category.DateTime;
        } else if (type instanceof DecimalType
            && ((DecimalType)type).getScale() == 0)
        {
            return Category.Integer;
        } else if (type instanceof NumericType) {
            return Category.Numeric;
        } else if (type instanceof BooleanType) {
            return Category.Logical;
        } else if (type instanceof DimensionType) {
            return Category.Dimension;
        } else if (type instanceof HierarchyType) {
            return Category.Hierarchy;
        } else if (type instanceof MemberType) {
            return Category.Member;
        } else if (type instanceof LevelType) {
            return Category.Level;
        } else if (type instanceof SymbolType) {
            return Category.Symbol;
        } else if (type instanceof StringType) {
            return Category.String;
        } else if (type instanceof ScalarType) {
            return Category.Value;
        } else if (type instanceof SetType) {
            return Category.Set;
        } else if (type instanceof TupleType) {
            return Category.Tuple;
        } else {
            throw Util.newInternal("Unknown type " + type);
        }
    }

    /**
     * Returns a type sufficiently broad to hold any value of several types,
     * but as narrow as possible. If there is no such type, returns null.
     *
     * <p>The result is equivalent to calling
     * {@link Type#computeCommonType(Type, int[])} pairwise.
     *
     * @param allowConversions Whether to allow implicit conversions
     * @param types Array of types
     * @return Most general type which encompases all types
     */
    public static Type computeCommonType(
        boolean allowConversions,
        Type... types)
    {
        if (types.length == 0) {
            return null;
        }
        Type type = types[0];
        int[] conversionCount = allowConversions ? new int[] {0} : null;
        for (int i = 1; i < types.length; ++i) {
            if (type == null) {
                return null;
            }
            type = type.computeCommonType(types[i], conversionCount);
        }
        return type;
    }

    /**
     * Returns whether we can convert an argument of a given category to a
     * given parameter category.
     *
     * @param ordinal argument ordinal
     * @param fromType actual argument type
     * @param to   formal parameter category
     * @param conversions list of implicit conversions required (out)
     * @return whether can convert from 'from' to 'to'
     */
    public static boolean canConvert(
        int ordinal,
        Type fromType,
        int to,
        List<Resolver.Conversion> conversions)
    {
        final int from = typeToCategory(fromType);
        if (from == to) {
            return true;
        }
        RuntimeException e = null;
        switch (from) {
        case Category.Array:
            return false;
        case Category.Dimension:
            // We can go from Dimension to Hierarchy if the dimension has a
            // default hierarchy. From there, we can go to Member or Tuple.
            // Even if the dimension does not have a default hierarchy, we claim
            // now that we can do the conversion, to prevent other overloads
            // from being chosen; we will hit an error either at compile time or
            // at run time.
            switch (to) {
            case Category.Member:
            case Category.Tuple:
            case Category.Hierarchy:
                // It is more difficult to convert dimension->hierarchy (cost=3)
                // than hierarchy->dimension (cost=2)
                conversions.add(new ConversionImpl(from, to, ordinal, 3, e));
                return true;
            case Category.Level:
                // It is more difficult to convert dimension->level than
                // dimension->member or dimension->hierarchy->member.
                conversions.add(new ConversionImpl(from, to, ordinal, 4, null));
                return true;
            default:
                return false;
            }
        case Category.Hierarchy:
            // Seems funny that you can 'downcast' from a hierarchy, doesn't
            // it? But we add an implicit 'CurrentMember', for example,
            // '[Product].PrevMember' actually means
            // '[Product].CurrentMember.PrevMember'.
            switch (to) {
            case Category.Dimension:
                conversions.add(new ConversionImpl(from, to, ordinal, 2, null));
                return true;
            case Category.Member:
            case Category.Tuple:
                conversions.add(new ConversionImpl(from, to, ordinal, 1, null));
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
                conversions.add(new ConversionImpl(from, to, ordinal, 3, null));
                return true;
            case Category.Hierarchy:
                conversions.add(new ConversionImpl(from, to, ordinal, 2, null));
                return true;
            default:
                return false;
            }
        case Category.Logical:
            switch (to) {
            case Category.Value:
                return true;
            default:
                return false;
            }
        case Category.Member:
            switch (to) {
            case Category.Dimension:
            case Category.Hierarchy:
            case Category.Level:
            case Category.Tuple:
                conversions.add(new ConversionImpl(from, to, ordinal, 2, null));
                return true;
            case Category.Set:
                // It is more expensive to convert from Member->Set (cost=3)
                // than Member->Tuple (cost=1). In particular, m.Tuple(n) should
                // resolve to <Tuple>.Item(<Numeric>) rather than
                // <Set>.Item(<Numeric>).
                conversions.add(new ConversionImpl(from, to, ordinal, 3, null));
                return true;
            case Category.Numeric:
                // It is more expensive to convert from Member->Scalar (cost=4)
                // than Member->Set (cost=3). In particular, we want 'member *
                // set' to resolve to the crossjoin operator, '{m} * set'.
                conversions.add(new ConversionImpl(from, to, ordinal, 4, null));
                return true;
            case Category.Value:
            case Category.String:
                // We assume that measures are numeric, so a cast to a string or
                // general value expression is more expensive (cost=5) than a
                // conversion to a numeric expression (cost=4).
                conversions.add(new ConversionImpl(from, to, ordinal, 5, null));
                return true;
            default:
                return false;
            }
        case Category.Numeric | Category.Constant:
            switch (to) {
            case Category.Value:
            case Category.Numeric:
                return true;
            default:
                return false;
            }
        case Category.Numeric:
            switch (to) {
            case Category.Logical:
                conversions.add(new ConversionImpl(from, to, ordinal, 3, null));
                return true;
            case Category.Value:
            case Category.Integer:
            case (Category.Integer | Category.Constant):
            case (Category.Numeric | Category.Constant):
                return true;
            default:
                return false;
            }
        case Category.Integer:
            switch (to) {
            case Category.Value:
            case (Category.Integer | Category.Constant):
            case Category.Numeric:
            case (Category.Numeric | Category.Constant):
                return true;
            default:
                return false;
            }
        case Category.Set:
            return false;
        case Category.String | Category.Constant:
            switch (to) {
            case Category.Value:
            case Category.String:
                return true;
            default:
                return false;
            }
        case Category.String:
            switch (to) {
            case Category.Value:
            case (Category.String | Category.Constant):
                return true;
            default:
                return false;
            }
        case Category.DateTime | Category.Constant:
            switch (to) {
            case Category.Value:
            case Category.DateTime:
                return true;
            default:
                return false;
            }
        case Category.DateTime:
            switch (to) {
            case Category.Value:
            case (Category.DateTime | Category.Constant):
                return true;
            default:
                return false;
            }
        case Category.Tuple:
            switch (to) {
            case Category.Set:
                conversions.add(new ConversionImpl(from, to, ordinal, 3, null));
                return true;
            case Category.Numeric:
                // It is more expensive to convert from Tuple->Scalar (cost=5)
                // than Tuple->Set (cost=4). In particular, we want 'tuple *
                // set' to resolve to the crossjoin operator, '{tuple} * set'.
                // This is analogous to Member->Numeric conversion.
                conversions.add(new ConversionImpl(from, to, ordinal, 4, null));
                return true;
            case Category.String:
            case Category.Value:
                // We assume that measures are numeric, so a cast to a string or
                // general value expression is more expensive (cost=5) than a
                // conversion to a numeric expression (cost=4).
                conversions.add(new ConversionImpl(from, to, ordinal, 5, null));
                return true;
            default:
                return false;
            }
        case Category.Value:
            // We can implicitly cast from value to a more specific scalar type,
            // but the cost is significant.
            switch (to) {
            case Category.String:
            case Category.Numeric:
            case Category.Logical:
                conversions.add(new ConversionImpl(from, to, ordinal, 3, null));
                return true;
            default:
                return false;
            }
        case Category.Symbol:
            return false;
        case Category.Null:
            // now null supports members as well as scalars; but scalar is
            // preferred
            if (Category.isScalar(to)) {
                return true;
            } else if (to == Category.Member) {
                conversions.add(new ConversionImpl(from, to, ordinal, 3, null));
                return true;
            } else {
                return false;
            }
        case Category.Empty:
            return false;
        default:
            throw Util.newInternal(
                "unknown category " + from + " for type " + fromType);
        }
    }

    /**
     * Returns the hierarchies in a set, member, or tuple type.
     *
     * @param type Type
     * @return List of hierarchies
     */
    public static List<Hierarchy> getHierarchies(Type type) {
        if (type instanceof SetType) {
            type = ((SetType) type).getElementType();
        }
        if (type instanceof TupleType) {
            final TupleType tupleType = (TupleType) type;
            List<Hierarchy> hierarchyList = new ArrayList<Hierarchy>();
            for (Type elementType : tupleType.elementTypes) {
                hierarchyList.add(elementType.getHierarchy());
            }
            return hierarchyList;
        } else {
            return Collections.singletonList(type.getHierarchy());
        }
    }

    /**
     * Implementation of {@link mondrian.olap.fun.Resolver.Conversion}.
     */
    private static class ConversionImpl implements Resolver.Conversion {
        final int from;
        final int to;
        /**
         * Which argument. Arguments are 0-based, and in particular the 'this'
         * of a call of member or method call syntax is argument 0. Argument -1
         * is the return.
         */
        final int ordinal;

        /**
         * Score of the conversion. A higher value is more onerous and therefore
         * a call using such a conversion is less likly to be chosen.
         */
        final int cost;

        final RuntimeException e;

        /**
         * Creates a conversion.
         *
         * @param from From type
         * @param to To type
         * @param ordinal Ordinal of argument
         * @param cost Cost of conversion
         * @param e Exception
         */
        public ConversionImpl(
            int from,
            int to,
            int ordinal,
            int cost,
            RuntimeException e)
        {
            this.from = from;
            this.to = to;
            this.ordinal = ordinal;
            this.cost = cost;
            this.e = e;
        }

        public int getCost() {
            return cost;
        }

        public void checkValid() {
            if (e != null) {
                throw e;
            }
        }

        public void apply(Validator validator, List<Exp> args) {
            final Exp arg = args.get(ordinal);
            switch (from) {
            case Category.Member:
            case Category.Tuple:
                switch (to) {
                case Category.Set:
                    final Exp newArg =
                        validator.validate(
                            new UnresolvedFunCall(
                                "{}", Syntax.Braces, new Exp[]{arg}), false);
                    args.set(ordinal, newArg);
                    break;
                default:
                    // do nothing
                }
            default:
                // do nothing
            }
        }

        // for debug
        public String toString() {
            return "Conversion(from=" + Category.instance().getName(from)
                + ", to=" + Category.instance().getName(to)
                + ", ordinal="
                + ordinal + ", cost="
                + cost + ", e=" + e + ")";
        }
    }
}

// End TypeUtil.java
