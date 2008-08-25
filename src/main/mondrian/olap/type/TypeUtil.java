/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

import mondrian.olap.Hierarchy;
import mondrian.olap.Util;
import mondrian.olap.Category;

/**
 * Utility methods relating to types.
 *
 * @author jhyde
 * @since Feb 17, 2005
 * @version $Id$
 */
public class TypeUtil {
    public static Hierarchy typeToHierarchy(Type type) {
        if (type instanceof MemberType
            || type instanceof LevelType
            || type instanceof HierarchyType
            || type instanceof DimensionType) {
            return type.getHierarchy();
        } else {
            throw Util.newInternal("not an mdx object");
        }
    }

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
        } else if (type instanceof DimensionType ||
                type instanceof HierarchyType ||
                type instanceof LevelType) {
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
                if (tupleType1.elementTypes.length ==
                        tupleType2.elementTypes.length) {
                    for (int i = 0; i < tupleType1.elementTypes.length; i++) {
                        if (!isUnionCompatible(
                                tupleType1.elementTypes[i],
                                tupleType2.elementTypes[i])) {
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

    private static boolean equal(
            final Hierarchy hierarchy1, final Hierarchy hierarchy2) {
        //noinspection RedundantIfStatement
        if (hierarchy1 == null ||
                hierarchy2 == null ||
                hierarchy2.getUniqueName().equals(
                        hierarchy1.getUniqueName())) {
            // They are compatible.
            return true;
        } else {
            return false;
        }
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
        return ! (type instanceof SetType ||
                type instanceof CubeType ||
                type instanceof LevelType);
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
        return type instanceof MemberType ||
                type instanceof HierarchyType ||
                type instanceof DimensionType;
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
        } else if (type instanceof DateTimeType) {
            return Category.DateTime;
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
     * @param from actual argument category
     * @param to   formal parameter category
     * @param conversionCount in/out count of number of conversions performed;
     *             is incremented if the conversion is non-trivial (for
     *             example, converting a member to a level).
     * @return whether can convert from 'from' to 'to'
     */
    public static boolean canConvert(
        int from,
        int to,
        int[] conversionCount)
    {
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
                conversionCount[0]++;
                return true;
            case Category.Numeric:
                // We assume that members are numeric, so a cast to a numeric
                // expression is less expensive than a conversion to a string
                // expression.
                conversionCount[0]++;
                return true;
            case Category.Value:
            case Category.String:
                conversionCount[0] += 2;
                return true;
            default:
                return false;
            }
        case Category.Numeric | Category.Constant:
            return to == Category.Value ||
                to == Category.Numeric;
        case Category.Numeric:
            switch (to) {
            case Category.Logical:
                conversionCount[0]++;
                return true;
            default:
                return to == Category.Value ||
                    to == Category.Integer ||
                    to == (Category.Integer | Category.Constant) ||
                    to == (Category.Numeric | Category.Constant);
            }
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
        case Category.DateTime | Category.Constant:
            return to == Category.Value ||
                to == Category.DateTime;
        case Category.DateTime:
            return to == Category.Value ||
                to == (Category.DateTime | Category.Constant);
        case Category.Tuple:
            switch (to) {
            case Category.Value:
            case Category.Numeric:
                conversionCount[0]++;
                return true;
            default:
                return false;
            }
        case Category.Value:
            return false;
        case Category.Symbol:
            return false;
        case Category.Null:
            // now null supports members as well as scalars; but scalar is
            // preferred
            if (Category.isScalar(to)) {
                return true;
            } else if (to == Category.Member) {
                conversionCount[0] += 2;
                return true;
            } else {
                return false;
            }
        case Category.Empty:
            return false;
        default:
            throw Util.newInternal("unknown category " + from);
        }
    }

    static <T> T neq(T t1, T t2) {
        return t1 == null  ? t2
            : t2 == null ? t1
                : t1.equals(t2) ? t1
                    : null;
    }
}

// End TypeUtil.java
