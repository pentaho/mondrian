/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde
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
        if (type instanceof MemberType) {
            return ((MemberType) type).getHierarchy();
        } else if (type instanceof LevelType) {
            return ((LevelType) type).getHierarchy();
        } else if (type instanceof HierarchyType) {
            return ((HierarchyType) type).getHierarchy();
        } else if (type instanceof DimensionType) {
            return ((DimensionType) type).getHierarchy();
        } else {
            throw Util.newInternal("not an mdx object");
        }
    }

    /**
     * Given a set type, returns the element type. Or its element type, if it
     * is a set type. And so on.
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
     */
    public static Type toMemberOrTupleType(Type type) {
        type = stripSetType(type);
        if (type instanceof TupleType) {
            return (TupleType) type;
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
     */
    public static int typeToCategory(Type type) {
        if (type instanceof NumericType) {
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
}

// End TypeUtil.java
