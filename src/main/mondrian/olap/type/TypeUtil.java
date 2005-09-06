/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

import mondrian.olap.Hierarchy;
import mondrian.olap.Util;
import mondrian.olap.Level;

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

    public static Level typeToLevel(Type type) {
        if (type instanceof LevelType) {
            return ((LevelType) type).getLevel();
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
     * Converts a type to a member (or tuple) type. If it is a set, strips
     * the set. If it is a dimension, hierarchy or level type, converts it to
     * a member.
     */
    public static Type toMemberType(Type type) {
        type = stripSetType(type);
        if (type instanceof DimensionType) {
            DimensionType dimensionType = (DimensionType) type;
            return new MemberType(dimensionType.getHierarchy(),
                    dimensionType.getLevel(), null);
        } else if (type instanceof HierarchyType) {
            HierarchyType hierarchyType = (HierarchyType) type;
            return new MemberType(hierarchyType.getHierarchy(),
                    hierarchyType.getLevel(), null);
        } else if (type instanceof LevelType) {
            LevelType levelType = (LevelType) type;
            return new MemberType(levelType.getHierarchy(),
                    levelType.getLevel(), null);
        } else {
            return type;
        }
    }

    /**
     * Returns whether this type is union-compatible with another.
     * In general, to be union-compatible, types must have the same
     * dimensionality.
     */
    public static boolean isUnionCompatible(Type type1, Type type2) {
        if (type1 instanceof MemberType) {
            MemberType memberType1 = (MemberType) type1;
            if (type2 instanceof MemberType) {
                MemberType memberType2 = (MemberType) type2;
                final Hierarchy hierarchy1 = memberType1.getHierarchy();
                final Hierarchy hierarchy2 = memberType2.getHierarchy();
                if (hierarchy1 == null ||
                        hierarchy2 == null ||
                        hierarchy2.getUniqueName().equals(
                                hierarchy1.getUniqueName())) {
                    // They are compatible.
                    return true;
                }
            }
            return false;
        } else if (type1 instanceof TupleType) {
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
}

// End TypeUtil.java
