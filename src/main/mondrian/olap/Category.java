/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho
// All Rights Reserved.
*/

package mondrian.olap;

/**
 * <code>Category</code> enumerates the possible expression types.
 *
 * <p>Values of this enumeration are returned by {@link Exp#getCategory()},
 * {@link FunDef#getParameterCategories()}, and
 * {@link FunDef#getReturnCategory()}.
 *
 * <p>For modern code, the more descriptive type system
 * ({@link mondrian.olap.type.Type}) is preferred.
 *
 * @author jhyde
 * @since Feb 21, 2003
 */
public class Category extends EnumeratedValues {
    /**
     * The singleton instance of <code>Category</code>.
     */
    public static final Category instance = new Category();

    private Category() {
        super(
            new String[] {
                "unknown", "array", "dimension", "hierarchy", "level",
                "logical", "member", "numeric", "set",
                "string", "tuple", "symbol", "cube", "value", "integer",
                "null", "empty", "datetime",
            },
            new int[] {
                Unknown, Array, Dimension, Hierarchy, Level,
                Logical, Member, Numeric, Set,
                String, Tuple, Symbol, Cube, Value, Integer,
                Null, Empty, DateTime,
            },
            new String[] {
                "Unknown", "Array", "Dimension", "Hierarchy", "Level",
                "Logical Expression", "Member", "Numeric Expression", "Set",
                "String", "Tuple", "Symbol", "Cube", "Value", "Integer",
                "Null", "Empty", "DateTime",
            }
        );
    }

    /**
     * Returns the singleton instance of <code>Category</code>.
     *
     * @return the singleton instance
     */
    public static Category instance() {
        return instance;
    }

    /**
     * <code>Unknown</code> is an expression whose type is as yet unknown.
     */
    public static final int Unknown   = 0;

    /**
     * <code>Array</code> is an expression of array type.
     */
    public static final int Array     = 1;

    /**
     * <code>Dimension</code> is a dimension expression.
     * @see Dimension
     */
    public static final int Dimension = 2;

    /**
     * <code>Hierarchy</code> is a hierarchy expression.
     * @see Hierarchy
     */
    public static final int Hierarchy = 3;

    /**
     * <code>Level</code> is a level expression.
     * @see Level
     */
    public static final int Level     = 4;

    /**
     * <code>Logical</code> is a boolean expression.
     */
    public static final int Logical   = 5;

    /**
     * <code>Member</code> is a member expression.
     * @see Member
     */
    public static final int Member    = 6;

    /**
     * <code>Numeric</code> is a numeric expression.
     */
    public static final int Numeric   = 7;

    /**
     * <code>Set</code> is a set of members or tuples.
     */
    public static final int Set       = 8;

    /**
     * <code>String</code> is a string expression.
     */
    public static final int String    = 9;

    /**
     * <code>Tuple</code> is a tuple expression.
     */
    public static final int Tuple     = 10;

    /**
     * <code>Symbol</code> is a symbol, for example the <code>BASC</code>
     * keyword to the <code>Order()</code> function.
     */
    public static final int Symbol    = 11;

    /**
     * <code>Cube</code> is a cube expression.
     * @see Cube
     */
    public static final int Cube      = 12;

    /**
     * <code>Value</code> is any expression yielding a string or numeric value.
     */
    public static final int Value     = 13;

    /**
     * <code>Integer</code> is an integer expression. This is a subtype of
     * {@link #Numeric}.
     */
    public static final int Integer   = 15;

    /**
     * Represents a <code>Null</code> value
     */
    public static final int Null      = 16;

    /**
     * Represents an empty expression.
     */
    public static final int Empty     = 17;

    /**
     * Represents a DataTime expression.
     */
    public static final int DateTime  = 18;

    /**
     * <code>Expression</code> is a flag which, when bitwise-OR-ed with a
     * category value, indicates an expression (as opposed to a constant).
     */
    public static final int Expression = 0;
    /** <code>Constant</code> is a flag which, when bitwise-OR-ed with a
     * category value, indicates a constant (as opposed to an expression). */
    public static final int Constant = 64;
    /** <code>Mask</code> is a mask to remove flags. */
    public static final int Mask = 31;

    /**
     * Returns whether a category represents a scalar type.
     *
     * @param category Category
     * @return Whether is scalar
     */
    public static boolean isScalar(int category) {
        switch (category & Mask) {
        case Value:
        case Logical:
        case Numeric:
        case Integer:
        case String:
        case DateTime:
            return true;
        default:
            return false;
        }
    }
}

// End Category.java
