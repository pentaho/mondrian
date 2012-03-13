/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1998-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.ConstantCalc;
import mondrian.mdx.MdxVisitor;
import mondrian.olap.type.*;

import org.olap4j.impl.UnmodifiableArrayMap;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.Map;

/**
 * Represents a constant value, such as a string or number, in a parse tree.
 *
 * <p>Symbols, such as the <code>ASC</code> keyword in
 * <code>Order([Store].Members, [Measures].[Unit Sales], ASC)</code>, are
 * also represented as Literals.
 *
 * @author jhyde, 21 January, 1999
 */
public class Literal extends ExpBase {

    // Data members.

    public final int category;
    private final Object o;


    // Constants for commonly used literals.

    public static final Literal nullValue = new Literal(Category.Null, null);

    public static final Literal emptyString = new Literal(Category.String, "");

    public static final Literal zero =
        new Literal(Category.Numeric, BigDecimal.ZERO);

    public static final Literal one =
        new Literal(Category.Numeric, BigDecimal.ONE);

    public static final Literal negativeOne =
        new Literal(Category.Numeric, BigDecimal.ONE.negate());

    public static final Literal doubleZero = zero;

    public static final Literal doubleOne = one;

    public static final Literal doubleNegativeOne = negativeOne;

    private static final Map<BigDecimal, Literal> MAP =
        UnmodifiableArrayMap.of(
            BigDecimal.ZERO, zero,
            BigDecimal.ONE, one,
            BigDecimal.ONE.negate(), negativeOne);

    /**
     * Private constructor.
     *
     * <p>Use the creation methods {@link #createString(String)} etc.
     */
    private Literal(int type, Object o) {
        this.category = type;
        this.o = o;
    }

    /**
     * Creates a string literal.
     * @see #createSymbol
     */
    public static Literal createString(String s) {
        return (s.equals(""))
            ? emptyString
            : new Literal(Category.String, s);
    }

    /**
     * Creates a symbol.
     *
     * @see #createString
     */
    public static Literal createSymbol(String s) {
        return new Literal(Category.Symbol, s);
    }

    /**
     * Creates a numeric literal.
     *
     * @deprecated Use {@link #create(java.math.BigDecimal)}
     */
    public static Literal create(Double d) {
        return new Literal(Category.Numeric, new BigDecimal(d));
    }

    /**
     * Creates an integer literal.
     *
     * @deprecated Use {@link #create(java.math.BigDecimal)}
     */
    public static Literal create(Integer i) {
        return new Literal(Category.Numeric, new BigDecimal(i));
    }

    /**
     * Creates a numeric literal.
     *
     * <p>Using a {@link BigDecimal} allows us to store the precise value that
     * the user typed. We will have to fit the value into a native
     * {@code double} or {@code int} later on, but parse time is not the time to
     * be throwing away information.
     */
    public static Literal create(BigDecimal d) {
        final Literal literal = MAP.get(d);
        if (literal != null) {
            return literal;
        }
        return new Literal(Category.Numeric, d);
    }

    public Literal clone() {
        return this;
    }

    public void unparse(PrintWriter pw) {
        switch (category) {
        case Category.Symbol:
        case Category.Numeric:
            pw.print(o);
            break;
        case Category.String:
            pw.print(Util.quoteForMdx((String) o));
            break;
        case Category.Null:
            pw.print("NULL");
            break;
        default:
            throw Util.newInternal("bad literal type " + category);
        }
    }

    public int getCategory() {
        return category;
    }

    public Type getType() {
        switch (category) {
        case Category.Symbol:
            return new SymbolType();
        case Category.Numeric:
            return new NumericType();
        case Category.String:
            return new StringType();
        case Category.Null:
            return new NullType();
        default:
            throw Category.instance.badValue(category);
        }
    }

    public Exp accept(Validator validator) {
        return this;
    }

    public Calc accept(ExpCompiler compiler) {
        return new ConstantCalc(getType(), o);
    }

    public Object accept(MdxVisitor visitor) {
        return visitor.visit(this);
    }

    public Object getValue() {
        return o;
    }

    public int getIntValue() {
        if (o instanceof Number) {
            return ((Number) o).intValue();
        } else {
            throw Util.newInternal("cannot convert " + o + " to int");
        }
    }

}

// End Literal.java
