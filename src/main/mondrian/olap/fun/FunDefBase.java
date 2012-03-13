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
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.olap.type.DimensionType;
import mondrian.olap.type.LevelType;

import java.io.PrintWriter;

/**
 * <code>FunDefBase</code> is the default implementation of {@link FunDef}.
 *
 * <h3>Signatures</h3>
 *
 * <p>A function is defined by the following:</p>
 *
 * <table border="1">
 * <tr><th>Parameter</th><th>Meaning</th><th>Example</th></tr>
 * <tr>
 * <td>name</td><td>Name of the function</td><td>"Members"</td>
 * </tr>
 * <tr>
 * <td>signature</td>
 * <td>Signature of the function</td>
 * <td>"&lt;Dimension&gt;.Members"</td>
 * </tr>
 * <tr>
 * <td>description</td>
 * <td>Description of the function</td>
 * <td>"Returns the set of all members in a dimension."</td>
 * </tr>
 * <tr>
 * <td>flags</td>
 * <td>Encoding of the syntactic type, return type, and parameter
 * types of this operator. The encoding is described below.</td>
 * <td>"pxd"</tr>
 * </table>
 *
 * The <code>flags</code> field is an string which encodes
 * the syntactic type, return type, and parameter types of this operator.
 * <ul>
 * <li>The first character determines the syntactic type, as described by
 * {@link FunUtil#decodeSyntacticType(String)}.
 * <li>The second character determines the return type, as described by
 * {@link FunUtil#decodeReturnCategory(String)}.
 * <li>The third and subsequence characters determine the types of the
 * arguments arguments, as described by
 * {@link FunUtil#decodeParameterCategories(String)}.
 * </ul><p/>
 *
 * For example,  <code>"pxd"</code> means "an operator with
 * {@link Syntax#Property property} syntax (p) which returns a set
 * (x) and takes a dimension (d) as its argument".<p/>
 *
 * The arguments are always read from left to right, regardless of the
 * syntactic type of the operator. For example, the
 * <code>"&lt;Set&gt;.Item(&lt;Index&gt;)"</code> operator
 * (signature <code>"mmxn"</code>) has the
 * syntax of a method-call, and takes two parameters:
 * a set (x) and a numeric (n).<p/>
 *
 * @author jhyde
 * @since 26 February, 2002
 */
public abstract class FunDefBase extends FunUtil implements FunDef {
    protected final int flags;
    private final String name;
    final String signature;
    private final String description;
    protected final int returnCategory;
    protected final int[] parameterCategories;

    /**
     * Creates an operator.
     *
     * @param name           Name of the function, for example "Members".
     * @param signature      Signature of the function, for example
     *                       "&lt;Dimension&gt;.Members".
     * @param description    Description of the function, for example
     *                       "Returns the set of all members in a dimension."
     * @param syntax         Syntactic type of the operator (for
     *                       example, function, method, infix operator)
     * @param returnCategory The {@link Category} of the value returned by this
     *                       operator.
     * @param parameterCategories An array of {@link Category} codes, one for
     *                       each parameter.
     */
    FunDefBase(
        String name,
        String signature,
        String description,
        Syntax syntax,
        int returnCategory,
        int[] parameterCategories)
    {
        assert name != null;
        assert syntax != null;
        this.name = name;
        this.signature = signature;
        this.description = description;
        this.flags = syntax.ordinal();
        this.returnCategory = returnCategory;
        this.parameterCategories = parameterCategories;
    }

    /**
     * Creates an operator.
     *
     * @param name        Name of the function, for example "Members".
     * @param description Description of the function, for example
     *                    "Returns the set of all members in a dimension."
     * @param flags       Encoding of the syntactic type, return type,
     *                    and parameter types of this operator. The
     *                    "Members" operator has a syntactic type
     *                    "pxd" which means "an operator with
     *                    {@link Syntax#Property property} syntax (p) which
     *                    returns a set (x) and takes a dimension (d) as its
     *                    argument".
     *                    See {@link FunUtil#decodeSyntacticType(String)},
     *                    {@link FunUtil#decodeReturnCategory(String)},
     *                    {@link FunUtil#decodeParameterCategories(String)}.
     */
    protected FunDefBase(
        String name,
        String description,
        String flags)
    {
        this(
            name,
            null,
            description,
            decodeSyntacticType(flags),
            decodeReturnCategory(flags),
            decodeParameterCategories(flags));
    }

    /**
     * Creates an operator with an explicit signature.
     *
     * <p>In most cases, the signature can be generated automatically, and
     * you should use the constructor which creates an implicit signature,
     * {@link #FunDefBase(String, String, String, String)}
     * instead.
     *
     * @param name        Name of the function, for example "Members".
     * @param signature   Signature of the function, for example
     *                    "&lt;Dimension&gt;.Members".
     * @param description Description of the function, for example
     *                    "Returns the set of all members in a dimension."
     * @param flags       Encoding of the syntactic type, return type, and
     *                    parameter types of this operator. The "Members"
     *                    operator has a syntactic type "pxd" which means "an
     *                    operator with {@link Syntax#Property property} syntax
     *                    (p) which returns a set (x) and takes a dimension (d)
     *                    as its argument".  See
     *                    {@link FunUtil#decodeSyntacticType(String)},
     *                    {@link FunUtil#decodeReturnCategory(String)},
     *                    {@link FunUtil#decodeParameterCategories(String)}.
     */
    protected FunDefBase(
        String name,
        String signature,
        String description,
        String flags)
    {
        this(
            name,
            signature,
            description,
            decodeSyntacticType(flags),
            decodeReturnCategory(flags),
            decodeParameterCategories(flags));
    }

    /**
     * Convenience constructor when we are created by a {@link Resolver}.
     *
     * @param resolver Resolver
     * @param returnType Return type
     * @param parameterTypes Parameter types
     */
    FunDefBase(Resolver resolver, int returnType, int[] parameterTypes) {
        this(
            resolver.getName(),
            null,
            null,
            resolver.getSyntax(),
            returnType,
            parameterTypes);
    }

    /**
     * Copy constructor.
     *
     * @param funDef Function definition to copy
     */
    FunDefBase(FunDef funDef) {
        this(
            funDef.getName(), funDef.getSignature(),
            funDef.getDescription(), funDef.getSyntax(),
            funDef.getReturnCategory(), funDef.getParameterCategories());
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Syntax getSyntax() {
        return Syntax.class.getEnumConstants()[flags];
    }

    public int getReturnCategory() {
        return returnCategory;
    }

    public int[] getParameterCategories() {
        return parameterCategories;
    }

    public Exp createCall(Validator validator, Exp[] args) {
        int[] categories = getParameterCategories();
        Util.assertTrue(categories.length == args.length);
        for (int i = 0; i < args.length; i++) {
            args[i] = validateArg(validator, args, i, categories[i]);
        }
        final Type type = getResultType(validator, args);
        if (type == null) {
            throw Util.newInternal("could not derive type");
        }
        return new ResolvedFunCall(this, args, type);
    }

    /**
     * Validates an argument to a call to this function.
     *
     * <p>The default implementation of this method adds an implicit
     * conversion to the correct type. Derived classes may override.
     *
     * @param validator Validator
     * @param args Arguments to this function
     * @param i Ordinal of argument
     * @param category Expected {@link Category category} of argument
     * @return Validated argument
     */
    protected Exp validateArg(
        Validator validator,
        Exp[] args,
        int i,
        int category)
    {
        return args[i];
    }

    /**
     * Converts a type to a different category, maintaining as much type
     * information as possible.
     *
     * For example, given <code>LevelType(dimension=Time, hierarchy=unknown,
     * level=unkown)</code> and category=Hierarchy, returns
     * <code>HierarchyType(dimension=Time)</code>.
     *
     * @param type Type
     * @param category Desired category
     * @return Type after conversion to desired category
     */
    static Type castType(Type type, int category) {
        switch (category) {
        case Category.Logical:
            return new BooleanType();
        case Category.Numeric:
            return new NumericType();
        case Category.Numeric | Category.Integer:
            return new DecimalType(Integer.MAX_VALUE, 0);
        case Category.String:
            return new StringType();
        case Category.DateTime:
            return new DateTimeType();
        case Category.Symbol:
            return new SymbolType();
        case Category.Value:
            return new ScalarType();
        case Category.Cube:
            if (type instanceof Cube) {
                return new CubeType((Cube) type);
            }
            return null;
        case Category.Dimension:
            if (type != null) {
                return DimensionType.forType(type);
            }
            return null;
        case Category.Hierarchy:
            if (type != null) {
                return HierarchyType.forType(type);
            }
            return null;
        case Category.Level:
            if (type != null) {
                return LevelType.forType(type);
            }
            return null;
        case Category.Member:
            if (type != null) {
                final MemberType memberType = TypeUtil.toMemberType(type);
                if (memberType != null) {
                    return memberType;
                }
            }
            // Take a wild guess.
            return MemberType.Unknown;
        case Category.Tuple:
            if (type != null) {
                final Type memberType = TypeUtil.toMemberOrTupleType(type);
                if (memberType != null) {
                    return memberType;
                }
            }
            return null;
        case Category.Set:
            if (type != null) {
                final Type memberType = TypeUtil.toMemberOrTupleType(type);
                if (memberType != null) {
                    return new SetType(memberType);
                }
            }
            return null;
        case Category.Empty:
            return new EmptyType();
        default:
            throw Category.instance.badValue(category);
        }
    }

    /**
     * Returns the type of a call to this function with a given set of
     * arguments.<p/>
     *
     * The default implementation makes the coarse assumption that the return
     * type is in some way related to the type of the first argument.
     * Operators whose arguments don't follow the requirements of this
     * implementation should override this method.<p/>
     *
     * If the function definition says it returns a literal type (numeric,
     * string, symbol) then it's a fair guess that the function call
     * returns the same kind of value.<p/>
     *
     * If the function definition says it returns an object type (cube,
     * dimension, hierarchy, level, member) then we check the first
     * argument of the function. Suppose that the function definition says
     * that it returns a hierarchy, and the first argument of the function
     * happens to be a member. Then it's reasonable to assume that this
     * function returns a member.
     *
     * @param validator Validator
     * @param args Arguments to the call to this operator
     * @return result type of a call this function
     */
    public Type getResultType(Validator validator, Exp[] args) {
        Type firstArgType =
            args.length > 0
            ? args[0].getType()
            : null;
        Type type = castType(firstArgType, getReturnCategory());
        if (type != null) {
            return type;
        }
        throw Util.newInternal(
            "Cannot deduce type of call to function '" + this.name + "'");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        throw Util.newInternal(
            "function '" + getSignature()
            + "' has not been implemented");
    }

    public String getSignature() {
        return getSyntax().getSignature(
            getName(),
            getReturnCategory(),
            getParameterCategories());
    }

    public void unparse(Exp[] args, PrintWriter pw) {
        getSyntax().unparse(getName(), args, pw);
    }
}

// End FunDefBase.java
