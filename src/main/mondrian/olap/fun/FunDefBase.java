/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 February, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.olap.type.LevelType;
import mondrian.olap.type.DimensionType;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.mdx.ResolvedFunCall;

import java.io.PrintWriter;

/**
 * <code>FunDefBase</code> is the default implementation of {@link FunDef}.<p/>
 *
 * <h3>Signatures</h3>
 *
 * A function is defined by the following:<p/>
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
 * @version $Id$
 * @since 26 February, 2002
 */
public abstract class FunDefBase extends FunUtil implements FunDef {
    protected final int flags;
    private final String name;
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
     * @param syntax         Syntactic type of the operator (for example, function,
     *                       method, infix operator)
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
            int[] parameterCategories) {
        this.name = name;
        Util.discard(signature);
        this.description = description;
        this.flags = syntax.ordinal;
        this.returnCategory = returnCategory;
        this.parameterCategories = parameterCategories;
    }

    /**
     * Creates an operator.
     *
     * @param name        Name of the function, for example "Members".
     * @param signature   Signature of the function, for example
     *                    "&lt;Dimension&gt;.Members".
     * @param description Description of the function, for example
     *                    "Returns the set of all members in a dimension."
     * @param flags       Encoding of the syntactic type, return type, and parameter
     *                    types of this operator. The "Members" operator has a syntactic
     *                    type "pxd" which means "an operator with
     *                    {@link Syntax#Property property} syntax (p) which returns a set
     *                    (x) and takes a dimension (d) as its argument".
     */
    protected FunDefBase(String name,
            String signature,
            String description,
            String flags) {
        this(name,
                signature,
                description,
                decodeSyntacticType(flags),
                decodeReturnCategory(flags),
                decodeParameterCategories(flags));
    }

    /**
     * Convenience constructor when we are created by a {@link Resolver}.
     */
    FunDefBase(Resolver resolver, int returnType, int[] parameterTypes) {
        this(resolver.getName(),
                null,
                null,
                resolver.getSyntax(),
                returnType,
                parameterTypes);
    }

    /**
     * Copy constructor.
     */
    FunDefBase(FunDef funDef) {
        this(funDef.getName(), funDef.getSignature(),
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
        return Syntax.get(flags);
    }

    public int getReturnCategory() {
        return returnCategory;
    }

    public int[] getParameterCategories() {
        return parameterCategories;
    }

    public Exp createCall(Validator validator, Exp[] args) {
        int[] types = getParameterCategories();
        Util.assertTrue(types.length == args.length);
        for (int i = 0; i < args.length; i++) {
            args[i] = validateArg(validator, args, i, types[i]);
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
     * @param type Expected type of argument
     * @return Validated argument
     */
    protected Exp validateArg(
            Validator validator,
            Exp[] args,
            int i,
            int type) {
        return args[i];
    }

    /**
     * Returns a first approximation as to the type of a function call,
     * assuming that the return type is in some way related to the type of
     * the first argument.<p/>
     *
     * So, this function serves as a good default implementation for
     * {@link #getResultType}. Methods whose arguments don't follow the
     * requirements of this implementation should use a different
     * implementation.<p/>
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
     */
    static Type guessResultType(Exp[] args, int category, String name) {
        switch (category) {
        case Category.Logical:
            return new BooleanType();
        case Category.Numeric:
            return new NumericType();
        case Category.Numeric | Category.Integer:
            return new DecimalType(Integer.MAX_VALUE, 0);
        case Category.String:
            return new StringType();
        case Category.Symbol:
            return new SymbolType();
        case Category.Value:
            return new ScalarType();
        case Category.Cube:
            if (args.length > 0 && args[0] instanceof Cube) {
                return new CubeType((Cube) args[0]);
            }
            break;
        case Category.Dimension:
            if (args.length > 0) {
                final Type type = args[0].getType();
                return DimensionType.forType(type);
            }
            break;
        case Category.Hierarchy:
            if (args.length > 0) {
                final Type type = args[0].getType();
                return HierarchyType.forType(type);
            }
            break;
        case Category.Level:
            if (args.length > 0) {
                final Type type = args[0].getType();
                return LevelType.forType(type);
            }
            break;
        case Category.Member:
            if (args.length > 0) {
                final Type type = args[0].getType();
                final MemberType memberType = TypeUtil.toMemberType(type);
                if (memberType != null) {
                    return memberType;
                }
            }
            // Take a wild guess.
            return MemberType.Unknown;
        case Category.Tuple:
            if (args.length > 0) {
                final Type type = args[0].getType();
                final Type memberType = TypeUtil.toMemberOrTupleType(type);
                if (memberType != null) {
                    return memberType;
                }
            }
            break;
        case Category.Set:
            if (args.length > 0) {
                final Type type = args[0].getType();
                final Type memberType = TypeUtil.toMemberOrTupleType(type);
                if (memberType != null) {
                    return new SetType(memberType);
                }
            }
            break;
        default:
            throw Category.instance.badValue(category);
        }
        throw Util.newInternal("Cannot deduce type of call to function '" +
                name + "'");
    }

    /**
     * Returns the type of a call to this function with a given set of
     * arguments.
     */
    public Type getResultType(Validator validator, Exp[] args) {
        return guessResultType(args, getReturnCategory(), this.name);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        throw Util.newInternal("function '" + getSignature() +
                "' has not been implemented");
    }

    public String getSignature() {
        return getSyntax().getSignature(getName(), getReturnCategory(),
                getParameterCategories());
    }

    public void unparse(Exp[] args, PrintWriter pw) {
        getSyntax().unparse(getName(), args, pw);
    }
}

// End FunDefBase.java
