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

import java.io.PrintWriter;

/**
 * <code>FunDefBase</code> is the default implementation of {@link FunDef}.
 * <p/>
 * <h3>Signatures</h3>
 * <p/>
 * A function is defined by the following:<ul>
 * <p/>
 * <table border="1">
 * <p/>
 * <tr><th>Parameter</th><th>Meaning</th><th>Example</th></tr>
 * <p/>
 * <tr>
 * <td>name</td><td>Name of the function</td><td>"Members"</td>
 * </tr>
 * <p/>
 * <tr>
 * <td>signature</td>
 * <td>Signature of the function</td>
 * <td>"&lt;Dimension&gt;.Members"</td>
 * </tr>
 * <p/>
 * <tr>
 * <td>description</td>
 * <td>Description of the function</td>
 * <td>"Returns the set of all members in a dimension."</td>
 * </tr>
 * <p/>
 * <tr>
 * <td>flags</td>
 * <td>Encoding of the syntactic type, return type, and parameter
 * types of this operator. The encoding is described below.</td>
 * <td>"pxd"</tr>
 * </table>
 * <p/>
 * <p>The <code>flags</code> field is an string which encodes
 * the syntactic type, return type, and parameter types of this operator.
 * <ul>
 * <li>The first character determines the syntactic type, as described by
 * {@link FunUtil#decodeSyntacticType(String)}.
 * <li>The second character determines the return type, as described by
 * {@link FunUtil#decodeReturnType(String)}.
 * <li>The third and subsequence characters determine the types of the
 * arguments arguments, as described by
 * {@link FunUtil#decodeParameterTypes(String)}.
 * </ul>
 * <p/>
 * <p>For example,  <code>"pxd"</code> means "an operator with
 * {@link Syntax#Property property} syntax (p) which returns a set
 * (x) and takes a dimension (d) as its argument".</p>
 * <p/>
 * <p>The arguments are always read from left to right, regardless of the
 * syntactic type of the operator. For example, the
 * <code>"&lt;Set&gt;.Item(&lt;Index&gt;)"</code> operator
 * (signature <code>"mmxn"</code>) has the
 * syntax of a method-call, and takes two parameters:
 * a set (x) and a numeric (n).</p>
 *
 * @author jhyde
 * @version $Id$
 * @since 26 February, 2002
 */
public class FunDefBase extends FunUtil implements FunDef {
    protected final int flags;
    private final String name;
    private final String description;
    protected final int returnType;
    protected final int[] parameterTypes;

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
     * @param returnType     The {@link Category} of the value returned by this
     *                       operator.
     * @param parameterTypes An array of {@link Category} codes, one for
     *                       each parameter.
     */
    FunDefBase(String name,
            String signature,
            String description,
            Syntax syntax,
            int returnType,
            int[] parameterTypes) {
        this.name = name;
        Util.discard(signature);
        this.description = description;
        this.flags = syntax.ordinal_;
        this.returnType = returnType;
        this.parameterTypes = parameterTypes;
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
                decodeReturnType(flags),
                decodeParameterTypes(flags));
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
                funDef.getReturnCategory(), funDef.getParameterTypes());
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
        return returnType;
    }

    public int[] getParameterTypes() {
        return parameterTypes;
    }

    public Exp validateCall(Validator validator, FunCall call) {
        int[] types = getParameterTypes();
        final Exp[] args = call.getArgs();
        Util.assertTrue(types.length == args.length);
        final FunTable funTable = validator.getFunTable();
        for (int i = 0; i < args.length; i++) {
            Exp arg = args[i];
            args[i] = funTable.convert(arg, types[i], validator);
        }
        final Type type = getResultType(validator, args);
        if (type == null) {
            throw Util.newInternal("could not derive type");
        }
        call.setType(type);
        return call;
    }

    /**
     * Returns a first approximation as to the type of a function call,
     * assuming that the return type is in some way related to the type of
     * the first argument.
     * <p/>
     * So, this function serves as a good default implementation for
     * {@link #getResultType}. Methods whose arguments don't follow the
     * requirements of this implementation should use a different
     * implementation.
     * <p/>
     * If the function definition says it returns a literal type (numeric,
     * string, symbol) then it's a fair guess that the function call
     * returns the same kind of value.
     * <p/>
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
                final Type type = args[0].getTypeX();
                final Hierarchy hierarchy = type.getHierarchy();
                final Dimension dimension = hierarchy == null ? null :
                        hierarchy.getDimension();
                return new mondrian.olap.type.DimensionType(dimension);
            }
            break;
        case Category.Hierarchy:
            if (args.length > 0) {
                final Type type = args[0].getTypeX();
                final Hierarchy hierarchy = type.getHierarchy();
                return new HierarchyType(hierarchy);
            }
            break;
        case Category.Level:
            if (args.length > 0 && args[0] instanceof Level) {
                final Type type = args[0].getTypeX();
                final Level level = TypeUtil.typeToLevel(type);
                return new LevelType(level.getHierarchy(), level);
            }
            break;
        case Category.Member:
        case Category.Tuple:
            if (args.length > 0) {
                final Type type = args[0].getTypeX();
                return TypeUtil.toMemberType(type);
            }
            break;
        case Category.Set:
            if (args.length > 0) {
                final Type type = args[0].getTypeX();
                return new SetType(TypeUtil.toMemberType(type));
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
     **/
    public Type getResultType(Validator validator, Exp[] args) {
        return guessResultType(args, getReturnCategory(), this.name);
    }

    // implement FunDef
    public Object evaluate(Evaluator evaluator, Exp[] args) {
        throw Util.newInternal("function '" + getSignature() + "' has not been implemented");
    }

    public String getSignature() {
        return getSyntax().getSignature(getName(), getReturnCategory(),
                getParameterTypes());
    }

    public void unparse(Exp[] args, PrintWriter pw) {
        getSyntax().unparse(getName(), args, pw);
    }

    /**
     * Default implementation returns true if at least one
     * of the arguments depends on <code>dimension</code>
     */
    public boolean dependsOn(Exp[] args, Dimension dimension) {
        for (int i = 0; i < args.length; i++) {
            Exp exp = args[i];
            if ((exp != null) && args[i].dependsOn(dimension)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Computes the dependsOn() for functions like Tupel and Filter.
     * The result of these functions depend on <code>dimension</dimension>
     * if all of their arguments depend on it.
     *
     * <p>Derived classes may overload dependsOn() and call this method
     * instead of the default implementation.
     */
    protected boolean dependsOnIntersection(Exp[] args, Dimension dimension) {
        for (int i = 0; i < args.length; i++) {
            Exp exp = args[i];
            if ((exp != null) && !exp.dependsOn(dimension)) {
                return false;
            }
        }
        return true;
    }
}

// End FunDefBase.java
