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

import java.io.PrintWriter;

/**
 * <code>FunDefBase</code> is the default implementation of {@link FunDef}.
 *
 * <h3>Signatures</h3>
 *
 * A function is defined by the following:<ul>
 *
 * <table border="1">
 *
 * <tr><th>Parameter</th><th>Meaning</th><th>Example</th></tr>
 *
 * <tr>
 * <td>name</td><td>Name of the function</td><td>"Members"</td>
 * </tr>
 *
 * <tr>
 * <td>signature</td>
 * <td>Signature of the function</td>
 * <td>"&lt;Dimension&gt;.Members"</td>
 * </tr>
 *
 * <tr>
 * <td>description</td>
 * <td>Description of the function</td>
 * <td>"Returns the set of all members in a dimension."</td>
 * </tr>
 *
 * <tr>
 * <td>flags</td>
 * <td>Encoding of the syntactic type, return type, and parameter
 *    types of this operator. The encoding is described below.</td>
 * <td>"pxd"</tr>
 * </table>
 *
 * <p>The <code>flags</code> field is an string which encodes
 * the syntactic type, return type, and parameter types of this operator.
 * <ul>
 * <li>The first character determines the syntactic type, as described by
 *     {@link FunUtil#decodeSyntacticType(String)}.
 * <li>The second character determines the return type, as described by
 *     {@link FunUtil#decodeReturnType(String)}.
 * <li>The third and subsequence characters determine the types of the
 *     arguments arguments, as described by
 *     {@link FunUtil#decodeParameterTypes(String)}.
 * </ul>
 *
 * <p>For example,  <code>"pxd"</code> means "an operator with
 * {@link Syntax#Property property} syntax (p) which returns a set
 * (x) and takes a dimension (d) as its argument".</p>
 *
 * <p>The arguments are always read from left to right, regardless of the
 * syntactic type of the operator. For example, the
 * <code>"&lt;Set&gt;.Item(&lt;Index&gt;)"</code> operator
 * (signature <code>"mmxn"</code>) has the
 * syntax of a method-call, and takes two parameters:
 * a set (x) and a numeric (n).</p>
 *
 * @author jhyde
 * @since 26 February, 2002
 * @version $Id$
 **/
public class FunDefBase extends FunUtil implements FunDef {
    protected int flags;
    private String name;
    private String description;
    protected int returnType;
    protected int[] parameterTypes;
    boolean isAbstract = false;

    /**
     * Creates an operator.
     *
     * @param name Name of the function, for example "Members".
     *
     * @param signature Signature of the function, for example
     *     "&lt;Dimension&gt;.Members".
     *
     * @param description Description of the function, for example
     *    "Returns the set of all members in a dimension."
     *
     * @param syntax Syntactic type of the operator (for example, function,
     *   method, infix operator)
     *
     * @param returnType The {@link Category} of the value returned by this
     *   operator.
     *
     * @param parameterTypes An array of {@link Category} codes, one for
     *   each parameter.
     */
    FunDefBase(
            String name, String signature, String description,
            Syntax syntax, int returnType, int[] parameterTypes) {
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
     * @param name Name of the function, for example "Members".
     *
     * @param signature Signature of the function, for example
     *     "&lt;Dimension&gt;.Members".
     *
     * @param description Description of the function, for example
     *    "Returns the set of all members in a dimension."
     *
     * @param flags Encoding of the syntactic type, return type, and parameter
     *    types of this operator. The "Members" operator has a syntactic
     *    type "pxd" which means "an operator with
     *    {@link Syntax#Property property} syntax (p) which returns a set
     *    (x) and takes a dimension (d) as its argument".
     */
    protected FunDefBase(
            String name, String signature, String description, String flags) {
        this(name,
            signature,
            description,
            decodeSyntacticType(flags),
            decodeReturnType(flags),
            decodeParameterTypes(flags));
    }
    /**
     * Convenience constructor when we are created by a {@link Resolver}.
     **/
    FunDefBase(Resolver resolver, int returnType, int[] parameterTypes) {
        this(resolver.getName(), null, null, resolver.getSyntax(), returnType,
                parameterTypes);
    }

    /**
     * Copy constructor.
     */
    FunDefBase(FunDef funDef) {
        this(funDef.getName(), funDef.getSignature(),
                funDef.getDescription(), funDef.getSyntax(),
                funDef.getReturnType(), funDef.getParameterTypes());
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
    public int getReturnType() {
        return returnType;
    }
    public int[] getParameterTypes() {
        return parameterTypes;
    }

    // implement FunDef
    public Hierarchy getHierarchy(Exp[] args)
    {
        switch (getReturnType()) {
        case Category.Set:
        case Category.Tuple:
        case Category.Member:
        case Category.Hierarchy:
            // In most cases, the hierarchy of the result is the same as that
            // of the 0th arg.
            int iArg = 0;
            Exp arg = args[iArg];
            return arg.getHierarchy();
        default:
            // Other types of expression don't have a hierarchy.
            return null;
        }
    }

    // implement FunDef
    public Object evaluate(Evaluator evaluator, Exp[] args) {
        throw Util.newInternal(
            "function '" + getSignature() + "' has not been implemented");
    }

    public String getSignature() {
        return getSyntax().getSignature(getName(), getReturnType(),
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
        for (int i = 0; i < args.length; i++)
            if (args[i] != null && args[i].dependsOn(dimension))
                return true;
        return false;
    }

    /**
     * computes the dependsOn() for functions like Tupel and Filter.
     * The result of these functions depend on <code>dimension</dimension>
     * if all of their arguments depend on it.
     * <p>
     * Derived classes may overload dependsOn() and call this method
     * instead of the default implementation.
     */
    protected boolean dependsOnIntersection(Exp[] args, Dimension dimension) {
        for (int i = 0; i < args.length; i++)
            if (args[i] != null && !args[i].dependsOn(dimension))
                return false;
        return true;
    }
}

// End FunDefBase.java
