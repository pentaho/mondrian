/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2003 Kana Software, Inc. and others.
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
 * @author jhyde
 * @since 26 February, 2002
 * @version $Id$
 **/
class FunDefBase extends FunUtil implements FunDef {
	protected int flags;
    private String name;
    private String description;
	protected int returnType;
	protected int[] parameterTypes;
	boolean isAbstract = false;

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
	FunDefBase(
			String name, String signature, String description, String flags) {
		this(name,
			signature,
			description,
			BuiltinFunTable.decodeSyntacticType(flags),
			BuiltinFunTable.decodeReturnType(flags),
			BuiltinFunTable.decodeParameterTypes(flags));
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

}

// End FunDefBase.java
