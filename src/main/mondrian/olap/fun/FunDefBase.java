/*
// $Id$
// (C) Copyright 2002 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
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
			int syntacticType, int returnType, int[] parameterTypes) {
		this.name = name;
		Util.discard(signature);
		this.description = description;
		this.flags = syntacticType;
		this.returnType = returnType;
		this.parameterTypes = parameterTypes;
	}
	FunDefBase(
			String name, String signature, String description, String flags) {
		this(
			name,
			signature,
			description,
			BuiltinFunTable.decodeSyntacticType(flags),
			BuiltinFunTable.decodeReturnType(flags),
			BuiltinFunTable.decodeParameterTypes(flags));
	}
	/**
	 * Convenience constructor when we are created by a {@link Resolver}.
	 **/
	FunDefBase(
			Resolver resolver, int syntacticType,
			int returnType, int[] parameterTypes) {
		this(
			resolver.getName(), null, null, syntacticType, returnType,
			parameterTypes);
	}

	/**
	 * Copy constructor.
	 */ 
	FunDefBase(FunDef funDef) {
		this(
				funDef.getName(), funDef.getSignature(),
				funDef.getDescription(), funDef.getSyntacticType(),
				funDef.getReturnType(), funDef.getParameterTypes());
	}

    public String getName() {
        return name;
    }
    public String getDescription() {
        return description;
    }
	public int getSyntacticType() {
		return flags & TypeMask;
	}
	public int getReturnType() {
		return returnType;
	}
	public int[] getParameterTypes() {
		return parameterTypes;
	}
	// implement FunDef
	public boolean isFunction() {
		return getSyntacticType() == TypeFunction;
	}
	// implement FunDef
	public boolean isMethod() {
		return getSyntacticType() == TypeMethod;
	}
	// implement FunDef
	public boolean isProperty() {
		return getSyntacticType() == TypeProperty;
	}
	// implement FunDef
	public boolean isInfix() {
		return getSyntacticType() == TypeInfix;
	}
	// implement FunDef
	public boolean isPrefix() {
		return getSyntacticType() == TypePrefix;
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

	// implement FunDef
	public String getSignature() {
		return ExpBase.getSignature(
			getName(), getSyntacticType(), getReturnType(), getParameterTypes());
	}

	// implement FunDef
	public void unparse(Exp[] args, PrintWriter pw, ElementCallback callback) {
		String fun = getName();
		switch (getSyntacticType()) {
		case TypeInfix:
			ExpBase.unparseList(
				pw, args, "(", " " + fun + " ", ")", callback);
			return;

		case TypePrefix:
			ExpBase.unparseList(
				pw, args, "(" + fun + " ", null, ")", callback);
			return;

		case TypeFunction:
			ExpBase.unparseList(pw, args, fun + "(", ", ", ")", callback);
			return;

		case TypeMethod:
			Util.assertTrue(args.length >= 1);
			args[0].unparse(pw, callback); // 'this'
			pw.print(".");
			pw.print(fun);
			pw.print("(");
			for (int i = 1; i < args.length; i++) {
				if (i > 1)
					pw.print(", ");
				args[i].unparse(pw, callback);
			}
			pw.print(")");
			return;

		case TypeProperty:
			Util.assertTrue(args.length >= 1);
			args[0].unparse(pw, callback); // 'this'
			pw.print(".");
			pw.print(fun);
			return;

		case TypeCase:
			pw.print("CASE");
			int j = 0;
			if (fun.equals("CaseTest")) {
				pw.print(" ");
				args[j++].unparse(pw,callback);
			} else {
				Util.assertTrue(fun.equals("CaseMatch"));
			}
			int clauseCount = (args.length - j) / 2;
			for (int i = 0; i < clauseCount; i++) {
				pw.print(" WHEN ");
				args[j++].unparse(pw, callback);
				pw.print(" THEN ");
				args[j++].unparse(pw, callback);
			}
			if (j < args.length) {
				pw.print(" ELSE ");
				args[j++].unparse(pw, callback);
			}
			Util.assertTrue(j == args.length);
			pw.print(" END");
			return;
		default:
			throw Util.newInternal(
				"unknown syntactic type " + getSyntacticType());
		}
	}
}

// End FunDefBase.java
