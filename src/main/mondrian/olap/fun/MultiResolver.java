/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2003 Kana Software, Inc. and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 12, 2003
*/
package mondrian.olap.fun;

import mondrian.olap.FunDef;
import mondrian.olap.Exp;
import mondrian.olap.Util;

/**
 * A <code>MultiResolver</code> considers several overloadings of the same
 * function. If one of these overloadings matches the actual arguments, it
 * calls the factory method {@link #createFunDef}.
 *
 * @author jhyde
 * @since Feb 12, 2003
 * @version $Id$
 **/
abstract class MultiResolver extends FunUtil implements Resolver {
	String name;
	String description;
	String[] signatures;
	int syntacticType;

	MultiResolver(
			String name, String signature, String description,
			String[] signatures) {
		this.name = name;
		this.description = description;
		this.signatures = signatures;
		Util.assertTrue(signatures.length > 0);
		this.syntacticType = BuiltinFunTable.decodeSyntacticType(signatures[0]);
		for (int i = 1; i < signatures.length; i++) {
			Util.assertTrue(BuiltinFunTable.decodeSyntacticType(signatures[i]) ==
						syntacticType);
		}
	}

	public String getName() {
		return name;
	}

	public FunDef resolve(int syntacticType, Exp[] args, int[] conversionCount) {
		if (syntacticType != this.syntacticType) {
			return null;
		}
outer:
		for (int j = 0; j < signatures.length; j++) {
			int[] parameterTypes =
				BuiltinFunTable.decodeParameterTypes(signatures[j]);
			if (parameterTypes.length != args.length) {
				continue;
			}
			for (int i = 0; i < args.length; i++) {
				if (!BuiltinFunTable.canConvert(
						args[i], parameterTypes[i], conversionCount)) {
					continue outer;
				}
			}
			final String signature = signatures[j];
			int returnType = BuiltinFunTable.decodeReturnType(signature);
			FunDef dummy = new FunDefBase(this, syntacticType, returnType, parameterTypes);
			return createFunDef(args, dummy);
		}
		return null;
	}

	protected abstract FunDef createFunDef(Exp[] args, FunDef dummyFunDef);
}

// End MultiResolver.java