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
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;

import junit.framework.TestSuite;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Util;

/**
 * A <code>MultiResolver</code> resolves a function with a finite set of
 * parameter combinations to a single function.
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
class MultiResolver extends FunUtil implements Resolver {
	Funk funk;
	String name;
	String description;
	String[] signatures;
	int syntacticType;
	MultiResolver(
			String name, String signature, String description,
			String[] signatures, Funk funk) {
		this.name = name;
		this.description = description;
		this.funk = funk;
		this.signatures = signatures;
		Util.assertTrue(signatures.length > 0);
		this.syntacticType = BuiltinFunTable.decodeSyntacticType(signatures[0]);
		for (int i = 1; i < signatures.length; i++) {
			Util.assertTrue(BuiltinFunTable.decodeSyntacticType(signatures[i]) ==
						syntacticType);
		}
	}

	public void addTests(TestSuite suite) {
		funk.addTests(suite);
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
			int returnType = BuiltinFunTable.decodeReturnType(signatures[j]);
			return new FunkFunDef(this,funk,syntacticType,returnType,parameterTypes);
		}
		return null;
	}
}

/**
 * A <code>FunkFunDef</code> is an adaptor to make a {@link Funk} look like a
 * {@link FunDef}.
 **/
class FunkFunDef extends FunDefBase {
	Funk funk;
	FunkFunDef(
			Resolver resolver, Funk funk, int syntacticType, int returnType,
			int[] parameterTypes) {
		super(resolver, syntacticType, returnType, parameterTypes);
		this.funk = funk;
	}
	public Object evaluate(Evaluator evaluator, Exp[] args) {
		return funk.evaluate(evaluator, args);
	}
}

// End MultiResolver.java
