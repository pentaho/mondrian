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

import mondrian.olap.Exp;
import mondrian.olap.FunDef;

/**
 * <code>ResolverBase</code> provides a skeleton implementation of
 * <code>interface {@link Resolver}</code>
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
abstract class ResolverBase extends FunUtil implements Resolver {
	String name;
	String description;
	int syntacticType;
	ResolverBase(
			String name, String signature, String description,
			int syntacticType) {
		this.name = name;
		this.description = description;
		this.syntacticType = syntacticType;
	}
	public String getName() {
		return name;
	}
	public FunDef resolve(
			int syntacticType, Exp[] args, int[] conversionCount) {
		if (syntacticType == this.syntacticType) {
			return resolve(args, conversionCount);
		} else {
			return null;
		}
	}
	protected abstract FunDef resolve(Exp[] args, int[] conversionCount);
}

// End ResolverBase.java
