/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.Category;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Syntax;

/**
 * A <code>SimpleResolver</code> resolves a single, non-overloaded function.
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
class SimpleResolver implements Resolver {
	FunDef funDef;
	SimpleResolver(FunDef funDef) {
		this.funDef = funDef;
	}
	public String getName() {
		return funDef.getName();
	}

    public Syntax getSyntax() {
        return funDef.getSyntax();
    }

    public String[] getReservedWords() {
        return FunUtil.emptyStringArray;
    }

	public FunDef resolve(Exp[] args, int[] conversionCount) {
		int[] parameterTypes = funDef.getParameterTypes();
		if (parameterTypes.length != args.length) {
			return null;
		}
		for (int i = 0; i < args.length; i++) {
			if (!BuiltinFunTable.canConvert(args[i], parameterTypes[i], conversionCount)) {
				return null;
			}
		}
		return funDef;
	}

    public boolean requiresExpression(int k) {
        int[] parameterTypes = funDef.getParameterTypes();
        if (k < parameterTypes.length &&
                parameterTypes[k] == Category.Set) {
            return false;
        }
        return true;
    }

//	public void addTests(TestSuite suite, Pattern pattern) {
//		funDef.addTests(suite, pattern);
//	}
}

// End SimpleResolver.java
