/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;

/**
 * A <code>FunkResolver</code> resolves a function with a finite set of
 * parameter combinations to a single function.
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
class FunkResolver extends MultiResolver {
    private final Funk funk;

    FunkResolver(
            String name, String signature, String description,
            String[] signatures, Funk funk) {
        super(name, signature, description, signatures);
        this.funk = funk;
    }

//  public void addTests(TestSuite suite, Pattern pattern) {
//      super.addTests(suite, pattern);
//      funk.addTests(suite, pattern);
//  }

    Funk getFunk() {
        return funk;
    }

    protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
        return new FunDefBase(this, dummyFunDef.getReturnCategory(),
                dummyFunDef.getParameterCategories()) {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                return funk.evaluate(evaluator, args);
            }
        };
    }
}

// End FunkResolver.java
