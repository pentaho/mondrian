/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 SAS Institute, Inc.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// sasebb, 16 December, 2004
*/
package mondrian.olap.fun;

import java.util.List;

import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;


/**
 * Definition of the <code>NONEMPTYCROSSJOIN</code> MDX function.
 */
public class NonEmptyCrossJoinFunDef extends CrossJoinFunDef {

    public NonEmptyCrossJoinFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        // evaluate the arguments in non empty mode
        evaluator = evaluator.push();
        evaluator.setNonEmpty(true);
        List result = (List)super.evaluate(evaluator, args);
        
        // remove any remaining empty crossings from the result
        result = nonEmptyList(evaluator, result);
        return result;
    }
}
