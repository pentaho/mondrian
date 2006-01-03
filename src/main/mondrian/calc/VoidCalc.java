/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import mondrian.olap.Evaluator;

/**
 * Expression which has a void result.
 * Since it doesn't return anything, any useful implementation of this
 * class will do its work by causing side-effects.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 29, 2005
 */
public interface VoidCalc extends Calc {
    void evaluateVoid(Evaluator evaluator);
}

// End VoidCalc.java
