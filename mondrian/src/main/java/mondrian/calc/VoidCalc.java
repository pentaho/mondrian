/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package mondrian.calc;

import mondrian.olap.Evaluator;

/**
 * Expression which has a void result.
 *
 * <p>Since it doesn't return anything, any useful implementation of this
 * class will do its work by causing side-effects.
 *
 * @author jhyde
 * @since Sep 29, 2005
 */
public interface VoidCalc extends Calc {
    void evaluateVoid(Evaluator evaluator);
}

// End VoidCalc.java
