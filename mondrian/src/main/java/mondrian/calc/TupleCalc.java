/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package mondrian.calc;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;

/**
 * Expression which yields a tuple.
 *
 * <p>The tuple is represented as an array of {@link Member} objects,
 * <code>null</code> to represent the null tuple.
 *
 * <p>When implementing this interface, it is convenient to extend
 * {@link mondrian.calc.impl.AbstractTupleCalc}, but it is not required.
 *
 * @author jhyde
 * @since Sep 27, 2005
 */
public interface TupleCalc extends Calc {
    /**
     * Evaluates this expression to yield a tuple.
     *
     * <p>A tuple cannot contain any null members. If any of the members is
     * null, this method must return a null.
     *
     * @post result == null || !tupleContainsNullMember(result)
     *
     * @param evaluator Evaluation context
     * @return an array of members, or null to represent the null tuple
     */
    Member[] evaluateTuple(Evaluator evaluator);
}

// End TupleCalc.java
