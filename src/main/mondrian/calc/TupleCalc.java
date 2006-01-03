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

import mondrian.olap.Member;
import mondrian.olap.Evaluator;

/**
 * Expression which yields a tuple.
 * The tuple is represented as an array of {@link Member} objects,
 * <code>null</code> to represent the null tuple.<p/>
 *
 * When implementing this interface, it is convenient to extend
 * {@link mondrian.calc.impl.AbstractTupleCalc}, but it is not required.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 27, 2005
 */
public interface TupleCalc extends Calc {
    /**
     * Evaluates this expression to yield a tuple.<p/>
     *
     * A tuple cannot contain any null members. If any of the members is null,
     * this method must return a null.
     *
     * @post result == null || !tupleContainsNullMember(result)
     *
     * @param evaluator Evaluation context
     * @return an array of members, or null to represent the null tuple
     */
    Member[] evaluateTuple(Evaluator evaluator);
}

// End TupleCalc.java
