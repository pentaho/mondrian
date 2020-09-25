/*
 *
 * // This software is subject to the terms of the Eclipse Public License v1.0
 * // Agreement, available at the following URL:
 * // http://www.eclipse.org/legal/epl-v10.html.
 * // You must accept the terms of that agreement to use this software.
 * //
 * // Copyright (C) 2001-2005 Julian Hyde
 * // Copyright (C) 2005-2020 Hitachi Vantara and others
 * // All Rights Reserved.
 * /
 *
 */

package mondrian.olap.fun.sort;

import mondrian.calc.Calc;
import mondrian.olap.Evaluator;
import mondrian.olap.Member;

import java.util.Comparator;
import java.util.List;

/**
 * Compares tuples, which are represented as lists of {@link Member}s.
 */
abstract class TupleComparator
  implements Comparator<List<Member>> {
  final int arity;

  TupleComparator( int arity ) {
    this.arity = arity;
  }

  /**
   * Extension to {@link TupleComparator} which compares tuples by evaluating an expression.
   */
  abstract static class TupleExpComparator extends TupleComparator {
    Evaluator evaluator;
    final Calc calc;

    TupleExpComparator( Evaluator evaluator, Calc calc, int arity ) {
      super( arity );
      this.evaluator = evaluator;
      this.calc = calc;
    }
  }
}
