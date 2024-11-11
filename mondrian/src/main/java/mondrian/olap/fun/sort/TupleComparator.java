/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


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
