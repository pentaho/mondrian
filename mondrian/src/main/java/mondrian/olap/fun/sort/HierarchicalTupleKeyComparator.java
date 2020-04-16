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
import mondrian.olap.Util;

import java.util.List;

class HierarchicalTupleKeyComparator extends TupleExpMemoComparator {

  HierarchicalTupleKeyComparator( Evaluator e, Calc calc, int arity ) {
    super( e, calc, arity );
  }

  @Override protected int nonEqualCompare( List<Member> a1, List<Member> a2 ) {
    OrderKey k1 = (OrderKey) eval( a1 );
    OrderKey k2 = (OrderKey) eval( a2 );
    return compareMemberOrderKeysHierarchically( k1, k2 );
  }

  private int compareMemberOrderKeysHierarchically(
    OrderKey k1, OrderKey k2 ) {
    // null is less than anything else
    if ( k1 == Util.nullValue ) {
      return -1;
    }
    if ( k2 == Util.nullValue ) {
      return 1;
    }
    Member m1 = k1.member;
    Member m2 = k2.member;
    if ( Util.equals( m1, m2 ) ) {
      return 0;
    }
    while ( true ) {
      int depth1 = m1.getDepth();
      int depth2 = m2.getDepth();
      if ( depth1 < depth2 ) {
        m2 = m2.getParentMember();
        if ( Util.equals( m1, m2 ) ) {
          return -1;
        }
      } else if ( depth1 > depth2 ) {
        m1 = m1.getParentMember();
        if ( Util.equals( m1, m2 ) ) {
          return 1;
        }
      } else {
        Member prev1 = m1;
        Member prev2 = m2;
        m1 = m1.getParentMember();
        m2 = m2.getParentMember();
        if ( Util.equals( m1, m2 ) ) {
          OrderKey pk1 = new OrderKey( prev1 );
          OrderKey pk2 = new OrderKey( prev2 );
          return Sorter.compareValues( pk1, pk2 );
        }
      }
    }
  }
}
