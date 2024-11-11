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

import mondrian.olap.Member;

import java.util.List;

/**
 * Compares lists of {@link Member}s so as to convert them into hierarchical order. Applies lexicographic order to the
 * array.
 */
class HierarchizeTupleComparator extends TupleComparator {
  private final boolean post;

  HierarchizeTupleComparator( int arity, boolean post ) {
    super( arity );
    this.post = post;
  }

  public int compare( List<Member> a1, List<Member> a2 ) {
    for ( int i = 0; i < arity; i++ ) {
      Member m1 = a1.get( i );
      Member m2 = a2.get( i );
      int c = Sorter.compareHierarchically( m1, m2, post );
      if ( c != 0 ) {
        return c;
      }
    }
    return 0;
  }
}
