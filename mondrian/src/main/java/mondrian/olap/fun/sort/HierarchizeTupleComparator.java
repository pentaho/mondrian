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
