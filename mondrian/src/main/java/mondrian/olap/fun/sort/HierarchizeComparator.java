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

import java.util.Comparator;

/**
 * Compares {@link Member}s so as to arrange them in prefix or postfix hierarchical order.
 */
class HierarchizeComparator implements Comparator<Member> {
  private final boolean post;

  HierarchizeComparator( boolean post ) {
    this.post = post;
  }

  public int compare( Member m1, Member m2 ) {
    return Sorter.compareHierarchically( m1, m2, post );
  }
}
