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
