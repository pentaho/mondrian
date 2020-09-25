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

public class OrderKey implements Comparable {
  final Member member;

  public OrderKey( Member member ) {
    super();
    this.member = member;
  }

  public int compareTo( Object o ) {
    assert o instanceof OrderKey;
    Member otherMember = ( (OrderKey) o ).member;
    final boolean thisCalculated = this.member.isCalculatedInQuery();
    final boolean otherCalculated = otherMember.isCalculatedInQuery();
    if ( thisCalculated ) {
      if ( !otherCalculated ) {
        return 1;
      }
    } else {
      if ( otherCalculated ) {
        return -1;
      }
    }
    final Comparable thisKey = this.member.getOrderKey();
    final Comparable otherKey = otherMember.getOrderKey();
    if ( ( thisKey != null ) && ( otherKey != null ) ) {
      return thisKey.compareTo( otherKey );
    } else {
      return this.member.compareTo( otherMember );
    }
  }
}
