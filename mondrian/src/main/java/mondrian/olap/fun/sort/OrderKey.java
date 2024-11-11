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
