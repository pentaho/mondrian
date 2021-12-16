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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Comparator for members.
 *
 * <p>Could genericize this to <code>class&lt;T&gt; MemorizingComparator
 * implements Comparator&lt;T&gt;</code>, but not if it adds a run time cost, since the comparator is at the heart of
 * the sort algorithms.
 */
abstract class MemberComparator implements Comparator<Member> {
  private static final Logger LOGGER =
    LogManager.getLogger( MemberComparator.class );
  final Evaluator evaluator;
  final Calc exp;

  private final int descMask;
  private final Map<Member, Object> valueMap;

  MemberComparator( Evaluator evaluator, Calc exp, boolean desc ) {
    this.evaluator = evaluator;
    this.exp = exp;
    this.descMask = desc ? -1 : 1;
    this.valueMap = new HashMap<>();
  }

  private int maybeNegate( int c ) {
    return descMask * c;
  }

  // applies the Calc to a member, memorizing results
  protected Object eval( Member m ) {
    Object val = valueMap.get( m );
    if ( val == null ) {
      evaluator.setContext( m );
      val = exp.evaluate( evaluator );
      if ( val == null ) {
        val = Util.nullValue;
      }
      valueMap.put( m, val );
    }
    return val;
  }

  // wraps comparison with tracing
  Comparator<Member> wrap() {
    final MemberComparator comparator = this;
    if ( LOGGER.isDebugEnabled() ) {
      return ( m1, m2 ) -> {
        final int c = comparator.compare( m1, m2 );
        // here guaranteed that eval(m) finds a memorized value
        LOGGER.debug(
          "compare "
            + m1.getUniqueName() + "(" + eval( m1 ) + "), "
            + m2.getUniqueName() + "(" + eval( m2 ) + ")"
            + " yields " + c );
        return c;
      };
    } else {
      return this;
    }
  }

  // Preloads the value map with precomputed members (supplied as a map).
  void preloadValues( Map<Member, Object> map ) {
    valueMap.putAll( map );
  }

  // Preloads the value map by applying the expression to a Collection of
  // members.
  void preloadValues( Collection<Member> members ) {
    for ( Member m : members ) {
      eval( m );
    }
  }

  protected final int compareByValue( Member m1, Member m2 ) {
    final int c = Sorter.compareValues( eval( m1 ), eval( m2 ) );
    return maybeNegate( c );
  }

  protected final int compareHierarchicallyButSiblingsByValue(
    Member m1, Member m2 ) {
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
          // including case where both parents are null
          int c = compareByValue( prev1, prev2 );
          if ( c != 0 ) {
            return c;
          }
          // prev1 and prev2 are siblings.  Order according to
          // hierarchy, if the values do not differ.  Needed to
          // have a consistent sortMembers if members with equal
          // (null!)  values are compared.
          c = Sorter.compareSiblingMembers( prev1, prev2 );

          // Do not negate c, even if we are sorting descending.
          // This comparison is to achieve the 'natural order'.
          return c;
        }
      }
    }
  }

  static class BreakMemberComparator extends MemberComparator {
    BreakMemberComparator( Evaluator evaluator, Calc exp, boolean desc ) {
      super( evaluator, exp, desc );
    }

    public final int compare( Member m1, Member m2 ) {
      return compareByValue( m1, m2 );
    }
  }

  static class HierarchicalMemberComparator
    extends MemberComparator {
    HierarchicalMemberComparator(
      Evaluator evaluator, Calc exp, boolean desc ) {
      super( evaluator, exp, desc );
    }

    public int compare( Member m1, Member m2 ) {
      return compareHierarchicallyButSiblingsByValue( m1, m2 );
    }
  }
}
